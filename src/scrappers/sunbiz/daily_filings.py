"""P3.2: Florida's daily corporate filings file, parsed and filtered for newly formed real estate LLCs.

The state's Division of Corporations publishes the filings entered each day as a fixed-width file on its public SFTP
server (``config/sunbiz_re_llc.py``). This module parses it (pure, no I/O) and picks the active Florida LLCs whose name
holds one of the brief's keywords. ``SunbizSftp`` is the thin adapter that downloads one day's file.

Unlike ``sunbiz_engine.py`` (which looks up the LLCs that already own a property, one name at a time on
search.sunbiz.org), this reads every new filing statewide in one download.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Iterable, Iterator, Optional, Protocol

from config.sunbiz_re_llc import (
    ACTIVE_STATUS,
    FILE_ENCODING,
    FILING_TYPES,
    KEYWORDS,
    OFFICER_SLOTS,
    RECORD_LENGTH,
    SFTP_DIR,
    SFTP_FILE,
    SFTP_PORT,
    SFTP_TIMEOUT_SECONDS,
)

logger = logging.getLogger(__name__)

# (start position, length) from https://dos.sunbiz.org/data-definitions/cor.html (1-based starts).
_FIELDS = {
    "document_number": (1, 12), "name": (13, 192), "status": (205, 1), "filing_type": (206, 15),
    "address_1": (221, 42), "address_2": (263, 42), "city": (305, 28), "state": (333, 2), "zip": (335, 10),
    "country": (345, 2), "mail_address_1": (347, 42), "mail_address_2": (389, 42), "mail_city": (431, 28),
    "mail_state": (459, 2), "mail_zip": (461, 10), "mail_country": (471, 2), "file_date": (473, 8),
    "agent_name": (545, 42), "agent_type": (587, 1), "agent_address": (588, 42), "agent_city": (630, 28),
    "agent_state": (658, 2), "agent_zip": (660, 9),
}
_OFFICER_START, _OFFICER_WIDTH = 669, 128
_OFFICER_FIELDS = {"title": (0, 4), "type": (4, 1), "name": (5, 42), "address": (47, 42), "city": (89, 28),
                   "state": (117, 2), "zip": (119, 9)}
_SPACES = re.compile(r"\s{2,}")
_KEYWORD = re.compile(r"\b(" + "|".join(re.escape(k) for k in KEYWORDS) + r")\b", re.IGNORECASE)


@dataclass(frozen=True)
class Officer:
    title: str
    type: str
    name: str
    address: str
    city: str
    state: str
    zip: str


@dataclass(frozen=True)
class Filing:
    document_number: str
    name: str
    status: str
    filing_type: str
    file_date: Optional[date]
    address_1: str
    address_2: str
    city: str
    state: str
    zip: str
    country: str
    mail_address_1: str
    mail_address_2: str
    mail_city: str
    mail_state: str
    mail_zip: str
    mail_country: str
    agent_name: str
    agent_type: str
    agent_address: str
    agent_city: str
    agent_state: str
    agent_zip: str
    officers: tuple[Officer, ...]


def _cut(line: str, start: int, length: int) -> str:
    """One field, its padding removed and inner runs of spaces collapsed ("SMITH      JOHN" -> "SMITH JOHN")."""
    return _SPACES.sub(" ", line[start - 1:start - 1 + length].strip())


def _date(raw: str) -> Optional[date]:
    """MMDDYYYY (the real files use it, though the definitions page does not say so); None when blank or invalid."""
    try:
        return datetime.strptime(raw, "%m%d%Y").date() if raw else None
    except ValueError:
        return None


def _officers(line: str) -> tuple[Officer, ...]:
    officers = []
    for slot in range(OFFICER_SLOTS):
        base = _OFFICER_START + slot * _OFFICER_WIDTH
        values = {k: _cut(line, base + offset, length) for k, (offset, length) in _OFFICER_FIELDS.items()}
        if values["name"]:
            officers.append(Officer(**values))
    return tuple(officers)


def parse_record(line: str) -> Optional[Filing]:
    """One fixed-width record; None for a line of the wrong length (logged by the caller's count)."""
    line = line.rstrip("\r\n")
    if len(line) != RECORD_LENGTH:
        return None
    values = {k: _cut(line, start, length) for k, (start, length) in _FIELDS.items()}
    file_date = _date(values.pop("file_date"))
    return Filing(file_date=file_date, officers=_officers(line), **values)


def parse_file(data: bytes) -> tuple[list[Filing], int]:
    """(filings, rejected lines) for one daily file."""
    filings, rejected = [], 0
    # split("\n"), not splitlines(): splitlines() also breaks on control characters (\x85, \x0c, \x1c-\x1e) that
    # a stray byte inside a record decodes to, which would cut that record in two. parse_record strips the "\r".
    for line in data.decode(FILE_ENCODING).split("\n"):
        if not line.strip():
            continue
        filing = parse_record(line)
        if filing is None:
            rejected += 1
        else:
            filings.append(filing)
    return filings, rejected


def matched_keyword(name: str) -> Optional[str]:
    match = _KEYWORD.search(name)
    return match.group(1).upper() if match else None


def real_estate_llcs(filings: Iterable[Filing]) -> Iterator[tuple[Filing, str]]:
    """Active Florida LLCs whose name holds a keyword, with the keyword matched."""
    for filing in filings:
        if filing.filing_type not in FILING_TYPES or filing.status != ACTIVE_STATUS:
            continue
        keyword = matched_keyword(filing.name)
        if keyword:
            yield filing, keyword


# ── download ──────────────────────────────────────────────────────────────────

class DailyFileSource(Protocol):
    def fetch(self, day: date) -> Optional[bytes]: ...


class SunbizSftp:
    """The state's public SFTP server. ``fetch`` returns the day's file, or None when the state produced none (no
    filings that day). Connection and read errors raise; the caller logs and alerts."""

    def __init__(self, host: str, username: str, password: str, host_key: str):
        """``host_key``: the server's public key as ``"<type> <base64>"`` (``config/sunbiz_re_llc.SFTP_HOST_KEY``)."""
        self._host, self._username, self._password = host, username, password
        self._host_key = host_key

    def fetch(self, day: date) -> Optional[bytes]:
        import base64
        import io
        import socket

        import paramiko

        path = f"{SFTP_DIR}/{SFTP_FILE.format(day=day)}"
        key_type, _, key_data = self._host_key.strip().partition(" ")
        host_key = paramiko.PKey.from_type_string(key_type, base64.b64decode(key_data.split(" ")[0]))
        # Our own socket: paramiko's Transport((host, port)) connects with no timeout, so an unreachable server would
        # hang the job.
        sock = socket.create_connection((self._host, SFTP_PORT), timeout=SFTP_TIMEOUT_SECONDS)
        try:
            transport = paramiko.Transport(sock)
        except Exception:
            sock.close()
            raise
        try:
            transport.banner_timeout = SFTP_TIMEOUT_SECONDS
            transport.auth_timeout = SFTP_TIMEOUT_SECONDS
            # hostkey: paramiko refuses a server whose key differs (SSHException), before sending the login
            transport.connect(hostkey=host_key, username=self._username, password=self._password)
            sftp = paramiko.SFTPClient.from_transport(transport)
            if sftp is None:
                raise ConnectionError("SFTP session not opened")
            sftp.get_channel().settimeout(SFTP_TIMEOUT_SECONDS)
            buffer = io.BytesIO()
            try:
                sftp.getfo(path, buffer)
            except FileNotFoundError:
                return None
            return buffer.getvalue()
        finally:
            transport.close()
