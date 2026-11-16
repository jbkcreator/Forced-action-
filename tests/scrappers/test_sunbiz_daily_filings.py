"""P3.2: parsing and filtering the state's daily corporate filings file. Made-up records only; no network."""
from __future__ import annotations

from datetime import date

import paramiko
import pytest

from config.sunbiz_re_llc import RECORD_LENGTH, SFTP_HOST_KEY
from src.scrappers.sunbiz import daily_filings as mod


def _record(officers: tuple[dict, ...] = (), **fields: str) -> str:
    """One fixed-width line with ``fields`` placed at the state's positions (made-up values)."""
    line = [" "] * RECORD_LENGTH
    defaults = {"document_number": "L26000000001", "name": "SAMPLE HOMES LLC", "status": "A", "filing_type": "FLAL",
                "file_date": "11142026", "address_1": "100 TEST ST", "city": "TESTVILLE", "zip": "33000",
                "mail_state": "FL", "agent_name": "AGENT   TEST", "agent_state": "FL"}
    for key, value in {**defaults, **fields}.items():
        start, length = mod._FIELDS[key]
        assert len(value) <= length, key
        line[start - 1:start - 1 + len(value)] = value
    for slot, officer in enumerate(officers):
        base = mod._OFFICER_START + slot * mod._OFFICER_WIDTH
        for key, value in officer.items():
            offset, length = mod._OFFICER_FIELDS[key]
            line[base + offset - 1:base + offset - 1 + len(value)] = value
    return "".join(line)


def _at(line: list[str], start: int, value: str) -> None:
    """Place ``value`` at a 1-based position, as the state's layout page numbers them."""
    line[start - 1:start - 1 + len(value)] = value


def test_positions_match_the_published_layout_independently_of_the_parser_tables():
    """Hard-coded from https://dos.sunbiz.org/data-definitions/cor.html, not from the module's own tables, so a wrong
    edit to the parser's positions fails here."""
    line = [" "] * 1440
    for start, value in [(1, "L26000000009"), (13, "LAYOUT HOMES LLC"), (205, "A"), (206, "FLAL"),
                         (221, "1 PRINCIPAL ST"), (263, "STE 2"), (305, "PCITY"), (335, "33100"), (345, "US"),
                         (347, "3 MAIL ST"), (431, "MCITY"), (459, "FL"), (461, "33200"), (473, "11132026"),
                         (481, "123456789"),  # FEI: present in the file, never read
                         (545, "AGENT NAME"), (587, "C"), (588, "4 AGENT ST"), (630, "ACITY"), (658, "FL"),
                         (660, "33300"),
                         # officer 1 at 669, officer 6 at 669 + 5 * 128 = 1309
                         (669, "MGR"), (673, "P"), (674, "FIRST OFFICER"), (716, "5 OFC ST"), (758, "OCITY"),
                         (786, "FL"), (788, "33400"),
                         (1309, "AMBR"), (1314, "SIXTH OFFICER"), (1426, "GA"), (1428, "30300")]:
        _at(line, start, value)
    f = mod.parse_record("".join(line))
    assert f is not None
    assert (f.document_number, f.name, f.status, f.filing_type) == ("L26000000009", "LAYOUT HOMES LLC", "A", "FLAL")
    assert (f.address_1, f.address_2, f.city, f.state, f.zip, f.country) == ("1 PRINCIPAL ST", "STE 2", "PCITY", "",
                                                                             "33100", "US")
    assert (f.mail_address_1, f.mail_city, f.mail_state, f.mail_zip) == ("3 MAIL ST", "MCITY", "FL", "33200")
    assert f.file_date == date(2026, 11, 13)
    assert (f.agent_name, f.agent_type, f.agent_address, f.agent_city, f.agent_state, f.agent_zip) == (
        "AGENT NAME", "C", "4 AGENT ST", "ACITY", "FL", "33300")
    first, sixth = f.officers
    assert (first.title, first.type, first.name, first.address, first.city, first.state, first.zip) == (
        "MGR", "P", "FIRST OFFICER", "5 OFC ST", "OCITY", "FL", "33400")
    assert (sixth.title, sixth.name, sixth.state, sixth.zip) == ("AMBR", "SIXTH OFFICER", "GA", "30300")
    assert "123456789" not in repr(f)


def test_a_record_is_read_at_the_published_positions():
    filing = mod.parse_record(_record(
        officers=({"title": "MGR", "type": "P", "name": "DOE      JANE", "address": "1 FAKE AVE", "city": "NOWHERE",
                   "state": "FL", "zip": "33001"},),
        name="SAMPLE   CAPITAL   LLC"))
    assert filing is not None
    assert filing.document_number == "L26000000001"
    assert filing.name == "SAMPLE CAPITAL LLC"           # padding inside the name collapsed
    assert (filing.status, filing.filing_type) == ("A", "FLAL")
    assert filing.file_date == date(2026, 11, 14)          # MMDDYYYY
    assert filing.state == ""                              # blank in the real files; kept blank, not guessed
    assert filing.mail_state == "FL" and filing.agent_name == "AGENT TEST"
    assert len(filing.officers) == 1
    assert filing.officers[0].name == "DOE JANE" and filing.officers[0].zip == "33001"


def test_empty_officer_slots_are_dropped_and_all_six_slots_are_read():
    officers = tuple({"title": "AMBR", "name": f"PERSON {n}"} for n in range(1, 7))
    filing = mod.parse_record(_record(officers=officers))
    assert filing is not None
    assert [o.name for o in filing.officers] == [f"PERSON {n}" for n in range(1, 7)]
    assert mod.parse_record(_record()).officers == ()


@pytest.mark.parametrize("raw", ["", "13012026", "20261114", "ABCDEFGH"])
def test_a_blank_or_invalid_date_is_none_not_an_error(raw):
    assert mod.parse_record(_record(file_date=raw)).file_date is None


@pytest.mark.parametrize("delta", [-1, 1])
def test_a_line_of_the_wrong_length_is_rejected(delta):
    line = _record()
    line = line[:delta] if delta < 0 else line + "X"
    assert len(line) == RECORD_LENGTH + delta
    assert mod.parse_record(line) is None


def test_a_crlf_line_ending_is_not_counted_as_length():
    assert mod.parse_record(_record() + "\r\n") is not None


def test_parse_file_counts_rejected_lines_and_skips_blank_ones():
    data = "\r\n".join([_record(), "", "short line", _record(document_number="L26000000002"), "   "]).encode("latin-1")
    filings, rejected = mod.parse_file(data)
    assert [f.document_number for f in filings] == ["L26000000001", "L26000000002"]
    assert rejected == 1


@pytest.mark.parametrize("byte", ["\x85", "\x0c", "\x1c", "\x0b"])
def test_a_control_byte_inside_a_record_does_not_split_it(byte):
    data = (_record(name=f"ACME{byte} HOMES LLC") + "\r\n" + _record(document_number="L2")).encode("latin-1")
    filings, rejected = mod.parse_file(data)
    assert rejected == 0 and [f.document_number for f in filings] == ["L26000000001", "L2"]


def test_parse_file_never_fails_on_a_non_ascii_byte():
    data = _record(name="CAF\xc9 HOMES LLC").encode("latin-1")
    filings, rejected = mod.parse_file(data)
    assert rejected == 0 and filings[0].name == "CAF\xc9 HOMES LLC"


@pytest.mark.parametrize("name,keyword", [
    ("SUNSHINE REI LLC", "REI"),
    ("sunshine homes llc", "HOMES"),            # case-insensitive
    ("ACME INVESTMENTS, LLC", "INVESTMENTS"),   # punctuation after the word
    ("BAY CAPITAL LLC", "CAPITAL"),
    ("REI-HOLDINGS LLC", "REI"),                # hyphen is a word boundary
    ("REIGN LLC", None),                        # whole word only
    ("HOMESTEAD PROPERTIES LLC", None),
    ("CAPITALIZE LLC", None),
    ("INVESTMENT GROUP LLC", None),             # singular is not in the brief's list
    ("PLAIN LLC", None),
])
def test_keywords_match_as_whole_words(name, keyword):
    assert mod.matched_keyword(name) == keyword


def test_only_active_florida_llcs_with_a_keyword_are_kept():
    lines = [
        _record(document_number="KEEP1", name="ONE HOMES LLC"),
        _record(document_number="FOREIGN", name="TWO HOMES LLC", filing_type="FORL"),
        _record(document_number="CORP", name="THREE HOMES INC", filing_type="DOMP"),
        _record(document_number="INACTIVE", name="FOUR HOMES LLC", status="I"),
        _record(document_number="NOKEY", name="FIVE PROPERTIES LLC"),
        _record(document_number="KEEP2", name="SIX REI CAPITAL LLC"),
    ]
    filings, _ = mod.parse_file("\n".join(lines).encode("latin-1"))
    assert [(f.document_number, k) for f, k in mod.real_estate_llcs(filings)] == [("KEEP1", "HOMES"),
                                                                                    ("KEEP2", "REI")]


# ── SunbizSftp (paramiko faked; no network) ──────────────────────────────────

class _FakeSftp:
    def __init__(self, files: dict[str, bytes]):
        self.files, self.timeout = files, None

    def get_channel(self):
        return self

    def settimeout(self, value):
        self.timeout = value

    def getfo(self, path, buffer):
        if path not in self.files:
            raise FileNotFoundError(path)
        buffer.write(self.files[path])


class _FakeTransport:
    instances: list["_FakeTransport"] = []

    def __init__(self, sock):
        self.sock, self.closed, self.login = sock, False, None
        _FakeTransport.instances.append(self)

    def connect(self, hostkey, username, password):
        self.login, self.hostkey = (username, password), hostkey

    def close(self):
        self.closed = True


@pytest.fixture
def fake_paramiko(monkeypatch):
    import socket

    import paramiko

    connects = []
    files: dict[str, bytes] = {}
    _FakeTransport.instances = []
    monkeypatch.setattr(socket, "create_connection", lambda addr, timeout: connects.append((addr, timeout)) or "sock")
    monkeypatch.setattr(paramiko, "Transport", _FakeTransport)
    monkeypatch.setattr(paramiko.SFTPClient, "from_transport", staticmethod(lambda t: _FakeSftp(files)))
    return connects, files


def _sftp() -> mod.SunbizSftp:
    return mod.SunbizSftp("host.example", "user", "pw", SFTP_HOST_KEY)


def test_sftp_fetch_reads_the_days_file_with_a_timeout_and_closes(fake_paramiko):
    connects, files = fake_paramiko
    files["doc/cor/20261115c.txt"] = b"data"
    assert _sftp().fetch(date(2026, 11, 15)) == b"data"
    assert connects == [(("host.example", 22), 60)]
    assert _FakeTransport.instances[0].login == ("user", "pw")
    assert _FakeTransport.instances[0].closed


def test_sftp_checks_the_pinned_host_key(fake_paramiko):
    fake_paramiko[1]["doc/cor/20261115c.txt"] = b"data"
    _sftp().fetch(date(2026, 11, 15))
    hostkey = _FakeTransport.instances[0].hostkey
    assert f"{hostkey.get_name()} {hostkey.get_base64()}" == SFTP_HOST_KEY


def test_a_malformed_host_key_fails_before_connecting(fake_paramiko):
    with pytest.raises(paramiko.pkey.UnknownKeyType):
        mod.SunbizSftp("host.example", "user", "pw", "not-a-key").fetch(date(2026, 11, 15))
    assert fake_paramiko[0] == [] and _FakeTransport.instances == []


def test_sftp_fetch_returns_none_when_the_state_made_no_file_and_still_closes(fake_paramiko):
    assert _sftp().fetch(date(2026, 11, 16)) is None
    assert _FakeTransport.instances[0].closed


def test_sftp_login_failure_raises_and_closes(fake_paramiko, monkeypatch):
    def refuse(self, hostkey, username, password):
        raise PermissionError("denied")

    monkeypatch.setattr(_FakeTransport, "connect", refuse)
    with pytest.raises(PermissionError):
        _sftp().fetch(date(2026, 11, 15))
    assert _FakeTransport.instances[0].closed
