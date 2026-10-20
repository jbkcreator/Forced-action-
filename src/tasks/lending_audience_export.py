"""Nightly retargeting audience export (Runbook § 4.7, Item 7).

Builds hashed contact files for Google Customer Match and Meta Custom Audiences from the non-suppressed lending leads
(LendingFlow leads and website form submissions) and stages them in LENDING_AUDIENCE_EXPORT_DIR. Nothing is uploaded
to the ad platforms. Past borrowers are not included: no lending table holds them yet.

* Excluded: a lead marked suppressed, and anyone whose phone or email is on lending.suppression_list or belongs to a
  lending.contacts row with do_not_contact. A match on either identifier excludes the whole contact.
* Hashing (SHA-256 hex of the normalised value): email trimmed and lower-cased (Google also drops the periods before
  @gmail.com / @googlemail.com); phone E.164 with "+" for Google, E.164 digits without "+" for Meta.
* Each email and each phone appears once per file; files hold hash values only, never raw contact data, and are
  written owner-only. The last KEEP_NIGHTS files per platform are kept.
* Logs: counts only.

Off until LENDING_AUDIENCE_EXPORT_ENABLED=true and LENDING_AUDIENCE_EXPORT_DIR is set. Cron nightly:

    python -m src.tasks.lending_audience_export [--dry-run]
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import logging
import os
from collections import Counter
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional, Sequence

from sqlalchemy import text

from config.lending_audience_export import (
    GOOGLE_DOTLESS_DOMAINS,
    GOOGLE_FILE,
    GOOGLE_HEADERS,
    KEEP_NIGHTS,
    META_FILE,
    META_HEADERS,
)
from src.services.phone_utils import normalize

logger = logging.getLogger(__name__)

_CONTACTS = text("""
    WITH leads AS (
        SELECT 'lendingflow' AS source, phone, NULLIF(lower(btrim(email)), '') AS email
          FROM lending.lendingflow_leads WHERE NOT suppressed
        UNION ALL
        SELECT 'web', phone, NULLIF(lower(btrim(email)), '')
          FROM lending.web_leads WHERE NOT suppressed
    )
    SELECT l.source, l.phone, l.email
      FROM leads l
     WHERE NOT EXISTS (SELECT 1 FROM lending.suppression_list s
                        WHERE (l.phone IS NOT NULL AND s.phone = l.phone)
                           OR (l.email IS NOT NULL AND lower(s.email) = l.email))
       AND NOT EXISTS (SELECT 1 FROM lending.contacts c
                        WHERE c.do_not_contact
                          AND ((l.phone IS NOT NULL AND c.phone = l.phone)
                               OR (l.email IS NOT NULL AND lower(c.email) = l.email)))
     ORDER BY 1, 2, 3
""")


@dataclass(frozen=True)
class Contact:
    source: str
    phone: Optional[str]  # E.164, "+1XXXXXXXXXX"
    email: Optional[str]  # trimmed, lower-cased


@dataclass
class ExportResult:
    contacts_by_source: Counter = field(default_factory=Counter)
    google_rows: int = 0
    meta_rows: int = 0
    files: list[Path] = field(default_factory=list)


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def google_email(email: str) -> str:
    local, _, domain = email.partition("@")
    return f"{local.replace('.', '')}@{domain}" if domain in GOOGLE_DOTLESS_DOMAINS else email


def hashed_rows(contacts: Iterable[Contact], *, platform: str) -> list[tuple[str, str]]:
    """(email hash, phone hash) per contact, empty string for a missing value. An email or phone already written for
    another contact is left out, and a contact with nothing new is skipped, so each value appears once."""
    seen_emails: set[str] = set()
    seen_phones: set[str] = set()
    rows = []
    for contact in contacts:
        email = contact.email
        if email and platform == "google":
            email = google_email(email)
        phone = contact.phone if platform == "google" else (contact.phone or "").lstrip("+") or None
        email = email if email and email not in seen_emails else None
        phone = phone if phone and phone not in seen_phones else None
        if not (email or phone):
            continue
        if email:
            seen_emails.add(email)
        if phone:
            seen_phones.add(phone)
        rows.append((_sha256(email) if email else "", _sha256(phone) if phone else ""))
    return rows


def eligible_contacts(db) -> Iterator[Contact]:
    """The non-suppressed leads of both sources, with the phone normalised again and an invalid one dropped."""
    for source, raw_phone, email in db.execute(_CONTACTS):
        phone = normalize(raw_phone) if raw_phone else None
        email = email if email and "@" in email else None
        if phone or email:
            yield Contact(source, phone, email)


def _write(path: Path, headers: Sequence[str], rows: list[tuple[str, str]]) -> None:
    """The file is created owner-only before any hash is written (a leftover partial is removed first, since an
    existing file keeps its old mode), and a partial file is never left behind when the write fails."""
    partial = path.with_suffix(".partial")
    partial.unlink(missing_ok=True)
    try:
        with os.fdopen(os.open(partial, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w", newline="",
                       encoding="utf-8") as handle:
            os.chmod(partial, 0o600)  # the umask can only remove bits; this pins the mode whatever it is
            writer = csv.writer(handle)
            writer.writerow(headers)
            writer.writerows(rows)
        os.replace(partial, path)
    except BaseException:
        partial.unlink(missing_ok=True)
        raise


def _prune(directory: Path, pattern: str, keep: int) -> None:
    for old in sorted(directory.glob(pattern.format(day="*")))[:-keep]:
        old.unlink()


def export(db, directory: Optional[Path], *, day: Optional[date] = None, dry_run: bool = False) -> ExportResult:
    """Build both files for ``day`` (UTC today by default). ``dry_run`` counts only and writes nothing."""
    day = day or datetime.now(timezone.utc).date()
    contacts = list(eligible_contacts(db))
    result = ExportResult(contacts_by_source=Counter(c.source for c in contacts))
    google = hashed_rows(contacts, platform="google")
    meta = hashed_rows(contacts, platform="meta")
    result.google_rows, result.meta_rows = len(google), len(meta)
    if dry_run:
        return result
    if directory is None:
        raise ValueError("no export directory")
    directory.mkdir(parents=True, exist_ok=True)
    for pattern, headers, rows in ((GOOGLE_FILE, GOOGLE_HEADERS, google), (META_FILE, META_HEADERS, meta)):
        path = directory / pattern.format(day=day.isoformat())
        _write(path, headers, rows)
        _prune(directory, pattern, KEEP_NIGHTS)
        result.files.append(path)
    return result


def main(argv: Optional[Sequence[str]] = None) -> int:
    from config.settings import get_settings
    from src.lending.db import lending_session

    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description="Nightly hashed retargeting audiences (Runbook § 4.7)")
    parser.add_argument("--dry-run", action="store_true", help="count contacts and rows; write no files")
    args = parser.parse_args(argv)

    settings = get_settings()
    directory = Path(settings.lending_audience_export_dir) if settings.lending_audience_export_dir else None
    if not args.dry_run and not (settings.lending_audience_export_enabled and directory):
        logger.info("[audience-export] off (needs LENDING_AUDIENCE_EXPORT_ENABLED=true and LENDING_AUDIENCE_EXPORT_DIR)")
        return 0
    try:
        with lending_session() as db:
            result = export(db, directory, dry_run=args.dry_run)
    except Exception as exc:  # class only: SQL and OS errors can embed values or paths
        logger.error("[audience-export] failed (%s)", type(exc).__name__)
        return 1
    logger.info("[audience-export] %scontacts lendingflow=%d web=%d google_rows=%d meta_rows=%d files=%d",
                "dry run: " if args.dry_run else "", result.contacts_by_source["lendingflow"],
                result.contacts_by_source["web"], result.google_rows, result.meta_rows, len(result.files))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
