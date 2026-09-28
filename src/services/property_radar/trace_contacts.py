"""Load phones and emails from an existing Tracerfy results file.

The file is the CSV written by the standalone batch trace (one row per
PropertyRadar record, with ';'-separated phones and emails). Reading it costs
nothing — no new trace is run.
"""
from __future__ import annotations

import csv
import logging
from dataclasses import dataclass
from pathlib import Path

from src.services.phone_utils import normalize as normalize_phone

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LeadContacts:
    """Normalized contact points for one lead. Phones are E.164, emails lower-case."""

    emails: tuple[str, ...] = ()
    phones: tuple[str, ...] = ()

    @property
    def is_empty(self) -> bool:
        return not self.emails and not self.phones


def _split(value: str | None) -> list[str]:
    return [part.strip() for part in (value or "").split(";") if part.strip()]


def parse_contacts(emails: list[str], phones: list[str]) -> LeadContacts:
    clean_emails = tuple(dict.fromkeys(e.lower() for e in emails if "@" in e))
    clean_phones = tuple(dict.fromkeys(p for p in (normalize_phone(raw) for raw in phones) if p))
    return LeadContacts(emails=clean_emails, phones=clean_phones)


def load_trace_contacts(path: Path) -> dict[str, LeadContacts]:
    """Map radar_id -> contacts from a Tracerfy results CSV."""
    contacts: dict[str, LeadContacts] = {}
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            radar_id = (row.get("RadarID") or "").strip()
            if not radar_id:
                continue
            parsed = parse_contacts(_split(row.get("emails")), _split(row.get("phones")))
            if not parsed.is_empty:
                contacts[radar_id] = parsed
    logger.info("trace_contacts: loaded contacts for %d records from %s", len(contacts), path.name)
    return contacts
