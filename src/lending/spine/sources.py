"""R1 1.1 identity sources: one streamed query per lending table, each row a ``SourceRecord`` of its identifiers."""
from __future__ import annotations

import json
import logging
from typing import Any, Callable, Iterator, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.lending_contact_spine import BATCHDIALER_CONTACT, GHL_CONTACT
from src.lending.spine.identity import (
    Identifier, SourceRecord, email_identifier, entity_identifier, external_identifier, phone_identifier,
)

logger = logging.getLogger(__name__)

Reader = Callable[[Session], Iterator[SourceRecord]]
_STREAM = {"yield_per": 1000}


def _name(*parts: Optional[str]) -> Optional[str]:
    joined = " ".join(p.strip() for p in parts if p and p.strip())
    return joined or None


def _as_list(value: Any) -> list[str]:
    if not value:
        return []
    if isinstance(value, str):  # SQLite tests store arrays as JSON text; Postgres returns a list
        return json.loads(value)
    return list(value)


def _record(source: str, ref: Any, name: Optional[str], *identifiers: Optional[Identifier]) -> SourceRecord:
    return SourceRecord(source=source, ref=str(ref), name=name,
                        identifiers=frozenset(i for i in identifiers if i is not None))


def _rows(db: Session, sql: str):
    return db.execute(text(sql), execution_options=_STREAM)


def _loan_files(db: Session) -> Iterator[SourceRecord]:
    for r in _rows(db, """
            SELECT f.id, COALESCE(b.first_name, f.borrower_first_name) AS first_name,
                   COALESCE(b.last_name, f.borrower_last_name) AS last_name,
                   COALESCE(b.phone, f.borrower_phone) AS phone, COALESCE(b.email, f.borrower_email) AS email,
                   e.name AS entity
              FROM lending.loan_files f
              LEFT JOIN lending.borrower_profiles b ON b.id = f.borrower_id
              LEFT JOIN lending.entities e ON e.id = f.entity_id"""):
        yield _record("loan_files", r.id, _name(r.first_name, r.last_name), phone_identifier(r.phone),
                      email_identifier(r.email), entity_identifier(r.entity))


def _lendingflow_leads(db: Session) -> Iterator[SourceRecord]:
    for r in _rows(db, "SELECT id, first_name, last_name, phone, email, ghl_contact_id FROM lending.lendingflow_leads"):
        yield _record("lendingflow_leads", r.id, _name(r.first_name, r.last_name), phone_identifier(r.phone),
                      email_identifier(r.email), external_identifier(GHL_CONTACT, r.ghl_contact_id))


def _web_leads(db: Session) -> Iterator[SourceRecord]:
    for r in _rows(db, "SELECT id, name, phone, email, ghl_contact_id FROM lending.web_leads"):
        yield _record("web_leads", r.id, _name(r.name), phone_identifier(r.phone), email_identifier(r.email),
                      external_identifier(GHL_CONTACT, r.ghl_contact_id))


def _after_hours_conversations(db: Session) -> Iterator[SourceRecord]:
    for r in _rows(db, "SELECT id, first_name, phone, email, ghl_contact_id FROM lending.after_hours_conversations"):
        yield _record("after_hours_conversations", r.id, _name(r.first_name), phone_identifier(r.phone),
                      email_identifier(r.email), external_identifier(GHL_CONTACT, r.ghl_contact_id))


def _dialer_load_records(db: Session) -> Iterator[SourceRecord]:
    for r in _rows(db, "SELECT id, phone, borrower_name, entity_name, ghl_contact_id, dialer_contact_id "
                       "FROM lending.dialer_load_records"):
        yield _record("dialer_load_records", r.id, _name(r.borrower_name), phone_identifier(r.phone),
                      entity_identifier(r.entity_name), external_identifier(GHL_CONTACT, r.ghl_contact_id),
                      external_identifier(BATCHDIALER_CONTACT, r.dialer_contact_id))


def _instantly_replies(db: Session) -> Iterator[SourceRecord]:
    for r in _rows(db, "SELECT id, first_name, lead_email FROM lending.instantly_replies"):
        yield _record("instantly_replies", r.id, _name(r.first_name), email_identifier(r.lead_email))


def _sunbiz_llc_contacts(db: Session) -> Iterator[SourceRecord]:
    for r in _rows(db, "SELECT document_number, entity_name, subject_name, phones, emails "
                       "FROM lending.sunbiz_llc_contacts"):
        yield _record("sunbiz_llc_contacts", r.document_number, _name(r.subject_name), entity_identifier(r.entity_name),
                      *(phone_identifier(p) for p in _as_list(r.phones)),
                      *(email_identifier(e) for e in _as_list(r.emails)))


def _call_dispositions(db: Session) -> Iterator[SourceRecord]:
    for r in _rows(db, "SELECT dialer_call_id, phone, dialer_contact_id FROM lending.call_dispositions "
                       "WHERE phone IS NOT NULL AND dialer_call_id IS NOT NULL"):
        yield _record("call_dispositions", r.dialer_call_id, None, phone_identifier(r.phone),
                      external_identifier(BATCHDIALER_CONTACT, r.dialer_contact_id))


def _contacts(db: Session) -> Iterator[SourceRecord]:
    for r in _rows(db, "SELECT id, phone, email FROM lending.contacts"):
        yield _record("contacts", r.id, None, phone_identifier(r.phone), email_identifier(r.email))


SOURCE_READERS: tuple[tuple[str, Reader], ...] = (
    ("loan_files", _loan_files),
    ("lendingflow_leads", _lendingflow_leads),
    ("web_leads", _web_leads),
    ("after_hours_conversations", _after_hours_conversations),
    ("dialer_load_records", _dialer_load_records),
    ("instantly_replies", _instantly_replies),
    ("sunbiz_llc_contacts", _sunbiz_llc_contacts),
    ("call_dispositions", _call_dispositions),
    ("contacts", _contacts),
)


def read_records(db: Session, readers: tuple[tuple[str, Reader], ...] = SOURCE_READERS,
                 ) -> tuple[list[SourceRecord], list[str]]:
    """Every source's records, and the names of sources that could not be read (the others still are)."""
    records: list[SourceRecord] = []
    failed: list[str] = []
    for name, reader in readers:
        try:
            with db.begin_nested():
                records.extend(reader(db))
        except Exception as exc:
            logger.warning("contact spine: source %s unreadable (%s)", name, type(exc).__name__)
            failed.append(name)
    return records, failed


def known_landlines(db: Session) -> frozenset[str]:
    rows = db.execute(text("SELECT phone FROM lending.contacts WHERE phone IS NOT NULL AND line_type IS NOT NULL "
                           "AND trim(line_type) <> '' AND lower(trim(line_type)) <> 'mobile'")).scalars()
    return frozenset(i.value for i in map(phone_identifier, rows) if i)
