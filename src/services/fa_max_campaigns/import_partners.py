"""Campaign 3 (Rescue Circuit) partner list import — plan Section 6.12.

Josh builds this list himself: title companies, Georgia closing attorneys,
mortgage brokers, and loan officers. No property data is involved. Each
row either matches an existing fa_max_persons row (by email or phone) or
creates a new one.

Fixes carried from the plan's review pass (Section 0):
  - R2: new people get fa_max_persons.source='partner' — the ONLY source
    channel_split_reason() allows besides deed/permit/distress/maturity.
    'manual_import' is never written to fa_max_persons.source; it is the
    fa_max_partners.source value that marks these rows as import-only
    (Gap D), which is a different column entirely.
  - R4: partners are created with status='identified', never 'active' —
    importing a list must never itself grant Tier B auto-send context
    (fa_max_send_governance.autonomous_tier_context_verified()).
  - Never writes consent (plan Section 6.5) — an imported partner is not
    enrolled into Rescue Circuit until they separately have a consent row
    for whichever channel a step needs.
"""
from __future__ import annotations

import csv
import logging
from dataclasses import dataclass

from sqlalchemy.orm import Session
from sqlalchemy import text

from config import fa_max_campaigns as cfg
from src.services.phone_utils import normalize as normalize_phone
from src.services.state_engine import ensure_entity_registry

logger = logging.getLogger(__name__)

_IMPORT_SOURCE = cfg.RESCUE_CIRCUIT_PARTNER_SOURCE  # 'manual_import' — fa_max_partners.source only
_PERSON_SOURCE = "partner"  # fa_max_persons.source — an FA-channel-allowed value

_REQUIRED_COLUMNS = {"name", "company", "partner_type"}


@dataclass
class ImportSummary:
    created: int = 0
    matched_existing: int = 0
    skipped_invalid: int = 0
    skipped_suppressed: int = 0


def _find_person_by_identifier(session: Session, *, kind: str, value: str) -> str | None:
    row = session.execute(
        text(
            "SELECT person_id::text FROM fa_max_person_contact_identifiers "
            "WHERE identifier_kind = :kind AND identifier_value = :value"
        ),
        {"kind": kind, "value": value},
    ).fetchone()
    return row[0] if row else None


def _is_suppressed(session: Session, person_id: str) -> bool:
    row = session.execute(
        text("SELECT lifecycle_state FROM fa_max_persons WHERE person_id = CAST(:pid AS uuid)"),
        {"pid": person_id},
    ).fetchone()
    return bool(row) and row[0] in ("do_not_contact", "suppressed")


def _find_or_create_person(
    session: Session, *, full_name: str, email: str | None, phone: str | None,
) -> tuple[str, bool]:
    """Returns (person_id, created). Matches on email or phone first — the
    client's own instruction: don't 'contact the same owner twice'."""
    if email:
        existing = _find_person_by_identifier(session, kind="email", value=email)
        if existing:
            return existing, False
    if phone:
        existing = _find_person_by_identifier(session, kind="phone", value=phone)
        if existing:
            return existing, False

    row = session.execute(
        text(
            "INSERT INTO fa_max_persons (source, full_name, email, phone) "
            "VALUES (:source, :name, :email, :phone) RETURNING person_id::text"
        ),
        {"source": _PERSON_SOURCE, "name": full_name, "email": email, "phone": phone},
    ).fetchone()
    person_id = row[0]
    ensure_entity_registry(session=session, entity_type="person", native_id=person_id)

    if email:
        session.execute(
            text(
                "INSERT INTO fa_max_person_contact_identifiers "
                "(identifier_kind, identifier_value, person_id, source) "
                "VALUES ('email', :value, CAST(:pid AS uuid), :source) "
                "ON CONFLICT (identifier_kind, identifier_value) DO NOTHING"
            ),
            {"value": email, "pid": person_id, "source": _IMPORT_SOURCE},
        )
    if phone:
        session.execute(
            text(
                "INSERT INTO fa_max_person_contact_identifiers "
                "(identifier_kind, identifier_value, person_id, source) "
                "VALUES ('phone', :value, CAST(:pid AS uuid), :source) "
                "ON CONFLICT (identifier_kind, identifier_value) DO NOTHING"
            ),
            {"value": phone, "pid": person_id, "source": _IMPORT_SOURCE},
        )
    return person_id, True


def _upsert_partner(session: Session, *, person_id: str, partner_class: str, county_id: str | None) -> None:
    session.execute(
        text(
            "INSERT INTO fa_max_partners (person_id, partner_class, status, source, county_id) "
            "VALUES (CAST(:pid AS uuid), :pclass, 'identified', :source, :county) "
            "ON CONFLICT (person_id, partner_class) DO UPDATE SET "
            "county_id = COALESCE(EXCLUDED.county_id, fa_max_partners.county_id)"
        ),
        {"pid": person_id, "pclass": partner_class, "source": _IMPORT_SOURCE, "county": county_id},
    )


def import_partners_csv(session: Session, file_path: str, *, dry_run: bool = False) -> ImportSummary:
    """Columns: name, company, email, phone, partner_type, state, county, notes.
    partner_type must be one of RESCUE_CIRCUIT_PARTNER_CLASSES (config)."""
    summary = ImportSummary()

    with open(file_path, newline="", encoding="utf-8-sig") as fh:
        rows = list(csv.DictReader(fh))

    if rows and not _REQUIRED_COLUMNS.issubset({c.strip().lower() for c in rows[0].keys()}):
        raise ValueError(f"CSV must have columns: {sorted(_REQUIRED_COLUMNS)}")

    for row_num, row in enumerate(rows, start=2):
        name = (row.get("name") or "").strip()
        company = (row.get("company") or "").strip()
        partner_type = (row.get("partner_type") or "").strip().lower()
        email = (row.get("email") or "").strip().lower() or None
        phone = normalize_phone(row.get("phone")) if row.get("phone") else None
        county = (row.get("county") or "").strip() or None

        if not name or partner_type not in cfg.RESCUE_CIRCUIT_PARTNER_CLASSES:
            logger.warning("import_partners: row %d skipped — invalid name/partner_type=%r", row_num, partner_type)
            summary.skipped_invalid += 1
            continue
        if not email and not phone:
            logger.warning("import_partners: row %d skipped — no email or phone", row_num)
            summary.skipped_invalid += 1
            continue

        if dry_run:
            summary.created += 1
            continue

        full_name = f"{name} ({company})" if company else name
        person_id, created = _find_or_create_person(session, full_name=full_name, email=email, phone=phone)
        if created:
            summary.created += 1
        else:
            summary.matched_existing += 1

        _upsert_partner(session, person_id=person_id, partner_class=partner_type, county_id=county)
        session.commit()

        if _is_suppressed(session, person_id):
            summary.skipped_suppressed += 1

    if dry_run:
        session.rollback()
    return summary


if __name__ == "__main__":
    import argparse
    import sys

    from src.core.database import get_db_context

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("file")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    with get_db_context() as _session:
        result = import_partners_csv(_session, args.file, dry_run=args.dry_run)

    print(
        f"created={result.created} matched_existing={result.matched_existing} "
        f"skipped_invalid={result.skipped_invalid} skipped_suppressed={result.skipped_suppressed}"
    )
    sys.exit(0)
