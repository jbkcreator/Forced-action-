"""Backflip borrower conflict check: schema.

Idempotent. Safe to re-run (the constraint is dropped and re-created).
Apply after apply_fa_max_wp2_queues.py, which creates the table extended here.

Widens fa_max_backflip_campaign_contacts.identifier_kind to accept
entity_name and parcel_id alongside email and phone, so a Backflip export can
carry every identifier the per-borrower conflict check matches on. Existing
readers (the FA Max send gate and the self-serve handoff) filter on
identifier_kind = 'email' / 'phone' explicitly, so rows of the new kinds do
not change what they block.

Conflict-check decisions are lending data and are written to
lending.load_exclusions, never to the FA Max audit tables.
"""
from __future__ import annotations

from sqlalchemy import text

from src.core.database import get_db_context

STATEMENTS: list[tuple[str, str]] = [
    (
        "WIDEN fa_max_backflip_campaign_contacts.identifier_kind column to VARCHAR(20)",
        # The column was created as VARCHAR(10) (apply_fa_max_wp2_queues.py); 'entity_name'
        # is 11 characters, so widening only the CHECK constraint below still leaves every
        # entity_name insert failing with "value too long for type character varying(10)".
        """
        ALTER TABLE fa_max_backflip_campaign_contacts
            ALTER COLUMN identifier_kind TYPE VARCHAR(20);
        """,
    ),
    (
        "WIDEN fa_max_backflip_campaign_contacts.identifier_kind constraint",
        """
        ALTER TABLE fa_max_backflip_campaign_contacts
            DROP CONSTRAINT IF EXISTS ck_fa_max_backflip_identifier_kind;
        ALTER TABLE fa_max_backflip_campaign_contacts
            ADD CONSTRAINT ck_fa_max_backflip_identifier_kind
                CHECK (identifier_kind IN ('email', 'phone', 'entity_name', 'parcel_id'));
        """,
    ),
]


def run() -> None:
    with get_db_context() as session:
        for label, sql in STATEMENTS:
            print(f"  apply: {label}")
            session.execute(text(sql))
    print("apply_backflip_conflict_identifiers: done")


if __name__ == "__main__":
    run()
