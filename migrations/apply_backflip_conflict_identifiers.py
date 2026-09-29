"""Backflip borrower conflict check: schema.

Idempotent. Safe to re-run (constraints are dropped and re-created,
ADD COLUMN IF NOT EXISTS). Apply after apply_fa_max_wp2_queues.py and
apply_fa_max_wp_t2_3.py, which create the two tables extended here.

What this does:
1. Widens fa_max_backflip_campaign_contacts.identifier_kind to accept
   entity_name and parcel_id alongside email and phone, so a Backflip export
   can carry every identifier the per-borrower conflict check matches on.
   Existing readers (the FA Max send gate and the self-serve handoff) filter
   on identifier_kind = 'email' / 'phone' explicitly, so rows of the new kinds
   do not change what they block.
2. Widens fa_max_backflip_suppression_decisions.gate to accept 'dialer'
   (a pool record checked before it is loaded into the dialer) and adds
   subject_ref (the pool record checked) and matched_criteria (every
   identifier kind that matched), so each dialer-load decision is auditable.
"""
from __future__ import annotations

from sqlalchemy import text

from src.core.database import get_db_context

STATEMENTS: list[tuple[str, str]] = [
    (
        "WIDEN fa_max_backflip_campaign_contacts.identifier_kind",
        """
        ALTER TABLE fa_max_backflip_campaign_contacts
            DROP CONSTRAINT IF EXISTS ck_fa_max_backflip_identifier_kind;
        ALTER TABLE fa_max_backflip_campaign_contacts
            ADD CONSTRAINT ck_fa_max_backflip_identifier_kind
                CHECK (identifier_kind IN ('email', 'phone', 'entity_name', 'parcel_id'));
        """,
    ),
    (
        "ADD dialer gate + subject_ref + matched_criteria to fa_max_backflip_suppression_decisions",
        """
        ALTER TABLE fa_max_backflip_suppression_decisions
            ADD COLUMN IF NOT EXISTS subject_ref TEXT,
            ADD COLUMN IF NOT EXISTS matched_criteria TEXT[];
        ALTER TABLE fa_max_backflip_suppression_decisions
            DROP CONSTRAINT IF EXISTS ck_fa_max_bsd_gate;
        ALTER TABLE fa_max_backflip_suppression_decisions
            ADD CONSTRAINT ck_fa_max_bsd_gate
                CHECK (gate IN ('draft', 'send', 'dialer'));
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
