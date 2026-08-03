"""REVINT I3/I4 schema fixes.

  1. VerticalVerdict.verdict constraint — add 'awaiting_ruling'.
  2. VerticalCandidatePacket.status constraint — add 'awaiting_ruling'.
  3. VerticalProbe compliance columns — drop NOT NULL so stubs write NULL, not True.

Idempotent. Usage:
    PYTHONPATH=. python migrations/apply_revint_i3_i4_fixes.py
"""

from sqlalchemy import text
from src.core.database import Database

DDL = [
    # 1. Drop and recreate verdict check constraint with awaiting_ruling
    """
    DO $$
    BEGIN
        ALTER TABLE vertical_verdicts DROP CONSTRAINT IF EXISTS ck_vverdict_verdict;
        ALTER TABLE vertical_verdicts ADD CONSTRAINT ck_vverdict_verdict
            CHECK (verdict IN ('won','killed','running','awaiting_ruling'));
    END$$
    """,

    # 2. Drop and recreate packet status check constraint with awaiting_ruling
    """
    DO $$
    BEGIN
        ALTER TABLE vertical_candidate_packets DROP CONSTRAINT IF EXISTS ck_vcp_status;
        ALTER TABLE vertical_candidate_packets ADD CONSTRAINT ck_vcp_status
            CHECK (status IN ('candidate','probing','won','killed','pending_legal','awaiting_ruling'));
    END$$
    """,

    # 3. Make compliance flag columns nullable (stubs must write NULL, not True)
    "ALTER TABLE vertical_probes ALTER COLUMN tcpa_preflight_passed DROP NOT NULL",
    "ALTER TABLE vertical_probes ALTER COLUMN suppression_checked DROP NOT NULL",
    "ALTER TABLE vertical_probes ALTER COLUMN touch_collision_checked DROP NOT NULL",
    "ALTER TABLE vertical_probes ALTER COLUMN frequency_cap_checked DROP NOT NULL",
    "ALTER TABLE vertical_probes ALTER COLUMN quiet_hours_checked DROP NOT NULL",
    "ALTER TABLE vertical_probes ALTER COLUMN channel_limits_checked DROP NOT NULL",
    "ALTER TABLE vertical_probes ALTER COLUMN kill_switch_active DROP NOT NULL",

    # 4. Flip existing rows that have stub True → NULL (retroactive correction)
    """
    UPDATE vertical_probes
    SET
        tcpa_preflight_passed  = NULL,
        suppression_checked    = NULL,
        touch_collision_checked = NULL,
        frequency_cap_checked  = NULL,
        quiet_hours_checked    = NULL,
        channel_limits_checked = NULL
    WHERE
        tcpa_preflight_passed  IS TRUE
        AND suppression_checked IS TRUE
        AND touch_collision_checked IS TRUE
        AND frequency_cap_checked IS TRUE
        AND quiet_hours_checked IS TRUE
        AND channel_limits_checked IS TRUE
        AND kill_switch_active IS FALSE
    """,
]


def main() -> None:
    db = Database()
    with db.session_scope() as s:
        for stmt in DDL:
            s.execute(text(stmt))
    print("apply_revint_i3_i4_fixes: done.")


if __name__ == "__main__":
    main()
