"""FA Max WP-T2-2 review fix — enforce origin_interaction_id write-once at
the database level, not just by convention in state_engine.py.

fa_max_opportunities.origin_interaction_id (added by
apply_fa_max_wp_t2_2_agent_infra.py) is the causal-attribution FK consumed
by fa_max_autonomy.get_funded_loan_count() for the Tier C graduation gate.
create_fa_max_opportunity() (src/services/state_engine.py) is documented as
the single write path and never exposes an UPDATE for this column -- but a
future caller could still issue a raw UPDATE directly against the table.
This migration closes that gap with a BEFORE UPDATE trigger: once
origin_interaction_id is non-NULL, any UPDATE attempting to change it
(including setting it back to NULL) is rejected. Setting it from NULL to a
value is still allowed exactly once, matching "write-once, set at creation."

Idempotent. Safe to re-run (CREATE OR REPLACE FUNCTION / DROP TRIGGER IF
EXISTS + CREATE TRIGGER).
"""
from __future__ import annotations

from sqlalchemy import text

from src.core.database import get_db_context

STATEMENTS: list[tuple[str, str]] = [
    (
        "CREATE/REPLACE trigger function fa_max_opp_origin_interaction_immutable",
        """
        CREATE OR REPLACE FUNCTION fa_max_opp_origin_interaction_immutable()
        RETURNS trigger AS $$
        BEGIN
            IF OLD.origin_interaction_id IS NOT NULL
               AND NEW.origin_interaction_id IS DISTINCT FROM OLD.origin_interaction_id THEN
                RAISE EXCEPTION
                    'fa_max_opportunities.origin_interaction_id is write-once: '
                    'opportunity % already has origin_interaction_id % -- cannot change to %',
                    OLD.opportunity_id, OLD.origin_interaction_id, NEW.origin_interaction_id;
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """,
    ),
    (
        "ATTACH trigger to fa_max_opportunities",
        """
        DROP TRIGGER IF EXISTS trg_fa_max_opp_origin_interaction_immutable ON fa_max_opportunities;
        CREATE TRIGGER trg_fa_max_opp_origin_interaction_immutable
            BEFORE UPDATE ON fa_max_opportunities
            FOR EACH ROW
            EXECUTE FUNCTION fa_max_opp_origin_interaction_immutable();
        """,
    ),
]


def main() -> None:
    with get_db_context() as session:
        for label, sql in STATEMENTS:
            print(f"  -> {label}")
            session.execute(text(sql))
        session.commit()

    with get_db_context() as session:
        trigger_present = session.execute(
            text(
                "SELECT COUNT(*) FROM pg_trigger "
                "WHERE tgname = 'trg_fa_max_opp_origin_interaction_immutable'"
            )
        ).scalar()

    print("\nVerification:")
    print(f"  trg_fa_max_opp_origin_interaction_immutable present: {bool(trigger_present)}")


if __name__ == "__main__":
    main()
