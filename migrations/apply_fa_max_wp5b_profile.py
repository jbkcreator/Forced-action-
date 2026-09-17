"""
FA Max WP-5B — Borrower Buy Box, Velocity & Next-Need Prediction schema.

Creates:
  1. fa_max_person_profiles — per-person intelligence profile table.
  2. buyer_entity_id (nullable FK) column on fa_max_persons — provisional
     bridge to buyer_entities.id, populated by the WP-5B nightly sweep.
     WP-3/WP-4 will formalize this bridge with reversible-merge logging
     once those work packages ship.

Idempotent: safe to re-run (ADD COLUMN IF NOT EXISTS,
CREATE TABLE IF NOT EXISTS, CREATE INDEX IF NOT EXISTS).

Depends on:
  - apply_fa_max_state_engine.py   (fa_max_persons table)
  - apply_fa_max_wp1_remaining.py  (state_version on fa_max_persons)
  - Hunter BuyerEntity resolution  (buyer_entities table must exist)

Run:
  PYTHONPATH=. python migrations/apply_fa_max_wp5b_profile.py
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

_DDL = [
    # ------------------------------------------------------------------
    # 1. Provisional entity bridge on fa_max_persons
    # ------------------------------------------------------------------
    """
    ALTER TABLE fa_max_persons
        ADD COLUMN IF NOT EXISTS buyer_entity_id INTEGER
            REFERENCES buyer_entities(id) ON DELETE SET NULL;
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_fa_max_persons_buyer_entity_id
        ON fa_max_persons (buyer_entity_id)
        WHERE buyer_entity_id IS NOT NULL;
    """,

    # ------------------------------------------------------------------
    # 2. fa_max_person_profiles — buy-box, velocity, next-need
    # ------------------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS fa_max_person_profiles (
        person_id           UUID        PRIMARY KEY
                                        REFERENCES fa_max_persons(person_id)
                                        ON DELETE CASCADE,

        -- Provisional bridge (WP-3/WP-4 will formalize with merge logging)
        buyer_entity_id     INTEGER     REFERENCES buyer_entities(id)
                                        ON DELETE SET NULL,

        -- Buy-box profile — all NULL when confidence_tier = 'unknown'
        -- Source: public-record deed sale prices and property attributes
        buy_box_geography       JSONB,   -- [{city, county_id, count}]
        buy_box_property_types  JSONB,   -- [{property_type, property_use_code, count}]
        buy_box_price_band      JSONB,   -- {min_cents, median_cents, max_cents, sample_count}

        -- Deal velocity (mirrored from BuyerEntity cadence fields)
        velocity_purchases_per_year     NUMERIC(6, 2),
        last_transaction_date           DATE,
        avg_days_between_transactions   NUMERIC(8, 1),
        active_property_count           INTEGER,

        -- Predicted next financing need — internal product category only,
        -- never a rate, term, or commitment to a borrower (SOT.md compliance)
        predicted_next_need      VARCHAR(50)
            CHECK (predicted_next_need IS NULL OR predicted_next_need IN (
                'bridge', 'hard_money_purchase', 'renovation_capital',
                'heloc', 'cash_out_refi', 'buyout_refi'
            )),
        predicted_next_need_date TIMESTAMPTZ,
        next_need_evidence       JSONB,  -- top-3 source properties + signals

        -- Confidence / data-sufficiency tier
        confidence_tier VARCHAR(20) NOT NULL DEFAULT 'unknown'
            CHECK (confidence_tier IN ('high', 'medium', 'low', 'unknown')),

        computed_at TIMESTAMPTZ,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """,
    # Add buy_box_preferences column (idempotent, safe if table already exists)
    """
    ALTER TABLE fa_max_person_profiles
        ADD COLUMN IF NOT EXISTS buy_box_preferences JSONB;
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_fa_max_person_profile_buyer_entity
        ON fa_max_person_profiles (buyer_entity_id)
        WHERE buyer_entity_id IS NOT NULL;
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_fa_max_person_profile_confidence
        ON fa_max_person_profiles (confidence_tier);
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_fa_max_person_profile_computed_at
        ON fa_max_person_profiles (computed_at)
        WHERE computed_at IS NOT NULL;
    """,
]


def main() -> None:
    with get_db_context() as session:
        for ddl in _DDL:
            session.execute(text(ddl))
            session.commit()
    print("apply_fa_max_wp5b_profile: done")


if __name__ == "__main__":
    main()
