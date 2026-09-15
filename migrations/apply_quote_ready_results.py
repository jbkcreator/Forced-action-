"""
Create fa_max_quote_ready_results — WP-8A Quote Ready result store.

Append-only, versioned. WP-8 owns this domain table; it attaches to Dev 1's
spine via opportunity_id / person_id and never forms a parallel identity system.

Idempotent (CREATE TABLE IF NOT EXISTS). GUARDED: the FK to
fa_max_opportunities requires Dev 1's WP-1 spine, which is not yet on `dev`.
Until the spine table exists this script no-ops with a clear message. Run again
once WP-1 lands.

Usage:
    PYTHONPATH=. python migrations/apply_quote_ready_results.py
"""
from __future__ import annotations

from sqlalchemy import create_engine, text

SPINE_TABLE = "fa_max_opportunities"

DDL = """
CREATE TABLE IF NOT EXISTS fa_max_quote_ready_results (
    result_id             UUID PRIMARY KEY DEFAULT generate_uuidv7(),
    opportunity_id        UUID NOT NULL REFERENCES fa_max_opportunities(opportunity_id),
    person_id             UUID,
    property_id           INTEGER REFERENCES properties(id),
    calculation_version   TEXT NOT NULL,
    input_hash            TEXT NOT NULL,
    status                TEXT NOT NULL,
    inputs                JSONB NOT NULL,
    outputs               JSONB NOT NULL,
    provenance            JSONB NOT NULL,
    confidence            JSONB NOT NULL,
    missing_inputs        JSONB NOT NULL DEFAULT '[]'::jsonb,
    computed_by           TEXT NOT NULL,
    computed_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    supersedes_result_id  UUID REFERENCES fa_max_quote_ready_results(result_id),
    reviewed_by           TEXT,
    reviewed_at           TIMESTAMPTZ,
    review_status         TEXT,
    CONSTRAINT uq_quote_ready_opp_hash_version
        UNIQUE (opportunity_id, input_hash, calculation_version)
);
CREATE INDEX IF NOT EXISTS ix_quote_ready_opportunity ON fa_max_quote_ready_results (opportunity_id);
CREATE INDEX IF NOT EXISTS ix_quote_ready_property   ON fa_max_quote_ready_results (property_id);
"""


def run(conn) -> None:
    exists = conn.execute(
        text("SELECT to_regclass(:t)"), {"t": SPINE_TABLE}
    ).scalar()
    if exists is None:
        print(
            f"SKIP: {SPINE_TABLE} not found — Dev 1 WP-1 spine not on this DB yet. "
            "fa_max_quote_ready_results not created. Re-run after WP-1 lands."
        )
        return
    conn.execute(text(DDL))
    print("Done. fa_max_quote_ready_results ready (idempotent).")


if __name__ == "__main__":
    from config.settings import get_settings
    engine = create_engine(str(get_settings().database_url))
    with engine.begin() as conn:
        run(conn)
