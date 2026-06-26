"""Idempotent DDL script for fa_a6: underwriting_feedback.

Run with:
    PYTHONPATH=. python scripts/apply_fa_a6_underwriting_feedback.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text as sa_text

from src.core.database import get_db_context

_VALID_CODES = (
    "ltv_too_high",
    "structural_damage",
    "commercial_zoning",
    "title_defect",
    "flood_zone",
    "environmental_hazard",
    "deferred_maintenance",
    "unpermitted_additions",
    "tenant_occupied",
    "market_saturation",
)

_CODES_SQL = ", ".join(f"'{c}'" for c in _VALID_CODES)

DDL = f"""
CREATE TABLE IF NOT EXISTS underwriting_feedback (
    id            SERIAL PRIMARY KEY,
    property_id   INTEGER      NOT NULL REFERENCES properties(id) ON DELETE CASCADE,
    reason_code   VARCHAR(60)  NOT NULL,
    reason_detail TEXT,
    lender_id     VARCHAR(80),
    loan_amount   NUMERIC(12, 2),
    submitted_by  VARCHAR(80)  NOT NULL,
    submitted_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT ck_uw_feedback_reason_code CHECK (reason_code IN ({_CODES_SQL}))
);
CREATE INDEX IF NOT EXISTS idx_uw_feedback_property_id
    ON underwriting_feedback (property_id);
CREATE INDEX IF NOT EXISTS idx_uw_feedback_submitted_at
    ON underwriting_feedback (submitted_at DESC);
CREATE INDEX IF NOT EXISTS idx_uw_feedback_reason_code
    ON underwriting_feedback (reason_code);
"""


def main() -> None:
    with get_db_context() as db:
        for stmt in DDL.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                db.execute(sa_text(stmt))
    print("apply_fa_a6_underwriting_feedback: table ready")


if __name__ == "__main__":
    main()
