"""Apply fa076 — widen unmatched_records.check_unmatched_match_method.

The loader match-method taxonomy (src/loaders/base.py) grew the granular
cascade stages — normalized_address, owner_name_zip, owner_name_city — but the
CHECK constraint on unmatched_records still only allowed the original four
values. Records whose best match attempt reached a new stage failed quarantine
(CheckViolation) and were dropped instead of parked for re-matching.

This drops and recreates the constraint with the full non-LLM method set.
'llm_verified' is intentionally excluded (LLM-promoted matches clear the
destination threshold and never reach quarantine). 'address' is kept because
existing rows still carry that legacy value.

Idempotent: DROP ... IF EXISTS then recreate. Safe to run multiple times.

Usage:
    PYTHONPATH=. python scripts/apply_fa076_ddl.py
"""
import logging
import sys

from sqlalchemy import text

from src.core.database import Database

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_ALLOWED = (
    "'address', 'normalized_address', "
    "'owner_name', 'owner_name_zip', 'owner_name_city', "
    "'legal_desc', 'parcel_id'"
)

_DDL = [
    "ALTER TABLE unmatched_records DROP CONSTRAINT IF EXISTS check_unmatched_match_method",
    (
        "ALTER TABLE unmatched_records ADD CONSTRAINT check_unmatched_match_method "
        f"CHECK (match_method IN ({_ALLOWED}) OR match_method IS NULL)"
    ),
]


def run() -> None:
    db = Database()
    with db.session_scope() as session:
        for stmt in _DDL:
            session.execute(text(stmt))
            logger.info("OK: %s", stmt[:80])
    logger.info("fa076 DDL applied successfully.")


if __name__ == "__main__":
    run()
    sys.exit(0)
