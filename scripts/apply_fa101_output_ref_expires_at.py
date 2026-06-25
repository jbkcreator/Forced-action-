"""
Migration fa101: Add output_ref_expires_at to premium_purchases.

Applies to the shared Postgres DB via script (per repo convention, not Alembic CLI).

Usage:
    python scripts/apply_fa101_output_ref_expires_at.py
"""

import logging
import os
import sys

# Ensure project root is on sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from sqlalchemy import text
from src.core.database import get_db_context

logger = logging.getLogger(__name__)


SQL = """
ALTER TABLE premium_purchases
  ADD COLUMN IF NOT EXISTS output_ref_expires_at TIMESTAMP;
"""


def apply() -> None:
    with get_db_context() as db:
        db.execute(text(SQL))
        db.commit()
        logger.info("fa101: output_ref_expires_at column added to premium_purchases")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    apply()