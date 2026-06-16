"""
Apply fa078_contactability_detail DDL (ADR 0015).

House convention: alembic file alembic/versions/fa078_contactability_detail.py
is the record; this script is what actually runs against the DB. Idempotent —
safe to re-run. Does not touch alembic_version.

Usage:
  python scripts/apply_contactability_detail_migration.py
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import text  # noqa: E402

from src.core.database import get_db_context  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("fa078_apply")


def main() -> None:
    with get_db_context() as session:
        session.execute(text(
            "ALTER TABLE owners ADD COLUMN IF NOT EXISTS contactability_detail JSONB"
        ))
        session.commit()
        exists = session.execute(text(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_name = 'owners' AND column_name = 'contactability_detail'"
        )).scalar()
        if not exists:
            raise SystemExit("contactability_detail column missing after apply — investigate")
    log.info("owners.contactability_detail present — fa078 applied.")


if __name__ == "__main__":
    main()
