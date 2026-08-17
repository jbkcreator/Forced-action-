"""
Add instantly_campaign_id to vertical_probes.

Idempotent — safe to re-run.
"""
import sys
import logging
from src.core.database import Database

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = """
ALTER TABLE vertical_probes
    ADD COLUMN IF NOT EXISTS instantly_campaign_id VARCHAR(100);
"""


def main() -> None:
    with Database().session_scope() as db:
        db.execute(__import__("sqlalchemy").text(SQL))
        db.commit()
    logger.info("apply_vertical_probe_instantly_campaign_id: done")


if __name__ == "__main__":
    main()
