"""
Apply the composite index (subscriber_id, started_at DESC) on agent_decisions.

CREATE INDEX CONCURRENTLY cannot run inside a transaction, so this script
connects in autocommit mode and runs the DDL directly. Run once on any
environment after deploying the a1b2c3_timeline_idx migration revision.

Usage:
    python scripts/apply_timeline_index.py
"""
import sys
import logging
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Import settings after adding project root to path
sys.path.insert(0, ".")
from config.settings import get_settings


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, isolation_level="AUTOCOMMIT")

    index_name = "idx_agent_decisions_subscriber_started"

    with engine.connect() as conn:
        existing = conn.execute(
            text("SELECT 1 FROM pg_indexes WHERE indexname = :name"),
            {"name": index_name},
        ).fetchone()

        if existing:
            logger.info("Index %s already exists — nothing to do.", index_name)
            return

        logger.info("Creating index %s CONCURRENTLY (this may take a moment)…", index_name)
        conn.execute(
            text(
                f"CREATE INDEX CONCURRENTLY {index_name} "
                "ON agent_decisions (subscriber_id, started_at DESC)"
            )
        )
        logger.info("Index created successfully.")


if __name__ == "__main__":
    main()
