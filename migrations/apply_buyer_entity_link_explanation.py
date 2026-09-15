"""
Add match_explanation column to buyer_entity_links.

Stores a human-readable, machine-generated string explaining exactly why
two records were linked — the specific scores, signals, and evidence used
by the resolver at match time. For human audit only; not parsed programmatically.
"""
import logging
from sqlalchemy import text
from config.settings import get_settings
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)


def apply(engine=None):
    if engine is None:
        engine = create_engine(get_settings().database_url)

    with engine.begin() as conn:
        conn.execute(text("""
            ALTER TABLE buyer_entity_links
                ADD COLUMN IF NOT EXISTS match_explanation text
        """))
        logger.info("buyer_entity_links.match_explanation column applied")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    apply()
    print("Done.")
