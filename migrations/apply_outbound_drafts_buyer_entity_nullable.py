"""
Make outbound_drafts.buyer_entity_id nullable.

DBPR contractor drafts (cell_id='dbpr_storm_blitz') are not associated with a
buyer entity — contractors are not property investors. This relaxes the NOT NULL
constraint so those drafts can be persisted with buyer_entity_id=NULL while all
existing whale drafts continue to carry a real buyer_entity_id.

Postgres executes DROP NOT NULL instantly — no row rewrite, no table lock.
"""
import logging

import psycopg2

from config.settings import get_settings

logger = logging.getLogger(__name__)


def run() -> None:
    # Reads through get_settings() (which loads .env) rather than os.environ:
    # deploy.sh never exports DATABASE_URL into the shell, so the previous
    # os.environ["DATABASE_URL"] raised KeyError and aborted the deploy step
    # that would have applied this. Every other migration in this directory
    # goes through settings for the same reason.
    conn = psycopg2.connect(get_settings().database_url)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT is_nullable
                FROM information_schema.columns
                WHERE table_name = 'outbound_drafts'
                  AND column_name = 'buyer_entity_id'
            """)
            row = cur.fetchone()
            if row and row[0].upper() == "YES":
                logger.info("outbound_drafts.buyer_entity_id already nullable — skipping")
                return

            cur.execute(
                "ALTER TABLE outbound_drafts ALTER COLUMN buyer_entity_id DROP NOT NULL"
            )
        conn.commit()
        logger.info("outbound_drafts.buyer_entity_id is now nullable")
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
