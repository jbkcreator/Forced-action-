"""
Make outbound_drafts.buyer_entity_id nullable.

DBPR contractor drafts (cell_id='dbpr_storm_blitz') are not associated with a
buyer entity — contractors are not property investors. This relaxes the NOT NULL
constraint so those drafts can be persisted with buyer_entity_id=NULL while all
existing whale drafts continue to carry a real buyer_entity_id.

Postgres executes DROP NOT NULL instantly — no row rewrite, no table lock.
"""
import logging
import os

import psycopg2

logger = logging.getLogger(__name__)


def run() -> None:
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
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
