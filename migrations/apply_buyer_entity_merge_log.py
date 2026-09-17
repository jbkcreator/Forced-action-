"""
Create buyer_entity_merge_log table.

Append-only audit log for manual buyer entity merges and reversals.
A merge collapses two buyer_entities rows by reassigning buyer_entity_links
from the absorbed entity to the surviving one, then deleting the absorbed row.
The absorbed row's full state is snapshotted into absorbed_snapshot so an
unmerge can restore it. absorbed_id carries no FK since the row is deleted.
restored_id is populated by unmerge_entity() with the new PK of the restored entity.
"""
import logging
from sqlalchemy import text, create_engine
from config.settings import get_settings

logger = logging.getLogger(__name__)


def apply(engine=None):
    if engine is None:
        engine = create_engine(get_settings().database_url)

    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS buyer_entity_merge_log (
                id              bigserial PRIMARY KEY,
                surviving_id    int NOT NULL REFERENCES buyer_entities(id),
                absorbed_id     int NOT NULL,
                absorbed_snapshot jsonb NOT NULL,
                links_moved     int NOT NULL DEFAULT 0,
                merged_by       text NOT NULL,
                merge_reason    text,
                merged_at       timestamptz NOT NULL DEFAULT now(),
                reversed_at     timestamptz,
                reversed_by     text,
                restored_id     int
            )
        """))

        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_merge_log_surviving
                ON buyer_entity_merge_log (surviving_id)
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_merge_log_absorbed
                ON buyer_entity_merge_log (absorbed_id)
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_merge_log_active
                ON buyer_entity_merge_log (id)
                WHERE reversed_at IS NULL
        """))

        logger.info("buyer_entity_merge_log table and indexes applied")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    apply()
    print("Done.")
