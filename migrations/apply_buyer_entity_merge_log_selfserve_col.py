"""
Add moved_selfserve_session_ids to buyer_entity_merge_log.

merge_entities() deleted the absorbed buyer_entities row without moving its
selfserve_sessions rows first. selfserve_sessions.buyer_entity_id has no
ondelete clause (Postgres default NO ACTION), so an unmoved row there made
the DELETE raise a raw FK violation instead -- blocking the merge entirely
for any absorbed entity with self-serve session history. This column lets
unmerge_entity() restore ownership by exact ID, the same way
moved_closer_call_ids already restores closer_calls.
"""
import logging
from sqlalchemy import create_engine, text
from config.settings import get_settings

logger = logging.getLogger(__name__)


def apply(engine=None):
    if engine is None:
        engine = create_engine(get_settings().database_url)

    with engine.begin() as conn:
        conn.execute(text("""
            ALTER TABLE buyer_entity_merge_log
                ADD COLUMN IF NOT EXISTS moved_selfserve_session_ids jsonb
        """))
        logger.info("buyer_entity_merge_log.moved_selfserve_session_ids applied")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    apply()
    print("Done.")
