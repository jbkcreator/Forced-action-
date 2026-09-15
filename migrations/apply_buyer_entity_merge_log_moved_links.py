"""
Add moved_link_ids to buyer_entity_merge_log.

unmerge_entity() previously identified links to restore via
`linked_at <= merged_at`, which also matches the surviving entity's own
pre-existing links (they too predate the merge) -- an unmerge could hand
the survivor's original links to the restored entity, corrupting both
sides. moved_link_ids records the EXACT buyer_entity_links.id values
reassigned at merge time, so unmerge restores precisely those and nothing
else. A merge logged before this column existed (none in production as of
this migration -- verified via `SELECT count(*) FROM buyer_entity_merge_log`
returning 0) has moved_link_ids IS NULL; unmerge_entity() refuses to guess
via the old timestamp heuristic and raises instead.
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
            ALTER TABLE buyer_entity_merge_log
                ADD COLUMN IF NOT EXISTS moved_link_ids jsonb
        """))
        logger.info("buyer_entity_merge_log.moved_link_ids column applied")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    apply()
    print("Done.")
