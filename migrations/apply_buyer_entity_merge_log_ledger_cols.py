"""
Add moved_ledger_event_ids / moved_monitor_log_ids / moved_closer_call_ids
to buyer_entity_merge_log.

Fixes two data-loss/availability bugs: merge_entities() deleted the absorbed
buyer_entities row without moving its borrower_ledger_events /
borrower_monitor_log / closer_calls rows first. The first two FKs are ON
DELETE CASCADE, so the absorbed entity's whole history was destroyed instead
of transferred to the survivor. closer_calls.buyer_entity_id has no ondelete
clause at all (Postgres default NO ACTION), so an unmoved row there made the
DELETE raise a raw FK violation instead -- blocking the merge entirely for
any absorbed entity with call history. These columns let unmerge_entity()
restore ownership by exact ID, the same way moved_link_ids already restores
buyer_entity_links.
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
                ADD COLUMN IF NOT EXISTS moved_ledger_event_ids jsonb,
                ADD COLUMN IF NOT EXISTS moved_monitor_log_ids  jsonb,
                ADD COLUMN IF NOT EXISTS moved_closer_call_ids  jsonb
        """))
        logger.info(
            "buyer_entity_merge_log.moved_ledger_event_ids / "
            "moved_monitor_log_ids / moved_closer_call_ids applied",
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    apply()
    print("Done.")
