"""
Create buyer_entity_match_exception table.

The resolver correctly refuses to auto-merge an ambiguous pair or a cluster
touching 2+ existing buyer_entities anchors -- but previously that refusal
was logged (logger.warning) and then forgotten. The client's spec: "Identity
resolution is uncertain. Records stay separate and a possible-match flag
routes to EXCEPTIONS. Never auto-merged below a confidence threshold." This
table is that durable, auditable record -- the missing input to
merge_entities() (src/services/buyer_entity_merge.py), which previously had
no caller and no way to be told what to merge.

Upserted on (kind, left_ref, right_ref) so the nightly sweep re-seeing the
same ambiguous pair every run bumps last_seen_at instead of creating a new
row per run.
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
            CREATE TABLE IF NOT EXISTS buyer_entity_match_exception (
                id             bigserial PRIMARY KEY,
                kind           text NOT NULL
                    CHECK (kind IN ('ambiguous_pair', 'multi_anchor_conflict', 'llm_different')),
                left_ref       text NOT NULL,
                right_ref      text NOT NULL,
                entity_ids     jsonb,
                name_score     int,
                address_score  int,
                explanation    text NOT NULL,
                status         text NOT NULL DEFAULT 'open'
                    CHECK (status IN ('open', 'merged', 'rejected', 'stale')),
                resolved_by    text,
                resolved_at    timestamptz,
                merge_log_id   bigint REFERENCES buyer_entity_merge_log(id),
                first_seen_at  timestamptz NOT NULL DEFAULT now(),
                last_seen_at   timestamptz NOT NULL DEFAULT now()
            )
        """))
        conn.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_match_exception_pair
                ON buyer_entity_match_exception (kind, left_ref, right_ref)
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_match_exception_open
                ON buyer_entity_match_exception (first_seen_at)
                WHERE status = 'open'
        """))
        logger.info("buyer_entity_match_exception table and indexes applied")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    apply()
    print("Done.")
