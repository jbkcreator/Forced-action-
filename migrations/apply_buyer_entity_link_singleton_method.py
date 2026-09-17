"""
Rename the 'manual' match_method fallback to 'singleton_no_edge' where it
was never actually a human decision.

attach_or_create_entities() defaults an unresolved-record link to
("manual", ..., 100) whenever a record's key isn't in the evidence_index --
i.e. a single-record cluster with no corroborating edge at all, not a human
approving anything. Under a client requirement for an audit trail
explaining every link, 'manual' at this volume (747,801 of 988,903 links as
of this migration -- verified via
`SELECT count(*) FROM buyer_entity_links WHERE match_method='manual'`) reads
as "a human approved this" when none did.

'manual' stays in the CHECK constraint's allowed set -- it still means
exactly what it says for any future row a human genuinely sets by hand.
This migration only relabels the PAST default-fallback rows and adds
'singleton_no_edge' as a new allowed value the resolver code now writes for
that case going forward (see attach_or_create_entities in
buyer_entity_resolution.py).
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
            ALTER TABLE buyer_entity_links
                DROP CONSTRAINT IF EXISTS check_buyer_entity_link_match_method
        """))
        conn.execute(text("""
            ALTER TABLE buyer_entity_links
                ADD CONSTRAINT check_buyer_entity_link_match_method
                CHECK (match_method IN (
                    'sunbiz_llc_piercing', 'exact_name_address', 'fuzzy_name',
                    'llm_adjudicated', 'manual', 'exact_name_only',
                    'auction_name_only_unverified', 'singleton_no_edge'
                ))
        """))

        result = conn.execute(text("""
            UPDATE buyer_entity_links
               SET match_method = 'singleton_no_edge',
                   match_explanation = COALESCE(
                       match_explanation, 'single-record cluster; no corroborating edge'
                   )
             WHERE match_method = 'manual'
        """))
        logger.info(
            "buyer_entity_links.match_method CHECK constraint updated; "
            "%d 'manual' rows relabeled to 'singleton_no_edge'",
            result.rowcount,
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    apply()
    print("Done.")
