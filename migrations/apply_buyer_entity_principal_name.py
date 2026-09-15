"""
Add principal_name to buyer_entities.

An LLC-only cluster (no Individual/Trust owner in it) resolved via Sunbiz
LLC-piercing previously kept an LLC's name as canonical_name even when the
controlling person's name was already known from the piercing edge -- e.g.
nine LLCs sharing one managing member resolved to an entity literally named
after one of the LLCs, not the person who owns all twelve properties. This
column stores that person's name separately from canonical_name (which is
still an LLC's name when there's no Individual/Trust candidate AND no
pierced principal) so the resolver can now prefer the principal's name for
canonical_name() while still recording, distinctly, that the entity is
structurally an LLC (entity_type stays 'LLC' -- principal_name answers WHO
holds it, entity_type answers HOW it's held).
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
            ALTER TABLE buyer_entities
                ADD COLUMN IF NOT EXISTS principal_name text
        """))
        logger.info("buyer_entities.principal_name column applied")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    apply()
    print("Done.")
