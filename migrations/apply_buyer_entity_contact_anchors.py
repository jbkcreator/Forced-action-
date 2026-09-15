"""
Add primary_email/primary_phone to buyer_entities.

The resolver's incremental mode (run_incremental) re-hydrates existing
buyer_entities rows as CandidateRecords via load_existing_entity_candidates()
so a new owners/deeds row can be matched against something that already
exists. Without a contact column on buyer_entities, a new record could never
contact-match an EXISTING entity in nightly mode -- only two records seen in
the same run could ever contact-corroborate. These columns are the modal
(most common) normalized email/phone across a cluster's member records,
denormalized the same way canonical_name/primary_mailing_address already are.
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
                ADD COLUMN IF NOT EXISTS primary_email text,
                ADD COLUMN IF NOT EXISTS primary_phone varchar(20)
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_buyer_entities_primary_email
                ON buyer_entities (primary_email)
                WHERE primary_email IS NOT NULL
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_buyer_entities_primary_phone
                ON buyer_entities (primary_phone)
                WHERE primary_phone IS NOT NULL
        """))
        logger.info("buyer_entities.primary_email/primary_phone columns and indexes applied")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    apply()
    print("Done.")
