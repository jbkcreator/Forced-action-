"""Auto-converted from alembic migration `ebf68e5bc028_expand_incident_type_constraint` (revision ebf68e5bc028).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_ebf68e5bc028_expand_incident_type_constraint.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE incidents DROP CONSTRAINT check_incident_type;

ALTER TABLE incidents ADD CONSTRAINT check_incident_type CHECK (incident_type IN ('Arrest', 'Police Dispatch', 'Fire', 'roofing_permit', 'storm_damage', 'flood_damage', 'insurance_claim'));

DROP INDEX idx_building_permits_county_id;

DROP INDEX idx_building_permits_date_added;

CREATE INDEX ix_building_permits_county_id ON building_permits (county_id);

CREATE INDEX ix_building_permits_date_added ON building_permits (date_added);

DROP INDEX idx_code_violations_county_id;

DROP INDEX idx_code_violations_date_added;

CREATE INDEX ix_code_violations_county_id ON code_violations (county_id);

CREATE INDEX ix_code_violations_date_added ON code_violations (date_added);

DROP INDEX idx_deeds_county_id;

DROP INDEX idx_deeds_date_added;

CREATE INDEX ix_deeds_county_id ON deeds (county_id);

CREATE INDEX ix_deeds_date_added ON deeds (date_added);

DROP INDEX idx_distress_scores_county_id;

DROP INDEX ix_distress_scores_vertical_scores;

CREATE INDEX idx_score_county_id ON distress_scores (county_id);

CREATE INDEX ix_distress_scores_county_id ON distress_scores (county_id);

ALTER TABLE enriched_contacts ALTER COLUMN match_success DROP DEFAULT;

ALTER TABLE enriched_contacts ALTER COLUMN enriched_at DROP DEFAULT;

DROP INDEX idx_enriched_county_id;

DROP INDEX idx_enriched_ghl_contact;

DROP INDEX idx_enriched_property_id;

CREATE INDEX ix_enriched_contacts_county_id ON enriched_contacts (county_id);

CREATE INDEX ix_enriched_contacts_ghl_contact_id ON enriched_contacts (ghl_contact_id);

CREATE INDEX ix_enriched_contacts_property_id ON enriched_contacts (property_id);

DROP INDEX idx_financials_county_id;

CREATE INDEX idx_financial_county_id ON financials (county_id);

CREATE INDEX ix_financials_county_id ON financials (county_id);

DROP INDEX idx_foreclosures_county_id;

DROP INDEX idx_foreclosures_date_added;

CREATE INDEX ix_foreclosures_county_id ON foreclosures (county_id);

CREATE INDEX ix_foreclosures_date_added ON foreclosures (date_added);

ALTER TABLE founding_subscriber_counts ALTER COLUMN count DROP DEFAULT;

ALTER TABLE founding_subscriber_counts ALTER COLUMN updated_at DROP DEFAULT;

DROP INDEX idx_incidents_county_id;

DROP INDEX idx_incidents_date_added;

CREATE INDEX ix_incidents_county_id ON incidents (county_id);

CREATE INDEX ix_incidents_date_added ON incidents (date_added);

DROP INDEX idx_legal_and_liens_county_id;

DROP INDEX idx_legal_and_liens_date_added;

CREATE INDEX ix_legal_and_liens_county_id ON legal_and_liens (county_id);

CREATE INDEX ix_legal_and_liens_date_added ON legal_and_liens (date_added);

DROP INDEX idx_legal_proceedings_county_id;

DROP INDEX idx_legal_proceedings_date_added;

CREATE INDEX ix_legal_proceedings_county_id ON legal_proceedings (county_id);

CREATE INDEX ix_legal_proceedings_date_added ON legal_proceedings (date_added);

DROP INDEX idx_owner_name_trgm;

DROP INDEX idx_owners_county_id;

CREATE INDEX idx_owner_county_id ON owners (county_id);

CREATE INDEX ix_owners_county_id ON owners (county_id);

DROP INDEX idx_properties_county_id;

DROP INDEX idx_property_address_trgm;

DROP INDEX idx_property_legal_desc_trgm;

CREATE INDEX idx_property_county_id ON properties (county_id);

CREATE INDEX ix_properties_county_id ON properties (county_id);

ALTER TABLE subscribers ALTER COLUMN founding_member DROP DEFAULT;

ALTER TABLE subscribers ALTER COLUMN status DROP DEFAULT;

ALTER TABLE subscribers ALTER COLUMN created_at DROP DEFAULT;

ALTER TABLE subscribers ALTER COLUMN updated_at DROP DEFAULT;

DROP INDEX idx_subscriber_email;

DROP INDEX idx_subscriber_event_feed_uuid;

DROP INDEX idx_subscriber_ghl_contact;

DROP INDEX idx_subscriber_stripe_customer;

DROP INDEX idx_subscriber_stripe_sub;

ALTER TABLE subscribers DROP CONSTRAINT subscribers_event_feed_uuid_key;

ALTER TABLE subscribers DROP CONSTRAINT subscribers_stripe_customer_id_key;

ALTER TABLE subscribers DROP CONSTRAINT subscribers_stripe_subscription_id_key;

CREATE INDEX ix_subscribers_email ON subscribers (email);

CREATE UNIQUE INDEX ix_subscribers_event_feed_uuid ON subscribers (event_feed_uuid);

CREATE INDEX ix_subscribers_ghl_contact_id ON subscribers (ghl_contact_id);

CREATE UNIQUE INDEX ix_subscribers_stripe_customer_id ON subscribers (stripe_customer_id);

CREATE UNIQUE INDEX ix_subscribers_stripe_subscription_id ON subscribers (stripe_subscription_id);

DROP INDEX idx_tax_delinquencies_county_id;

DROP INDEX idx_tax_delinquencies_date_added;

CREATE INDEX ix_tax_delinquencies_county_id ON tax_delinquencies (county_id);

CREATE INDEX ix_tax_delinquencies_date_added ON tax_delinquencies (date_added);

ALTER TABLE zip_territories ALTER COLUMN status DROP DEFAULT;

ALTER TABLE zip_territories ALTER COLUMN waitlist_emails DROP DEFAULT;

ALTER TABLE zip_territories ALTER COLUMN updated_at DROP DEFAULT;

DROP INDEX idx_zip_territory_subscriber_id;

CREATE INDEX ix_zip_territories_subscriber_id ON zip_territories (subscriber_id);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied ebf68e5bc028_expand_incident_type_constraint")


if __name__ == "__main__":
    main()
