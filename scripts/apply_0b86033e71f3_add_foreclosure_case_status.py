"""Auto-converted from alembic migration `0b86033e71f3_add_foreclosure_case_status` (revision 0b86033e71f3).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_0b86033e71f3_add_foreclosure_case_status.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE building_permits ALTER COLUMN is_enforcement_permit DROP DEFAULT;

DROP INDEX idx_building_permits_is_enforcement;

CREATE INDEX ix_building_permits_is_enforcement_permit ON building_permits (is_enforcement_permit);

ALTER TABLE foreclosures ADD COLUMN case_status VARCHAR(100);

CREATE INDEX idx_foreclosure_case_status ON foreclosures (case_status);

ALTER TABLE lead_pack_purchases ALTER COLUMN county_id DROP DEFAULT;

ALTER TABLE lead_pack_purchases ALTER COLUMN status DROP DEFAULT;

ALTER TABLE lead_pack_purchases ALTER COLUMN purchased_at DROP DEFAULT;

DROP INDEX idx_lead_pack_subscriber;

ALTER TABLE lead_pack_purchases DROP CONSTRAINT uq_lead_pack_payment_intent;

CREATE UNIQUE INDEX ix_lead_pack_purchases_stripe_payment_intent_id ON lead_pack_purchases (stripe_payment_intent_id);

CREATE INDEX ix_lead_pack_purchases_subscriber_id ON lead_pack_purchases (subscriber_id);

COMMENT ON COLUMN legal_and_liens.match_confidence IS NULL;

COMMENT ON COLUMN legal_and_liens.match_method IS NULL;

DROP INDEX idx_owner_name_trgm;

ALTER TABLE platform_daily_stats ALTER COLUMN county_id DROP DEFAULT;

ALTER TABLE platform_daily_stats ALTER COLUMN signals_scraped DROP DEFAULT;

ALTER TABLE platform_daily_stats ALTER COLUMN signals_matched DROP DEFAULT;

ALTER TABLE platform_daily_stats ALTER COLUMN signals_skipped DROP DEFAULT;

ALTER TABLE platform_daily_stats ALTER COLUMN properties_scored DROP DEFAULT;

ALTER TABLE platform_daily_stats ALTER COLUMN properties_with_signals DROP DEFAULT;

ALTER TABLE platform_daily_stats ALTER COLUMN score_runs_total DROP DEFAULT;

ALTER TABLE platform_daily_stats ALTER COLUMN leads_new DROP DEFAULT;

ALTER TABLE platform_daily_stats ALTER COLUMN leads_updated DROP DEFAULT;

ALTER TABLE platform_daily_stats ALTER COLUMN leads_unchanged DROP DEFAULT;

ALTER TABLE platform_daily_stats ALTER COLUMN leads_qualified DROP DEFAULT;

ALTER TABLE platform_daily_stats ALTER COLUMN leads_upgraded DROP DEFAULT;

ALTER TABLE platform_daily_stats ALTER COLUMN tier_ultra_platinum DROP DEFAULT;

ALTER TABLE platform_daily_stats ALTER COLUMN tier_platinum DROP DEFAULT;

ALTER TABLE platform_daily_stats ALTER COLUMN tier_gold DROP DEFAULT;

ALTER TABLE platform_daily_stats ALTER COLUMN tier_silver DROP DEFAULT;

ALTER TABLE platform_daily_stats ALTER COLUMN tier_bronze DROP DEFAULT;

ALTER TABLE platform_daily_stats ALTER COLUMN created_at DROP DEFAULT;

ALTER TABLE platform_daily_stats ALTER COLUMN updated_at DROP DEFAULT;

DROP INDEX idx_platform_stats_county_id;

CREATE INDEX ix_platform_daily_stats_county_id ON platform_daily_stats (county_id);

CREATE INDEX ix_platform_daily_stats_run_date ON platform_daily_stats (run_date);

DROP INDEX idx_property_address_trgm;

DROP INDEX idx_property_legal_desc_trgm;

ALTER TABLE scraper_run_stats ALTER COLUMN county_id DROP DEFAULT;

ALTER TABLE scraper_run_stats ALTER COLUMN total_scraped DROP DEFAULT;

ALTER TABLE scraper_run_stats ALTER COLUMN matched DROP DEFAULT;

ALTER TABLE scraper_run_stats ALTER COLUMN unmatched DROP DEFAULT;

ALTER TABLE scraper_run_stats ALTER COLUMN skipped DROP DEFAULT;

ALTER TABLE scraper_run_stats ALTER COLUMN scored DROP DEFAULT;

ALTER TABLE scraper_run_stats ALTER COLUMN run_success DROP DEFAULT;

ALTER TABLE scraper_run_stats ALTER COLUMN created_at DROP DEFAULT;

DROP INDEX idx_run_stats_county_id;

DROP INDEX idx_run_stats_run_date;

DROP INDEX idx_run_stats_source_type;

CREATE INDEX ix_scraper_run_stats_county_id ON scraper_run_stats (county_id);

CREATE INDEX ix_scraper_run_stats_run_date ON scraper_run_stats (run_date);

CREATE INDEX ix_scraper_run_stats_source_type ON scraper_run_stats (source_type);

ALTER TABLE sent_leads ALTER COLUMN sent_at DROP DEFAULT;

ALTER TABLE unmatched_records ALTER COLUMN county_id DROP DEFAULT;

ALTER TABLE unmatched_records ALTER COLUMN raw_data TYPE JSONB;

ALTER TABLE unmatched_records ALTER COLUMN match_status DROP DEFAULT;
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied 0b86033e71f3_add_foreclosure_case_status")


if __name__ == "__main__":
    main()
