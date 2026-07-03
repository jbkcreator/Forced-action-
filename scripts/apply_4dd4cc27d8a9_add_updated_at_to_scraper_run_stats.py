"""Auto-converted from alembic migration `4dd4cc27d8a9_add_updated_at_to_scraper_run_stats` (revision 4dd4cc27d8a9).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_4dd4cc27d8a9_add_updated_at_to_scraper_run_stats.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
DROP TABLE distress_scores_backup_today_v2;

DROP TABLE sync_status_backup_today_v2;

DROP INDEX checkpoint_blobs_thread_id_idx;

DROP TABLE checkpoint_blobs;

DROP INDEX checkpoint_writes_thread_id_idx;

DROP TABLE checkpoint_writes;

DROP INDEX checkpoints_thread_id_idx;

DROP TABLE checkpoints;

DROP TABLE checkpoint_migrations;

ALTER TABLE agent_decisions ALTER COLUMN started_at DROP DEFAULT;

ALTER TABLE agent_decisions ALTER COLUMN tokens_used DROP DEFAULT;

ALTER TABLE agent_decisions ALTER COLUMN cost_usd DROP DEFAULT;

ALTER TABLE bundle_purchases ALTER COLUMN status DROP DEFAULT;

ALTER TABLE bundle_purchases ALTER COLUMN county_id DROP DEFAULT;

ALTER TABLE bundle_purchases ALTER COLUMN credits_awarded DROP DEFAULT;

ALTER TABLE bundle_purchases ALTER COLUMN purchased_at SET NOT NULL;

DROP INDEX idx_bundle_purchase_intent;

CREATE UNIQUE INDEX ix_bundle_purchases_stripe_payment_intent_id ON bundle_purchases (stripe_payment_intent_id);

CREATE INDEX ix_bundle_purchases_subscriber_id ON bundle_purchases (subscriber_id);

ALTER TABLE counties ALTER COLUMN parcel_id_format DROP DEFAULT;

ALTER TABLE counties ALTER COLUMN city_filer_keywords DROP DEFAULT;

ALTER TABLE counties ALTER COLUMN code_lien_type_map DROP DEFAULT;

ALTER TABLE counties ALTER COLUMN is_active DROP DEFAULT;

ALTER TABLE counties DROP COLUMN address_city_tokens;

ALTER TABLE county_column_mappings ALTER COLUMN is_approved DROP DEFAULT;

ALTER TABLE county_column_mappings ALTER COLUMN mapped_by DROP DEFAULT;

ALTER TABLE county_column_mappings DROP CONSTRAINT county_column_mappings_source_id_fkey;

ALTER TABLE county_column_mappings ADD FOREIGN KEY(source_id) REFERENCES county_sources (id);

ALTER TABLE county_launch_audit ALTER COLUMN id TYPE INTEGER;

ALTER TABLE county_launch_audit ALTER COLUMN created_at DROP DEFAULT;

ALTER TABLE county_sources ALTER COLUMN date_range_available DROP DEFAULT;

ALTER TABLE county_sources ALTER COLUMN frequency DROP DEFAULT;

ALTER TABLE county_sources ALTER COLUMN is_active DROP DEFAULT;

ALTER TABLE county_sources ALTER COLUMN special_flags DROP DEFAULT;

ALTER TABLE county_sources DROP CONSTRAINT county_sources_county_id_fkey;

ALTER TABLE county_sources ADD FOREIGN KEY(county_id) REFERENCES counties (county_id);

ALTER TABLE dbpr_contacts ALTER COLUMN state DROP DEFAULT;

ALTER TABLE dbpr_contacts ALTER COLUMN data_source DROP DEFAULT;

ALTER TABLE dbpr_contacts ALTER COLUMN enrichment_status DROP DEFAULT;

ALTER TABLE dbpr_contacts ALTER COLUMN enrichment_attempted_at TYPE TIMESTAMP WITHOUT TIME ZONE;

ALTER TABLE dbpr_contacts ALTER COLUMN email_status DROP DEFAULT;

ALTER TABLE dbpr_contacts ALTER COLUMN email_sent_at TYPE TIMESTAMP WITHOUT TIME ZONE;

ALTER TABLE dbpr_contacts ALTER COLUMN signed_up_at TYPE TIMESTAMP WITHOUT TIME ZONE;

ALTER TABLE dbpr_contacts ALTER COLUMN last_synced_at TYPE TIMESTAMP WITHOUT TIME ZONE;

ALTER TABLE dbpr_contacts ALTER COLUMN created_at TYPE TIMESTAMP WITHOUT TIME ZONE;

ALTER TABLE dbpr_contacts ALTER COLUMN created_at DROP DEFAULT;

ALTER TABLE dbpr_contacts ALTER COLUMN updated_at TYPE TIMESTAMP WITHOUT TIME ZONE;

ALTER TABLE dbpr_contacts ALTER COLUMN updated_at DROP DEFAULT;

ALTER TABLE dbpr_contacts DROP CONSTRAINT dbpr_contacts_license_number_key;

DROP INDEX ix_dbpr_county_id;

DROP INDEX ix_dbpr_county_vertical;

DROP INDEX ix_dbpr_email_status;

DROP INDEX ix_dbpr_enrichment_status;

DROP INDEX ix_dbpr_last_synced;

DROP INDEX ix_dbpr_license_number;

DROP INDEX ix_dbpr_subscriber_id;

DROP INDEX ix_dbpr_vertical;

DROP INDEX ix_dbpr_zip_code;

CREATE INDEX idx_dbpr_county_vertical ON dbpr_contacts (county_id, vertical);

CREATE INDEX idx_dbpr_email_status ON dbpr_contacts (email_status);

CREATE INDEX idx_dbpr_enrichment_status ON dbpr_contacts (enrichment_status);

CREATE INDEX idx_dbpr_last_synced ON dbpr_contacts (last_synced_at);

CREATE INDEX ix_dbpr_contacts_county_id ON dbpr_contacts (county_id);

CREATE UNIQUE INDEX ix_dbpr_contacts_license_number ON dbpr_contacts (license_number);

CREATE INDEX ix_dbpr_contacts_subscriber_id ON dbpr_contacts (subscriber_id);

CREATE INDEX ix_dbpr_contacts_vertical ON dbpr_contacts (vertical);

CREATE INDEX ix_dbpr_contacts_zip_code ON dbpr_contacts (zip_code);

ALTER TABLE enrichment_usage_logs ALTER COLUMN cost_cents DROP DEFAULT;

ALTER TABLE enrichment_usage_logs ALTER COLUMN success DROP DEFAULT;

ALTER TABLE enrichment_usage_logs ALTER COLUMN created_at DROP DEFAULT;

DROP INDEX ix_enrichment_created_at;

DROP INDEX ix_enrichment_property_id;

DROP INDEX ix_enrichment_subscriber_id;

CREATE INDEX ix_enrichment_usage_logs_created_at ON enrichment_usage_logs (created_at);

CREATE INDEX ix_enrichment_usage_logs_property_id ON enrichment_usage_logs (property_id);

CREATE INDEX ix_enrichment_usage_logs_subscriber_id ON enrichment_usage_logs (subscriber_id);

ALTER TABLE expansion_candidates ALTER COLUMN priority DROP DEFAULT;

ALTER TABLE expansion_candidates ALTER COLUMN status DROP DEFAULT;

ALTER TABLE expansion_candidates ALTER COLUMN created_at DROP DEFAULT;

ALTER TABLE human_close_escalations ALTER COLUMN routed_at DROP DEFAULT;

ALTER TABLE human_close_escalations ALTER COLUMN post_attempts DROP DEFAULT;

ALTER TABLE human_close_escalations ALTER COLUMN target_tier_price_cents DROP DEFAULT;

DROP INDEX idx_hce_retry;

ALTER TABLE manual_action_log ALTER COLUMN created_at DROP DEFAULT;

ALTER TABLE nws_alerts ALTER COLUMN county_id DROP DEFAULT;

ALTER TABLE nws_alerts ALTER COLUMN storm_pack_triggered DROP DEFAULT;

ALTER TABLE nws_alerts ALTER COLUMN cora_urgency_sent DROP DEFAULT;

ALTER TABLE nws_alerts ALTER COLUMN subscriber_count DROP DEFAULT;

DROP INDEX ix_nws_alerts_affected_zips_gin;

DROP INDEX ix_nws_alerts_event_processed;

ALTER TABLE nws_alerts DROP CONSTRAINT nws_alerts_alert_id_key;

CREATE INDEX idx_nws_alerts_affected_zips ON nws_alerts USING gin (affected_zips);

CREATE INDEX idx_nws_alerts_event_processed ON nws_alerts (event, processed_at);

ALTER TABLE partner_subscriptions ALTER COLUMN max_zips DROP DEFAULT;

ALTER TABLE partner_subscriptions ALTER COLUMN activated_at DROP DEFAULT;

DROP INDEX idx_partner_sub;

ALTER TABLE phone_deliverability_snapshots ALTER COLUMN tier_filter DROP DEFAULT;

ALTER TABLE phone_deliverability_snapshots ALTER COLUMN lookups_cached DROP DEFAULT;

ALTER TABLE phone_deliverability_snapshots ALTER COLUMN lookups_attempted DROP DEFAULT;

ALTER TABLE phone_deliverability_snapshots ALTER COLUMN lookups_succeeded DROP DEFAULT;

ALTER TABLE phone_deliverability_snapshots ALTER COLUMN mobile_count DROP DEFAULT;

ALTER TABLE phone_deliverability_snapshots ALTER COLUMN voip_count DROP DEFAULT;

ALTER TABLE phone_deliverability_snapshots ALTER COLUMN landline_count DROP DEFAULT;

ALTER TABLE phone_deliverability_snapshots ALTER COLUMN unknown_count DROP DEFAULT;

ALTER TABLE phone_deliverability_snapshots ALTER COLUMN no_phone_count DROP DEFAULT;

ALTER TABLE phone_deliverability_snapshots ALTER COLUMN vendor DROP DEFAULT;

ALTER TABLE phone_deliverability_snapshots ALTER COLUMN cost_cents DROP DEFAULT;

ALTER TABLE playwright_code_history ALTER COLUMN is_approved DROP DEFAULT;

ALTER TABLE premium_purchases ALTER COLUMN status DROP DEFAULT;

ALTER TABLE premium_purchases ALTER COLUMN purchased_at DROP DEFAULT;

DROP INDEX idx_premium_stripe_charge_id;

ALTER TABLE premium_purchases DROP CONSTRAINT uq_premium_purchase_pi;

DROP INDEX ix_premium_purchases_stripe_payment_intent_id;

CREATE UNIQUE INDEX ix_premium_purchases_stripe_payment_intent_id ON premium_purchases (stripe_payment_intent_id);

CREATE INDEX ix_premium_purchases_stripe_charge_id ON premium_purchases (stripe_charge_id);

ALTER TABLE referral_forward_copy ALTER COLUMN generated_at DROP DEFAULT;

ALTER TABLE referral_milestone_awards ALTER COLUMN awarded_at DROP DEFAULT;

DROP INDEX idx_referral_milestone_referrer;

CREATE INDEX ix_referral_milestone_awards_referrer_subscriber_id ON referral_milestone_awards (referrer_subscriber_id);

ALTER TABLE referral_teams ALTER COLUMN status DROP DEFAULT;

ALTER TABLE referral_teams ALTER COLUMN unlocked_at DROP DEFAULT;

ALTER TABLE sandbox_outbox ALTER COLUMN id TYPE INTEGER;

ALTER TABLE sandbox_outbox ALTER COLUMN compliance_allowed DROP DEFAULT;

ALTER TABLE sandbox_outbox ALTER COLUMN would_have_delivered DROP DEFAULT;

ALTER TABLE sandbox_outbox ALTER COLUMN sandbox_flag DROP DEFAULT;

ALTER TABLE sandbox_outbox ALTER COLUMN created_at DROP DEFAULT;

ALTER TABLE scraper_alert_log ALTER COLUMN county_id DROP DEFAULT;

ALTER TABLE scraper_run_stats ADD COLUMN updated_at TIMESTAMP WITHOUT TIME ZONE;

DROP INDEX idx_sent_leads_pi_id;

CREATE INDEX ix_sent_leads_stripe_payment_intent_id ON sent_leads (stripe_payment_intent_id);

DROP INDEX idx_sms_opt_in_phone;

ALTER TABLE sms_opt_ins DROP CONSTRAINT sms_opt_ins_phone_key;

CREATE UNIQUE INDEX ix_sms_opt_ins_phone ON sms_opt_ins (phone);

CREATE INDEX ix_sms_opt_ins_subscriber_id ON sms_opt_ins (subscriber_id);

ALTER TABLE sms_opt_outs ALTER COLUMN source DROP DEFAULT;

ALTER TABLE sms_send_logs ALTER COLUMN vendor DROP DEFAULT;

DROP INDEX idx_sub_lock_candidate;

DROP INDEX idx_sub_paused;

DROP INDEX idx_subscriber_signup_source;

DROP INDEX idx_subscribers_phone;

ALTER TABLE subscribers DROP CONSTRAINT uq_subscribers_phone;

CREATE UNIQUE INDEX ix_subscribers_phone ON subscribers (phone);

CREATE INDEX ix_subscribers_signup_source ON subscribers (signup_source);

ALTER TABLE wallet_push_offers ALTER COLUMN offered_at DROP DEFAULT;

DROP INDEX idx_wallet_push_offers_decision;

DROP INDEX idx_wallet_push_offers_subscription;

CREATE INDEX ix_wallet_push_offers_decision_id ON wallet_push_offers (decision_id);

CREATE INDEX ix_wallet_push_offers_stripe_subscription_id ON wallet_push_offers (stripe_subscription_id);

CREATE INDEX ix_wallet_push_offers_subscriber_id ON wallet_push_offers (subscriber_id);

ALTER TABLE webhook_events ALTER COLUMN direction DROP DEFAULT;

ALTER TABLE webhook_events ALTER COLUMN status DROP DEFAULT;
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied 4dd4cc27d8a9_add_updated_at_to_scraper_run_stats")


if __name__ == "__main__":
    main()
