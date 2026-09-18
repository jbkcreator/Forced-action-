"""Application configuration powered by Pydantic settings."""

from functools import lru_cache
from datetime import date

from typing import Optional

from pydantic import AliasChoices, AnyUrl, Field, SecretStr, PostgresDsn, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
	"""Central place for environment-driven configuration."""

	model_config = SettingsConfigDict(
		env_file=".env",
		env_file_encoding="utf-8",
		case_sensitive=False,
		extra="ignore",
	)

	# API Keys
	anthropic_api_key: SecretStr = Field(..., env="ANTHROPIC_API_KEY")
	firecrawl_api_key: SecretStr = Field(..., env="FIRECRAWL_API_KEY")
	court_listener_api_key: SecretStr = Field(..., env="COURT_LISTENER_API_KEY")
	# FRED (Federal Reserve Economic Data) — free key at https://fred.stlouisfed.org/docs/api/api_key.html
	fred_api_key: Optional[SecretStr] = Field(default=None, env="FRED_API_KEY")
	# Census Bureau ACS API — free key at https://api.census.gov/data/key_signup.html
	census_api_key: Optional[SecretStr] = Field(default=None, env="CENSUS_API_KEY")

	# Property-matching LLM tiebreak. When False, borderline name matches
	# (rapidfuzz 80-94) are NOT sent to Claude; matching falls back to pure
	# score-based tiering (auto_match / review_min in config/matching.py).
	llm_match_enabled: bool = Field(default=False, env="LLM_MATCH_ENABLED")

	# Telnyx (phone deliverability sampler — carrier / line-type lookup)
	telnyx_api_key: Optional[SecretStr] = Field(default=None, env="TELNYX_API_KEY")
	telnyx_lookup_type: str = Field(default="carrier", env="TELNYX_LOOKUP_TYPE")
	phone_sample_size: int = Field(default=200, env="PHONE_SAMPLE_SIZE")
	phone_sample_mobile_alert_pct: float = Field(
		default=70.0, env="PHONE_SAMPLE_MOBILE_ALERT_PCT"
	)

	debug: bool = Field(default=True, env="DEBUG")

	@field_validator("debug", mode="before")
	@classmethod
	def _coerce_debug(cls, value):
		if isinstance(value, str):
			normalized = value.strip().lower()
			if normalized in {"release", "prod", "production"}:
				return False
			if normalized in {"dev", "development"}:
				return True
		return value

	# Set to true in local/staging environments to expose the /dev admin route.
	# Keep false (or unset) in production — the /api/admin/dev/ping endpoint
	# returns 403 when this is false, which blocks the DevPage from rendering.
	dev_tools_enabled: bool = Field(default=False, env="DEV_TOOLS_ENABLED")


	# 2Captcha — reCAPTCHA solving service (optional, used by Pinellas evictions scraper)
	twocaptcha_api_key: Optional[SecretStr] = Field(default=None, env="TWOCAPTCHA_API_KEY")

	# Oxylabs proxy (optional — used by foreclosure + tax delinquency scrapers)
	oxylabs_username: Optional[str] = Field(default=None, env="OXYLABS_USERNAME")
	oxylabs_password: Optional[SecretStr] = Field(default=None, env="OXYLABS_PASSWORD")
	oxylabs_rotate: bool = Field(default=False, env="OXYLABS_ROTATE")

	# GoHighLevel CRM integration (optional — feature disabled if not set)
	ghl_api_key: Optional[SecretStr] = Field(default=None, env="GHL_API_KEY")
	ghl_location_id: Optional[str] = Field(default=None, env="GHL_LOCATION_ID")
	ghl_pipeline_id: Optional[str] = Field(default=None, env="GHL_PIPELINE_ID")
	# Stage IDs within the GHL pipeline (map to urgency levels)
	ghl_stage_immediate: Optional[str] = Field(default=None, env="GHL_STAGE_IMMEDIATE")
	ghl_stage_high: Optional[str] = Field(default=None, env="GHL_STAGE_HIGH")
	ghl_stage_medium: Optional[str] = Field(default=None, env="GHL_STAGE_MEDIUM")
	# M1 subscriber pipeline stages
	ghl_stage_paid_subscriber: Optional[str] = Field(default=None, env="GHL_STAGE_PAID_SUBSCRIBER")   # stage 5
	ghl_stage_churned: Optional[str] = Field(default=None, env="GHL_STAGE_CHURNED")                   # stage 7
	# Set to false to disable all GHL pushes (e.g. bulk rescores)
	ghl_push_enabled: bool = Field(default=True, env="GHL_PUSH_ENABLED")
	# Subscriber-context custom field IDs (create in GHL → Settings → Custom Fields, then add IDs here)
	ghl_cf_fa_tier: Optional[str] = Field(default=None, env="GHL_CF_FA_TIER")
	ghl_cf_fa_zip: Optional[str] = Field(default=None, env="GHL_CF_FA_ZIP")
	ghl_cf_fa_founding: Optional[str] = Field(default=None, env="GHL_CF_FA_FOUNDING")
	ghl_cf_fa_dashboard_url: Optional[str] = Field(default=None, env="GHL_CF_FA_DASHBOARD_URL")
	# UTM attribution custom fields (Part 2 — GA4/UTM tracking spec)
	ghl_cf_utm_source: Optional[str] = Field(default=None, env="GHL_CF_UTM_SOURCE")
	ghl_cf_utm_medium: Optional[str] = Field(default=None, env="GHL_CF_UTM_MEDIUM")
	ghl_cf_utm_campaign: Optional[str] = Field(default=None, env="GHL_CF_UTM_CAMPAIGN")
	ghl_cf_utm_content: Optional[str] = Field(default=None, env="GHL_CF_UTM_CONTENT")
	ghl_cf_utm_term: Optional[str] = Field(default=None, env="GHL_CF_UTM_TERM")
	ghl_cf_utm_landing_path: Optional[str] = Field(default=None, env="GHL_CF_UTM_LANDING_PATH")
	ghl_cf_utm_referrer: Optional[str] = Field(default=None, env="GHL_CF_UTM_REFERRER")

	# GA4 Measurement Protocol (Part 3 — server-side purchase event from Stripe webhook)
	# API secret generated in GA4 → Admin → Data Streams → Measurement Protocol API secrets
	ga4_measurement_id: str = Field(default="G-LJJ4W7082K", env="GA4_MEASUREMENT_ID")
	ga4_api_secret: Optional[SecretStr] = Field(default=None, env="GA4_API_SECRET")

	# Instantly.ai email campaign integration (optional — feature disabled if not set)
	instantly_api_key: Optional[SecretStr] = Field(default=None, env="INSTANTLY_API_KEY")
	instantly_base_url: str = Field(default="https://api.instantly.ai", env="INSTANTLY_BASE_URL")
	instantly_enabled: bool = Field(default=True, env="INSTANTLY_ENABLED")

	# Instantly webhook receiver (WP-T2-1 go-live review, 2026-09) — Instantly's
	# webhook registration UI lets a custom header be attached to every delivery;
	# set this to whatever value is configured there so /webhooks/instantly can
	# reject deliveries that don't carry it. Unset = the endpoint fails closed
	# (rejects everything) rather than accepting unauthenticated bounce claims.
	instantly_webhook_secret: Optional[SecretStr] = Field(default=None, env="INSTANTLY_WEBHOOK_SECRET")

	# Non-buyer nurture — shared Instantly campaign. Per the 2026-07-22 domain
	# decision this shares the same warmed mailbox as the DBPR cold campaign
	# (leads@forcedactionleads.com), not a separate dedicated domain — see
	# scripts/create_non_buyer_nurture_campaign.py's DOMAIN DECISION note.
	non_buyer_nurture_campaign_id: Optional[str] = Field(default=None, env="NON_BUYER_NURTURE_CAMPAIGN_ID")
	# Max leads enrolled per daily sweep. NOT netted against the DBPR cold
	# campaign's own daily_limit on the same mailbox — set both together so
	# their sum stays under the mailbox's overall send ceiling.
	non_buyer_nurture_daily_cap: int = Field(default=25, env="NON_BUYER_NURTURE_DAILY_CAP")

	# Abandoned-checkout recovery (Task 7). Off by default — the sweep captures
	# and ages rows but sends nothing until this is enabled after review.
	checkout_recovery_enabled: bool = Field(default=False, env="CHECKOUT_RECOVERY_ENABLED")
	# E3 — only prompt for testimonial/referral asks on deal wins recorded after
	# the feature's go-live date. Closed ticket explicitly deferred historical
	# backfill; default to the implementation ship date.
	deal_win_testimonial_go_live_at: date = Field(
		default=date(2026, 7, 28),
		env="DEAL_WIN_TESTIMONIAL_GO_LIVE_AT",
	)
	# Lead-pack recovery targets EXISTING paying subscribers who abandon a $99
	# add-on — a different (dunning) motion from prospect cart recovery. Off by
	# default so enabling checkout_recovery_enabled does NOT start emailing
	# paying customers. Flip only if we deliberately want to dun add-on abandons.
	checkout_recovery_lead_pack_enabled: bool = Field(default=False, env="CHECKOUT_RECOVERY_LEAD_PACK_ENABLED")

	# Stripe — set STRIPE_TEST_MODE=true to use test credentials/prices instead of live
	stripe_test_mode: bool = Field(default=False, env="STRIPE_TEST_MODE")

	# Live credentials
	stripe_secret_key: Optional[SecretStr] = Field(default=None, env="STRIPE_SECRET_KEY")
	stripe_webhook_secret: Optional[SecretStr] = Field(default=None, env="STRIPE_WEBHOOK_SECRET")
	stripe_publishable_key: Optional[str] = Field(default=None, env="STRIPE_PUBLISHABLE_KEY")

	# Test credentials
	stripe_test_secret_key: Optional[SecretStr] = Field(default=None, env="STRIPE_TEST_SECRET_KEY")
	stripe_test_webhook_secret: Optional[SecretStr] = Field(default=None, env="STRIPE_TEST_WEBHOOK_SECRET")
	stripe_test_publishable_key: Optional[str] = Field(default=None, env="STRIPE_TEST_PUBLISHABLE_KEY")

	# Live price IDs
	stripe_price_starter_founding: Optional[str] = Field(default=None, env="STRIPE_PRICE_STARTER_FOUNDING")
	stripe_price_starter_regular: Optional[str] = Field(default=None, env="STRIPE_PRICE_STARTER_REGULAR")
	stripe_price_pro_founding: Optional[str] = Field(default=None, env="STRIPE_PRICE_PRO_FOUNDING")
	stripe_price_pro_regular: Optional[str] = Field(default=None, env="STRIPE_PRICE_PRO_REGULAR")
	stripe_price_starter_annual: Optional[str] = Field(default=None, env="STRIPE_PRICE_STARTER_ANNUAL")
	stripe_price_pro_annual: Optional[str] = Field(default=None, env="STRIPE_PRICE_PRO_ANNUAL")
	stripe_price_desk_setup: Optional[str] = Field(default=None, env="STRIPE_PRICE_DESK_SETUP")
	stripe_price_desk_monthly: Optional[str] = Field(default=None, env="STRIPE_PRICE_DESK_MONTHLY")
	stripe_price_founder_monthly: Optional[str] = Field(default=None, env="STRIPE_PRICE_FOUNDER_MONTHLY")
	stripe_price_founder_annual: Optional[str] = Field(default=None, env="STRIPE_PRICE_FOUNDER_ANNUAL")
	stripe_price_lead_pack: Optional[str] = Field(default=None, env="STRIPE_PRICE_LEAD_PACK")
	# Task 7 / ADR 0032 — premium insurance-distress segment pack. Price set by Josh in Stripe.
	stripe_price_insurance_distress_pack: Optional[str] = Field(default=None, env="STRIPE_PRICE_INSURANCE_DISTRESS_PACK")
	stripe_price_hot_lead_unlock: Optional[str] = Field(default=None, env="STRIPE_PRICE_HOT_LEAD_UNLOCK")
	# Kill switch for the $150/$99 hot lead unlock checkout. Metadata now
	# propagates to the PaymentIntent (payment_intent_data in
	# create_hot_lead_unlock_link) and fulfillment routes through
	# _on_lead_unlock_payment, so the product is enabled by default.
	hot_lead_unlock_enabled: bool = Field(default=True, env="HOT_LEAD_UNLOCK_ENABLED")
	# 2B: Wallet tiers
	stripe_price_wallet_starter: Optional[str] = Field(default=None, env="STRIPE_PRICE_WALLET_STARTER")
	stripe_price_wallet_growth: Optional[str] = Field(default=None, env="STRIPE_PRICE_WALLET_GROWTH")
	stripe_price_wallet_power: Optional[str] = Field(default=None, env="STRIPE_PRICE_WALLET_POWER")
	# 2B: New subscription tiers
	stripe_price_data_only: Optional[str] = Field(default=None, env="STRIPE_PRICE_DATA_ONLY")
	stripe_price_autopilot_lite: Optional[str] = Field(default=None, env="STRIPE_PRICE_AUTOPILOT_LITE")
	stripe_price_autopilot_pro: Optional[str] = Field(default=None, env="STRIPE_PRICE_AUTOPILOT_PRO")
	stripe_price_annual_lock: Optional[str] = Field(default=None, env="STRIPE_PRICE_ANNUAL_LOCK")
	stripe_price_auto_mode: Optional[str] = Field(default=None, env="STRIPE_PRICE_AUTO_MODE")
	hold_deposit_price_id: str = Field(default="", env="HOLD_DEPOSIT_PRICE_ID")
	stripe_test_price_hold_deposit: Optional[str] = Field(default=None, env="STRIPE_TEST_PRICE_HOLD_DEPOSIT")
	stripe_price_partner: Optional[str] = Field(default=None, env="STRIPE_PRICE_PARTNER")
	# 2B: Bundles
	stripe_price_bundle_weekend: Optional[str] = Field(default=None, env="STRIPE_PRICE_BUNDLE_WEEKEND")
	stripe_price_bundle_storm: Optional[str] = Field(default=None, env="STRIPE_PRICE_BUNDLE_STORM")
	stripe_price_bundle_zip_booster: Optional[str] = Field(default=None, env="STRIPE_PRICE_BUNDLE_ZIP_BOOSTER")
	stripe_price_bundle_monthly_reload: Optional[str] = Field(default=None, env="STRIPE_PRICE_BUNDLE_MONTHLY_RELOAD")
	# Stage 5: Premium credits (cash retail prices — credits path uses CREDIT_COSTS)
	stripe_price_premium_report: Optional[str] = Field(default=None, env="STRIPE_PRICE_PREMIUM_REPORT")
	stripe_price_premium_brief: Optional[str] = Field(default=None, env="STRIPE_PRICE_PREMIUM_BRIEF")
	stripe_price_premium_transfer: Optional[str] = Field(default=None, env="STRIPE_PRICE_PREMIUM_TRANSFER")
	stripe_price_premium_byol: Optional[str] = Field(default=None, env="STRIPE_PRICE_PREMIUM_BYOL")
	# Stage 12: White-label + Bankruptcy
	stripe_price_wl_standard: Optional[str] = Field(default=None, env="STRIPE_PRICE_WL_STANDARD")
	stripe_price_wl_premium: Optional[str] = Field(default=None, env="STRIPE_PRICE_WL_PREMIUM")
	stripe_price_bankruptcy_alerts: Optional[str] = Field(default=None, env="STRIPE_PRICE_BANKRUPTCY_ALERTS")
	# Supplier Intelligence Foundation — 3 tiers ($497/$997/$1,497 /mo)
	stripe_price_supplier_intel_foundation: Optional[str] = Field(default=None, env="STRIPE_PRICE_SUPPLIER_INTEL_FOUNDATION")
	stripe_price_supplier_intel_standard:   Optional[str] = Field(default=None, env="STRIPE_PRICE_SUPPLIER_INTEL_STANDARD")
	stripe_price_supplier_intel_premium:    Optional[str] = Field(default=None, env="STRIPE_PRICE_SUPPLIER_INTEL_PREMIUM")
	stripe_test_price_supplier_intel_foundation: Optional[str] = Field(default=None, env="STRIPE_TEST_PRICE_SUPPLIER_INTEL_FOUNDATION")
	stripe_test_price_supplier_intel_standard:   Optional[str] = Field(default=None, env="STRIPE_TEST_PRICE_SUPPLIER_INTEL_STANDARD")
	stripe_test_price_supplier_intel_premium:    Optional[str] = Field(default=None, env="STRIPE_TEST_PRICE_SUPPLIER_INTEL_PREMIUM")

	# ICP-scoped pricing — expansion ICPs each have their own Stripe price
	stripe_price_icp_rei_investor:       Optional[str] = Field(default=None, env="STRIPE_PRICE_ICP_REI_INVESTOR")
	stripe_price_icp_insurance_adjuster: Optional[str] = Field(default=None, env="STRIPE_PRICE_ICP_INSURANCE_ADJUSTER")
	stripe_price_icp_hard_money_lender:  Optional[str] = Field(default=None, env="STRIPE_PRICE_ICP_HARD_MONEY_LENDER")
	stripe_price_icp_property_manager:   Optional[str] = Field(default=None, env="STRIPE_PRICE_ICP_PROPERTY_MANAGER")
	stripe_price_icp_bankruptcy_attorney:Optional[str] = Field(default=None, env="STRIPE_PRICE_ICP_BANKRUPTCY_ATTORNEY")
	stripe_price_icp_title_company:      Optional[str] = Field(default=None, env="STRIPE_PRICE_ICP_TITLE_COMPANY")
	stripe_test_price_icp_rei_investor:       Optional[str] = Field(default=None, env="STRIPE_TEST_PRICE_ICP_REI_INVESTOR")
	stripe_test_price_icp_insurance_adjuster: Optional[str] = Field(default=None, env="STRIPE_TEST_PRICE_ICP_INSURANCE_ADJUSTER")
	stripe_test_price_icp_hard_money_lender:  Optional[str] = Field(default=None, env="STRIPE_TEST_PRICE_ICP_HARD_MONEY_LENDER")
	stripe_test_price_icp_property_manager:   Optional[str] = Field(default=None, env="STRIPE_TEST_PRICE_ICP_PROPERTY_MANAGER")
	stripe_test_price_icp_bankruptcy_attorney:Optional[str] = Field(default=None, env="STRIPE_TEST_PRICE_ICP_BANKRUPTCY_ATTORNEY")
	stripe_test_price_icp_title_company:      Optional[str] = Field(default=None, env="STRIPE_TEST_PRICE_ICP_TITLE_COMPANY")

	# Referral Core Loop
	referral_free_month_coupon_id: Optional[str] = Field(default=None, env="REFERRAL_FREE_MONTH_COUPON_ID")

	# T-B12-07: Tier3 win-back zip_held offer — 50% off the return month.
	# Applied as a Stripe coupon at checkout session creation when a valid
	# winback_offers token accompanies the request (PR #172 review fix).
	winback_50_off_coupon_id: Optional[str] = Field(default=None, env="WINBACK_50_OFF_COUPON_ID")

	# Stage 12: Bankruptcy Filing Alert product ($297/mo). Shares the common
	# Stripe webhook endpoint + signing secret (active_stripe_webhook_secret) —
	# events are routed by product in stripe_webhooks.handle_webhook.
	stripe_price_bankruptcy_alerts: Optional[str] = Field(default=None, env="STRIPE_PRICE_BANKRUPTCY_ALERTS")
	stripe_test_price_bankruptcy_alerts: Optional[str] = Field(default=None, env="STRIPE_TEST_PRICE_BANKRUPTCY_ALERTS")
	# Delay (minutes) after a new signup before the bankruptcy-alert invite email
	# is sent. The signup path schedules it; the invite-sweep cron sends it.
	bankruptcy_invite_delay_minutes: int = Field(default=30, env="BANKRUPTCY_INVITE_DELAY_MINUTES")

	# Test price IDs
	stripe_test_price_starter_founding: Optional[str] = Field(default=None, env="STRIPE_TEST_PRICE_STARTER_FOUNDING")
	stripe_test_price_starter_regular: Optional[str] = Field(default=None, env="STRIPE_TEST_PRICE_STARTER_REGULAR")
	stripe_test_price_pro_founding: Optional[str] = Field(default=None, env="STRIPE_TEST_PRICE_PRO_FOUNDING")
	stripe_test_price_pro_regular: Optional[str] = Field(default=None, env="STRIPE_TEST_PRICE_PRO_REGULAR")
	stripe_test_price_starter_annual: Optional[str] = Field(default=None, env="STRIPE_TEST_PRICE_STARTER_ANNUAL")
	stripe_test_price_pro_annual: Optional[str] = Field(default=None, env="STRIPE_TEST_PRICE_PRO_ANNUAL")
	stripe_test_price_desk_setup: Optional[str] = Field(default=None, env="STRIPE_TEST_PRICE_DESK_SETUP")
	stripe_test_price_desk_monthly: Optional[str] = Field(default=None, env="STRIPE_TEST_PRICE_DESK_MONTHLY")
	# Not read by get_price_id_for_checkout (founder resolves via plans.stripe_price_id,
	# not this env var) — kept for mapping-table parity with every other tier only.
	stripe_test_price_founder_monthly: Optional[str] = Field(default=None, env="STRIPE_TEST_PRICE_FOUNDER_MONTHLY")
	stripe_test_price_founder_annual: Optional[str] = Field(default=None, env="STRIPE_TEST_PRICE_FOUNDER_ANNUAL")
	stripe_test_price_lead_pack: Optional[str] = Field(default=None, env="STRIPE_TEST_PRICE_LEAD_PACK")
	stripe_test_price_insurance_distress_pack: Optional[str] = Field(default=None, env="STRIPE_TEST_PRICE_INSURANCE_DISTRESS_PACK")
	stripe_test_price_hot_lead_unlock: Optional[str] = Field(default=None, env="STRIPE_TEST_PRICE_HOT_LEAD_UNLOCK")
	# 2B test prices
	stripe_test_price_wallet_starter: Optional[str] = Field(default=None, env="STRIPE_TEST_PRICE_WALLET_STARTER")
	stripe_test_price_wallet_growth: Optional[str] = Field(default=None, env="STRIPE_TEST_PRICE_WALLET_GROWTH")
	stripe_test_price_wallet_power: Optional[str] = Field(default=None, env="STRIPE_TEST_PRICE_WALLET_POWER")
	stripe_test_price_data_only: Optional[str] = Field(default=None, env="STRIPE_TEST_PRICE_DATA_ONLY")
	stripe_test_price_autopilot_lite: Optional[str] = Field(default=None, env="STRIPE_TEST_PRICE_AUTOPILOT_LITE")
	stripe_test_price_autopilot_pro: Optional[str] = Field(default=None, env="STRIPE_TEST_PRICE_AUTOPILOT_PRO")
	stripe_test_price_annual_lock: Optional[str] = Field(default=None, env="STRIPE_TEST_PRICE_ANNUAL_LOCK")
	stripe_test_price_auto_mode: Optional[str] = Field(default=None, env="STRIPE_TEST_PRICE_AUTO_MODE")
	stripe_test_price_partner: Optional[str] = Field(default=None, env="STRIPE_TEST_PRICE_PARTNER")
	stripe_test_price_bundle_weekend: Optional[str] = Field(default=None, env="STRIPE_TEST_PRICE_BUNDLE_WEEKEND")
	stripe_test_price_bundle_storm: Optional[str] = Field(default=None, env="STRIPE_TEST_PRICE_BUNDLE_STORM")
	stripe_test_price_bundle_zip_booster: Optional[str] = Field(default=None, env="STRIPE_TEST_PRICE_BUNDLE_ZIP_BOOSTER")
	stripe_test_price_bundle_monthly_reload: Optional[str] = Field(default=None, env="STRIPE_TEST_PRICE_BUNDLE_MONTHLY_RELOAD")
	# Stage 5: Premium credits — test prices
	stripe_test_price_premium_report: Optional[str] = Field(default=None, env="STRIPE_TEST_PRICE_PREMIUM_REPORT")
	stripe_test_price_premium_brief: Optional[str] = Field(default=None, env="STRIPE_TEST_PRICE_PREMIUM_BRIEF")
	stripe_test_price_premium_transfer: Optional[str] = Field(default=None, env="STRIPE_TEST_PRICE_PREMIUM_TRANSFER")
	stripe_test_price_premium_byol: Optional[str] = Field(default=None, env="STRIPE_TEST_PRICE_PREMIUM_BYOL")
	# Stage 12: White-label + Bankruptcy — test prices
	stripe_test_price_wl_standard: Optional[str] = Field(default=None, env="STRIPE_TEST_PRICE_WL_STANDARD")
	stripe_test_price_wl_premium: Optional[str] = Field(default=None, env="STRIPE_TEST_PRICE_WL_PREMIUM")
	stripe_test_price_bankruptcy_alerts: Optional[str] = Field(default=None, env="STRIPE_TEST_PRICE_BANKRUPTCY_ALERTS")

	clay_dbpr_webhook_url: Optional[str] = Field(default=None, env="CLAY_DBPR_WEBHOOK_URL")

	# DBPR contractor registry — comma-separated county_ids to filter to
	# (e.g. "hillsborough,pinellas,polk"). Empty (default) falls back to
	# hillsborough,pinellas. Unrecognized county_ids are skipped with a
	# warning at run time, not raised.
	dbpr_target_counties: str = Field(default="", env="DBPR_TARGET_COUNTIES")

	# Cora reply-mailbox ingestion (Gmail API, service account + domain-wide
	# delegation, gmail.readonly scope only). Both must be set for
	# src.agents.cora.ingestion.reply_mailbox_poller to activate — absent
	# either one, it silently no-ops every poll rather than raising.
	cora_gmail_service_account_key_path: Optional[str] = Field(default=None, env="CORA_GMAIL_SERVICE_ACCOUNT_KEY_PATH")
	cora_reply_mailbox_address: Optional[str] = Field(default=None, env="CORA_REPLY_MAILBOX_ADDRESS")
	# "whale" = Hunter→buyer_entity flow (default). "dbpr_storm" = Aug blitz targeting
	# storm/restoration contractors from dbpr_contacts. Flip in env, no code deploy.
	cora_target_mode: str = Field(default="whale", env="CORA_TARGET_MODE")


	# ── Mode-aware helpers ────────────────────────────────────────────────────
	# Use these everywhere instead of accessing live/test fields directly.

	@property
	def active_stripe_secret_key(self) -> Optional[SecretStr]:
		return self.stripe_test_secret_key if self.stripe_test_mode else self.stripe_secret_key

	@property
	def active_stripe_webhook_secret(self) -> Optional[SecretStr]:
		return self.stripe_test_webhook_secret if self.stripe_test_mode else self.stripe_webhook_secret

	@property
	def active_stripe_publishable_key(self) -> Optional[str]:
		return self.stripe_test_publishable_key if self.stripe_test_mode else self.stripe_publishable_key

	@property
	def active_hold_deposit_price_id(self) -> Optional[str]:
		"""Mode-aware $97 hold-deposit price: test price in test mode, else live."""
		if self.stripe_test_mode:
			return self.stripe_test_price_hold_deposit
		return self.hold_deposit_price_id or None

	def active_stripe_price(self, name: str) -> Optional[str]:
		"""Return the mode-aware price ID for a given price name (e.g. 'lead_pack', 'starter_founding')."""
		if self.stripe_test_mode:
			return getattr(self, f"stripe_test_price_{name}", None)
		return getattr(self, f"stripe_price_{name}", None)

	# Founding subscriber spot limit (default 10, changeable without redeploy)
	founding_spot_limit: int = Field(default=10, env="FOUNDING_SPOT_LIMIT")

	# Grace period after subscription deletion — 7 days lets payment_failure_day5 trigger fire.
	# Set GRACE_PERIOD_HOURS=0.017 (≈1 min) for rapid local testing.
	grace_period_hours: float = Field(default=168.0, env="GRACE_PERIOD_HOURS")

	# Alert deduplication — suppress repeat alerts for the same failure within this window
	alert_cooldown_hours: float = Field(default=4.0, env="ALERT_COOLDOWN_HOURS")

	# Demo booking link — sent via SMS when prospect requests a demo on the call
	demo_calendly_url: Optional[str] = Field(default=None, env="DEMO_CALENDLY_URL")

	# Application base URL — used for Stripe return/success URLs
	app_base_url: str = Field(
		default="http://localhost:8000",
		env="APP_BASE_URL",
		description="Public base URL of this app (e.g. https://app.forcedactionleads.com)",
	)

	# CAN-SPAM footer requirement — placeholder until client supplies the real one
	company_postal_address: str = Field(
		default="Forced Action, Tampa, FL",
		env="COMPANY_POSTAL_ADDRESS",
	)

	# Contact enrichment (M1)
	batch_skip_tracing_api_key: Optional[SecretStr] = Field(default=None, env="BATCH_SKIP_TRACING_API_KEY")
	whitepages_api_key: Optional[SecretStr] = Field(default=None, env="WHITEPAGES_API_KEY")  # deprecated — no-op, kept to avoid breaking existing .env files
	tracerfy_api_key: Optional[SecretStr] = Field(default=None, env="TRACERFY_API_KEY")
	tracerfy_cost_cents: int = Field(default=2, env="TRACERFY_COST_CENTS")
	dnc_recheck_days: int = Field(default=30, env="DNC_RECHECK_DAYS")
	# Multi-heir probate enrichment (default OFF).
	# When True, skip-trace enumerates every named heir from
	# legal_proceedings.meta_data->'heirs' for probate-derived leads instead of
	# only the first heir (secondary_party). Capped at MAX_HEIRS_PER_PROPERTY
	# to bound BatchData spend (200/day quota). Enable only after the audit at
	# `scripts/_probate_multi_heir_audit.py` confirms >20% multi-heir prevalence.
	multi_heir_enrichment_enabled: bool = Field(default=False, env="MULTI_HEIR_ENRICHMENT_ENABLED")
	# Hard-clamped to [1, 10] regardless of env value — bounds BatchData spend so a
	# fat-fingered or pathological large-estate value can't drain the 200/day quota.
	max_heirs_per_property: int = Field(default=5, env="MAX_HEIRS_PER_PROPERTY")

	@field_validator("max_heirs_per_property")
	@classmethod
	def _clamp_max_heirs(cls, v: int) -> int:
		return max(1, min(int(v), 10))
	pdl_api_key: Optional[SecretStr] = Field(default=None, env="PDL_API_KEY")
	idi_api_key: Optional[SecretStr] = Field(default=None, env="IDI_API_KEY")

	# Enrichment cascade costs (ADR 0016 / ADR 0017).
	# batchdata_cost_cents: contracted rate — was wrong at 2¢, corrected to 7¢.
	# tracerfy_advanced_cost_cents: Address-Only pass (trace_type='advanced').
	# Set idi_cost_cents once contracted rate is known (currently key-gated).
	batchdata_cost_cents: int = Field(default=7, env="BATCHDATA_COST_CENTS")
	tracerfy_advanced_cost_cents: int = Field(default=4, env="TRACERFY_ADVANCED_COST_CENTS")

	# Event-driven cascade master switch (ADR 0016).
	# False = nightly batch only; True = consumer fires on every gold_lead_scored event.
	enrichment_cascade_enabled: bool = Field(default=False, env="ENRICHMENT_CASCADE_ENABLED")

	# EnrichmentBatcher flush controls (ADR 0016).
	enrichment_batch_flush_size: int = Field(default=200, env="ENRICHMENT_BATCH_FLUSH_SIZE")
	enrichment_batch_flush_seconds: int = Field(default=120, env="ENRICHMENT_BATCH_FLUSH_SECONDS")

	# A4 — degraded-provider detection. A provider is degraded when its hit rate
	# over `window_hours` falls below its per-provider floor (with a min-sample
	# guard). Floors are placeholders — replace with real healthy rates pulled
	# from enrichment_usage_logs before enabling in production.
	enrichment_anomaly_enabled: bool = Field(default=True, env="ENRICHMENT_ANOMALY_ENABLED")
	enrichment_window_hours: int = Field(default=48, env="ENRICHMENT_WINDOW_HOURS")
	enrichment_min_sample: int = Field(default=30, env="ENRICHMENT_MIN_SAMPLE")
	enrichment_realert_cooldown_hours: int = Field(default=24, env="ENRICHMENT_REALERT_COOLDOWN_HOURS")
	# Floors = ~40% of each provider's 30-day healthy hit rate (measured
	# 2026-06-26 from enrichment_usage_logs): tracerfy mean ~0.50, batchdata
	# mean ~0.035. NOTE: batchdata is a low-volume residual tail with a
	# near-zero rate — a single zero-hit day at/above min_sample WILL trip the
	# 0.02 floor (accepted trade-off; kept monitored by deliberate choice).
	enrichment_provider_floors: dict[str, float] = Field(
		default_factory=lambda: {"tracerfy": 0.20, "batchdata": 0.02}
	)
	# When a provider is flagged degraded, haircut the quality rating
	# (owners.contact_info_confidence_score — the column CDS reads) of that
	# provider's hits inside the window, so a degraded batch can't poison
	# scoring. Feature-flagged + reversible.
	enrichment_degraded_discount_enabled: bool = Field(default=True, env="ENRICHMENT_DEGRADED_DISCOUNT_ENABLED")
	enrichment_degraded_confidence_multiplier: float = Field(default=0.55, env="ENRICHMENT_DEGRADED_CONFIDENCE_MULTIPLIER")

	# Cross-source contact triangulation (ADR 0015).
	# triangulation_enabled — nightly sweep + inline hook compute corroboration
	# and write contact_info_confidence / contactability_detail.
	# cds_use_contactability — CDS reads the tiered contact bonus and the sweep
	# dispatches delta-rescores for changed labels. Keep off until the Stage E
	# shadow-diff has been reviewed (rollout stage 3).
	triangulation_enabled: bool = Field(default=False, env="TRIANGULATION_ENABLED")
	cds_use_contactability: bool = Field(default=False, env="CDS_USE_CONTACTABILITY")

	# Skip trace waterfall behaviour
	skip_trace_confidence_threshold: float = Field(default=0.70, env="SKIP_TRACE_CONFIDENCE_THRESHOLD")
	skip_trace_cost_ceiling_cents: int = Field(default=80, env="SKIP_TRACE_COST_CEILING_CENTS")
	# Hard per-run spend ceiling for any single skip-trace fallback invocation — a
	# backstop independent of the dedup ledger so a future bug cannot run up a large
	# bill. Worst-case projected (every submitted record assumed a hit). Default $10.
	# Vendor-agnostic so Tracerfy/BatchData/PDL fallbacks share it.
	skip_trace_max_run_cost_cents: int = Field(default=1000, env="SKIP_TRACE_MAX_RUN_COST_CENTS")

	# Task 6.2 — Algorithmic Variance Control Layer. Rolling paid-enrichment
	# spend / captured subscription revenue ratio; at or above this, paid
	# provider calls are blocked in favor of the free voter-registry cross
	# match (see src/services/budget_manager.py).
	enrichment_spend_ratio_threshold: float = Field(default=0.25, env="ENRICHMENT_SPEND_RATIO_THRESHOLD")
	enrichment_spend_ratio_window_days: int = Field(default=30, env="ENRICHMENT_SPEND_RATIO_WINDOW_DAYS")

	# SMTP (used by welcome email, payment receipts, grace period alerts)
	smtp_host: Optional[str] = Field(default=None, env="SMTP_HOST")
	smtp_port: int = Field(default=587, env="SMTP_PORT")
	smtp_user: Optional[str] = Field(default=None, env="SMTP_USER")
	smtp_pass: Optional[SecretStr] = Field(default=None, env="SMTP_PASS")
	mandrill_webhook_key: Optional[SecretStr] = Field(default=None, env="MANDRILL_WEBHOOK_KEY")
	email_from: Optional[str] = Field(default=None, env="EMAIL_FROM")  # falls back to smtp_user if not set
	alert_email: Optional[str] = Field(default=None, env="ALERT_EMAIL")  # ops alert recipient
	# QUALITY-v2.2 Q4 — dedicated inbox for the mail-probe canary (decision
	# F11). Falls back to ALERT_EMAIL if unset — but a dedicated address is
	# recommended: this probe sends one email every 5 minutes (288/day).
	canary_mail_to: Optional[str] = Field(default=None, env="CANARY_MAIL_TO")
	report_recipients: Optional[str] = Field(default=None, env="REPORT_RECIPIENTS")  # comma-separated emails for daily/weekly reports
	report_cc_recipients: Optional[str] = Field(default=None, env="REPORT_CC_RECIPIENTS")  # CC'd on every non-CC recipient send (visibility/confirmation)

	# Telnyx SMS — outbound + inbound webhook (replaces Twilio as of the
	# hard-cut migration; see docs/plan: mellow-strolling-fairy.md).
	# `telnyx_api_key` (defined earlier in this file) is the carrier-lookup key
	# for the phone-deliverability sampler. The SMS key is a SEPARATE Telnyx
	# API key so blast radius on a key rotation is contained per-product.
	telnyx_sms_api_key:           Optional[SecretStr] = Field(default=None, env="TELNYX_SMS_API_KEY")
	telnyx_public_key:            Optional[str]       = Field(default=None, env="TELNYX_PUBLIC_KEY")          # Ed25519 webhook verify key (base64)
	telnyx_messaging_profile_id:  Optional[str]       = Field(default=None, env="TELNYX_MESSAGING_PROFILE_ID")
	telnyx_from_number:           Optional[str]       = Field(default=None, env="TELNYX_FROM_NUMBER")          # E.164 sender
	telnyx_voice_app_id:          Optional[str]       = Field(default=None, env="TELNYX_VOICE_APP_ID")          # Voice API Application UUID
	telnyx_sms_enabled:           bool                = Field(default=False, env="TELNYX_SMS_ENABLED")           # master kill-switch (dry-run when False)

	# Sandbox flags — when set, outbound/integration side effects are captured to
	# local tables instead of calling real services. Used for scenario tests.
	telnyx_sandbox: bool = Field(default=False, env="TELNYX_SANDBOX")
	redis_sandbox:  bool = Field(default=False, env="REDIS_SANDBOX")

	# Claude model routing (update model IDs here without touching code)
	claude_haiku_model: str = Field(default="claude-haiku-4-5-20251001", env="CLAUDE_HAIKU_MODEL")
	claude_sonnet_model: str = Field(default="claude-sonnet-4-6", env="CLAUDE_SONNET_MODEL")
	claude_opus_model: str = Field(default="claude-opus-4-7", env="CLAUDE_OPUS_MODEL")

	# Redis (placeholder — provisioned in 2B-2; leave blank locally)
	redis_url: Optional[str] = Field(default=None, env="REDIS_URL")  # e.g. redis://localhost:6379/0

	# Revenue Pulse — founder SMS recipient
	founder_phone: Optional[str] = Field(default=None, env="FOUNDER_PHONE")  # E.164 format

	# Alerting
	alert_sms_number: Optional[str] = Field(default=None, env="ALERT_SMS_NUMBER")  # SMS target for match-rate alerts
	alert_sms_carrier: Optional[str] = Field(default=None, env="ALERT_SMS_CARRIER")  # e.g. tmomail.net, vtext.com

	# UptimeRobot (optional — used by scripts/setup_uptimerobot.py to create monitors)
	uptimerobot_api_key: Optional[SecretStr] = Field(default=None, env="UPTIMEROBOT_API_KEY")

	# Human close routing (Phase A)
	slack_human_close_webhook: Optional[str] = Field(default=None, env="SLACK_HUMAN_CLOSE_WEBHOOK")

	# Accelerated Wallet Push (fa016) — master kill switch.
	# Detector, sweep, Lifecycle graph, and API endpoints all skip when False.
	# Auto-flipped to False if Day-35 take_rate < wallet_adoption.floor_pct (12%).
	accelerated_wallet_push_enabled: bool = Field(default=False, env="ACCELERATED_WALLET_PUSH_ENABLED")

	# Freemium blurred-feed funnel (T-B3-01) — master launch toggle ANDed in
	# front of every funnel leg: free signup, blurred teaser/proof moment,
	# monetization wall, flash scarcity, abandonment nudges. Default True:
	# these flows are already live in prod; per-flow Lifecycle kill-switches
	# (first_payment_rate) still apply when this is ON.
	freemium_funnel_enabled: bool = Field(default=True, env="FREEMIUM_FUNNEL_ENABLED")

	# TCPA quiet-hours enforcement (8am–9pm recipient local time).
	# Set False in dev/staging to send SMS at any hour during testing.
	sms_quiet_hours_enabled: bool = Field(default=True, env="SMS_QUIET_HOURS_ENABLED")

	# Human review of Lifecycle outbound SMS. OFF by default — Lifecycle's messages send
	# immediately. This is only the *baseline*; operators flip the switch at
	# runtime via the admin queue (Redis override, see services/lifecycle_review_switch).
	# When ON, marketing sends are held as pending_review for manual approve/cancel.
	# Falls back to the pre-rename CORA_HUMAN_REVIEW_ENABLED name for one
	# release so existing prod overrides don't silently reset to the default
	# on deploy — drop the fallback once prod .env is confirmed migrated.
	lifecycle_human_review_enabled: bool = Field(
		default=False,
		validation_alias=AliasChoices("LIFECYCLE_HUMAN_REVIEW_ENABLED", "CORA_HUMAN_REVIEW_ENABLED"),
	)

	# NWS Weather / Storm Pack (fa018)
	# nws_weather_enabled      — master kill switch for entire NWS subsystem
	# nws_revenue_polling_enabled — controls whether poller triggers storm pack / Lifecycle
	# storm_pack_enabled        — controls bundle offer dispatch specifically
	# nws_lifecycle_urgency_enabled  — controls Lifecycle urgency graph dispatch specifically
	nws_weather_enabled: bool = Field(default=True, env="NWS_WEATHER_ENABLED")
	nws_revenue_polling_enabled: bool = Field(default=True, env="NWS_REVENUE_POLLING_ENABLED")
	storm_pack_enabled: bool = Field(default=True, env="STORM_PACK_ENABLED")
	# Falls back to the pre-rename NWS_CORA_URGENCY_ENABLED name for one release
	# — same rationale as lifecycle_human_review_enabled above.
	nws_lifecycle_urgency_enabled: bool = Field(
		default=True,
		validation_alias=AliasChoices("NWS_LIFECYCLE_URGENCY_ENABLED", "NWS_CORA_URGENCY_ENABLED"),
	)
	# Referenced by nws_webhook.process_alert steps 9+11 but missing until 2026-07 —
	# the AttributeError killed signal tagging + rescore on every alert (incidents
	# from weather stayed at 0 since fa018).
	storm_signal_tagging_enabled: bool = Field(default=True, env="STORM_SIGNAL_TAGGING_ENABLED")
	storm_signal_rescore_enabled: bool = Field(default=True, env="STORM_SIGNAL_RESCORE_ENABLED")
	nws_poll_interval_seconds: int = Field(default=300, env="NWS_POLL_INTERVAL_SECONDS")
	nws_supported_states: list = Field(default=["FL"], env="NWS_SUPPORTED_STATES")
	nws_relevant_events: list = Field(default=[
		"Tornado Warning", "Tornado Watch",
		"Severe Thunderstorm Warning", "Severe Thunderstorm Watch",
		"Hurricane Warning", "Hurricane Watch",
		"Tropical Storm Warning", "Tropical Storm Watch",
		"Flash Flood Warning", "Flood Warning",
		"High Wind Warning", "Wind Advisory",
		"Storm Surge Warning", "Storm Surge Watch",
		"Special Weather Statement",
	], env="NWS_RELEVANT_EVENTS")

	# Landing token secret (fa017) — HS256 key for signed missed-call /
	# email-onboarding links. Token carries (subscriber_id, source, exp) and
	# is validated by POST /api/landing/resolve-token. Falls back to
	# admin_jwt_secret when unset so local-dev works without extra env config.
	landing_token_secret: Optional[SecretStr] = Field(default=None, env="LANDING_TOKEN_SECRET")

	# WP-7 self-serve pre-fill — Backflip handoff adapter selection.
	# handoff_only (default) | fake | url | api. See src/services/backflip_port.py.
	backflip_adapter: str = Field(default="handoff_only", env="BACKFLIP_ADAPTER")
	backflip_prequal_url: Optional[str] = Field(default=None, env="BACKFLIP_PREQUAL_URL")

	# Synthflow outbound voice drops (Phase C)
	synthflow_api_base: str = Field(default="https://api.synthflow.ai/v2", env="SYNTHFLOW_API_BASE")
	synthflow_api_key: Optional[SecretStr] = Field(default=None, env="SYNTHFLOW_API_KEY")
	synthflow_outbound_agent_roofing: Optional[str] = Field(default=None, env="SYNTHFLOW_OUTBOUND_AGENT_ROOFING")
	synthflow_outbound_agent_remediation: Optional[str] = Field(default=None, env="SYNTHFLOW_OUTBOUND_AGENT_REMEDIATION")
	synthflow_outbound_agent_revenue_recovery: Optional[str] = Field(default=None, env="SYNTHFLOW_OUTBOUND_AGENT_REVENUE_RECOVERY")

	# Synthflow inbound signup flow (fa057)
	synthflow_inbound_agent: Optional[str] = Field(default=None, env="SYNTHFLOW_INBOUND_AGENT")
	synthflow_webhook_secret: Optional[SecretStr] = Field(default=None, env="SYNTHFLOW_WEBHOOK_SECRET")
	synthflow_inbound_did: Optional[str] = Field(default=None, env="SYNTHFLOW_INBOUND_DID")

	# QA-only: comma-separated E.164 numbers (incl. non-US) that phone_utils.normalize
	# lets through for staging voice-drop tests. Unset in production → strict US-only
	# guard is unchanged. Never enable in prod for real traffic.
	qa_test_phone_allowlist: Optional[str] = Field(default=None, env="QA_TEST_PHONE_ALLOWLIST")

	# Admin upload layer
	admin_username: str = Field(default="admin", env="ADMIN_USERNAME")
	admin_password: Optional[SecretStr] = Field(default=None, env="ADMIN_PASSWORD")
	admin_jwt_secret: Optional[SecretStr] = Field(default=None, env="ADMIN_JWT_SECRET")

	# Demo deal-room generator — standalone /demo/deal-room page, gated by a
	# shared passcode (not the admin JWT). Unset => the demo endpoint returns 503.
	demo_passcode: Optional[SecretStr] = Field(default=None, env="DEMO_PASSCODE")

	# Broker portal auth — separate secret so broker and admin tokens are independently
	# rotatable and cannot be cross-accepted. Required when broker portal is in use.
	broker_jwt_secret: Optional[SecretStr] = Field(default=None, env="BROKER_JWT_SECRET")

	# DB Backup — S3
	backup_s3_bucket: Optional[str] = Field(default=None, env="BACKUP_S3_BUCKET")
	backup_s3_prefix: str = Field(default="db-backups", env="BACKUP_S3_PREFIX")
	backup_s3_region: str = Field(default="us-east-1", env="BACKUP_S3_REGION")
	backup_aws_access_key_id: Optional[str] = Field(default=None, env="BACKUP_AWS_ACCESS_KEY_ID")
	backup_aws_secret_access_key: Optional[SecretStr] = Field(default=None, env="BACKUP_AWS_SECRET_ACCESS_KEY")
	backup_retention_daily: int = Field(default=7, env="BACKUP_RETENTION_DAILY")
	backup_retention_weekly: int = Field(default=4, env="BACKUP_RETENTION_WEEKLY")

	# Database Configuration
	database_url: str = Field(
		default="postgresql://user:password@localhost:5432/distressed_properties",
		env="DATABASE_URL",
		description="PostgreSQL connection string"
	)
	db_echo: bool = Field(
		default=False,
		env="DB_ECHO",
		description="Echo SQL queries for debugging"
	)
	db_pool_size: int = Field(
		default=5,
		env="DB_POOL_SIZE",
		description="Database connection pool size"
	)
	db_max_overflow: int = Field(
		default=10,
		env="DB_MAX_OVERFLOW",
		description="Max overflow connections beyond pool_size"
	)

	# Vera (Agent Lane) — read-only audit role connection
	vera_database_url: str = Field(
		default="",
		env="VERA_DATABASE_URL",
		description="Read-only Postgres DSN for the Vera audit agent (vera_readonly role). "
		"Empty until the role is provisioned via migrations/apply_vera_readonly_role.py."
	)
	vera_db_password: Optional[SecretStr] = Field(
		default=None,
		env="VERA_DB_PASSWORD",
		description="Password to set on the vera_readonly role. Only read by "
		"migrations/apply_vera_readonly_role.py at provisioning time; not used at runtime."
	)

	# County Launch Automation
	county_launch_source_county: str = Field(default="hillsborough", env="COUNTY_LAUNCH_SOURCE_COUNTY")
	county_launch_approvers: list = Field(default=[], env="COUNTY_LAUNCH_APPROVERS")
	county_launch_slack_channel: str = Field(default="", env="COUNTY_LAUNCH_SLACK_CHANNEL")
	county_launch_cooldown_hours: int = Field(default=24, env="COUNTY_LAUNCH_COOLDOWN_HOURS")
	county_launch_reminder_days: int = Field(default=7, env="COUNTY_LAUNCH_REMINDER_DAYS")
	slack_bot_token: Optional[SecretStr] = Field(default=None, env="SLACK_BOT_TOKEN")
	slack_signing_secret: Optional[SecretStr] = Field(default=None, env="SLACK_SIGNING_SECRET")
	# Dedicated FA Max Slack app. It must not reuse the shared Slack app
	# credentials used by County Launch and other Relay ventures.
	fa_max_slack_bot_token: Optional[SecretStr] = Field(default=None, env="FA_MAX_SLACK_BOT_TOKEN")
	fa_max_slack_app_token: Optional[SecretStr] = Field(default=None, env="FA_MAX_SLACK_APP_TOKEN")
	vera_slack_channel: Optional[str] = Field(default=None, env="VERA_SLACK_CHANNEL")

	# Relay approval queue (RELAY-v2.2 sub-task R1). Non-FA-Max Relay ventures
	# reuse slack_bot_token / slack_signing_secret; FA Max uses its dedicated app.
	relay_slack_channel: str = Field(default="", env="RELAY_SLACK_CHANNEL")
	relay_approvers: list = Field(default=[], env="RELAY_APPROVERS")

	# THROUGH-v2.2 batch-approval layer. Reuses slack_bot_token/slack_signing_secret
	# above — no separate Slack app. Deliberately its own channel/approver list,
	# not relay_slack_channel/relay_approvers — a batch of N cold-outreach drafts
	# is a different review surface than Relay's per-item send approvals.
	cora_throughput_slack_channel: str = Field(default="", env="CORA_THROUGHPUT_SLACK_CHANNEL")
	cora_throughput_approvers: list = Field(default=[], env="CORA_THROUGHPUT_APPROVERS")
	cora_batch_expiry_hours: int = Field(default=72, env="CORA_BATCH_EXPIRY_HOURS", description="Hours before a pending Cora draft batch auto-expires. Override via env var.")

	# LEARN-v2.2 Layer 4 (Step 12) — lesson-hygiene sweep digest. Reuses
	# slack_bot_token above. Deliberately its own channel, not
	# county_launch_slack_channel — a lesson-hygiene digest is not a
	# county-launch approval and posting it there would surface in the wrong
	# review surface.
	learning_hygiene_slack_channel: str = Field(default="", env="LEARNING_HYGIENE_SLACK_CHANNEL")

	# WP-9 Dial List daily digest — the "MONEY" queue (FA MAX amendment 1).
	# Reuses slack_bot_token above; its own channel, not cora_throughput's
	# (cold-outreach draft approvals) nor relay's (per-item sends) — the dial
	# list is a read-only ranked calling aid, a distinct review surface.
	dial_list_slack_channel: str = Field(default="", env="DIAL_LIST_SLACK_CHANNEL")
	# App-level token (xapp-…, scope connections:write) for the dial-list action
	# listener's Socket Mode connection — only the interactive button/thread
	# handler needs it; the read-only digest does not. Unset → listener no-ops.
	dial_list_slack_app_token: Optional[SecretStr] = Field(
		default=None, env="DIAL_LIST_SLACK_APP_TOKEN"
	)
	# The single operator (Josh) allowed to act on dial-list cards; a tap from
	# anyone else is ignored. Unset → no gate (dev/local only).
	dial_list_approver_user_id: str = Field(
		default="", env="DIAL_LIST_APPROVER_USER_ID"
	)
	# A dial-list source (deeds, permits, foreclosures, tax_deed_auction,
	# probate) whose last successful scraper run is older than this many days is
	# flagged "stale" in the digest header (failure-behavior visibility).
	dial_list_source_sla_days: int = Field(
		default=2, env="DIAL_LIST_SOURCE_SLA_DAYS"
	)

	# Relay email channel (RELAY-v2.2 sub-task R2). relay_instantly_campaign_id
	# is set once after running `python -m src.services.relay --setup-email-channel`
	# (see src/services/relay/channels_email.py) — unset means the email
	# channel is not yet provisioned.
	relay_instantly_campaign_id: Optional[str] = Field(default=None, env="RELAY_INSTANTLY_CAMPAIGN_ID")
	relay_instantly_sender_email: str = Field(
		default="noreply@forcedactionleads.com", env="RELAY_INSTANTLY_SENDER_EMAIL"
	)

	# Relay execution guards (RELAY-v2.2 sub-task R3). Send window is a
	# DELIVERABILITY control, not a legal one -- CAN-SPAM places no time
	# restriction on commercial email. 11:00-18:00 ET is inside 08:00-18:00
	# local for every continental US timezone, so Relay needs no per-recipient
	# timezone data. Set start=0/end=24 to disable.
	relay_send_window_start: int = Field(default=11, env="RELAY_SEND_WINDOW_START")
	relay_send_window_end: int = Field(default=18, env="RELAY_SEND_WINDOW_END")
	relay_send_window_timezone: str = Field(default="America/New_York", env="RELAY_SEND_WINDOW_TIMEZONE")
	# Per-channel sends per calendar day (build spec §9.1: "Gmail 20/day").
	relay_daily_ceiling: int = Field(default=20, env="RELAY_DAILY_CEILING")

	# Lifecycle self-healing (fa034). Default OFF — must be opted in per environment.
	# When false, src/tasks/lifecycle_self_healing.py is a no-op (returns 0 without
	# touching DB/Redis). Rollout: ship code → enable in dev → soak in staging
	# → enable in prod after a quiet week.
	# Falls back to the pre-rename CORA_SELF_HEALING_ENABLED name for one
	# release — same rationale as lifecycle_human_review_enabled above.
	lifecycle_self_healing_enabled: bool = Field(
		default=False,
		validation_alias=AliasChoices("LIFECYCLE_SELF_HEALING_ENABLED", "CORA_SELF_HEALING_ENABLED"),
	)
	# Slack channel for Lifecycle incident posts. When unset, post_incident_slack()
	# falls back to email.send_alert (same path as heartbeat_monitor /
	# anomaly_pager). Slack-disabled is NEVER a silent failure.
	lifecycle_incident_slack_channel: Optional[str] = Field(default=None, env="LIFECYCLE_INCIDENT_SLACK_CHANNEL")

	# QUALITY-v2.2 Q3 — handoff-contract auto-reject notifications (decision D2:
	# "auto-reject triggers a Slack message to Josh"). Reuses slack_bot_token
	# above, same convention as relay_slack_channel/lifecycle_incident_slack_channel.
	quality_contracts_slack_channel: Optional[str] = Field(default=None, env="QUALITY_CONTRACTS_SLACK_CHANNEL")

	# Forced Action MAX — WP-6 Repeat & Maturity Engine. Alert channel for
	# borrower relationship touchpoints (loan maturity, next project, DSCR, expansion).
	relationships_slack_channel: Optional[str] = Field(default=None, env="RELATIONSHIPS_SLACK_CHANNEL")

	# Stage 10 — Prometheus metrics exposition (fa055).
	# When true, GET /metrics returns Prometheus text format with kill-switch
	# business metrics and variant slot performance.
	prometheus_enabled: bool = Field(default=False, env="PROMETHEUS_ENABLED")
	# Shared secret for Alertmanager → /webhooks/alerts/prometheus.
	# When unset the endpoint is open (acceptable for internal-only deploys).
	prometheus_alert_webhook_secret: Optional[str] = Field(
		default=None, env="PROMETHEUS_ALERT_WEBHOOK_SECRET"
	)

	# ── Stage 12 — White-label Tier (fa056) ─────────────────────────────────
	# Separate JWT secret from admin_jwt_secret so WL and admin auth are
	# independently rotatable. Falls back to admin_jwt_secret when unset (dev).
	wl_jwt_secret: Optional[SecretStr] = Field(default=None, env="WL_JWT_SECRET")
	# Stripe price IDs for $2,500/mo and $5,000/mo WL plans (live + test).
	# Read via active_stripe_price("wl_standard" / "wl_premium") so STRIPE_TEST_MODE
	# selects the right one automatically.
	stripe_price_wl_standard: Optional[str] = Field(default=None, env="STRIPE_PRICE_WL_STANDARD")
	stripe_price_wl_premium: Optional[str] = Field(default=None, env="STRIPE_PRICE_WL_PREMIUM")
	stripe_test_price_wl_standard: Optional[str] = Field(default=None, env="STRIPE_TEST_PRICE_WL_STANDARD")
	stripe_test_price_wl_premium: Optional[str] = Field(default=None, env="STRIPE_TEST_PRICE_WL_PREMIUM")
	# Public base URL of the React frontend — used to build links in WL emails
	# (verify-email, password reset, invites, login). Dev default points at the
	# Vite dev server; set WL_FRONTEND_BASE_URL=https://app.forcedactionleads.com in prod.
	wl_frontend_base_url: str = Field(default="http://localhost:5173", env="WL_FRONTEND_BASE_URL")
	# Separate webhook secret for the /webhooks/stripe/white-label endpoint
	wl_stripe_webhook_secret: Optional[SecretStr] = Field(default=None, env="WL_STRIPE_WEBHOOK_SECRET")
	# Trial duration; set to 0 to disable trials
	wl_trial_period_days: int = Field(default=14, env="WL_TRIAL_PERIOD_DAYS")
	# Default daily API request cap per WL client (overridable per client in DB)
	wl_api_requests_per_day: int = Field(default=10000, env="WL_API_REQUESTS_PER_DAY")
	# Directory for WL client logo uploads (relative to repo root)
	wl_logo_upload_dir: str = Field(default="reports/white_label_logos", env="WL_LOGO_UPLOAD_DIR")
	# Comma-separated extra CORS origins for white-label client frontends.
	# wl_frontend_base_url is always included; add client-hosted app domains here.
	wl_allowed_origins: str = Field(default="", env="WL_ALLOWED_ORIGINS")
	# Clay enrichment API key (for /api/wl/data/contractors)
	clay_api_key: Optional[SecretStr] = Field(default=None, env="CLAY_API_KEY")
	clay_api_base: str = Field(default="https://api.clay.com/v1", env="CLAY_API_BASE")
	# Days before cached Clay enrichment data is considered stale
	clay_enrichment_ttl_days: int = Field(default=7, env="CLAY_ENRICHMENT_TTL_DAYS")
	# Shared bearer secret Clay must send when calling our HTTP API enrichment
	# endpoints (e.g. POST /api/clay/resolve-linkedin-url). Header:
	#   Authorization: Bearer <CLAY_HTTP_API_SECRET>
	# When unset, those endpoints return 503 (refuse to run unauthenticated).
	clay_http_api_secret: Optional[SecretStr] = Field(default=None, env="CLAY_HTTP_API_SECRET")

	# ── 3M Deal-Room ROI config ─────────────────────────────────────────────
	# Cost of a single shared Angi lead — shown in the per-tier ROI comparison.
	# Editable without code deploy.
	angi_shared_lead_cost: int = Field(default=65, env="ANGI_SHARED_LEAD_COST")

	# ── Subscriber feed auth (fa061) ────────────────────────────────────────
	# JWT secret for subscriber feed login. Separate from admin/WL so it's
	# independently rotatable; falls back to admin_jwt_secret when unset (dev).
	subscriber_jwt_secret: Optional[SecretStr] = Field(default=None, env="SUBSCRIBER_JWT_SECRET")

	# ── Aircall (Closer Cockpit, S1b) ───────────────────────────────────────
	# Basic-auth pair for the Aircall REST API (api_id:api_token) and the
	# signing token used to verify inbound webhook events. All optional so the
	# app boots without Aircall configured (feature-gated).
	aircall_api_id: Optional[SecretStr] = Field(default=None, env="AIRCALL_API_ID")
	aircall_api_token: Optional[SecretStr] = Field(default=None, env="AIRCALL_API_TOKEN")
	aircall_webhook_token: Optional[SecretStr] = Field(default=None, env="AIRCALL_WEBHOOK_TOKEN")
	# ── Meta Conversions API (CAPI) — S2 ────────────────────────────────────
	# Server-side Purchase reporting for closed-loop Meta ad attribution.
	# Feature-gated and OFF by default — when disabled, or when pixel_id /
	# access_token is missing, the CAPI call is skipped safely and never
	# affects checkout, subscription activation, lead-pack fulfillment, or
	# Stripe webhook processing. test_event_code is only included in the Meta
	# payload when meta_capi_test_mode is True.
	meta_capi_enabled: bool = Field(default=False, env="META_CAPI_ENABLED")
	meta_capi_test_mode: bool = Field(default=False, env="META_CAPI_TEST_MODE")
	meta_pixel_id: Optional[str] = Field(default=None, env="META_PIXEL_ID")
	meta_access_token: Optional[SecretStr] = Field(default=None, env="META_ACCESS_TOKEN")
	meta_test_event_code: Optional[str] = Field(default=None, env="META_TEST_EVENT_CODE")
	meta_graph_api_version: str = Field(default="v17.0", env="META_GRAPH_API_VERSION")


	# ── Task 5.2 — Programmatic SEO Engine ──────────────────────────────────
	# Sitemap-first discovery only (ADR 0022); the optional Google Indexing API
	# integration was dropped — unsupported for content pages, was a dead stub.
	seo_eligibility_floor: int = Field(default=25, env="SEO_ELIGIBILITY_FLOOR")
	seo_output_dir: str = Field(default="dist/seo/florida", env="SEO_OUTPUT_DIR")
	seo_site_base_url: str = Field(default="https://forcedactionleads.com", env="SEO_SITE_BASE_URL")
	seo_retire_hysteresis_runs: int = Field(default=2, env="SEO_RETIRE_HYSTERESIS_RUNS")

	# ── FA Max WP-2 — Slack Operating Queues & Send Governance ───────────────
	# 10DLC registration must be confirmed before ANY FA Max SMS can be sent.
	# Defaults to False (fail-closed). Flip to True only after 10DLC campaign
	# registration is complete and confirmed with Telnyx/carrier.
	fa_max_10dlc_registered: bool = Field(default=False, env="FA_MAX_10DLC_REGISTERED")

	# Lane-specific Slack channels for FA Max operating queues.
	# Empty string = channel not configured; relay will no-op the post (same
	# as existing relay_slack_channel behaviour when unconfigured).
	fa_max_slack_channel_money: str = Field(default="", env="FA_MAX_SLACK_CHANNEL_MONEY")
	fa_max_slack_channel_exceptions: str = Field(default="", env="FA_MAX_SLACK_CHANNEL_EXCEPTIONS")
	fa_max_slack_channel_relationships: str = Field(default="", env="FA_MAX_SLACK_CHANNEL_RELATIONSHIPS")
	fa_max_backflip_feed_max_age_hours: int = Field(default=24, ge=1, env="FA_MAX_BACKFLIP_FEED_MAX_AGE_HOURS")

	# ── FA Max WP-T2-1 — Own-Lane Send Infrastructure ────────────────────────
	# "fake" (default) = every FA Max relay send goes through the in-memory
	# FakeInstantly/FakeTelnyx recorders in src/services/relay/fakes.py — no
	# network, no credentials, safe for every developer's tests and for local
	# dev. "live" = real Instantly/Telnyx calls. Flip only once the dedicated
	# outreach domain, DNS auth, and 10DLC registration are confirmed; this
	# flag does not itself replace the fa_max_10dlc_registered SMS gate below.
	fa_max_relay_send_mode: str = Field(default="fake", env="FA_MAX_RELAY_SEND_MODE")

	# Code-review finding (WP-T2-1, third round): flipping fa_max_relay_send_
	# mode to "live" alone must NOT auto-release the backlog of items
	# approved during the weeks-long warmup ramp while the lane was still in
	# fake mode -- their content or the underlying business decision behind
	# them may be stale by go-live. This flag is a SEPARATE, deliberate
	# confirmation (mirrors fa_max_10dlc_registered's manual, defaults-closed
	# pattern): relay.guards defers every FA Max item, regardless of channel,
	# until BOTH this and fa_max_relay_send_mode == "live" are true.
	#
	# What this flag does NOT do (code-review clarification, fourth round):
	# an earlier per-item Slack approval (relay_approval_queue.decided_by/
	# decided_at/decision_interaction_id -- WP-2's existing MONEY-lane
	# approve/reject flow) is NOT the same review this flag certifies. Josh
	# may have approved a message weeks before the lane was even ready to
	# send it. This flag only enforces that SOME go-live review happened; it
	# does not perform or record one. Before setting it true, the operator
	# enabling live sending must, as a manual go-live procedure (no UI or
	# code support exists for this -- deliberately, per WP-T2-1's scope):
	#   1. List every FA Max row still in 'approved' status (the backlog).
	#   2. Check each one's content and original approval date for staleness.
	#   3. If any approved item is stale, keep this flag false until those
	#      items are safely removed from the approved queue. The existing
	#      Slack Approve/Reject buttons only decide pending items; they cannot
	#      reject an item that is already approved. No approved-item cancellation
	#      workflow exists here. Once this flag is true, the next sweep can
	#      dispatch every remaining approved row, oldest first, up to the
	#      daily ceiling.
	#   4. Record who ran this review and when, outside this flag (e.g. in
	#      the go-live runbook/incident channel) -- this flag is a boolean,
	#      not an audit trail.
	# A real per-item release/reject mechanism was explicitly scoped OUT for
	# WP-T2-1 (2026-09 decision) in favor of this manual procedure plus the
	# single flag; do not read its existence as proof the review happened.
	fa_max_send_backlog_release_confirmed: bool = Field(
		default=False, env="FA_MAX_SEND_BACKLOG_RELEASE_CONFIRMED"
	)

	# ── FA Max WP-T2-2 — Bounded Agent Runtime & Autonomous Dispatch ─────────
	# Fail-closed gate on relay.queue.enqueue(auto_authorize=True). Mirrors
	# fa_max_10dlc_registered's manual, defaults-closed pattern: even an agent
	# that has graduated past its tier's send/edit-rate/funded-loan threshold
	# (fa_max_autonomy.check_tier_gate) must NOT be dispatched autonomously
	# until this flag is explicitly true. It exists because autonomous
	# dispatch (no human Slack approval in the loop) is not yet covered by
	# docs/constitutions/cora.md's drafts-only constitution -- see the
	# proposed amendment at
	# docs/constitutions/cora_autonomy_amendment_proposed.md, awaiting Josh's
	# direct sign-off per that constitution's own amendment rule. Until this
	# flag is flipped, the FA Max tool registry's send-gate check always
	# falls back to the human-approval path (auto_authorize=False),
	# regardless of tier graduation.
	fa_max_autonomous_dispatch_confirmed: bool = Field(
		default=False, env="FA_MAX_AUTONOMOUS_DISPATCH_CONFIRMED"
	)


@lru_cache
def get_settings() -> AppSettings:
	"""Load and cache settings so expensive validation runs once."""

	return AppSettings()


settings = get_settings()
