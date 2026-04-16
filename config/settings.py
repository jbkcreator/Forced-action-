"""Application configuration powered by Pydantic settings."""

from functools import lru_cache

from typing import Optional

from pydantic import AnyUrl, Field, SecretStr, PostgresDsn
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

	debug: bool = Field(default=True, env="DEBUG")


	# Oxylabs proxy (optional — used by foreclosure + tax delinquency scrapers)
	oxylabs_username: Optional[str] = Field(default=None, env="OXYLABS_USERNAME")
	oxylabs_password: Optional[SecretStr] = Field(default=None, env="OXYLABS_PASSWORD")

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

	# Stripe (M1)
	stripe_secret_key: Optional[SecretStr] = Field(default=None, env="STRIPE_SECRET_KEY")
	stripe_webhook_secret: Optional[SecretStr] = Field(default=None, env="STRIPE_WEBHOOK_SECRET")
	stripe_publishable_key: Optional[str] = Field(default=None, env="STRIPE_PUBLISHABLE_KEY")

	# Stripe price IDs
	stripe_price_starter_founding: Optional[str] = Field(default=None, env="STRIPE_PRICE_STARTER_FOUNDING")
	stripe_price_starter_regular: Optional[str] = Field(default=None, env="STRIPE_PRICE_STARTER_REGULAR")
	stripe_price_pro_founding: Optional[str] = Field(default=None, env="STRIPE_PRICE_PRO_FOUNDING")
	stripe_price_pro_regular: Optional[str] = Field(default=None, env="STRIPE_PRICE_PRO_REGULAR")
	stripe_price_dominator_founding: Optional[str] = Field(default=None, env="STRIPE_PRICE_DOMINATOR_FOUNDING")
	stripe_price_dominator_regular: Optional[str] = Field(default=None, env="STRIPE_PRICE_DOMINATOR_REGULAR")
	stripe_price_lead_pack: Optional[str] = Field(default=None, env="STRIPE_PRICE_LEAD_PACK")
	stripe_price_hot_lead_unlock: Optional[str] = Field(default=None, env="STRIPE_PRICE_HOT_LEAD_UNLOCK")

	# Founding subscriber spot limit (default 10, changeable without redeploy)
	founding_spot_limit: int = Field(default=10, env="FOUNDING_SPOT_LIMIT")

	# Demo booking link — sent via SMS when prospect requests a demo on the call
	demo_calendly_url: Optional[str] = Field(default=None, env="DEMO_CALENDLY_URL")

	# Application base URL — used for Stripe return/success URLs
	app_base_url: str = Field(
		default="http://localhost:8000",
		env="APP_BASE_URL",
		description="Public base URL of this app (e.g. https://app.forcedaction.io)",
	)

	# Contact enrichment (M1)
	batch_skip_tracing_api_key: Optional[SecretStr] = Field(default=None, env="BATCH_SKIP_TRACING_API_KEY")
	idi_api_key: Optional[SecretStr] = Field(default=None, env="IDI_API_KEY")

	# SMTP (used by welcome email, payment receipts, grace period alerts)
	smtp_host: Optional[str] = Field(default=None, env="SMTP_HOST")
	smtp_port: int = Field(default=587, env="SMTP_PORT")
	smtp_user: Optional[str] = Field(default=None, env="SMTP_USER")
	smtp_pass: Optional[SecretStr] = Field(default=None, env="SMTP_PASS")
	email_from: Optional[str] = Field(default=None, env="EMAIL_FROM")  # falls back to smtp_user if not set
	alert_email: Optional[str] = Field(default=None, env="ALERT_EMAIL")  # ops alert recipient

	# Alerting
	alert_sms_number: Optional[str] = Field(default=None, env="ALERT_SMS_NUMBER")  # SMS target for match-rate alerts
	alert_sms_carrier: Optional[str] = Field(default=None, env="ALERT_SMS_CARRIER")  # e.g. tmomail.net, vtext.com

	# UptimeRobot (optional — used by scripts/setup_uptimerobot.py to create monitors)
	uptimerobot_api_key: Optional[SecretStr] = Field(default=None, env="UPTIMEROBOT_API_KEY")

	# Admin upload layer
	admin_username: str = Field(default="admin", env="ADMIN_USERNAME")
	admin_password: Optional[SecretStr] = Field(default=None, env="ADMIN_PASSWORD")
	admin_jwt_secret: Optional[SecretStr] = Field(default=None, env="ADMIN_JWT_SECRET")

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


@lru_cache
def get_settings() -> AppSettings:
	"""Load and cache settings so expensive validation runs once."""

	return AppSettings()


settings = get_settings()
