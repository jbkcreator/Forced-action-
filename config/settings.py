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
	)

	# API Keys
	anthropic_api_key: SecretStr = Field(..., env="ANTHROPIC_API_KEY")
	firecrawl_api_key: SecretStr = Field(..., env="FIRECRAWL_API_KEY")
	court_listener_api_key: SecretStr = Field(..., env="COURT_LISTENER_API_KEY")

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

	# Application base URL — used for Stripe return/success URLs
	app_base_url: str = Field(
		default="http://localhost:8000",
		env="APP_BASE_URL",
		description="Public base URL of this app (e.g. https://app.forcedaction.io)",
	)

	# Contact enrichment (M1)
	batch_skip_tracing_api_key: Optional[SecretStr] = Field(default=None, env="BATCH_SKIP_TRACING_API_KEY")
	idi_api_key: Optional[SecretStr] = Field(default=None, env="IDI_API_KEY")

	# Alerting
	alert_sms_number: Optional[str] = Field(default=None, env="ALERT_SMS_NUMBER")  # SMS target for match-rate alerts

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
