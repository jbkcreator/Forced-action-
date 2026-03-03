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

	# GoHighLevel CRM integration (optional — feature disabled if not set)
	ghl_api_key: Optional[SecretStr] = Field(default=None, env="GHL_API_KEY")
	ghl_location_id: Optional[str] = Field(default=None, env="GHL_LOCATION_ID")
	ghl_pipeline_id: Optional[str] = Field(default=None, env="GHL_PIPELINE_ID")
	# Stage IDs within the GHL pipeline (map to urgency levels)
	ghl_stage_immediate: Optional[str] = Field(default=None, env="GHL_STAGE_IMMEDIATE")
	ghl_stage_high: Optional[str] = Field(default=None, env="GHL_STAGE_HIGH")
	ghl_stage_medium: Optional[str] = Field(default=None, env="GHL_STAGE_MEDIUM")

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
