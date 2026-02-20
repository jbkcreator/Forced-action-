"""Application configuration powered by Pydantic settings."""

from functools import lru_cache

from pydantic import AnyUrl, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
	"""Central place for environment-driven configuration."""

	model_config = SettingsConfigDict(
		env_file=".env",
		env_file_encoding="utf-8",
		case_sensitive=False,
	)

	anthropic_api_key: SecretStr = Field(..., env="ANTHROPIC_API_KEY")
	firecrawl_api_key: SecretStr = Field(..., env="FIRECRAWL_API_KEY")
	court_listener_api_key: SecretStr = Field(..., env="COURT_LISTENER_API_KEY")


@lru_cache
def get_settings() -> AppSettings:
	"""Load and cache settings so expensive validation runs once."""

	return AppSettings()


settings = get_settings()
