"""A7 macro signal loaders — data-discovery layer only.

Provides lightweight clients and normalizers for public macro data sources:
  - FRED (Federal Reserve Economic Data) — mortgage rates, treasury yields
  - BLS (Bureau of Labor Statistics) — unemployment, CPI, rental CPI
  - FHFA (Federal Housing Finance Agency) — House Price Index
  - Census ACS — income, vacancy, population by geography

No DB writes. Each loader returns normalized dicts ready for future DB ingestion.
"""
from src.loaders.macro_signals.normalization import normalize_record, REQUIRED_KEYS
from src.loaders.macro_signals.source_registry import SOURCES

__all__ = ["normalize_record", "REQUIRED_KEYS", "SOURCES"]
