"""Unit tests for src.utils.scraper_config.

These do NOT exercise live scraper output — only loader behavior:
malformed YAML raises, missing engine raises, helpers return correct types.
"""

import pytest

from src.utils import scraper_config


# --- import-level sanity --------------------------------------------------

def test_loader_imports_cleanly():
    # If any cross-cutting YAML is malformed, the module-level import in this
    # test file would already have raised. Reaching this assert means OK.
    assert hasattr(scraper_config, "get_keywords")
    assert hasattr(scraper_config, "get_patterns")
    assert hasattr(scraper_config, "get_timeout")
    assert hasattr(scraper_config, "get_selectors")


# --- error contracts ------------------------------------------------------

def test_get_keywords_unknown_engine_raises():
    with pytest.raises(KeyError, match="missing from scraper_keywords.yaml"):
        scraper_config.get_keywords("no_such_engine_zzz")


def test_get_patterns_unknown_engine_raises():
    with pytest.raises(KeyError, match="missing from scraper_patterns.yaml"):
        scraper_config.get_patterns("no_such_engine_zzz")


def test_get_timeout_unknown_engine_raises():
    with pytest.raises(KeyError, match="missing from scraper_timeouts.yaml"):
        scraper_config.get_timeout("no_such_engine_zzz", "download_wait")


def test_per_scraper_missing_file_raises():
    with pytest.raises(KeyError, match="No per-scraper YAML"):
        scraper_config._per_scraper("definitely_not_an_engine")


# --- all 17 stub files resolve -------------------------------------------

ENGINES = [
    "bankruptcy", "dbpr", "divorce", "evictions", "fire", "flood",
    "foreclosure", "insurance", "liens", "master", "permits", "probate",
    "roofing_permit", "storm", "sunbiz", "tax_delinquent", "violations",
]


@pytest.mark.parametrize("engine", ENGINES)
def test_per_scraper_yaml_exists(engine):
    cfg = scraper_config.get_scraper_config(engine)
    assert isinstance(cfg, dict)
