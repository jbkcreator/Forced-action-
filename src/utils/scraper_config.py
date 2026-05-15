"""Scraper configuration loader.

Loads YAML tunables for the 17 scraper engines and exposes typed helpers.
Mirrors the convention in `county_config.py`: eager load, lru_cache, KeyError
on miss. Malformed YAML or missing files crash at import, not 5 minutes into
a cron run.

YAML layout (under `config/`):
    scraper_keywords.yaml   — {engine: [keyword, ...]}
    scraper_patterns.yaml   — {engine: {regex/case_types/doc_types/...: ...}}
    scraper_timeouts.yaml   — {engine: {timeout_name: seconds}}
    scrapers/<engine>.yaml  — per-engine bag (selectors live here)

Usage:
    from src.utils.scraper_config import (
        get_keywords, get_patterns, get_timeout, get_selectors,
    )
    for kw in get_keywords("fire"):
        ...
    wait = get_timeout("permits", "download_wait")
    grid = get_selectors("foreclosure")["results_table_rows"]
"""

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

_CONFIG_DIR = Path(__file__).parent.parent.parent / "config"
_KEYWORDS_PATH = _CONFIG_DIR / "scraper_keywords.yaml"
_PATTERNS_PATH = _CONFIG_DIR / "scraper_patterns.yaml"
_TIMEOUTS_PATH = _CONFIG_DIR / "scraper_timeouts.yaml"
_SCRAPERS_DIR = _CONFIG_DIR / "scrapers"


def _load_yaml(path: Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data or {}


@lru_cache(maxsize=1)
def _keywords() -> dict[str, list[str]]:
    return _load_yaml(_KEYWORDS_PATH)


@lru_cache(maxsize=1)
def _patterns() -> dict[str, dict[str, Any]]:
    return _load_yaml(_PATTERNS_PATH)


@lru_cache(maxsize=1)
def _timeouts() -> dict[str, dict[str, int]]:
    return _load_yaml(_TIMEOUTS_PATH)


@lru_cache(maxsize=None)
def _per_scraper(engine: str) -> dict[str, Any]:
    path = _SCRAPERS_DIR / f"{engine}.yaml"
    if not path.exists():
        raise KeyError(
            f"No per-scraper YAML for engine '{engine}'. "
            f"Expected at {path.relative_to(_CONFIG_DIR.parent)}."
        )
    return _load_yaml(path)


def _require(container: dict[str, Any], engine: str, source: str) -> Any:
    if engine not in container:
        raise KeyError(
            f"Engine '{engine}' missing from {source}. "
            f"Known engines: {sorted(container.keys())}."
        )
    return container[engine]


def get_keywords(engine: str) -> list[str]:
    """Return the keyword list for an engine. KeyError if engine not configured."""
    return list(_require(_keywords(), engine, "scraper_keywords.yaml"))


def get_patterns(engine: str) -> dict[str, Any]:
    """Return the patterns block for an engine (regex, case-type codes, etc.)."""
    return _require(_patterns(), engine, "scraper_patterns.yaml")


def get_timeout(engine: str, name: str) -> int:
    """Return a named timeout (seconds) for an engine."""
    block = _require(_timeouts(), engine, "scraper_timeouts.yaml")
    if name not in block:
        raise KeyError(
            f"Timeout '{name}' not configured for engine '{engine}'. "
            f"Available: {sorted(block.keys())}."
        )
    return int(block[name])


def get_selectors(engine: str) -> dict[str, str]:
    """Return the selectors block from the per-scraper YAML."""
    block = _per_scraper(engine).get("selectors")
    if block is None:
        raise KeyError(f"No 'selectors' block in config/scrapers/{engine}.yaml.")
    return block


def get_scraper_config(engine: str) -> dict[str, Any]:
    """Return the full per-scraper YAML body (for engine-specific tunables)."""
    return _per_scraper(engine)
