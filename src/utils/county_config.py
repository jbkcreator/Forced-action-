"""
County configuration loader.

Loads /config/counties.json and provides typed access to per-county
portal URLs, file prefixes, and credentials references.

Usage:
    from src.utils.county_config import get_county

    county = get_county("hillsborough")
    url = county["portals"]["realforeclose_base_url"]
    prefix = county["file_prefix"]
"""

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

_COUNTIES_PATH = Path(__file__).parent.parent.parent / "config" / "counties.json"


@lru_cache(maxsize=1)
def _load() -> dict[str, Any]:
    with open(_COUNTIES_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def get_county(county_id: str) -> dict[str, Any]:
    """Return the config block for a given county_id. Raises KeyError if unknown."""
    data = _load()
    if county_id not in data:
        raise KeyError(
            f"Unknown county_id '{county_id}'. "
            f"Available: {list(data.keys())}. "
            f"Add it to config/counties.json first."
        )
    return data[county_id]


def get_portal(county_id: str, portal_key: str) -> str:
    """Convenience shortcut: get_portal('hillsborough', 'realforeclose_base_url')"""
    return get_county(county_id)["portals"][portal_key]


def get_file_prefix(county_id: str) -> str:
    """Return the file-naming prefix for a county (e.g. 'hillsborough')."""
    return get_county(county_id)["file_prefix"]


def list_counties() -> list[str]:
    """Return all configured county IDs."""
    return list(_load().keys())
