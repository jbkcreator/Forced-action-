"""
County configuration — DB-backed with a 5-minute in-memory cache.

Primary interface:
    get_county_config(county_id) -> dict   # full config dict used by scrapers/loaders
    get_county(county_id)        -> dict   # legacy alias, returns same shape
    list_counties()              -> list[str]

Secondary helpers (backwards compat):
    get_portal(county_id, portal_key)
    get_file_prefix(county_id)

Config is stored in the `counties` + `county_sources` DB tables (admin-managed).
Cache TTL is 5 minutes so live admin edits propagate without a restart.
"""

import logging
import time
from typing import Any
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

_CACHE_TTL_SECONDS = 300  # 5 minutes

_config_cache: dict[str, dict] = {}
_cache_ts: dict[str, float] = {}


def _origin(url: str) -> str:
    """Extract scheme+host from a URL string. Returns '' if empty/invalid."""
    if not url:
        return ""
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}" if p.scheme and p.netloc else ""


def _load_from_db(county_id: str) -> dict:
    """Query DB and return the full config dict for county_id."""
    from src.core.database import get_db_context
    from src.core.models import County, CountySource

    with get_db_context() as session:
        county = (
            session.query(County)
            .filter_by(county_id=county_id, is_active=True)
            .first()
        )
        if not county:
            raise KeyError(
                f"Unknown or inactive county_id '{county_id}'. "
                "Add it via the admin UI (/api/admin/counties) first."
            )

        sources: dict[str, Any] = {
            src.signal_type: {
                "source_id":            src.id,
                "url":                  src.url,
                "source_name":          src.source_name,
                "description":          src.description,
                "navigation_hint":      src.navigation_hint,
                "output_format":        src.output_format,
                "date_range_available": src.date_range_available,
                "frequency":            src.frequency,
                # Explicit ORI/CSV structure fields (surfaced as labeled admin UI inputs)
                "ori_column_map":       src.ori_column_map or {},
                "ori_book_page_col":    src.ori_book_page_col,
                "ori_doc_type_map":     src.ori_doc_type_map or {},
                # Remaining one-off flags (prr_only, style_col, bulk_tables, etc.)
                **(src.special_flags or {}),
            }
            for src in county.sources
            if src.is_active
        }

        # Derive commonly-needed URLs from sources so scrapers don't have to
        _court_url   = sources.get("court_records", {}).get("url", "")
        _clerk_base  = _origin(_court_url)
        _tax_base    = _origin(sources.get("tax_delinquency", {}).get("url", ""))

        # Backwards-compat "urls" sub-dict (mirrors config.constants shape)
        urls: dict[str, str] = {
            "foreclosure":  sources.get("foreclosures",    {}).get("url", ""),
            "permit":       sources.get("permits",         {}).get("url", ""),
            "violation":    sources.get("violations",      {}).get("url", ""),
            "civil":        _court_url,
            "probate":      f"{_clerk_base}/Probate/dailyfilings/" if _clerk_base else "",
            "clerk_base":   _clerk_base,
            "clerk_access": sources.get("liens",           {}).get("url", ""),
            "tax":          sources.get("tax_delinquency", {}).get("url", ""),
            "parcel":       f"{_tax_base}/public/real_estate/parcels" if _tax_base else "",
            "master":       sources.get("master_data",     {}).get("url", ""),
        }

        config: dict[str, Any] = {
            "county_id":           county.county_id,
            "name":                county.display_name,
            "display_name":        county.display_name,
            "fips":                county.fips or "",
            "nws_zone":            county.nws_zone,
            # Plural list expected by storm/flood/insurance scrapers
            "nws_zones":           [county.nws_zone] if county.nws_zone else [],
            "state":               "FL",
            "zip_prefixes":        [],
            "parcel_id_format":    county.parcel_id_format or "folio",
            "bankruptcy_division": county.bankruptcy_division,
            "city_filer_keywords": county.city_filer_keywords or [],
            "code_lien_type_map":  county.code_lien_type_map or {},
            "file_prefix":         county.county_id,
            # court sub-dict expected by bankruptcy_engine
            "court": {
                "bankruptcy_code":  "flmb",
                "division_prefix":  county.bankruptcy_division or "8:",
            },
            "sources": sources,
            "urls":    urls,
        }
    return config


def get_county_config(county_id: str) -> dict[str, Any]:
    """Return full config for county_id, cached for 5 minutes."""
    now = time.monotonic()
    if county_id in _config_cache and now - _cache_ts.get(county_id, 0) < _CACHE_TTL_SECONDS:
        return _config_cache[county_id]

    config = _load_from_db(county_id)
    _config_cache[county_id] = config
    _cache_ts[county_id] = now
    return config


def invalidate_cache(county_id: str | None = None) -> None:
    """
    Flush the in-memory cache so the next call re-reads from DB.
    Pass a county_id to flush a single county, or None to flush all.
    """
    if county_id is None:
        _config_cache.clear()
        _cache_ts.clear()
    else:
        _config_cache.pop(county_id, None)
        _cache_ts.pop(county_id, None)


# ---------------------------------------------------------------------------
# Legacy aliases (kept for backwards compat with existing callers)
# ---------------------------------------------------------------------------

def get_county(county_id: str) -> dict[str, Any]:
    """Alias for get_county_config() — existing callers use this name."""
    return get_county_config(county_id)


def get_portal(county_id: str, portal_key: str) -> str:
    """
    Backwards-compat helper. Previously read from counties.json portals block.
    Now reads from county_sources keyed by signal_type, falling back to
    sources dict for any key that matches a signal_type.
    """
    config = get_county_config(county_id)
    sources = config.get("sources", {})
    if portal_key in sources:
        return sources[portal_key]["url"]
    raise KeyError(
        f"Portal key '{portal_key}' not found in county '{county_id}' sources. "
        f"Available signal_types: {list(sources.keys())}"
    )


def get_file_prefix(county_id: str) -> str:
    """Return the file-naming prefix for a county (always the county_id itself)."""
    return get_county_config(county_id)["file_prefix"]


def list_counties() -> list[str]:
    """Return all active county IDs from the DB."""
    from src.core.database import get_db_context
    from src.core.models import County

    with get_db_context() as session:
        rows = session.query(County.county_id).filter_by(is_active=True).all()
        return [r.county_id for r in rows]
