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

Since CLONE-v2.2 / CL3 each county belongs to a venture (counties.venture_key)
and inherits its `state` and bankruptcy court from that venture's row rather
than from Florida-shaped literals in this module — see
src/utils/venture_config.py.
"""

import logging
import time
from contextlib import contextmanager
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


@contextmanager
def _session_scope(session):
    """Yield `session` if the caller supplied one, otherwise check out a new
    one and close it on the way out."""
    if session is not None:
        yield session
        return

    from src.core.database import get_db_context

    with get_db_context() as own_session:
        yield own_session


def _load_from_db(county_id: str, session=None) -> dict:
    """Query DB and return the full config dict for county_id.

    Pass `session` to read on an already-open session instead of checking out
    a new one — used by callers that are mid-transaction, and by tests, so a
    county can be resolved without committing it first.
    """
    from src.core.models import County, CountySource
    from src.utils.venture_config import get_venture_config

    with _session_scope(session) as session:
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

        # Build per-signal source dicts. special_flags is spread FIRST so that
        # any explicit first-class column below overrides a stale leftover key
        # in the JSONB (e.g. legacy `scrape_mode` or `playwright_code` keys
        # that haven't been swept out yet by the cleanup migration).
        sources: dict[str, Any] = {
            src.signal_type: {
                # Remaining one-off flags (prr_only, cf_bypass_required,
                # style_col, bulk_tables, permit_ai_strategy, etc.) come first.
                **(src.special_flags or {}),
                "source_id":            src.id,
                "url":                  src.url,
                "source_name":          src.source_name,
                "description":          src.description,
                "navigation_hint":      src.navigation_hint,
                "output_format":        src.output_format,
                "date_range_available": src.date_range_available,
                "frequency":            src.frequency,
                # First-class scrape-mode + Playwright-code fields. Explicit
                # values here win over any legacy values in special_flags.
                "scrape_mode":              src.scrape_mode,
                "playwright_code":          src.playwright_code,
                "playwright_code_version":  src.playwright_code_version,
                "playwright_code_approved": src.playwright_code_approved,
            }
            for src in county.sources
            if src.is_active
        }

        # Venture this county belongs to (CLONE-v2.2 / CL3). Supplies the
        # state and bankruptcy court that used to be hardcoded to Florida
        # below. Resolved on the open session so this doesn't check out a
        # second connection, and separately cached for 5 minutes of its own.
        venture = get_venture_config(county.venture_key, session=session)

        # Derive commonly-needed URLs from sources so scrapers don't have to
        _court_url   = sources.get("court_records", {}).get("url", "")
        _clerk_base  = _origin(_court_url)
        _tax_base    = _origin(sources.get("tax_delinquency", {}).get("url", ""))

        # An explicit `probate` source wins over the derived path: the
        # {clerk_base}/Probate/dailyfilings/ layout is the Hillsborough
        # clerk's, and a county on a different clerk platform needs to
        # configure its own URL rather than inherit that shape.
        _probate_url = sources.get("probate", {}).get("url", "") or (
            f"{_clerk_base}/Probate/dailyfilings/" if _clerk_base else ""
        )

        # Backwards-compat "urls" sub-dict (mirrors config.constants shape)
        urls: dict[str, str] = {
            "foreclosure":  sources.get("foreclosures",    {}).get("url", ""),
            "permit":       sources.get("permits",         {}).get("url", ""),
            "violation":    sources.get("violations",      {}).get("url", ""),
            "civil":        _court_url,
            "probate":      _probate_url,
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
            # Plural list expected by storm/flood/insurance scrapers.
            # nws_zone may be comma-separated (e.g. "FLZ151,FLZ251") for
            # counties that span multiple NWS forecast zones.
            "nws_zones":           [z.strip() for z in county.nws_zone.split(",")] if county.nws_zone else [],
            # Venture-derived, not hardcoded to Florida since CL3. `state` is
            # read by the flood/insurance/storm scrapers for NWS + FEMA
            # lookups; `court` by bankruptcy_engine.
            "venture_key":         county.venture_key,
            "state":               venture.state,
            "zip_prefixes":        county.zip_prefixes or [],
            "parcel_id_format":    county.parcel_id_format or "folio",
            "bankruptcy_division": county.bankruptcy_division,
            "city_filer_keywords": county.city_filer_keywords or [],
            "code_lien_type_map":  county.code_lien_type_map or {},
            "address_city_tokens": county.address_city_tokens or [],
            "file_prefix":         county.county_id,
            # court sub-dict expected by bankruptcy_engine
            "court": {
                "bankruptcy_code":  venture.bankruptcy_court_code,
                "division_prefix":  county.bankruptcy_division or venture.default_bankruptcy_division,
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


def is_zip_in_county(county_id: str, zip_code: str) -> bool:
    """True if zip_code's 3-digit prefix matches one configured for this county."""
    if not zip_code or len(zip_code) < 3:
        return False
    prefixes = get_county(county_id).get("zip_prefixes") or []
    return zip_code[:3] in prefixes


def is_county_launched(county_id: str, db) -> bool:
    """
    True if a county is live for selling. Launch status is DERIVED, not stored
    on `counties` (see ADR 0002):

      - the source county (settings.county_launch_source_county) is launched
        by definition — it predates the expansion machinery and has no
        expansion_candidates row, or
      - an expansion_candidates row exists with status='launched'.
    """
    from sqlalchemy import text
    from config.settings import get_settings

    if county_id == get_settings().county_launch_source_county:
        return True
    row = db.execute(
        text("SELECT 1 FROM expansion_candidates WHERE county_id = :c AND status = 'launched'"),
        {"c": county_id},
    ).first()
    return row is not None
