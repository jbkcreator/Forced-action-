"""
Venture configuration — DB-backed with a 5-minute in-memory cache.

Primary interface:
    get_venture_config(venture_key) -> VentureConfig
    list_ventures()                 -> list[str]
    invalidate_cache(venture_key)   -> None

Config is stored in the `ventures` DB table (see Venture in
src/core/models.py). Deliberately the same shape as county_config.py — same
TTL, same module-level cache dicts, same invalidate_cache() contract — since
it plays the same role one level up: county_config resolves a county, this
resolves the venture that county belongs to.

WHY THE ENV FALLBACK. Every field here existed before CL3 as a single-valued
env global in config/settings.py. When no `ventures` row matches (an
environment where the CL3 migration has not run yet, or a venture_key with
no row), get_venture_config() synthesizes the config from those same
settings rather than raising. That is what makes CL3 a no-op on day one: the
Relay send path reads a VentureConfig whose values are byte-identical to the
settings it read directly before.

Attribute names are chosen to match what src/services/relay/guards.py and
engine.py already read off the settings object (relay_send_window_start,
relay_daily_ceiling, ...) so a VentureConfig can be passed where a settings
object used to be, with no renaming at the call sites.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Optional

from config.venture_template import DEFAULT_KILL_SWITCH_FEATURE, DEFAULT_VENTURE_KEY

logger = logging.getLogger(__name__)

_CACHE_TTL_SECONDS = 300  # 5 minutes, matching county_config.py

_config_cache: dict[str, "VentureConfig"] = {}
_cache_ts: dict[str, float] = {}

# Brand shown in the CAN-SPAM footer when no `ventures` row supplies one.
# Venture #1's real operating name — the CL3 migration seeds it into the
# table, so this literal only applies pre-migration.
_FALLBACK_BRAND_NAME = "Forced Action"


@dataclass(frozen=True)
class VentureConfig:
    """Resolved configuration for one venture. Immutable — cached and shared
    across callers, so nothing may mutate it in place."""

    venture_key: str
    display_name: str
    brand_name: str
    postal_address: str

    # Geography — read by the flood/insurance/storm scrapers (`state`) and
    # bankruptcy_engine (`bankruptcy_court_code`, `default_bankruptcy_division`)
    # via county_config.get_county_config().
    state: str
    bankruptcy_court_code: str
    default_bankruptcy_division: str
    template_county_id: Optional[str]

    # Relay. Named to match the settings attributes these replace.
    relay_slack_channel: str
    relay_approvers: tuple[str, ...]
    relay_instantly_campaign_id: Optional[str]
    relay_instantly_sender_email: str
    relay_send_window_start: int
    relay_send_window_end: int
    relay_send_window_timezone: str
    relay_daily_ceiling: int
    kill_switch_feature: str


def _from_settings(venture_key: str) -> VentureConfig:
    """Build a VentureConfig from config/settings.py — the pre-CL3 values."""
    from config.settings import get_settings

    settings = get_settings()
    return VentureConfig(
        venture_key=venture_key,
        display_name=venture_key,
        brand_name=_FALLBACK_BRAND_NAME,
        postal_address=settings.company_postal_address,
        state="FL",
        bankruptcy_court_code="flmb",
        default_bankruptcy_division="8:",
        template_county_id=None,
        relay_slack_channel=settings.relay_slack_channel,
        relay_approvers=tuple(settings.relay_approvers or ()),
        relay_instantly_campaign_id=settings.relay_instantly_campaign_id,
        relay_instantly_sender_email=settings.relay_instantly_sender_email,
        relay_send_window_start=settings.relay_send_window_start,
        relay_send_window_end=settings.relay_send_window_end,
        relay_send_window_timezone=settings.relay_send_window_timezone,
        relay_daily_ceiling=settings.relay_daily_ceiling,
        kill_switch_feature=DEFAULT_KILL_SWITCH_FEATURE,
    )


def _from_row(row) -> VentureConfig:
    """Build a VentureConfig from a `ventures` row, falling back to settings
    for any nullable column the row leaves unset — a venture that has not
    provisioned its own Slack channel yet still resolves to a usable config
    instead of None.

    The Instantly campaign id and sender email are the exception: those two
    fields are this venture's outbound identity, and RELAY_INSTANTLY_* in
    settings is venture #1's identity specifically (the CL3 migration seeds
    it from that same env var). Falling back to it for any OTHER venture
    would silently route that venture's email through venture #1's campaign
    and from-address — cross-venture sends and false duplicate-contact
    failures (Instantly's dedup guard is campaign-scoped). Only the default
    venture may resolve those two fields from settings; every other venture
    with no row value stays unset so send_email()/cmd_setup_email_channel()
    fail closed instead.
    """
    from config.settings import get_settings

    settings = get_settings()
    is_default_venture = row.venture_key == DEFAULT_VENTURE_KEY
    return VentureConfig(
        venture_key=row.venture_key,
        display_name=row.display_name,
        brand_name=row.brand_name,
        postal_address=row.postal_address or settings.company_postal_address,
        state=row.state,
        bankruptcy_court_code=row.bankruptcy_court_code,
        default_bankruptcy_division=row.default_bankruptcy_division,
        template_county_id=row.template_county_id,
        relay_slack_channel=row.relay_slack_channel or settings.relay_slack_channel,
        relay_approvers=tuple(row.relay_approvers or settings.relay_approvers or ()),
        relay_instantly_campaign_id=(
            row.relay_instantly_campaign_id
            or (settings.relay_instantly_campaign_id if is_default_venture else None)
        ),
        relay_instantly_sender_email=(
            row.relay_instantly_sender_email
            or (settings.relay_instantly_sender_email if is_default_venture else "")
        ),
        relay_send_window_start=row.relay_send_window_start,
        relay_send_window_end=row.relay_send_window_end,
        relay_send_window_timezone=row.relay_send_window_timezone,
        relay_daily_ceiling=row.relay_daily_ceiling,
        kill_switch_feature=row.kill_switch_feature,
    )


_SELECT_VENTURE = """
    SELECT venture_key, display_name, brand_name, postal_address, state,
           bankruptcy_court_code, default_bankruptcy_division, template_county_id,
           relay_slack_channel, relay_approvers, relay_instantly_campaign_id,
           relay_instantly_sender_email, relay_send_window_start,
           relay_send_window_end, relay_send_window_timezone,
           relay_daily_ceiling, kill_switch_feature
    FROM ventures
    WHERE venture_key = :key AND is_active = true
"""


def _load_from_db(venture_key: str, session=None) -> VentureConfig:
    """Read one venture, or fall back to settings.

    Never raises: a missing row, an inactive venture, or a `ventures` table
    that does not exist yet (pre-migration) all resolve to the settings-based
    config. Relay must keep sending through a config-resolution problem.
    """
    from sqlalchemy import text

    try:
        if session is not None:
            row = session.execute(text(_SELECT_VENTURE), {"key": venture_key}).first()
        else:
            from src.core.database import get_db_context

            with get_db_context() as db:
                row = db.execute(text(_SELECT_VENTURE), {"key": venture_key}).first()
    except Exception:
        logger.warning(
            "[venture_config] could not read ventures row for %r — falling back "
            "to env settings", venture_key, exc_info=True,
        )
        return _from_settings(venture_key)

    if row is None:
        logger.info(
            "[venture_config] no active ventures row for %r — using env settings",
            venture_key,
        )
        return _from_settings(venture_key)

    return _from_row(row)


def get_venture_config(
    venture_key: str = DEFAULT_VENTURE_KEY, *, session=None
) -> VentureConfig:
    """Return config for venture_key, cached for 5 minutes.

    Pass `session` when already inside a DB session (county_config does) to
    reuse that connection instead of checking out a second one.
    """
    now = time.monotonic()
    if venture_key in _config_cache and now - _cache_ts.get(venture_key, 0) < _CACHE_TTL_SECONDS:
        return _config_cache[venture_key]

    config = _load_from_db(venture_key, session=session)
    _config_cache[venture_key] = config
    _cache_ts[venture_key] = now
    return config


def invalidate_cache(venture_key: str | None = None) -> None:
    """Flush the cache so the next call re-reads from DB. Pass a venture_key
    to flush one venture, or None to flush all."""
    if venture_key is None:
        _config_cache.clear()
        _cache_ts.clear()
    else:
        _config_cache.pop(venture_key, None)
        _cache_ts.pop(venture_key, None)


def list_ventures() -> list[str]:
    """Return all active venture keys. Empty if the table does not exist yet."""
    from sqlalchemy import text

    try:
        from src.core.database import get_db_context

        with get_db_context() as db:
            rows = db.execute(text(
                "SELECT venture_key FROM ventures WHERE is_active = true ORDER BY venture_key"
            )).fetchall()
            return [r.venture_key for r in rows]
    except Exception:
        logger.warning("[venture_config] could not list ventures", exc_info=True)
        return []
