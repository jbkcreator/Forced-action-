"""
Cloudflare-bypass Playwright launcher.

Wraps `playwright.chromium.launch_persistent_context` against the warmed
Edge profile that the cf_session_manager validates / auto-warms.

Public entry points:

  resolve_cf_profile(profile_name) -> dict
      Synchronous helper that returns {"edge_path", "profile_dir"} for
      callers that want to pass the profile directly to browser-use's
      Browser kwargs (the tax / lien scraper path). Does NOT call
      ensure_ready — caller is responsible for warming.

  launch_cf_bypass_context(...) -> async context manager
      Production entry for scrapers that drive Playwright directly.
      Calls cf_session_manager.ensure_ready() first (validate +
      auto-warm), then yields a Playwright BrowserContext bound to the
      validated profile.

  _launch_edge(profile_name, headless=False)
      Low-level helper used by cf_session_manager._validate_profile
      itself — skips ensure_ready (would recurse) — assumes the
      profile dir exists.

Edge is the runtime even though Playwright nominally drives Chromium —
the TLS fingerprint that earned the cf_clearance cookie was Edge's, so
the launch path must reuse the same binary.
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


_EDGE_CANDIDATES = [
    os.environ.get("CF_BYPASS_BROWSER_PATH"),
    r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
    r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
    "/usr/bin/microsoft-edge",
    "/usr/bin/microsoft-edge-stable",
]


PROJECT_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_PROFILE_BASE = PROJECT_ROOT / "data" / "cf_session"


class CFProfileNotWarmedError(RuntimeError):
    """Raised when the persistent Edge profile directory is missing on disk."""


def _profile_base_dir() -> Path:
    override = os.environ.get("CF_BYPASS_PROFILE_BASE_DIR")
    return Path(override) if override else _DEFAULT_PROFILE_BASE


def find_edge_binary() -> Optional[str]:
    """Return the first existing Edge / Chromium binary path, or None."""
    for p in _EDGE_CANDIDATES:
        if p and os.path.isfile(p):
            return p
    return None


def profile_dir_for(profile_name: str) -> Path:
    """Return the user-data-dir for the named CF-bypass profile."""
    return _profile_base_dir() / f"edge_profile_{profile_name}"


def resolve_cf_profile(profile_name: str) -> dict:
    """Build the {edge_path, profile_dir} dict consumed by the scraper engines.

    Raises CFProfileNotWarmedError if Edge isn't installed or the profile
    dir is missing. Sync helper — does NOT call ensure_ready (call that
    upstream if you want validate + auto-warm).
    """
    edge_path = find_edge_binary()
    if not edge_path:
        raise CFProfileNotWarmedError(
            "No Edge binary found. Install Microsoft Edge or set "
            "CF_BYPASS_BROWSER_PATH to a Chromium-family executable."
        )
    profile_dir = profile_dir_for(profile_name)
    if not profile_dir.exists():
        raise CFProfileNotWarmedError(
            f"Edge profile {profile_dir} does not exist on disk. "
            "Warm it via `python -m src.utils.cf_session_manager --warm "
            f"{profile_name}` (or pass through ensure_ready)."
        )
    return {"edge_path": edge_path, "profile_dir": str(profile_dir)}


@asynccontextmanager
async def _launch_edge(
    *,
    profile_name: str,
    headless: bool = False,
    accept_downloads: bool = True,
):
    """
    Low-level Playwright launch — assumes the profile dir already exists.
    cf_session_manager._validate_profile() uses this to do health checks
    without triggering an ensure_ready recursion.
    """
    from playwright.async_api import async_playwright

    edge_path = find_edge_binary()
    if not edge_path:
        raise CFProfileNotWarmedError(
            "No Edge binary found. Install Microsoft Edge or set "
            "CF_BYPASS_BROWSER_PATH to a Chromium-family executable."
        )

    profile_dir = profile_dir_for(profile_name)
    if not profile_dir.exists():
        raise CFProfileNotWarmedError(
            f"Edge profile {profile_dir} does not exist on disk."
        )

    async with async_playwright() as pw:
        context = await pw.chromium.launch_persistent_context(
            user_data_dir=str(profile_dir),
            executable_path=edge_path,
            headless=headless,
            accept_downloads=accept_downloads,
            args=[
                "--disable-blink-features=AutomationControlled",
            ],
            ignore_default_args=["--enable-automation"],
        )
        try:
            yield context
        finally:
            try:
                await context.close()
            except Exception:
                pass


@asynccontextmanager
async def launch_cf_bypass_context(
    *,
    profile_name: str,
    county_id: str,
    portal_url: str,
    headless: bool = False,
    accept_downloads: bool = True,
):
    """
    Production entry point for scraper engines that drive Playwright directly.

    Calls cf_session_manager.ensure_ready() first, which:
      - validates the existing profile if recently warmed
      - auto-warms if expired
      - alerts ops + raises if auto-warm fails

    Then yields a Playwright BrowserContext bound to the validated profile.
    """
    # Local import to keep cf_session_manager → cf_persistent_browser the only
    # direction of the module-level dependency.
    from src.utils.cf_session_manager import ensure_ready

    profile_dir = await ensure_ready(
        profile_name=profile_name,
        county_id=county_id,
        portal_url=portal_url,
    )
    logger.info("[cf_bypass] launching against profile=%s dir=%s",
                profile_name, profile_dir)

    async with _launch_edge(
        profile_name=profile_name,
        headless=headless,
        accept_downloads=accept_downloads,
    ) as ctx:
        yield ctx
