"""
Cloudflare-bypass Playwright launcher.

Wraps `playwright.chromium.launch_persistent_context` against the warmed
Edge profile that the cf_session_manager validates / auto-warms.

Two public entry points:

  launch_cf_bypass_context(profile_name, county_id, portal_url, headless=False)
      → async context manager that yields a Playwright BrowserContext
        bound to the warmed Edge profile. Calls cf_session_manager.ensure_ready
        BEFORE launching, so by the time the context is yielded the profile
        is validated (or auto-warmed). Raises CFBypassFailedError if the
        profile can't be made ready.

  _launch_edge(profile_name, headless=False)
      → low-level helper used by cf_session_manager itself for validation.
        Skips ensure_ready (would recurse) — assumes the profile dir exists.

Edge is the runtime even though Playwright nominally drives Chromium — the
TLS fingerprint that earned the cf_clearance cookie was Edge's, so the
launch path must reuse the same binary.
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator, Optional

logger = logging.getLogger(__name__)


_EDGE_CANDIDATES = [
    os.environ.get("CF_BYPASS_BROWSER_PATH"),
    r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
    r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
    "/usr/bin/microsoft-edge",
    "/usr/bin/microsoft-edge-stable",
]


PROJECT_ROOT = Path(__file__).resolve().parents[2]
_PROFILE_BASE_DIR = PROJECT_ROOT / "data" / "cf_session"


class CFProfileNotWarmedError(RuntimeError):
    """Raised when the persistent Edge profile directory is missing on disk."""


def find_edge_binary() -> Optional[str]:
    """Return the first existing Edge / Chromium binary path, or None."""
    for p in _EDGE_CANDIDATES:
        if p and os.path.isfile(p):
            return p
    return None


def profile_dir_for(profile_name: str) -> Path:
    """Return the user-data-dir for the named CF-bypass profile."""
    return _PROFILE_BASE_DIR / f"edge_profile_{profile_name}"


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
    Production entry point for scraper engines.

    Calls cf_session_manager.ensure_ready() first, which:
      - validates the existing profile if recently warmed
      - auto-warms if expired
      - alerts ops + raises if auto-warm fails

    Then yields a Playwright BrowserContext bound to the validated profile.
    """
    # Local import to keep cf_session_manager → cf_persistent_browser the only
    # direction of the dependency (cf_persistent_browser MUST NOT import from
    # cf_session_manager at module level, since _launch_edge is called by it).
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
