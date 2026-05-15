"""
Cloudflare-bypass helpers (lightweight, no DB-backed session manager).

Standalone port of the persistent-Edge-profile launch pattern from
scaling/county's lien engine. The DB-backed cf_session_manager isn't
ported — operators warm/refresh profiles manually:

    1. Install Microsoft Edge (or any Chromium-family browser).
       Optionally set CF_BYPASS_BROWSER_PATH to override auto-detect.
    2. Create profile dir:
           data/cf_session/edge_profile_<profile_name>/
       (override base dir with CF_BYPASS_PROFILE_BASE_DIR)
    3. Launch Edge with --user-data-dir=<that dir>, visit the protected
       portal, pass the Cloudflare challenge once. cf_clearance cookie
       persists in the profile.
    4. Run the scraper with --cf-bypass — Browser is launched against
       that profile (Edge binary + persistent user_data_dir + no proxy
       + no stealth init script), so the warmed TLS+cookie fingerprint
       stays intact.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional


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
    dir is missing (operator must warm the profile manually first).
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
            "Warm it by launching Edge with --user-data-dir=<that path> "
            "and clearing the Cloudflare challenge once."
        )
    return {"edge_path": edge_path, "profile_dir": str(profile_dir)}
