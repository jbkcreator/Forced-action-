"""
Cloudflare-bypass session lifecycle manager.

Owns the metadata, validation, and auto-warm logic for per-county
Cloudflare-protected portal sessions. Profile FILES live on disk;
metadata lives in the `cf_bypass_profiles` table (model: CFBypassProfile).

Public API:
    ensure_ready(profile_name, county_id, portal_url) -> Path
        Returns the on-disk profile dir, validating + auto-warming as
        needed. Raises CFBypassFailedError if the profile can't be made
        ready (sends operator alert before raising).

    mark_failed_during_scrape(profile_name, reason)
        Called by scraper engines when a CF challenge is hit MID-flow
        (after ensure_ready already approved the session). Flips status
        to 'expired' so the next run re-warms.

CLI:
    python -m src.utils.cf_session_manager --warm pinellas_clerk
    python -m src.utils.cf_session_manager --status
    python -m src.utils.cf_session_manager --validate pinellas_clerk
"""

from __future__ import annotations

import argparse
import asyncio
import io
import logging
import shutil
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from sqlalchemy import select

from src.core.database import get_db_context
from src.core.models import CFBypassProfile

logger = logging.getLogger(__name__)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROFILE_BASE_DIR = PROJECT_ROOT / "data" / "cf_session"

# Defaults — overridable per-profile in the DB row.
DEFAULT_VALIDATION_TTL_MINUTES = 540   # 9 hours
HEALTH_CHECK_TIMEOUT_SECONDS = 15
AUTO_WARM_TIMEOUT_SECONDS = 45


class CFBypassFailedError(RuntimeError):
    """Raised when a CF-bypass profile can't be made ready for scraping."""


# ─────────────────────────────────────────────────────────────────────────────
# DB lookups + upserts
# ─────────────────────────────────────────────────────────────────────────────

def _get_or_create(
    profile_name: str,
    county_id: str,
    portal_url: str,
) -> dict:
    """
    Return the profile row as a dict (detached from session). Creates a
    new row in 'unwarmed' status if none exists for this profile_name.
    """
    profile_dir = PROFILE_BASE_DIR / f"edge_profile_{profile_name}"
    with get_db_context() as session:
        row = session.execute(
            select(CFBypassProfile).where(CFBypassProfile.profile_name == profile_name)
        ).scalar_one_or_none()
        if row is None:
            row = CFBypassProfile(
                profile_name=profile_name,
                county_id=county_id,
                portal_url=portal_url,
                status="unwarmed",
                profile_dir_path=str(profile_dir),
                validation_ttl_minutes=DEFAULT_VALIDATION_TTL_MINUTES,
            )
            session.add(row)
            session.flush()
            logger.info("[CFSess] created new profile row name=%s county=%s",
                        profile_name, county_id)
        else:
            # Keep portal_url + county_id in sync with the source config in case
            # admin edited the source URL between runs.
            if row.portal_url != portal_url:
                row.portal_url = portal_url
            if row.county_id != county_id:
                row.county_id = county_id
            if row.profile_dir_path != str(profile_dir):
                row.profile_dir_path = str(profile_dir)
        return _to_dict(row)


def _to_dict(row: CFBypassProfile) -> dict:
    return {
        "id":                   row.id,
        "profile_name":         row.profile_name,
        "county_id":            row.county_id,
        "portal_url":           row.portal_url,
        "status":               row.status,
        "last_warmed_at":       row.last_warmed_at,
        "last_validated_at":    row.last_validated_at,
        "last_failure_at":      row.last_failure_at,
        "last_failure_reason":  row.last_failure_reason,
        "profile_dir_path":     row.profile_dir_path,
        "validation_ttl_minutes": row.validation_ttl_minutes,
        "profile_blob_size":    row.profile_blob_size,
        "profile_blob_at":      row.profile_blob_at,
    }


def _update_status(profile_name: str, **fields) -> None:
    """Patch fields on the profile row. Auto-stamps updated_at."""
    with get_db_context() as session:
        row = session.execute(
            select(CFBypassProfile).where(CFBypassProfile.profile_name == profile_name)
        ).scalar_one_or_none()
        if row is None:
            logger.warning("[CFSess] cannot update missing profile name=%s", profile_name)
            return
        for k, v in fields.items():
            setattr(row, k, v)
        row.updated_at = datetime.now(timezone.utc)


# ─────────────────────────────────────────────────────────────────────────────
# Blob backup (Phase 2)
# ─────────────────────────────────────────────────────────────────────────────

def _zip_profile_dir(profile_dir: Path) -> bytes:
    """Zip the profile directory into bytes for DB backup. Skips bulky caches."""
    skip_subdirs = {"Cache", "Code Cache", "GPUCache", "ShaderCache", "Service Worker"}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for path in profile_dir.rglob("*"):
            if path.is_dir():
                continue
            # Skip large, regeneratable caches
            if any(part in skip_subdirs for part in path.relative_to(profile_dir).parts):
                continue
            try:
                zf.write(path, arcname=str(path.relative_to(profile_dir)))
            except (OSError, FileNotFoundError):
                continue
    return buf.getvalue()


def _unzip_profile_blob(blob: bytes, profile_dir: Path) -> None:
    """Restore a zipped profile blob onto disk under profile_dir."""
    profile_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(io.BytesIO(blob), mode="r") as zf:
        zf.extractall(profile_dir)


def _save_blob_to_db(profile_name: str, profile_dir: Path) -> None:
    """Zip + persist the on-disk profile to the DB. Non-critical — swallows errors."""
    try:
        data = _zip_profile_dir(profile_dir)
    except Exception as exc:
        logger.warning("[CFSess] failed to zip profile %s for backup: %s", profile_name, exc)
        return
    try:
        with get_db_context() as session:
            row = session.execute(
                select(CFBypassProfile).where(CFBypassProfile.profile_name == profile_name)
            ).scalar_one_or_none()
            if row is not None:
                row.profile_blob = data
                row.profile_blob_size = len(data)
                row.profile_blob_at = datetime.now(timezone.utc)
                logger.info("[CFSess] backup saved name=%s size=%d bytes",
                            profile_name, len(data))
    except Exception as exc:
        logger.warning("[CFSess] failed to persist blob for %s: %s", profile_name, exc)


def _restore_blob_from_db_if_missing(profile_name: str, profile_dir: Path) -> bool:
    """If the on-disk profile is missing but DB has a backup, restore it. Returns True if restored."""
    if profile_dir.exists() and any(profile_dir.iterdir()):
        return False
    with get_db_context() as session:
        row = session.execute(
            select(CFBypassProfile).where(CFBypassProfile.profile_name == profile_name)
        ).scalar_one_or_none()
        if row is None or row.profile_blob is None:
            return False
        try:
            _unzip_profile_blob(row.profile_blob, profile_dir)
            logger.info("[CFSess] restored profile %s from DB backup (%d bytes)",
                        profile_name, row.profile_blob_size or 0)
            return True
        except Exception as exc:
            logger.warning("[CFSess] failed to restore blob for %s: %s", profile_name, exc)
            return False


# ─────────────────────────────────────────────────────────────────────────────
# Validation (cheap health check)
# ─────────────────────────────────────────────────────────────────────────────

async def _validate_profile(profile_name: str, portal_url: str) -> bool:
    """
    Launch Edge with the profile, GET the portal URL, check for CF challenge.
    Returns True if the session is healthy, False if blocked or unreachable.

    Per docs/PINELLAS_CLOUDFLARE_BYPASS.md: Playwright headless is detected
    by CF on Linux even with a warmed profile. Use headful via Xvfb (same
    pattern as _auto_warm) and poll up to 30s for CF to auto-resolve.
    """
    import os as _os
    import subprocess as _subprocess
    from src.utils.cf_persistent_browser import (
        CFProfileNotWarmedError, _launch_edge,
    )

    _xvfb_proc = None
    if not _os.environ.get("DISPLAY"):
        try:
            _xvfb_proc = _subprocess.Popen(
                ["Xvfb", ":99", "-screen", "0", "1280x800x24"],
                stdout=_subprocess.DEVNULL,
                stderr=_subprocess.DEVNULL,
            )
            _os.environ["DISPLAY"] = ":99"
            await asyncio.sleep(0.5)
        except FileNotFoundError:
            pass

    try:
        async with _launch_edge(
            profile_name=profile_name,
            headless=not bool(_os.environ.get("DISPLAY")),
        ) as ctx:
            page = await ctx.new_page()
            try:
                await page.goto(portal_url, wait_until="domcontentloaded",
                                timeout=HEALTH_CHECK_TIMEOUT_SECONDS * 1000)
            except Exception as exc:
                logger.warning("[CFSess] validate %s: page load failed: %s", profile_name, exc)
                return False

            challenge_markers = ("just a moment", "checking your browser",
                                 "cf-challenge", "cf-mitigated")
            # Poll up to 30s for CF JS challenge to auto-resolve.
            title = ""
            body = ""
            for _ in range(15):
                await asyncio.sleep(2)
                try:
                    title = (await page.title()) or ""
                    body = await page.evaluate(
                        "document.body ? document.body.innerText.slice(0,500) : ''"
                    )
                except Exception:
                    continue
                text = (title + " " + body).lower()
                if not any(m in text for m in challenge_markers):
                    return True

        logger.info("[CFSess] validate %s: CF challenge did not clear within 30s",
                    profile_name)
        return False
    except CFProfileNotWarmedError as exc:
        logger.info("[CFSess] validate %s: profile not warmed (%s)", profile_name, exc)
        return False
    except Exception as exc:
        logger.warning("[CFSess] validate %s: unexpected error: %s", profile_name, exc)
        return False
    finally:
        if _xvfb_proc is not None:
            try:
                _xvfb_proc.terminate()
                _xvfb_proc.wait(timeout=3)
            except Exception:
                pass


# ─────────────────────────────────────────────────────────────────────────────
# Auto-warm (nodriver headless)
# ─────────────────────────────────────────────────────────────────────────────

async def _auto_warm(profile_name: str, portal_url: str) -> bool:
    """
    Open the portal once via nodriver against the persistent Edge profile
    so Cloudflare's JS challenge can auto-resolve. Returns True on success.
    """
    from src.utils.cf_persistent_browser import find_edge_binary

    try:
        import nodriver as uc  # type: ignore
    except ImportError:
        logger.error("[CFSess] auto-warm %s: nodriver not installed", profile_name)
        return False

    edge_path = find_edge_binary()
    if not edge_path:
        logger.error("[CFSess] auto-warm %s: no Edge binary found", profile_name)
        return False

    profile_dir = PROFILE_BASE_DIR / f"edge_profile_{profile_name}"
    profile_dir.mkdir(parents=True, exist_ok=True)
    _patch_nodriver_cookie_parser()

    # Per docs/PINELLAS_CLOUDFLARE_BYPASS.md, headless=False is the validated
    # path — nodriver's automation-marker patches are less effective in headless
    # mode and CF can still detect the bot. On a headless Linux server we
    # provide a virtual display via Xvfb (auto-started below if DISPLAY is unset).
    import os as _os
    import subprocess as _subprocess
    _xvfb_proc = None
    if not _os.environ.get("DISPLAY"):
        try:
            _xvfb_proc = _subprocess.Popen(
                ["Xvfb", ":99", "-screen", "0", "1280x800x24"],
                stdout=_subprocess.DEVNULL,
                stderr=_subprocess.DEVNULL,
            )
            _os.environ["DISPLAY"] = ":99"
            await asyncio.sleep(0.5)
            logger.info("[CFSess] %s: started Xvfb on :99 for nodriver headful warm", profile_name)
        except FileNotFoundError:
            logger.warning("[CFSess] %s: Xvfb not installed — falling back to headless mode "
                           "(may fail to pass CF challenge)", profile_name)

    browser = None
    try:
        browser = await asyncio.wait_for(
            uc.start(
                headless=not bool(_os.environ.get("DISPLAY")),
                browser_executable_path=edge_path,
                user_data_dir=str(profile_dir),
            ),
            timeout=AUTO_WARM_TIMEOUT_SECONDS,
        )
        page = await browser.get(portal_url)
        # Give CF up to 30s to clear; check periodically for the search form
        deadline = asyncio.get_event_loop().time() + 30
        cleared = False
        while asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(2)
            try:
                title = await page.evaluate("document.title")
                body  = await page.evaluate("document.body ? document.body.innerText.slice(0,500) : ''")
            except Exception:
                continue
            text = (str(title) + " " + str(body)).lower()
            if "just a moment" not in text and "checking your browser" not in text:
                cleared = True
                break
        return cleared
    except asyncio.TimeoutError:
        logger.warning("[CFSess] auto-warm %s: timed out", profile_name)
        return False
    except Exception as exc:
        logger.warning("[CFSess] auto-warm %s: %s", profile_name, exc)
        return False
    finally:
        if browser is not None:
            try:
                result = browser.stop()
                if asyncio.iscoroutine(result):
                    await result
            except Exception:
                pass
        if _xvfb_proc is not None:
            try:
                _xvfb_proc.terminate()
                _xvfb_proc.wait(timeout=3)
            except Exception:
                pass


def _patch_nodriver_cookie_parser() -> None:
    """
    nodriver's Cookie.from_json crashes on missing sameParty / sourceScheme
    / sourcePort fields that newer Edge omits. Patch defensively (same fix
    as scripts/experiments/cf_capture_pinellas_clerk.py).
    """
    try:
        from nacl.signing import SigningKey  # noqa: F401 — ensure PyNaCl is loadable
    except ImportError:
        pass
    try:
        from nodriver.cdp import network as _nd_network  # type: ignore
        _orig = _nd_network.Cookie.from_json

        def _safe_from_json(json: dict):
            return _orig({
                **json,
                "sameParty":    json.get("sameParty", False),
                "sourceScheme": json.get("sourceScheme", "Unset"),
                "sourcePort":   json.get("sourcePort", -1),
            })

        if getattr(_nd_network.Cookie.from_json, "__name__", "") != "_safe_from_json":
            _nd_network.Cookie.from_json = staticmethod(_safe_from_json)  # type: ignore
    except Exception:
        # If anything goes wrong patching, fall through — nodriver may crash
        # later with a clear KeyError, which is loud enough to debug.
        pass


# ─────────────────────────────────────────────────────────────────────────────
# Alert routing
# ─────────────────────────────────────────────────────────────────────────────

def _send_operator_alert(profile_name: str, portal_url: str, reason: str) -> None:
    try:
        from src.services.email import send_alert
        send_alert(
            subject=f"[FA] CF-bypass profile {profile_name} needs operator attention",
            body=(
                f"The Cloudflare-bypass session for '{profile_name}' is no longer "
                f"usable and auto-warm failed.\n\n"
                f"Portal:  {portal_url}\n"
                f"Reason:  {reason}\n\n"
                f"To recover, run on the scraping host:\n"
                f"  python -m src.utils.cf_session_manager --warm {profile_name}\n\n"
                f"If a CAPTCHA appears during warming, solve it manually in the\n"
                f"Edge window. The session is durable across re-warms — once a\n"
                f"new clearance is earned, the engine will resume automatically."
            ),
        )
    except Exception as exc:
        logger.warning("[CFSess] failed to send operator alert: %s", exc)


# ─────────────────────────────────────────────────────────────────────────────
# Public entry points
# ─────────────────────────────────────────────────────────────────────────────

def _is_validation_fresh(row: dict) -> bool:
    """Return True if last_validated_at is inside the per-profile TTL window."""
    if row["status"] != "ready" or row["last_validated_at"] is None:
        return False
    ttl = timedelta(minutes=row["validation_ttl_minutes"] or DEFAULT_VALIDATION_TTL_MINUTES)
    last = row["last_validated_at"]
    if last.tzinfo is None:
        last = last.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) - last < ttl


async def ensure_ready(
    profile_name: str,
    county_id: str,
    portal_url: str,
) -> Path:
    """
    Return the on-disk profile dir, ready for Playwright to launch against.

    Sequence:
      1. Find or create the profile row.
      2. If validated recently (within TTL), return the dir path.
      3. Else: validate; on healthy → mark validated, return.
      4. On unhealthy: try auto-warm; on success → save backup, return.
      5. On auto-warm failure: alert operator, raise CFBypassFailedError.

    Caller is expected to use this from inside a Playwright async context.
    """
    row = _get_or_create(profile_name, county_id, portal_url)
    profile_dir = Path(row["profile_dir_path"])

    # Restore from DB backup if disk is empty but DB has a snapshot
    if not profile_dir.exists() or not any(profile_dir.iterdir()):
        _restore_blob_from_db_if_missing(profile_name, profile_dir)

    if _is_validation_fresh(row):
        logger.info("[CFSess] %s: using cached session (validated %s)",
                    profile_name, row["last_validated_at"])
        return profile_dir

    # Validate
    healthy = await _validate_profile(profile_name, portal_url)
    if healthy:
        _update_status(profile_name,
                       status="ready",
                       last_validated_at=datetime.now(timezone.utc),
                       last_failure_at=None,
                       last_failure_reason=None)
        return profile_dir

    # Unhealthy — try auto-warm
    logger.info("[CFSess] %s: validation failed, attempting auto-warm…", profile_name)
    _update_status(profile_name, status="warming")
    warmed = await _auto_warm(profile_name, portal_url)
    if warmed:
        now = datetime.now(timezone.utc)
        _update_status(profile_name,
                       status="ready",
                       last_warmed_at=now,
                       last_validated_at=now,
                       last_failure_at=None,
                       last_failure_reason=None)
        # Backup the freshly-warmed profile in the background of the same call
        _save_blob_to_db(profile_name, profile_dir)
        logger.info("[CFSess] %s: auto-warmed successfully", profile_name)
        return profile_dir

    # Auto-warm failed — escalate
    reason = "auto-warm timed out or CAPTCHA required"
    _update_status(profile_name,
                   status="expired",
                   last_failure_at=datetime.now(timezone.utc),
                   last_failure_reason=reason)
    _send_operator_alert(profile_name, portal_url, reason)
    raise CFBypassFailedError(
        f"CF-bypass profile '{profile_name}' is expired and auto-warm failed. "
        f"Run: python -m src.utils.cf_session_manager --warm {profile_name}"
    )


def mark_failed_during_scrape(profile_name: str, reason: str = "cf_challenge_mid_scrape") -> None:
    """
    Mark a profile as expired after a scraper engine hits a CF challenge
    mid-flow. The next ensure_ready() call will re-validate (or re-warm).
    """
    logger.info("[CFSess] %s: marked expired mid-scrape (%s)", profile_name, reason)
    _update_status(
        profile_name,
        status="expired",
        last_failure_at=datetime.now(timezone.utc),
        last_failure_reason=reason,
    )


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

async def _cli_warm(profile_name: str) -> int:
    with get_db_context() as session:
        row = session.execute(
            select(CFBypassProfile).where(CFBypassProfile.profile_name == profile_name)
        ).scalar_one_or_none()
        if row is None:
            print(f"[!] No profile '{profile_name}' found. Configure the source first "
                  f"so the row is created on first scrape run.")
            return 1
        portal_url = row.portal_url
        county_id = row.county_id

    try:
        path = await ensure_ready(profile_name, county_id, portal_url)
        print(f"[+] Profile '{profile_name}' ready at {path}")
        return 0
    except CFBypassFailedError as exc:
        print(f"[!] {exc}")
        return 2


def _cli_status() -> int:
    with get_db_context() as session:
        rows = session.execute(select(CFBypassProfile)).scalars().all()
        if not rows:
            print("No CF-bypass profiles configured yet.")
            return 0
        for r in rows:
            warmed = r.last_warmed_at.isoformat(timespec="seconds") if r.last_warmed_at else "-"
            validated = r.last_validated_at.isoformat(timespec="seconds") if r.last_validated_at else "-"
            blob = f"{r.profile_blob_size or 0} bytes" if r.profile_blob_size else "none"
            print(f"{r.profile_name:<24} {r.status:<10} "
                  f"warmed={warmed}  validated={validated}  blob={blob}")
            if r.last_failure_reason:
                print(f"    last_failure: {r.last_failure_reason}")
    return 0


def main() -> int:
    # Load .env so CF_BYPASS_BROWSER_PATH and other env vars are available
    # when running as a standalone CLI (they are not inherited from the shell).
    try:
        from dotenv import load_dotenv
        load_dotenv(PROJECT_ROOT / ".env", override=False)
    except ImportError:
        pass

    ap = argparse.ArgumentParser(description="CF-bypass session manager")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--warm",     metavar="PROFILE_NAME", help="Validate + auto-warm a profile")
    g.add_argument("--validate", metavar="PROFILE_NAME", help="Run a health check (no warm)")
    g.add_argument("--status",   action="store_true",   help="List all profiles")
    args = ap.parse_args()

    if args.status:
        return _cli_status()
    if args.warm:
        return asyncio.run(_cli_warm(args.warm))
    if args.validate:
        # Just a validate, no warm
        with get_db_context() as session:
            row = session.execute(
                select(CFBypassProfile).where(CFBypassProfile.profile_name == args.validate)
            ).scalar_one_or_none()
            if row is None:
                print(f"[!] No profile '{args.validate}' found.")
                return 1
            portal_url = row.portal_url
        ok = asyncio.run(_validate_profile(args.validate, portal_url))
        print(f"{args.validate}: {'HEALTHY' if ok else 'EXPIRED'}")
        if ok:
            _update_status(args.validate,
                           status="ready",
                           last_validated_at=datetime.now(timezone.utc))
        return 0 if ok else 2
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
