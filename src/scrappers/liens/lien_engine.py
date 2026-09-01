"""
Lien, Deed & Judgment Data Collection Pipeline — county-agnostic, browser-use only.

Navigates the county clerk's public access portal, performs a date-range Document Type
search with no type filter (returns all types), downloads the CSV export, then
normalises county-specific columns and categorises records in Python into
liens / deeds / judgments and saves them to the appropriate directories.

County differences are driven entirely by config — no county-specific code here:
  - Column renames:   source["ori_column_map"]          (e.g. DirectName→Grantor)
  - BookPage split:   source["ori_book_page_col"]        (e.g. "BookPage" → Book + Page)
  - Doc type remap:   source["ori_doc_type_map"]         (verbose → canonical labels)
  - Filer detection:  county_cfg["city_filer_keywords"]  (code lien detection)
  - Filer labels:     county_cfg["code_lien_type_map"]   (TCL/CCL style labels)

These fields are stored in the CountySource.special_flags JSONB and County table
and populated via the admin UI.

Usage:
    python -m src.scrappers.liens.lien_engine --county-id hillsborough
    python -m src.scrappers.liens.lien_engine --county-id pinellas --start-date 2026-05-01 --end-date 2026-05-07
    python -m src.scrappers.liens.lien_engine --county-id hillsborough --load-to-db --headful
"""

import asyncio
import json
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

from config.constants import (
    RAW_LIEN_DIR,
    PROCESSED_DATA_DIR,
    PROCESSED_LIENS_DIR,
    PROCESSED_DEEDS_DIR,
    PROCESSED_JUDGMENTS_DIR,
    DOWNLOAD_FILE_PATTERNS,
    TEMP_DOWNLOADS_DIR,
    BROWSER_DOWNLOAD_TEMP_PATTERN,
    HILLSCLERK_PUBLIC_ACCESS_URL,
    BROWSER_MODEL,
    BROWSER_TEMPERATURE,
)
from src.utils.action_sequence import PlaywrightCodeError
from src.utils.county_config import get_county_config
from src.utils.http_helpers import (
    STEALTH_UA, STEALTH_ARGS, apply_stealth_to_browser_use,
    get_playwright_proxy, get_browser_use_proxy,
)
from src.utils.logger import setup_logging, get_logger
from src.utils.scraper_run_tracking import scraper_run
from src.utils.scraper_outcome_classifier import classify_exception
from config.scraper_outcomes import ScraperOutcome

setup_logging()
logger = get_logger(__name__)

for _d in (RAW_LIEN_DIR, PROCESSED_DATA_DIR, PROCESSED_LIENS_DIR,
           PROCESSED_DEEDS_DIR, PROCESSED_JUDGMENTS_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# Document-type → canonical normalization, doc-type → signal-bucket routing,
# and column renames (DirectName→Grantor etc.) all live on the per-source
# CountyColumnMapping row. Build/edit them via the Column Mappings admin UI
# or via the migration `y9z0a1b2c3d4_promote_scrape_mode_and_collapse_ori`.

# HOA / IRS keywords are still applied below for sub-categorising LIEN rows
# (HOA lien vs IRS tax lien). Could move into a value-map on the bucketed
# DataFrame later; for now they stay here.
_HOA_KEYWORDS = frozenset(["ASSOCIATION", "HOA", "CONDO", "COMMUNITY",
                            "VILLAGE", "TOWNHOME", "PROPERTY OWNERS"])
_IRS_KEYWORDS = frozenset([
    "UNITED STATES", "INTERNAL REVENUE",
    "STATE OF FLORIDA", "DEPARTMENT OF REVENUE",
    "FLORIDA DEPARTMENT OF REVENUE", "FLORIDA STATE REVENUE", "FL DEPT OF REVENUE",
])
# Medical subrogation liens (healthcare filers against insurers) — not property liens.
_MEDICAL_FILER_KEYWORDS = frozenset([
    "HEALTH ADVENT", "ADVENT HEALTH", "ADVENTHEALTH",
    "HCA FLORIDA", "BAYCARE", "TAMPA GENERAL",
    "FLORIDA HOSPITAL", "JOHNS HOPKINS ALL CHILDRENS",
])
_INSURANCE_GRANTEE_KEYWORDS = frozenset([
    "GEICO", "PROGRESSIVE", "ALLSTATE", "STATE FARM", "FARM STATE",
    "DIRECT GENERAL", "USAA", "NATIONWIDE", "TRAVELERS", "LIBERTY MUTUAL",
])

# Bucket name → output directory. row_routing in the CountyColumnMapping uses
# these bucket names; this map tells the pipeline where each one lands.
_BUCKET_DIRS = {
    "liens":     PROCESSED_LIENS_DIR,
    "deeds":     PROCESSED_DEEDS_DIR,
    "judgments": PROCESSED_JUDGMENTS_DIR,
}


# ---------------------------------------------------------------------------
# LLM helpers
# ---------------------------------------------------------------------------

def _make_llm():
    from browser_use import ChatAnthropic
    from config.settings import get_settings
    settings = get_settings()
    return ChatAnthropic(
        model=BROWSER_MODEL,
        timeout=180,
        api_key=settings.anthropic_api_key.get_secret_value(),
        temperature=BROWSER_TEMPERATURE,
    )


def build_agent_task(source: dict, start_str: str, end_str: str) -> str:
    """
    Generate a browser-use agent task from county source metadata + date range.
    Uses Claude to produce step-by-step instructions tailored to the source's
    navigation_hint and description. Falls back to a template on LLM failure.
    """
    import anthropic
    from config.settings import get_settings

    clerk_url = source.get("url", HILLSCLERK_PUBLIC_ACCESS_URL)
    description = source.get("description", "")
    nav_hint = source.get("navigation_hint", "") or ""

    meta = {
        "clerk_url": clerk_url,
        "start_date": start_str,
        "end_date": end_str,
        "description": description,
        "navigation_hint": nav_hint,
    }

    system_prompt = (
        "You generate browser-automation task instructions for a browser-use Agent. "
        "The agent controls a Chromium browser. Write a concise, numbered task in plain English. "
        "The agent must trigger a file download (CSV export). "
        "Do NOT add any explanation outside the task text. "
        "Write all URLs as plain text only — do NOT wrap them in markdown bold (**), "
        "backticks, or any other formatting."
    )

    user_prompt = f"""Generate a browser-use agent task to download lien/deed/judgment records from a county clerk portal.

Source metadata:
{json.dumps(meta, indent=2)}

Requirements:
- Navigate to clerk_url.
- Select the "Document Type" search type from the left navigation panel.
- Leave the document type field EMPTY or unselected (to retrieve ALL document types).
- Set the filed-from date to start_date and filed-to date to end_date (MM/DD/YYYY format).
- Submit the search and wait for results to load.
- If an error appears saying results exceed 6000, note it and still attempt the export.
- Click the "Export to Spreadsheet" or "Export Results" button to download the CSV.
- Wait at least 15 seconds for the download to complete before finishing.
- Do not navigate away or open new tabs during the download.
- Keep the task under 300 words.
"""

    try:
        settings = get_settings()
        client = anthropic.Anthropic(
            api_key=settings.anthropic_api_key.get_secret_value()
        )
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=512,
            temperature=0,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}],
        )
        task = response.content[0].text.strip()
        # Strip markdown bold markers that browser-use's URL extractor would absorb
        # into the URL (e.g. **https://foo.com/** → https://foo.com/)
        import re as _re
        task = _re.sub(r'\*\*(https?://[^\s*]+)\*\*', r'\1', task)
        task = _re.sub(r'(https?://[^\s*]+)\*\*', r'\1', task)
        logger.info("[LLM] Generated agent task:\n%s", task)
        return task
    except Exception as e:
        logger.warning("[LLM] Task generation failed (%s) — using template fallback", e)
        return _template_task(source, start_str, end_str)


def _template_task(source: dict, start_str: str, end_str: str) -> str:
    clerk_url = source.get("url", HILLSCLERK_PUBLIC_ACCESS_URL)
    nav_hint = source.get("navigation_hint", "") or ""
    return (
        f"Go to {clerk_url}.\n"
        "Wait 5 seconds for the page to fully load.\n"
        f"{nav_hint}\n"
        "On the left panel, click 'Document Type' under Search Type.\n"
        "Leave the Document Type field blank — do NOT select a specific type.\n"
        f"Set the Filed From date to {start_str} and Filed To date to {end_str}.\n"
        "Click the Search button and wait for results to load.\n"
        "If a result-count pager appears, note the count.\n"
        "Click the 'Export to Spreadsheet' or 'Export Results' button.\n"
        "Wait 20 seconds for the CSV download to complete.\n"
        "Do not open new tabs or navigate away."
    )


# ---------------------------------------------------------------------------
# Browser-use agent (file download mode)
# ---------------------------------------------------------------------------



async def run_browser_agent(
    task: str,
    download_dir: Path,
    headful: bool = False,
    cf_profile: Optional[dict] = None,
    no_proxy: bool = False,
) -> tuple:
    """
    Run a browser-use Agent that triggers a file download.

    cf_profile = {"edge_path": "...", "profile_dir": "..."} switches the launch
    into persistent-Edge-profile mode for Cloudflare-protected portals:
      - Edge binary instead of bundled Chromium (TLS fingerprint that earned
        the cf_clearance cookie)
      - user_data_dir = warmed profile (cookie + Turnstile state persist there)
      - No proxy, no stealth init script — both would disturb the fingerprint
        Cloudflare hashed when issuing the cookie.

    Returns (history, start_time, error) — error is the real exception object
    if the agent run itself raised, so the caller can classify what happened
    instead of guessing from a bare None.
    """
    from browser_use import Agent, Browser

    llm = _make_llm()

    browser_kwargs = dict(
        headless=not headful,
        disable_security=True,
        downloads_path=str(download_dir),
        ignore_default_args=["--enable-automation"],
        minimum_wait_page_load_time=1.5,
        wait_between_actions=1.0,
        args=[
            "--no-sandbox",
            "--disable-blink-features=AutomationControlled",
            "--window-size=1920,1080",
        ],
    )

    if cf_profile:
        # Kill any lingering Edge processes holding the profile's singleton lock.
        # If a previous run crashed without cleanup, Edge holds the user_data_dir
        # and the new subprocess can't bind its CDP port → _wait_for_cdp_url times out.
        import psutil as _psutil
        profile_dir = cf_profile["profile_dir"]
        for _proc in _psutil.process_iter(["pid", "name", "cmdline"]):
            try:
                _cmdline = _proc.info.get("cmdline") or []
                if any(profile_dir in str(_arg) for _arg in _cmdline):
                    logger.info("[CF] Killing stale Edge PID=%s holding profile lock", _proc.pid)
                    _proc.kill()
                    _proc.wait(timeout=5)
            except (_psutil.NoSuchProcess, _psutil.AccessDenied, _psutil.TimeoutExpired):
                pass
        await asyncio.sleep(1)

        browser_kwargs.update(
            executable_path=cf_profile["edge_path"],
            user_data_dir=cf_profile["profile_dir"],
            proxy=None,
            enable_default_extensions=False,
        )
        logger.info(
            "[Agent] CF-bypass mode — Edge=%s profile=%s",
            cf_profile["edge_path"], cf_profile["profile_dir"],
        )
    else:
        browser_kwargs.update(
            user_agent=STEALTH_UA,
            enable_default_extensions=True,
            # proxy=None if no_proxy else get_browser_use_proxy(),
        )

    browser = Browser(**browser_kwargs)
    await browser.start()

    if not cf_profile:
        await apply_stealth_to_browser_use(browser)
        logger.info("[Stealth] Stealth fingerprint patches injected")

    agent = Agent(task=task, llm=llm, browser=browser, max_steps=60, use_judge=False)

    start_time = time.time()
    logger.info("[Agent] Starting browser-use agent (download mode)...")
    try:
        history = await agent.run()
        if not history.is_done():
            logger.warning("[Agent] Agent did not complete within step budget")
        return history, start_time, None
    except Exception as e:
        logger.error("[Agent] Run failed: %s", e)
        logger.debug(traceback.format_exc())
        return None, start_time, e


# ---------------------------------------------------------------------------
# Playwright selector mode (used when source has playwright_code stored)
# ---------------------------------------------------------------------------

async def _scrape_with_playwright(
    playwright_code: str,
    source: dict,
    start_str: str,
    end_str: str,
    download_dir: Path,
    headful: bool = False,
    cf_profile: Optional[dict] = None,
    no_proxy: bool = False,
) -> Optional[pd.DataFrame]:
    """
    Execute stored playwright_code against a Playwright page.

    For CF-bypass sources, launches Edge via launch_persistent_context so the
    warmed profile's clearance cookies are loaded natively — no subprocess CDP
    tricks, no watchdog deadlocks.
    """
    from playwright.async_api import async_playwright
    from src.utils.action_sequence import execute_playwright_code

    url = source.get("url", "")
    county_id = source.get("county_id", "")

    # CF-bypass requires headful Playwright on Linux — headless launches get
    # re-challenged by Cloudflare even on a warmed profile (see
    # docs/PINELLAS_CLOUDFLARE_BYPASS.md). Auto-start Xvfb to provide a virtual
    # display if one isn't already attached.
    import os as _os
    import subprocess as _subprocess
    xvfb_proc = None
    if cf_profile and not _os.environ.get("DISPLAY"):
        try:
            xvfb_proc = _subprocess.Popen(
                ["Xvfb", ":99", "-screen", "0", "1920x1080x24"],
                stdout=_subprocess.DEVNULL, stderr=_subprocess.DEVNULL,
            )
            _os.environ["DISPLAY"] = ":99"
            await asyncio.sleep(0.5)
            logger.info("[CF/PW] Started Xvfb on :99 for headful Playwright run")
        except FileNotFoundError:
            logger.warning("[CF/PW] Xvfb not installed — falling back to headless "
                           "(CF may re-challenge)")

    # Force headful when CF bypass is in play so automation markers don't
    # trigger a fresh CF JS challenge.
    pw_headless = (not headful) and (cf_profile is None)

    async with async_playwright() as p:
        if cf_profile:
            import psutil as _psutil
            profile_dir = cf_profile["profile_dir"]
            for _proc in _psutil.process_iter(["pid", "name", "cmdline"]):
                try:
                    _cmdline = _proc.info.get("cmdline") or []
                    if any(profile_dir in str(_arg) for _arg in _cmdline):
                        logger.info("[CF/PW] Killing stale Edge PID=%s before launch", _proc.pid)
                        _proc.kill()
                        _proc.wait(timeout=5)
                except (_psutil.NoSuchProcess, _psutil.AccessDenied, _psutil.TimeoutExpired):
                    pass
            await asyncio.sleep(1)

            context = await p.chromium.launch_persistent_context(
                user_data_dir=profile_dir,
                executable_path=cf_profile["edge_path"],
                headless=pw_headless,
                accept_downloads=True,
                downloads_path=str(download_dir),
                args=[
                    "--no-sandbox",
                    "--disable-blink-features=AutomationControlled",
                    "--window-size=1920,1080",
                ],
                ignore_default_args=["--enable-automation"],
            )
            page = context.pages[0] if context.pages else await context.new_page()
        else:
            browser = await p.chromium.launch(
                headless=not headful,
                downloads_path=str(download_dir),
                args=[
                    "--no-sandbox",
                    "--disable-blink-features=AutomationControlled",
                    "--window-size=1920,1080",
                ],
                ignore_default_args=["--enable-automation"],
            )
            # _proxy = None if no_proxy else get_playwright_proxy()
            context = await browser.new_context(accept_downloads=True)  # proxy=_proxy
            page = await context.new_page()

        try:
            df = await execute_playwright_code(
                playwright_code,
                page,
                download_dir,
                placeholders={"url": url, "start_date": start_str, "end_date": end_str},
                county_id=county_id,
            )
            # --- DEBUG: capture page state when result is empty ---
            if df is None or df.empty:
                try:
                    import datetime as _dt
                    import re as _re
                    _ts = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
                    _dbg = download_dir / "debug"
                    _dbg.mkdir(parents=True, exist_ok=True)

                    _url = page.url
                    _title = await page.title()
                    logger.error("[liens][debug] URL: %s | Title: %s", _url, _title)

                    _shot = _dbg / f"liens_debug_{county_id}_{_ts}.png"
                    await page.screenshot(path=str(_shot), full_page=True)
                    logger.error("[liens][debug] Screenshot: %s", _shot)

                    _html_path = _dbg / f"liens_debug_{county_id}_{_ts}.html"
                    _html_path.write_text(await page.content(), encoding="utf-8")
                    logger.error("[liens][debug] HTML: %s", _html_path)

                    _keywords = _re.compile(
                        r"export|results|search|download|excel|csv", _re.IGNORECASE
                    )
                    _links = await page.locator("a, button").all()
                    _found = []
                    for _el in _links:
                        try:
                            _txt = (await _el.inner_text()).strip()
                            _href = await _el.get_attribute("href") or ""
                            if _keywords.search(_txt) or _keywords.search(_href):
                                _found.append(f"{_txt!r} href={_href!r}")
                        except Exception:
                            pass
                    logger.error("[liens][debug] Export-related elements (%d): %s",
                                 len(_found), _found or ["<none found>"])
                except Exception as _dbg_exc:
                    logger.error("[liens][debug] Debug capture failed: %s", _dbg_exc)
            # --- END DEBUG ---
            logger.info("[Playwright] Scraped %d rows", len(df) if df is not None else 0)
            return df
        except Exception as e:
            logger.error("[Playwright] Scrape failed: %s", e)
            logger.debug(traceback.format_exc())
            # --- DEBUG: capture on unexpected exception bubble-up ---
            try:
                import datetime as _dt
                _ts = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
                _dbg = download_dir / "debug"
                _dbg.mkdir(parents=True, exist_ok=True)
                logger.error("[liens][debug] URL: %s | Title: %s", page.url, await page.title())
                _shot = _dbg / f"liens_debug_{county_id}_{_ts}.png"
                await page.screenshot(path=str(_shot), full_page=True)
                logger.error("[liens][debug] Screenshot: %s", _shot)
            except Exception as _dbg_exc:
                logger.error("[liens][debug] Debug capture failed: %s", _dbg_exc)
            # --- END DEBUG ---
            return None
        finally:
            try:
                await context.close()
            except Exception:
                pass
            if xvfb_proc is not None:
                try:
                    xvfb_proc.terminate()
                    xvfb_proc.wait(timeout=3)
                except Exception:
                    pass


# ---------------------------------------------------------------------------
# nodriver selector mode (scrape_mode in {"nodriver_only", "nodriver_then_ai"})
# ---------------------------------------------------------------------------
# Playwright's CDP automation fingerprint gets a fresh Cloudflare Turnstile
# challenge on *every* navigation, even against an already-warmed persistent
# Edge profile — cookie/profile reuse across engines stopped being enough
# once Cloudflare tightened Turnstile enforcement on Pinellas Clerk's portal
# (see docs/PINELLAS_CLOUDFLARE_BYPASS.md). nodriver is the only driver in
# this codebase that reliably clears the challenge (same driver
# cf_session_manager uses to warm/validate the profile).
#
# This function only launches the browser and hands off to the DB-stored
# `playwright_code` for the actual portal scrape steps, exactly like
# _scrape_with_playwright() does for the Playwright driver — see
# docs/MULTI_COUNTY_SCRAPING_ARCHITECTURE.md Section 5. `execute_playwright_code()`
# is driver-agnostic: it just calls the stored run_scrape(page, ...) with
# whatever `page`-like object it's given, so a nodriver Tab works exactly like
# a Playwright Page. Portal-specific selectors/steps belong in the stored
# code (DB-editable, versioned, audited) — never hardcoded here.

async def _scrape_with_nodriver(
    playwright_code: str,
    source: dict,
    start_str: str,
    end_str: str,
    download_dir: Path,
    cf_profile: dict,
) -> tuple:
    """Returns (df, outcome, error_message). outcome/error_message are None on
    success (df not None) — mirrors run_browser_agent's (history, start_time,
    error) triple so the caller can classify_exception()-quality failures
    instead of guessing from a bare None (see ScraperOutcome)."""
    import nodriver as uc
    from src.utils.action_sequence import execute_playwright_code
    from src.utils.cf_session_manager import _patch_nodriver_cookie_parser

    _patch_nodriver_cookie_parser()

    url = source.get("url", "")
    county_id = source.get("county_id", "")
    edge_path = cf_profile["edge_path"]
    profile_dir = cf_profile["profile_dir"]

    import os as _os
    import subprocess as _subprocess
    xvfb_proc = None
    if not _os.environ.get("DISPLAY"):
        try:
            xvfb_proc = _subprocess.Popen(
                ["Xvfb", ":99", "-screen", "0", "1920x1080x24"],
                stdout=_subprocess.DEVNULL, stderr=_subprocess.DEVNULL,
            )
            _os.environ["DISPLAY"] = ":99"
            await asyncio.sleep(0.5)
            logger.info("[CF/ND] Started Xvfb on :99 for nodriver scrape")
        except FileNotFoundError:
            logger.warning("[CF/ND] Xvfb not installed — nodriver may hit a fresh CF challenge")

    debug_dir = download_dir / "debug"

    async def _dump_debug(page, label: str) -> None:
        try:
            debug_dir.mkdir(parents=True, exist_ok=True)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            shot = debug_dir / f"liens_nd_debug_{label}_{ts}.jpg"
            html = debug_dir / f"liens_nd_debug_{label}_{ts}.html"
            await page.save_screenshot(filename=str(shot), full_page=True)
            content = await page.get_content()
            html.write_text(content or "", encoding="utf-8")
            logger.error("[CF/ND] Debug capture: %s | %s", shot, html)
        except Exception as exc:
            logger.warning("[CF/ND] Debug capture failed: %s", exc)

    browser = None
    page = None
    try:
        browser = await uc.start(
            headless=False,
            browser_executable_path=edge_path,
            user_data_dir=profile_dir,
        )
        page = await browser.get(url)

        df = await execute_playwright_code(
            playwright_code,
            page,
            download_dir,
            placeholders={"url": url, "start_date": start_str, "end_date": end_str},
            county_id=county_id,
        )
        logger.info("[CF/ND] Scraped %d rows", len(df) if df is not None else 0)
        return df, None, None
    except PlaywrightCodeError as exc:
        msg = str(exc)
        if "CF_CHALLENGE_NOT_CLEARED" in msg:
            # Not a code-quality failure — the stored scraper is fine, the
            # warmed profile just went stale (see cf_session_manager's
            # mark_failed_during_scrape). Do NOT clear_playwright_code() for
            # this; the caller re-warms the profile on the next run instead.
            # Cloudflare's own wall rejected the warmed profile — a
            # source-side gate, same bucket as the cf_bypass_failed case
            # above, not our internal bug or a bare timeout.
            logger.error("[CF/ND] Turnstile challenge never cleared: %s", exc)
            try:
                from src.utils.cf_session_manager import mark_failed_during_scrape
                mark_failed_during_scrape(
                    source.get("cf_bypass_profile_name") or f"{county_id}_clerk",
                    reason="turnstile_not_cleared",
                )
            except Exception:
                logger.warning("[CF/ND] failed to mark profile expired (non-critical)")
            outcome = ScraperOutcome.SOURCE_ERROR.value
        else:
            # A genuine code-quality failure (portal layout changed, download
            # never landed, etc.) — follow the documented self-heal cycle:
            # clear the stored code so an operator sees a to-do in
            # playwright_code_history (reason="cleared") and the next run
            # falls back to AI (nodriver_then_ai) or aborts (nodriver_only)
            # until a human re-authors and re-approves fresh code. Bucketed
            # as INTERNAL_ERROR — our stored selectors need a fix, same as
            # column_mapping_failed / no_edge_binary above.
            logger.error("[CF/ND] Stored playwright_code failed: %s", exc)
            source_id = source.get("source_id")
            if source_id is not None:
                try:
                    from src.utils.action_sequence import clear_playwright_code
                    clear_playwright_code(county_id, source_id)
                except Exception:
                    logger.warning("[CF/ND] failed to clear playwright_code (non-critical)")
            outcome = ScraperOutcome.INTERNAL_ERROR.value
        if page is not None:
            await _dump_debug(page, "code_error")
        return None, outcome, msg[:500]
    except Exception as e:
        logger.error("[CF/ND] nodriver scrape failed: %s", e)
        logger.debug(traceback.format_exc())
        if page is not None:
            await _dump_debug(page, "launch_error")
        return None, classify_exception(e), str(e)[:500]
    finally:
        if browser is not None:
            try:
                result = browser.stop()
                if asyncio.iscoroutine(result):
                    await result
            except Exception:
                pass
        if xvfb_proc is not None:
            try:
                xvfb_proc.terminate()
                xvfb_proc.wait(timeout=3)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Download file detection
# ---------------------------------------------------------------------------

def _locate_download(download_dir: Path, start_time: float) -> Optional[Path]:
    def recent_candidates(folder: Path):
        if not folder.exists():
            return []
        paths = []
        for pattern in DOWNLOAD_FILE_PATTERNS:
            for f in folder.glob(pattern):
                try:
                    if f.stat().st_mtime >= start_time:
                        paths.append(f)
                except FileNotFoundError:
                    continue
        return paths

    candidates = recent_candidates(download_dir)
    if TEMP_DOWNLOADS_DIR.exists():
        for temp_dir in TEMP_DOWNLOADS_DIR.glob(BROWSER_DOWNLOAD_TEMP_PATTERN):
            candidates.extend(recent_candidates(temp_dir))

    if not candidates:
        logger.warning("[Locate] No downloaded files found after agent run")
        return None

    most_recent = max(candidates, key=lambda p: p.stat().st_mtime)
    logger.info("[Locate] Found download: %s", most_recent)
    return most_recent


# ---------------------------------------------------------------------------
# Lien sub-categorisation for the LIEN bucket
# ---------------------------------------------------------------------------
# The row_routing on the CountyColumnMapping splits rows into top-level buckets
# (liens / deeds / judgments) by raw DocType value. The
# generic "liens" bucket is then further sub-categorised here, based on party
# names (HOA vs IRS vs city/county filer vs mechanics).
# This sub-bucketing relies on per-county filer keywords from county_cfg, so
# it can't easily move to the CountyColumnMapping today.

def _code_lien_label(grantor_upper: str, grantee_upper: str,
                     city_filer_keywords: list, code_lien_type_map: dict) -> Optional[str]:
    """
    Return the code-lien document_type label if either party is a known
    government filer, else None.
    """
    combined = grantor_upper + " " + grantee_upper
    matched_keyword = next(
        (kw for kw in city_filer_keywords if kw.upper() in combined),
        None,
    )
    if not matched_keyword:
        return None

    for type_code, city_name in code_lien_type_map.items():
        if city_name and city_name.upper() in matched_keyword.upper():
            return f"CODE LIENS ({type_code})"
        if city_name is None and "COUNTY" in matched_keyword.upper():
            return f"CODE LIENS ({type_code})"

    return "CODE LIEN"


def _sub_categorise_liens(df: pd.DataFrame, county_cfg: dict) -> pd.DataFrame:
    """
    Stamp a `document_type` column on the liens bucket based on doc type +
    party names. This is the only piece of routing left in code; everything
    else has moved into CountyColumnMapping.row_routing.
    """
    if df.empty:
        return df

    city_filer_keywords: list = county_cfg.get("city_filer_keywords") or []
    code_lien_type_map: dict = county_cfg.get("code_lien_type_map") or {}

    def label_row(row) -> str:
        doc_type = str(row.get("DocType", "") or "").strip().upper()
        grantor = str(row.get("Grantor", "") or "").upper()
        grantee = str(row.get("Grantee", "") or "").upper()

        if "LIS PENDENS" in doc_type:
            return "LIS PENDENS"
        if doc_type == "TAX LIEN":
            return "TAX LIEN"
        if doc_type in ("LIEN", "FINANCING STATEMENT", "CORPORATE LIEN"):
            if any(kw in grantor for kw in _MEDICAL_FILER_KEYWORDS):
                return "MEDICAL LIEN"
            if any(kw in grantee for kw in _INSURANCE_GRANTEE_KEYWORDS):
                return "MEDICAL LIEN"
            if any(kw in grantor or kw in grantee for kw in _HOA_KEYWORDS):
                return "HOA LIENS (HL)"
            if any(kw in grantor or kw in grantee for kw in _IRS_KEYWORDS):
                return "TAX LIEN"
            label = _code_lien_label(grantor, grantee, city_filer_keywords, code_lien_type_map)
            if label:
                return label
            return "MECHANICS LIENS (ML)"
        # Routing put this row in the liens bucket but we couldn't sub-label it.
        return doc_type or "LIEN"

    df = df.copy()
    df["document_type"] = df.apply(label_row, axis=1)
    return df


def _save_buckets(buckets: dict, county_cfg: dict) -> dict:
    """
    Persist each routed bucket DataFrame to its processed/<bucket>/new/ dir.
    The liens bucket goes through _sub_categorise_liens first so individual
    HOA / TAX / MECHANICS / CODE labels still land in the CSV.
    Returns {filename: row_count}.
    """
    today_str = datetime.now().strftime("%Y%m%d")
    file_counts: dict = {}

    bucket_filename = {
        "liens":     f"all_liens_{today_str}.csv",
        "deeds":     f"all_deeds_{today_str}.csv",
        "judgments": f"all_judgments_{today_str}.csv",
    }

    for bucket, df_bucket in buckets.items():
        if bucket == "_default":
            # No row_routing on this mapping — write the whole frame into liens.
            bucket = "liens"
        if df_bucket.empty:
            continue
        out_dir = _BUCKET_DIRS.get(bucket)
        if out_dir is None:
            logger.warning("[Categorize] Unknown bucket %r — skipping %d rows",
                           bucket, len(df_bucket))
            continue
        if bucket == "liens":
            df_bucket = _sub_categorise_liens(df_bucket, county_cfg)
        else:
            df_bucket = df_bucket.copy()
            if "document_type" not in df_bucket.columns:
                # Stamp a canonical label so downstream loaders have it
                df_bucket["document_type"] = bucket.upper()

        # Ensure Filing Amt column exists for loaders that expect it
        if "Filing Amt" not in df_bucket.columns:
            df_bucket["Filing Amt"] = None

        new_dir = out_dir / "new"
        new_dir.mkdir(parents=True, exist_ok=True)
        out = new_dir / bucket_filename[bucket]
        df_bucket.to_csv(out, index=False)
        file_counts[out.name] = len(df_bucket)
        logger.info("[Categorize] Saved %d records → %s", len(df_bucket), out.name)
        for dt in df_bucket["document_type"].unique():
            logger.info("  %s: %d", dt, (df_bucket["document_type"] == dt).sum())

    return file_counts


# ---------------------------------------------------------------------------
# Raw file loader
# ---------------------------------------------------------------------------

def process_lien_data(file_path: Path) -> pd.DataFrame:
    """Load the downloaded CSV/Excel file into a DataFrame."""
    logger.info("[Process] Loading: %s", file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"Lien data file not found: {file_path}")

    if file_path.suffix.lower() in (".xls", ".xlsx"):
        df = pd.read_excel(file_path)
        logger.info("[Process] Loaded %d records from Excel", len(df))
        return df

    for enc in ("utf-8", "latin1", "cp1252"):
        try:
            df = pd.read_csv(file_path, encoding=enc)
            logger.info("[Process] Loaded %d records (enc=%s)", len(df), enc)
            return df
        except (UnicodeDecodeError, pd.errors.ParserError):
            continue

    raise ValueError(f"Could not read file with any known encoding: {file_path}")


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

async def run_lien_pipeline(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    county_id: str = "hillsborough",
    headful: bool = False,
    load_to_db: bool = False,
    no_proxy: bool = False,
) -> bool:
    """County-agnostic lien/deed/judgment scrape for a date range.

    Every scrape goes through the browser-use Agent. When the source has
    `cf_bypass_required=true`, the Agent is launched against the warmed Edge
    profile validated by cf_session_manager.ensure_ready — same fingerprint
    that earned the cf_clearance cookie, so the Cloudflare wall stays down.
    """
    _t0 = time.monotonic()

    county_cfg = get_county_config(county_id)
    source = county_cfg["sources"].get("liens")
    if not source:
        logger.error("[%s] No liens source configured — add one via admin UI", county_id)
        return False

    if source.get("prr_only"):
        logger.info("[%s] Liens source is PRR-only — load CSV manually", county_id)
        return False

    with scraper_run("lien_unknown", county_id) as run:
        _today    = datetime.now()
        _end_dt   = datetime.strptime(end_date,   "%Y-%m-%d") if end_date   else _today
        _start_dt = datetime.strptime(start_date, "%Y-%m-%d") if start_date else _end_dt
        start_str = _start_dt.strftime("%m/%d/%Y")
        end_str   = _end_dt.strftime("%m/%d/%Y")

        cf_required = bool(source.get("cf_bypass_required"))

        logger.info("=" * 70)
        logger.info("LIEN/DEED/JUDGMENT PIPELINE — %s", county_cfg["display_name"].upper())
        logger.info("Date range: %s → %s  headful=%s  load_to_db=%s  cf_bypass=%s",
                    start_str, end_str, headful, load_to_db, cf_required)
        logger.info("=" * 70)

        # --- Warm / validate Cloudflare profile if portal requires it ------------
        cf_profile: Optional[dict] = None
        if cf_required:
            from src.utils.cf_session_manager import ensure_ready, CFBypassFailedError
            from src.utils.cf_persistent_browser import find_edge_binary, profile_dir_for

            profile_name = source.get("cf_bypass_profile_name") or f"{county_id}_clerk"
            portal_url = source.get("url", "")
            try:
                profile_dir = await ensure_ready(
                    profile_name=profile_name,
                    county_id=county_id,
                    portal_url=portal_url,
                )
            except CFBypassFailedError as exc:
                logger.error("[CF] Profile unusable for %s: %s", profile_name, exc)
                # Cloudflare's own anti-bot wall rejected the warmed profile —
                # a source-side gate, not our internal bug or a bare timeout.
                run.fail(ScraperOutcome.SOURCE_ERROR.value, error_message=f"cf_bypass_failed: {exc}"[:500])
                return False

            edge_path = find_edge_binary()
            if not edge_path:
                logger.error("[CF] No Edge binary found — set CF_BYPASS_BROWSER_PATH")
                run.fail(ScraperOutcome.INTERNAL_ERROR.value, error_message="no_edge_binary: CF_BYPASS_BROWSER_PATH not set")
                return False

            cf_profile = {"edge_path": edge_path, "profile_dir": str(profile_dir)}

        # --- Dispatch on scrape_mode (DB-driven — see
        # docs/MULTI_COUNTY_SCRAPING_ARCHITECTURE.md Section 4/9) --------------
        # nodriver_only / nodriver_then_ai: CF-protected portals (Cloudflare
        #   Turnstile defeats Playwright's CDP fingerprint even on a warmed
        #   profile) — same stored playwright_code contract, different browser
        #   driver. playwright_only / playwright_then_ai: standard Playwright
        #   selector mode. Both "_then_ai" variants fall back to the browser-use
        #   agent on failure; both "_only" variants abort.
        playwright_code = source.get("playwright_code") or ""
        scrape_mode = source.get("scrape_mode", "")
        use_ai_fallback = False

        used_selector_mode = False
        nd_outcome = nd_error = None
        if scrape_mode in ("nodriver_only", "nodriver_then_ai") and playwright_code:
            if cf_profile is None:
                logger.error("[Pipeline] scrape_mode=%s requires cf_bypass_required=true", scrape_mode)
                run.fail(
                    ScraperOutcome.INTERNAL_ERROR.value,
                    error_message=f"nodriver_mode_missing_cf_profile: scrape_mode={scrape_mode}",
                )
                return False
            logger.info("[Pipeline] Using nodriver selector mode (%s)", scrape_mode)
            df, nd_outcome, nd_error = await _scrape_with_nodriver(
                playwright_code, source, start_str, end_str, RAW_LIEN_DIR, cf_profile=cf_profile,
            )
            used_selector_mode = True
        elif scrape_mode in ("playwright_only", "playwright_then_ai") and playwright_code:
            logger.info("[Pipeline] Using Playwright selector mode (%s)", scrape_mode)
            df = await _scrape_with_playwright(
                playwright_code, source, start_str, end_str, RAW_LIEN_DIR,
                headful=headful, cf_profile=cf_profile, no_proxy=no_proxy,
            )
            used_selector_mode = True

        if used_selector_mode:
            if df is None:
                if scrape_mode in ("nodriver_then_ai", "playwright_then_ai"):
                    logger.warning(
                        "[Pipeline] Selector scrape failed for '%s' — falling back to browser-use AI agent",
                        county_id,
                    )
                    use_ai_fallback = True
                else:
                    logger.error("[Pipeline] Selector scrape failed")
                    if nd_outcome is not None:
                        # _scrape_with_nodriver classifies its own failures
                        # (CF challenge vs stored-code bug) — trust it.
                        run.fail(nd_outcome, error_message=nd_error)
                    else:
                        # _scrape_with_playwright swallows its own exception and
                        # returns None either way — can't tell empty-but-clean
                        # from actually-broken here.
                        run.fail(
                            ScraperOutcome.UNKNOWN.value,
                            error_message="selector_scrape_failed (ambiguous: empty result vs scrape failure)",
                        )
                    return False
            elif df.empty:
                logger.info("[Pipeline] Selector scrape returned no records")
                run.no_data()
                return True

        if not used_selector_mode or use_ai_fallback:
            task = build_agent_task(source, start_str, end_str)
            history, start_time, agent_exc = await run_browser_agent(
                task, RAW_LIEN_DIR,
                headful=headful,
                cf_profile=cf_profile,
                no_proxy=no_proxy,
            )

            if history is None:
                logger.error("[Pipeline] Agent failed to run")
                outcome = classify_exception(agent_exc) if agent_exc is not None else ScraperOutcome.UNKNOWN.value
                run.fail(outcome, error_message=str(agent_exc)[:500] if agent_exc is not None else "agent run failed with no exception captured")
                return False

            await asyncio.sleep(5)

            downloaded_file = _locate_download(RAW_LIEN_DIR, start_time)
            if not downloaded_file:
                logger.error("[Pipeline] No download detected after agent run")
                run.fail(ScraperOutcome.UNKNOWN.value, error_message="agent completed but no downloaded file was located")
                return False

            try:
                df = process_lien_data(downloaded_file)
            except Exception as e:
                logger.error("[Pipeline] Failed to process downloaded file: %s", e)
                run.fail(classify_exception(e), error_message=str(e)[:500])
                return False

            if df.empty:
                logger.info("[Pipeline] Downloaded file is empty — no records for this date range")
                run.no_data()
                return True

        # Resolve the ColumnMapping for this source. Renames, BookPage split, doc-
        # type value normalisation, and row routing all happen inside ColumnMapper.
        from src.loaders.column_mapper import ColumnMapper, NeedsMappingError, SkipMapping
        source_id = source.get("source_id")
        try:
            mapper = ColumnMapper()
            # Trigger the LLM auto-map only when no approved/pending mapping exists
            # for this source. The synthesized migration row keeps the legacy
            # Hillsborough / Pinellas configs working without an LLM call.
            mapper.get_or_create("liens", source_id, df)
            # Pass the scraped columns so a stale approved mapping (different shape)
            # falls back to a matching pending row instead of being returned as-is.
            mapping_row = ColumnMapper.fetch_mapping_row(
                source_id, raw_cols=list(df.columns)
            )
        except (NeedsMappingError, SkipMapping) as exc:
            logger.error("[Pipeline] ColumnMapper unavailable: %s", exc)
            # Missing/broken admin-configured mapping — our-side setup issue.
            run.fail(ScraperOutcome.INTERNAL_ERROR.value, error_message=f"column_mapping_failed: {exc}"[:500])
            return False

        if mapping_row is None:
            logger.error("[Pipeline] No CountyColumnMapping for source_id=%s", source_id)
            run.fail(ScraperOutcome.INTERNAL_ERROR.value, error_message="no_column_mapping")
            return False

        buckets = ColumnMapper.apply_transformations(df, mapping_row)
        logger.info(
            "[Pipeline] Routed %d records into %d buckets: %s",
            sum(len(b) for b in buckets.values()),
            len(buckets),
            {k: len(v) for k, v in buckets.items()},
        )

        file_counts = _save_buckets(buckets, county_cfg)
        total = sum(file_counts.values())

        logger.info("=" * 70)
        logger.info("LIEN PIPELINE COMPLETE — %d records categorised", total)
        logger.info("=" * 70)

        if load_to_db:
            # Per-subtype stats are written inside _load_to_database via
            # scraper_db_helper:_record_load_stats — one row each for
            # lien_tcl/lien_ccl/lien_hoa/lien_ml/lien_tl/lis_pendens plus
            # 'no_data' rows for absent subtypes. Suppress the wrapper's own
            # completion write so it doesn't clobber those via the
            # (run_date, source_type, county_id) upsert.
            _load_to_database(county_id, _t0)
            run.suppress_completion_write()
        else:
            run.success(total_scraped=total)

    return True


# ---------------------------------------------------------------------------
# DB loader
# ---------------------------------------------------------------------------

def _load_to_database(county_id: str, t0: float) -> None:
    from src.utils.scraper_db_helper import load_scraped_data_to_db

    load_targets = [
        ("liens",     PROCESSED_LIENS_DIR,     "liens"),
        ("deeds",     PROCESSED_DEEDS_DIR,     "deeds"),
        ("judgments", PROCESSED_JUDGMENTS_DIR, "judgments"),
    ]

    for label, type_dir, data_type in load_targets:
        new_dir = type_dir / "new"
        csv_files = sorted(new_dir.glob("*.csv"), key=lambda p: p.stat().st_mtime, reverse=True) \
            if new_dir.exists() else []
        if csv_files:
            logger.info("[DB] Loading %s: %s", label, csv_files[0])
            try:
                load_scraped_data_to_db(data_type, csv_files[0], destination_dir=type_dir,
                                        county_id=county_id)
            except Exception as e:
                logger.error("[DB] Failed to load %s: %s", label, e)
        else:
            logger.info("[DB] No new %s records to load", label)



# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    import argparse
    # Load .env so CF_BYPASS_BROWSER_PATH, ANTHROPIC_API_KEY, etc. are visible
    # to find_edge_binary() and other helpers that read os.environ directly.
    try:
        from dotenv import load_dotenv
        from pathlib import Path
        load_dotenv(Path(__file__).resolve().parents[3] / ".env", override=False)
    except ImportError:
        pass
    from src.utils.scraper_db_helper import add_load_to_db_arg

    parser = argparse.ArgumentParser(
        description="Lien/deed/judgment scraper — county-agnostic, browser-use only"
    )
    parser.add_argument("--county-id", default="hillsborough",
                        help="County identifier (default: hillsborough)")
    parser.add_argument("--start-date", default=None,
                        help="Start date YYYY-MM-DD (default: today)")
    parser.add_argument("--end-date", default=None,
                        help="End date YYYY-MM-DD (default: today)")
    parser.add_argument("--headful", action="store_true",
                        help="Run browser in visible mode")
    parser.add_argument("--no-proxy", dest="no_proxy", action="store_true", default=False,
                        help="Disable Oxylabs proxy for all requests")
    add_load_to_db_arg(parser)

    args = parser.parse_args()
    success = asyncio.run(run_lien_pipeline(
        start_date=args.start_date,
        end_date=args.end_date,
        county_id=args.county_id,
        headful=args.headful,
        load_to_db=args.load_to_db,
        no_proxy=args.no_proxy,
    ))

    import sys
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
