"""
DBPR Company Name (DBA) Scraper — per-license lookup on myfloridalicense.com

The bulk CILB CSV extract loaded by dbpr_engine.py has no business/DBA name,
only the individual qualifier (dbpr_contacts.full_name). This job fills the
company name (DBA) one license at a time from the public licensee search at
myfloridalicense.com, and is the only source for it. See docs/adr/0003.

Flow per license:
  1. Landing page (mode=0): select the "Search by License Number" radio
     (SearchType=LicNbr), submit (SelectSearchType).
  2. License-number form (mode=1): fill LicNbr, submit (Search1).
  3. Results page lists one row per name with a Name Type column. Each row is
     "<License Type>  <Name>  <DBA|Primary>  <License Number>  <Status>".
     Take the DBA row whose License Number == the searched license.
       - DBA row found       -> company_name = that name, status 'found'
       - only Primary row(s) -> company_name NULL, status 'none'
       - no row matches lic / zero records -> status 'failed' (retried)

The license number printed beside each name is the cross-check: a row is only
trusted when its license number matches the one we searched. (The per-result
detail page is NOT used — its links carry an empty SID and bounce back to the
landing page; the results table already exposes the DBA name + license number.)

Status state machine (company_name_status):
  pending -> found | none | failed
  found / none are terminal (never re-scraped).
  failed is retried on the next run (site down, mismatch, transient error).

Targets company_name_status IN ('pending', 'failed'), oldest first, table-wide
(not county-scoped). Serial with a polite delay. Aborts and alerts after
_MAX_CONSECUTIVE_FAILURES consecutive failures (site likely down / blocking).

Run:
    python -m src.scrappers.dbpr.dbpr_company_scraper
    python -m src.scrappers.dbpr.dbpr_company_scraper --limit 50 --dry-run
    python -m src.scrappers.dbpr.dbpr_company_scraper --headful --debug

Cron (weekly, Sunday 02:30 UTC — after dbpr_engine 02:00 creates new rows):
    30 2 * * 0 cd /app && python -m src.scrappers.dbpr.dbpr_company_scraper >> /var/log/cron/dbpr_company.log 2>&1
"""

import argparse
import asyncio
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from src.core.database import get_db_context
from src.core.models import DBPRContact
from src.utils.http_helpers import (
    STEALTH_UA,
    STEALTH_ARGS,
    apply_stealth_to_page,
    get_playwright_proxy,
)
from src.utils.scraper_db_helper import record_scraper_stats
from config.scraper_outcomes import ScraperOutcome
from src.utils.logger import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)

# Public licensee search — landing page with the search-type chooser.
_LANDING_URL = "https://www.myfloridalicense.com/wl11.asp?mode=0&SID="

_DELAY_SECONDS = 1.5                # polite delay between licenses
_MAX_CONSECUTIVE_FAILURES = 20      # abort + alert if the site is down/blocking
_NAV_TIMEOUT_MS = 30_000
_DEBUG_DIR = Path("data/dbpr/debug")

# Each results row renders (tab-separated) as, e.g.:
#   Certified General Contractor\t5 STAR QUALITY CONSTRUCTION, INC.\tDBA\tCGC062592
#   Certified General Contractor\tMINGLE, CRAIG S\tPrimary\tCGC062592
# Capture (name, name_type, license_number) per row.
_ROW_RE = re.compile(
    r"\t([^\t\n]+?)\t(DBA|Primary)\t([A-Za-z]{2,5}\d+)",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def _norm_license(value: Optional[str]) -> str:
    """Normalize a license number for comparison (strip, upper, no spaces)."""
    return re.sub(r"\s+", "", (value or "")).upper()


def _resolve_company(results_text: str, license_number: str) -> tuple[str, Optional[str]]:
    """
    Parse the results page inner-text and resolve the company name for the
    searched license. Returns (status, company_name).

    A row is only trusted when its license number matches the searched one
    (the cross-check). DBA row -> 'found'; only Primary -> 'none'; no matching
    row / zero records -> 'failed'.
    """
    want = _norm_license(license_number)
    rows = [
        (name.strip(), name_type.upper(), _norm_license(lic))
        for name, name_type, lic in _ROW_RE.findall(results_text)
    ]
    matching = [r for r in rows if r[2] == want]

    if not matching:
        return "failed", None

    dba = next((r for r in matching if r[1] == "DBA"), None)
    # "INDIVIDUAL" is the site's sentinel for a sole proprietor with no business
    # name — treat it as no DBA, not a company name.
    if dba and dba[0] and dba[0].strip().upper() != "INDIVIDUAL":
        return "found", dba[0][:255]

    # License found but only Primary name(s) / sole proprietor — no business DBA.
    return "none", None


# ---------------------------------------------------------------------------
# Single-license scrape
# ---------------------------------------------------------------------------

async def _scrape_one(page, license_number: str, debug: bool) -> tuple[str, Optional[str]]:
    """
    Look up one license. Returns (status, company_name) where status is one of
    'found' | 'none' | 'failed'. Raises only on hard navigation errors (caller
    counts those toward the consecutive-failure abort).
    """
    # Step 1 — landing page: choose "Search by License Number" and continue.
    await page.goto(_LANDING_URL, timeout=_NAV_TIMEOUT_MS, wait_until="domcontentloaded")
    await page.check("input[name=SearchType][value='LicNbr']", timeout=5_000)
    await page.click("button[name=SelectSearchType]", timeout=5_000)
    await page.wait_for_load_state("domcontentloaded", timeout=_NAV_TIMEOUT_MS)

    # Step 2 — license-number form: fill and search.
    await page.locator("input[name=LicNbr]").first.fill(license_number, timeout=5_000)
    await page.locator("button[name=Search1]").first.click(timeout=5_000)
    await page.wait_for_load_state("domcontentloaded", timeout=_NAV_TIMEOUT_MS)

    # Step 3 — results page.
    results_text = await page.inner_text("body")

    if debug:
        _DEBUG_DIR.mkdir(parents=True, exist_ok=True)
        await page.screenshot(path=str(_DEBUG_DIR / f"{license_number}.png"), full_page=True)

    if re.search(r"no records found", results_text, re.I):
        logger.info("[DBPRCompany] %s: zero results", license_number)
        return "failed", None

    status, company = _resolve_company(results_text, license_number)
    if status == "found":
        logger.info("[DBPRCompany] %s -> %s", license_number, company)
    elif status == "none":
        logger.info("[DBPRCompany] %s: license found, no DBA", license_number)
    else:
        logger.warning("[DBPRCompany] %s: no results row matched the license", license_number)
    return status, company


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def _persist(contact_id: int, status: str, company_name: Optional[str]) -> None:
    now = datetime.now(timezone.utc)
    with get_db_context() as db:
        contact = db.get(DBPRContact, contact_id)
        if contact is None:
            return
        contact.company_name_status = status
        if status == "found":
            contact.company_name = company_name
        contact.company_name_scraped_at = now
        contact.updated_at = now
        db.add(contact)


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------

async def _run(limit: Optional[int], dry_run: bool, headless: bool, debug: bool) -> dict:
    from playwright.async_api import async_playwright

    stats = {
        "scanned": 0, "found": 0, "none": 0, "failed": 0, "aborted": False,
    }
    started = time.monotonic()

    # Pull the work set: pending + failed (retries), oldest first.
    with get_db_context() as db:
        q = (
            db.query(DBPRContact.id, DBPRContact.license_number)
            .filter(DBPRContact.company_name_status.in_(("pending", "failed")))
            .order_by(DBPRContact.created_at.asc())
        )
        if limit:
            q = q.limit(limit)
        work = [(row.id, row.license_number) for row in q.all()]

    if not work:
        logger.info("[DBPRCompany] No pending/failed contacts — nothing to do")
        return stats

    logger.info("[DBPRCompany] %d licenses to scrape (limit=%s, dry_run=%s)",
                len(work), limit, dry_run)

    if dry_run:
        for cid, lic in work[:10]:
            logger.info("[DBPRCompany DRY RUN] would scrape %s (id=%d)", lic, cid)
        logger.info("[DBPRCompany DRY RUN] %d licenses — no browser, no DB writes", len(work))
        stats["scanned"] = len(work)
        return stats

    consecutive_failures = 0

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=headless,
            args=STEALTH_ARGS,
            # proxy=get_playwright_proxy(),
        )
        context = await browser.new_context(
            user_agent=STEALTH_UA,
            viewport={"width": 1920, "height": 1080},
        )
        page = await context.new_page()
        try:
            await apply_stealth_to_page(page)
        except (ModuleNotFoundError, ImportError):
            pass

        try:
            for cid, lic in work:
                stats["scanned"] += 1
                await asyncio.sleep(_DELAY_SECONDS)
                try:
                    status, company = await _scrape_one(page, lic, debug)
                except Exception as e:
                    logger.warning("[DBPRCompany] %s: scrape error: %s", lic, e)
                    status, company = "failed", None

                _persist(cid, status, company)
                stats[status] += 1
                consecutive_failures = consecutive_failures + 1 if status == "failed" else 0

                if consecutive_failures >= _MAX_CONSECUTIVE_FAILURES:
                    stats["aborted"] = True
                    logger.error(
                        "[DBPRCompany] %d consecutive failures — aborting run",
                        consecutive_failures,
                    )
                    _alert_site_down(consecutive_failures, stats)
                    break
        finally:
            await context.close()
            await browser.close()

    duration = time.monotonic() - started
    _record_stats(stats, duration)
    logger.info(
        "[DBPRCompany] Complete — scanned=%d found=%d none=%d failed=%d aborted=%s (%.0fs)",
        stats["scanned"], stats["found"], stats["none"], stats["failed"],
        stats["aborted"], duration,
    )
    return stats


def _record_stats(stats: dict, duration: float) -> None:
    """Write the run to scraper_run_stats (source_type='dbpr_company')."""
    # aborted means _MAX_CONSECUTIVE_FAILURES per-license lookups failed in a
    # row (site likely down/blocking, per this module's docstring) — a
    # source-side condition, not our bug. run_success is already False in
    # that branch (not stats["aborted"]), which agrees with SOURCE_ERROR's
    # forced derivation, so no override conflict.
    record_scraper_stats(
        source_type="dbpr_company",
        total_scraped=stats["scanned"],
        matched=stats["found"],          # DBA found
        unmatched=stats["failed"],       # mismatch / error / zero-result (retried)
        skipped=stats["none"],           # license legitimately has no DBA
        error_type="scraper_error" if stats["aborted"] else "none",
        error_message="aborted after consecutive failures" if stats["aborted"] else None,
        outcome=ScraperOutcome.SOURCE_ERROR.value if stats["aborted"] else None,
        duration_seconds=round(duration, 2),
        county_id="all",
    )


def _alert_site_down(consecutive: int, stats: dict) -> None:
    try:
        from src.services.email import send_alert
        send_alert(
            subject="[Forced Action] DBPR company scraper aborted — site likely down",
            body=(
                f"The DBPR company-name scraper hit {consecutive} consecutive "
                f"failures and aborted.\n\n"
                f"Progress this run: scanned={stats['scanned']} found={stats['found']} "
                f"none={stats['none']} failed={stats['failed']}.\n\n"
                f"Likely myfloridalicense.com is down, blocking, or its page layout "
                f"changed. Remaining rows stay 'pending'/'failed' and retry next run."
            ),
        )
    except Exception as e:
        logger.error("[DBPRCompany] Could not send abort alert: %s", e)


def run_dbpr_company_scraper(
    limit: Optional[int] = None,
    dry_run: bool = False,
    headless: bool = True,
    debug: bool = False,
) -> dict:
    return asyncio.run(_run(limit=limit, dry_run=dry_run, headless=headless, debug=debug))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DBPR company-name (DBA) per-license scraper")
    parser.add_argument("--limit", type=int, default=None,
                        help="Max licenses to scrape this run (default: all pending/failed)")
    parser.add_argument("--dry-run", action="store_true",
                        help="List the work set without launching a browser or writing")
    parser.add_argument("--headful", action="store_true",
                        help="Run with a visible browser (debugging)")
    parser.add_argument("--debug", action="store_true",
                        help="Save a full-page screenshot per license to data/dbpr/debug/")
    args = parser.parse_args()

    result = run_dbpr_company_scraper(
        limit=args.limit,
        dry_run=args.dry_run,
        headless=not args.headful,
        debug=args.debug,
    )
    print(result)
