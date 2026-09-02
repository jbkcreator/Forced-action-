"""
Florida Sunbiz (FL Division of Corporations) — LLC piercing enrichment.

For each LLC-owned property pending Sunbiz enrichment, this scraper:
  1. Navigates to search.sunbiz.org via Playwright + playwright-stealth
  2. Searches by LLC name, finds the exact matching row in #search-results
  3. Opens the entity detail page and captures the full HTML
  4. Hands HTML to `parser.parse_sunbiz_detail` (pure function, unit-tested)
  5. Writes a sunbiz_snapshots row (raw HTML + parsed JSONB) and updates the
     Owner row with doc_number, principal_address, registered agent + email,
     entity_status, formation_date, managing_members, sunbiz_enriched_at, and
     sunbiz_status.

On Playwright failure or parser_failed (layout drift): marks the owner
parser_failed, increments stats["failed"], and records run_success=False in
scraper_run_stats so the heartbeat can alert. No browser-use fallback.

No-match (name not found in Sunbiz search results) is not an error — it marks
the owner not_found and increments stats["skipped"].

Usage:
    python -m src.scrappers.sunbiz.sunbiz_engine
    python -m src.scrappers.sunbiz.sunbiz_engine --limit 100
    python -m src.scrappers.sunbiz.sunbiz_engine --dry-run
    python -m src.scrappers.sunbiz.sunbiz_engine --rescore
"""

import asyncio
import re
from datetime import datetime, timezone
from typing import Optional, Tuple

from sqlalchemy.orm import Session

from src.core.database import get_db_context
from src.core.models import Owner, Property, SunbizSnapshot as SunbizSnapshotRow
from src.scrappers.sunbiz.parser import (
    PARSER_VERSION,
    SunbizSnapshot,
    parse_sunbiz_detail,
)
from src.utils.logger import setup_logging, get_logger
from src.utils.http_helpers import STEALTH_UA, STEALTH_ARGS, apply_stealth_to_page

setup_logging()
logger = get_logger(__name__)

SUNBIZ_SEARCH_URL = "https://search.sunbiz.org/Inquiry/CorporationSearch/ByName"
SUNBIZ_BASE = "https://search.sunbiz.org"

_DELAY_SECONDS = 1.5  # polite crawl rate between owners

# Consecutive (not spaced-out) hard Playwright timeouts before treating it as
# Sunbiz throttling the whole session rather than per-owner bad luck. A
# genuine not_found/parser_failed result resets this counter since the site
# clearly responded fine.
_CIRCUIT_BREAKER_THRESHOLD = 4
_CIRCUIT_BREAKER_BACKOFF_SECONDS = 60


# ---------------------------------------------------------------------------
# Name normalization for exact-match comparison
# ---------------------------------------------------------------------------

def _normalize(name: str) -> str:
    name = name.upper()
    name = name.replace("&", "AND")
    name = re.sub(r"[.,;'\"]", "", name)
    # "/" and ":" are treated as word separators for comparison purposes only —
    # Sunbiz's own search route can't handle either character in the query
    # (see _sanitize_search_term), but the registered entity name itself may
    # legitimately contain one (e.g. "MATTAMY TAMPA/SARASOTA LLC"), so a result
    # row's raw text must still normalize the same way as our space-substituted
    # search term for the exact-match comparison to succeed.
    name = re.sub(r"[/:]", " ", name)
    name = re.sub(r"\s+", " ", name)
    return name.strip()


# ---------------------------------------------------------------------------
# Playwright scraper
# ---------------------------------------------------------------------------

async def _scrape_entity_detail(
    page, company_name: str
) -> Tuple[Optional[str], Optional[SunbizSnapshot]]:
    """
    Search Sunbiz for company_name and return (raw_html, parsed_snapshot).

    Returns (None, None) when no exact name match exists in the result table —
    caller should mark owner.sunbiz_status='not_found' (no snapshot written).
    Returns (html, snapshot) on detail-page hit; snapshot.status reflects parser
    outcome ('ok' / 'partial' / 'parser_failed'). On Playwright exception during
    search nav, raises — caller handles AI fallback.
    """
    try:
        await page.goto(SUNBIZ_SEARCH_URL, wait_until="domcontentloaded", timeout=20000)
        await page.fill("#SearchTerm", company_name)
        await page.click("input[value='Search Now']")
        await page.wait_for_selector("#search-results", timeout=15000)
    except Exception as e:
        logger.debug(f"[Sunbiz] Search navigation failed for '{company_name}': {e}")
        raise

    rows = await page.query_selector_all("#search-results table tbody tr")
    detail_url: Optional[str] = None
    normalized_search = _normalize(company_name)

    for row in rows:
        link = await row.query_selector("td.large-width a")
        if not link:
            continue
        result_text = await link.inner_text()
        if _normalize(result_text) == normalized_search:
            href = await link.get_attribute("href")
            if href:
                detail_url = href if href.startswith("http") else SUNBIZ_BASE + href
            break

    if not detail_url:
        logger.debug(f"[Sunbiz] No exact match in results for '{company_name}'")
        return None, None

    try:
        await page.goto(detail_url, wait_until="domcontentloaded", timeout=20000)
    except Exception as e:
        logger.debug(f"[Sunbiz] Detail page load failed: {e}")
        return None, None

    html = await page.content()
    snap = parse_sunbiz_detail(html)
    return html, snap


_SEARCH_NAME_TRUNCATE_SUFFIXES_RE = re.compile(
    r"\s*/\s*(TTEE|TRUSTEE|TR)\s*$", re.IGNORECASE
)

# search.sunbiz.org's own client-side JS builds the results-page URL by
# embedding the raw search term into a route path segment
# (.../SearchResults/EntityName/{term}/Page1) without percent-encoding it.
# A literal "/" or ":" in the term therefore splits into an extra path
# segment the ASP.NET route doesn't recognize, and the server returns a
# generic "resource ... unavailable" page instead of a results table — which
# looks identical to a slow/unresponsive site from the caller's side (a
# #search-results wait_for_selector timeout). Confirmed live against
# search.sunbiz.org for both characters. There is no way to percent-encode
# around this (verified: %2F is rejected by the route the same way).
_URL_BREAKING_CHARS_RE = re.compile(r"[/:]")


def _clean_search_name(name: str) -> str:
    """Build a Sunbiz-searchable term from a raw owner/property name.

    Two distinct problems, both turning into the exact same symptom (a
    #search-results timeout that looks like a scraper failure):

    1. Property records often append a role or mailing annotation to the
       owner name (e.g. "BKE REALTY INVESTMENTS LLC/TTEE", "WATERS XF LLC
       C/O ALTUS GROUP") that isn't part of what's actually registered with
       Sunbiz at all. These are stripped outright.
    2. Any remaining "/" or ":" — whether or not it's part of the real
       registered name (e.g. "MATTAMY TAMPA/SARASOTA LLC", "SEFFNER
       GALATIANS 5:22 LLC") — breaks Sunbiz's own search URL routing. These
       are replaced with a space rather than stripped, since _normalize()
       applies the same substitution when comparing a result row's name, so
       the exact-match comparison still succeeds.
    """
    cleaned = _SEARCH_NAME_TRUNCATE_SUFFIXES_RE.sub("", name).strip()
    upper = cleaned.upper()

    co_idx = upper.find(" C/O ")
    if co_idx != -1:
        cleaned = cleaned[:co_idx].strip()

    cleaned = _URL_BREAKING_CHARS_RE.sub(" ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()

    return cleaned or name


async def _run_playwright_batch(
    owners: list,
    dry_run: bool,
    stats: dict,
    session: Session,
    headless: bool = True,
) -> None:
    from playwright.async_api import async_playwright

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=headless,
            args=STEALTH_ARGS,
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
            total = len(owners)
            consecutive_timeouts = 0
            for idx, owner in enumerate(owners, 1):
                stats["processed"] += 1
                name = owner.owner_name.strip()

                skip_patterns = [
                    "ASSESSED BY DEPT", "ASSESSED BY STATE", "STATE OF FL",
                    "TRUSTEE", " TRUST", "ESTATE OF",
                ]
                if any(p in name.upper() for p in skip_patterns):
                    logger.debug(f"[Sunbiz] Skipping non-LLC entity: {name}")
                    if not dry_run:
                        _mark_not_an_llc(session, owner)
                    stats["skipped"] += 1
                    continue

                search_name = _clean_search_name(name)
                if search_name != name:
                    logger.debug("[Sunbiz] Cleaned search name: '%s' -> '%s'", name, search_name)
                logger.info(f"[Sunbiz] [{idx}/{total}] Searching: {search_name}")
                await asyncio.sleep(_DELAY_SECONDS)

                html: Optional[str] = None
                snap: Optional[SunbizSnapshot] = None
                try:
                    html, snap = await _scrape_entity_detail(page, search_name)
                except Exception as e:
                    # Per-lookup retry: wait briefly and try once more before
                    # marking parser_failed — handles transient portal hiccups
                    # that cause the same company to fail every run otherwise.
                    await asyncio.sleep(3)
                    try:
                        html, snap = await _scrape_entity_detail(page, search_name)
                    except Exception as e2:
                        consecutive_timeouts += 1
                        if consecutive_timeouts >= _CIRCUIT_BREAKER_THRESHOLD:
                            logger.warning(
                                "[Sunbiz] %d consecutive Playwright failures — Sunbiz likely "
                                "throttling this session. Backing off %ds and retrying '%s' once.",
                                consecutive_timeouts, _CIRCUIT_BREAKER_BACKOFF_SECONDS, name,
                            )
                            await asyncio.sleep(_CIRCUIT_BREAKER_BACKOFF_SECONDS)
                            try:
                                html, snap = await _scrape_entity_detail(page, search_name)
                                consecutive_timeouts = 0
                            except Exception as e3:
                                stats["rate_limited"] = True
                                stats["remaining_unprocessed"] = total - idx
                                logger.error(
                                    "[Sunbiz] Still failing after backoff — aborting run with "
                                    "%d owner(s) unprocessed; will resume next cron run. "
                                    "Last error: %s",
                                    stats["remaining_unprocessed"], e3,
                                )
                                break
                        else:
                            logger.warning("[Sunbiz] Playwright failed for '%s': %s", name, e2)
                            if not dry_run:
                                _mark_status(session, owner, "parser_failed")
                            stats["failed"] += 1
                            continue

                consecutive_timeouts = 0

                # No exact match in Sunbiz search results — not an error.
                if snap is None:
                    if not dry_run:
                        _mark_status(session, owner, "not_found")
                    stats["skipped"] += 1
                    continue

                # Parser could not extract detail sections (layout drift).
                if snap.status == "parser_failed":
                    logger.warning("[Sunbiz] Parser failed for '%s' — layout drift", name)

                logger.info(
                    f"[Sunbiz] '{name}' status={snap.status} doc={snap.doc_number} "
                    f"agent={snap.registered_agent_name} members={len(snap.managing_members)}"
                )

                if dry_run:
                    stats["enriched" if snap.status in ("ok", "partial") else "failed"] += 1
                    continue

                _persist_snapshot_and_owner(session, owner, html, snap)
                if snap.status in ("ok", "partial"):
                    stats["enriched"] += 1
                else:
                    stats["failed"] += 1

        finally:
            await context.close()
            await browser.close()

    # Structured outcome log consumed by sunbiz_anomaly_check and log aggregators.
    logger.info(
        "[Sunbiz] batch_complete processed=%d enriched=%d skipped=%d failed=%d "
        "rate_limited=%s remaining_unprocessed=%d",
        stats.get("processed", 0),
        stats.get("enriched", 0),
        stats.get("skipped", 0),
        stats.get("failed", 0),
        stats.get("rate_limited", False),
        stats.get("remaining_unprocessed", 0),
    )


# ---------------------------------------------------------------------------
# DB writers (sync; called from inside async loop via attached Session)
# ---------------------------------------------------------------------------


def _persist_snapshot_and_owner(
    session: Session,
    owner: Owner,
    html: Optional[str],
    snap: SunbizSnapshot,
) -> None:
    """
    Row-locked update of the Owner row + append-only snapshot insert.
    Snapshot is only written when we have a Sunbiz doc number (i.e. status is
    'ok' or 'partial'); parser_failed runs produce no snapshot row but still
    update sunbiz_status on the Owner so the daily task doesn't re-pick them
    until the staleness window elapses.
    """
    session.refresh(owner, with_for_update=True)

    if snap.doc_number and snap.status in ("ok", "partial"):
        session.add(
            SunbizSnapshotRow(
                sunbiz_doc_number=snap.doc_number,
                raw_html=html,
                raw_jsonb=snap.to_jsonb(),
                parser_version=snap.parser_version or PARSER_VERSION,
                status=snap.status,
            )
        )

        owner.sunbiz_doc_number = snap.doc_number
        owner.principal_address = snap.principal_address
        owner.registered_agent_name = snap.registered_agent_name
        owner.registered_agent_address = snap.registered_agent_address
        owner.registered_agent_email = snap.registered_agent_email
        owner.entity_status = snap.entity_status
        owner.formation_date = snap.formation_date
        owner.managing_members = [m.__dict__ for m in snap.managing_members] or None
        owner.sunbiz_status = "matched"
        if owner.owner_type not in ("LLC", "Corporate"):
            owner.owner_type = "LLC"
    else:
        owner.sunbiz_status = "parser_failed"

    owner.sunbiz_enriched_at = datetime.now(timezone.utc)


def _mark_status(session: Session, owner: Owner, status: str) -> None:
    session.refresh(owner, with_for_update=True)
    owner.sunbiz_status = status
    owner.sunbiz_enriched_at = datetime.now(timezone.utc)


def _mark_not_an_llc(session: Session, owner: Owner) -> None:
    _mark_status(session, owner, "not_an_llc")


# ---------------------------------------------------------------------------
# Main enrichment loop (sync entry point)
# ---------------------------------------------------------------------------

def enrich_llc_owners(
    session: Session,
    limit: int = 0,
    dry_run: bool = False,
    county_id: str = "hillsborough",
    headless: bool = True,
) -> dict:
    """
    Query all LLC-owned scored leads (Property → DistressScore join) with no
    registered agent on file and scrape Sunbiz for each.
    """
    from sqlalchemy import or_, desc
    from src.core.models import DistressScore

    stats = {
        "processed": 0, "enriched": 0, "skipped": 0, "failed": 0,
        "rate_limited": False, "remaining_unprocessed": 0,
    }

    llc_keywords = ["%LLC%", "%INC%", "%CORP%", "%LLP%", "%PLLC%", "%LTD%"]

    # Targets: LLC owners that have never been enriched (sunbiz_status='pending')
    # OR previously matched rows past the active-lead staleness window. The
    # staleness refresh job is handled by `src/tasks/sunbiz_enrichment.py` in
    # Phase 5; this entry point picks up new pendings only.
    q = (
        session.query(Owner)
        .join(Property, Property.id == Owner.property_id)
        .join(DistressScore, DistressScore.property_id == Property.id)
        .filter(
            or_(*[Owner.owner_name.ilike(kw) for kw in llc_keywords]),
            Owner.sunbiz_status == "pending",
            Owner.owner_name.isnot(None),
            Property.county_id == county_id,
        )
        .distinct(Owner.id)
        .order_by(Owner.id, desc(DistressScore.final_cds_score))
    )

    if limit:
        q = q.limit(limit)

    owners = q.all()

    total = len(owners)
    logger.info(f"[Sunbiz] Found {total} LLC-owned scored leads without registered agent")

    if total == 0:
        return stats

    asyncio.run(_run_playwright_batch(owners, dry_run, stats, session, headless=headless))

    if not dry_run:
        session.commit()
        logger.info(f"[Sunbiz] Committed {stats['enriched']} registered agent updates to DB")

    return stats


def sunbiz_run_verdict(stats: dict) -> Tuple[bool, Optional[str], Optional[str], Optional[str]]:
    """Single source of truth for a Sunbiz run's health verdict.

    Returns ``(run_success, error_type, error_message, outcome)``. Three outcomes:

      - **rate_limited** — Sunbiz throttled the session (circuit breaker tripped
        on consecutive timeouts). Unprocessed owners keep sunbiz_status='pending'
        and resume next run, so this is informational: ``run_success=True``.
        ``outcome=None`` deliberately — this is a genuine source-side condition,
        but config.scraper_outcomes.derive_run_success would force run_success
        to False for any of the 5 real categories (SOURCE_ERROR included),
        which would regress the exact false-alarm bug this verdict function
        was written to fix (see below). It stays a legacy-only (error_type)
        signal rather than joining the enforced outcome_category vocabulary.
      - **broken** — failures are frequent (``failed >= 3`` AND ``> 15%`` rate),
        i.e. a real site-wide problem: ``run_success=False``,
        ``outcome=SOURCE_ERROR`` (site-wide Playwright failures, not our bug).
      - **healthy** — everything else, including the routine 1-2 no-match /
        one-off Playwright hiccups seen at ~200 owners/day: ``run_success=True``,
        ``outcome=None`` (clean success — no reclassification needed).

    Both entry points (the ``sunbiz_enrichment`` cron task and this standalone
    engine) call this, so the verdict can never drift. It did drift before: the
    cron task used a zero-tolerance ``failed == 0`` rule and false-alarmed every
    single day while enriching ~198/200 owners.
    """
    if stats.get("rate_limited"):
        return True, "rate_limited", (
            f"Circuit breaker tripped after {_CIRCUIT_BREAKER_THRESHOLD} "
            f"consecutive timeouts — aborted with "
            f"{stats.get('remaining_unprocessed', 0)} owner(s) unprocessed; "
            f"will resume next run"
        ), None
    processed = stats.get("processed", 0)
    failed = stats.get("failed", 0)
    failure_rate = failed / processed if processed else 0
    if failed >= 3 and failure_rate > 0.15:
        from config.scraper_outcomes import ScraperOutcome
        return False, "scraper_error", (
            f"{failed} owner(s) failed Playwright scrape "
            f"({failure_rate:.0%} failure rate)"
        ), ScraperOutcome.SOURCE_ERROR.value
    return True, None, None, None


def run_sunbiz_pipeline(
    limit: int = 0,
    dry_run: bool = False,
    rescore: bool = False,
    county_id: str = "hillsborough",
    headless: bool = True,
) -> dict:
    logger.info("=" * 60)
    logger.info("SUNBIZ REGISTERED AGENT ENRICHMENT")
    logger.info(f"Target: all LLC-owned scored leads  county={county_id}")
    logger.info("=" * 60)
    if dry_run:
        logger.info("DRY RUN mode — no DB writes")

    with get_db_context() as session:
        stats = enrich_llc_owners(
            session, limit=limit, dry_run=dry_run,
            county_id=county_id, headless=headless,
        )

    logger.info("=" * 60)
    logger.info("SUNBIZ ENRICHMENT COMPLETE")
    logger.info(f"  Processed : {stats['processed']}")
    logger.info(f"  Enriched  : {stats['enriched']}  (agent name + address found)")
    logger.info(f"  Skipped   : {stats['skipped']}  (no Sunbiz match)")
    logger.info(f"  Failed    : {stats['failed']}")
    logger.info("=" * 60)

    if rescore and stats["enriched"] > 0 and not dry_run:
        logger.info("[Sunbiz] Triggering CDS rescore for enriched LLC properties...")
        try:
            from src.services.cds_engine import MultiVerticalScorer
            with get_db_context() as score_session:
                property_ids = (
                    score_session.query(Owner.property_id)
                    .filter(
                        Owner.owner_type == "LLC",
                        Owner.registered_agent_name.isnot(None),
                    )
                    .all()
                )
                ids = [r[0] for r in property_ids]
                if ids:
                    scorer = MultiVerticalScorer(score_session)
                    scorer.score_properties_by_ids(ids, save_to_db=True)
                    score_session.commit()
                    logger.info(f"[Sunbiz] Rescored {len(ids)} LLC properties")
        except Exception as e:
            logger.warning(f"[Sunbiz] Rescore failed (non-critical): {e}")

    if not dry_run:
        try:
            from src.utils.scraper_db_helper import record_scraper_stats

            run_success, error_type, error_message, outcome = sunbiz_run_verdict(stats)
            record_scraper_stats(
                source_type="sunbiz",
                total_scraped=stats["processed"],
                matched=stats["enriched"],
                unmatched=stats["skipped"],
                skipped=0,
                run_success=run_success,
                error_type=error_type,
                error_message=error_message,
                outcome=outcome,
                county_id=county_id,
            )
        except Exception as e:
            logger.warning("[Sunbiz] Could not record scraper stats: %s", e)

    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Enrich LLC owner registered agent info from Florida Sunbiz"
    )
    parser.add_argument("--limit", type=int, default=0, help="Max owners to process (0 = all)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--rescore", action="store_true")
    parser.add_argument("--county-id", dest="county_id", default="hillsborough")
    parser.add_argument("--headful", action="store_true", help="Run browser in headful (visible) mode")
    args = parser.parse_args()

    run_sunbiz_pipeline(
        limit=args.limit,
        dry_run=args.dry_run,
        rescore=args.rescore,
        county_id=args.county_id,
        headless=not args.headful,
    )
