"""
Florida Sunbiz (FL Division of Corporations) — Registered Agent Enrichment

For each LLC-owned property with a high distress score and no registered agent
on file, this scraper:
  1. Navigates to search.sunbiz.org via Playwright + playwright-stealth
  2. Searches by LLC name, finds the exact matching row in #search-results
  3. Opens the entity detail page and reads the "Registered Agent Name & Address"
     detailSection (present on ~99% of active FL filings)
  4. Writes registered_agent_name + registered_agent_address back to the Owner row

Falls back to a browser-use AI agent if Playwright encounters a hard error.

Usage:
    python -m src.scrappers.sunbiz.sunbiz_engine
    python -m src.scrappers.sunbiz.sunbiz_engine --limit 100
    python -m src.scrappers.sunbiz.sunbiz_engine --dry-run
    python -m src.scrappers.sunbiz.sunbiz_engine --rescore
"""

import asyncio
import re
from typing import Optional, Tuple

from sqlalchemy.orm import Session

from src.core.database import get_db_context
from src.core.models import Owner, Property
from src.utils.logger import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)

SUNBIZ_SEARCH_URL = "https://search.sunbiz.org/Inquiry/CorporationSearch/ByName"
SUNBIZ_BASE = "https://search.sunbiz.org"

_DELAY_SECONDS = 1.5  # polite crawl rate between owners


# ---------------------------------------------------------------------------
# Name normalization for exact-match comparison
# ---------------------------------------------------------------------------

def _normalize(name: str) -> str:
    name = name.upper()
    name = name.replace("&", "AND")
    name = re.sub(r"[.,;'\"]", "", name)
    name = re.sub(r"\s+", " ", name)
    return name.strip()


# ---------------------------------------------------------------------------
# Playwright scraper
# ---------------------------------------------------------------------------

async def _scrape_registered_agent(
    page, company_name: str
) -> Tuple[Optional[str], Optional[str]]:
    """
    Search Sunbiz for company_name and return (agent_name, agent_address).
    Returns (None, None) if no exact match is found or the registered agent
    section is absent on the detail page.
    """
    try:
        await page.goto(SUNBIZ_SEARCH_URL, wait_until="domcontentloaded", timeout=20000)
        await page.fill("#SearchTerm", company_name)
        await page.click("input[value='Search Now']")
        await page.wait_for_selector("#search-results", timeout=15000)
    except Exception as e:
        logger.debug(f"[Sunbiz] Search navigation failed for '{company_name}': {e}")
        raise

    # Find exact name match in the results table
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

    # Navigate to detail page
    try:
        await page.goto(detail_url, wait_until="domcontentloaded", timeout=20000)
    except Exception as e:
        logger.debug(f"[Sunbiz] Detail page load failed: {e}")
        return None, None

    # Find the "Registered Agent Name & Address" detailSection
    sections = await page.query_selector_all("div.detailSection")
    for section in sections:
        header = await section.query_selector("span:first-child")
        if not header:
            continue
        header_text = await header.inner_text()
        if "registered agent" not in header_text.lower():
            continue

        # spans[0]=header, spans[1]=agent name, spans[2]=address block
        spans = await section.query_selector_all("span")
        if len(spans) < 2:
            return None, None

        agent_name = (await spans[1].inner_text()).strip()
        agent_address: Optional[str] = None
        if len(spans) >= 3:
            raw_addr = (await spans[2].inner_text()).strip()
            # Collapse whitespace / blank lines left by <br> tags
            agent_address = re.sub(r"\n{2,}", "\n", raw_addr).strip()

        return agent_name or None, agent_address or None

    logger.debug(f"[Sunbiz] No registered agent section found for '{company_name}'")
    return None, None


async def _run_playwright_batch(
    owners: list,
    dry_run: bool,
    stats: dict,
    session: Session,
    headless: bool = True,
) -> None:
    from playwright.async_api import async_playwright
    from playwright_stealth import Stealth
    from src.utils.http_helpers import get_playwright_proxy

    async with async_playwright() as pw:
        launch_args = [] if not headless else ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]
        browser = await pw.chromium.launch(
            headless=headless,
            proxy=get_playwright_proxy(),
            args=launch_args,
        )
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1920, "height": 1080},
        )
        page = await context.new_page()
        await Stealth().apply_stealth_async(page)

        try:
            total = len(owners)
            for idx, owner in enumerate(owners, 1):
                stats["processed"] += 1
                name = owner.owner_name.strip()

                skip_patterns = [
                    "ASSESSED BY DEPT", "ASSESSED BY STATE", "STATE OF FL",
                    "TRUSTEE", " TRUST", "ESTATE OF",
                ]
                if any(p in name.upper() for p in skip_patterns):
                    logger.debug(f"[Sunbiz] Skipping non-LLC entity: {name}")
                    stats["skipped"] += 1
                    continue

                logger.info(f"[Sunbiz] [{idx}/{total}] Searching: {name}")
                await asyncio.sleep(_DELAY_SECONDS)

                try:
                    agent_name, agent_address = await _scrape_registered_agent(page, name)
                except Exception as e:
                    logger.warning(f"[Sunbiz] Playwright failed for '{name}': {e} — trying AI fallback")
                    try:
                        agent_name, agent_address = await _ai_fallback(name)
                    except Exception as ae:
                        logger.warning(f"[Sunbiz] AI fallback also failed for '{name}': {ae}")
                        stats["failed"] += 1
                        continue

                if not agent_name:
                    stats["skipped"] += 1
                    continue

                logger.info(
                    f"[Sunbiz] Found agent for '{name}': {agent_name} | {agent_address}"
                )

                if not dry_run:
                    owner.registered_agent_name = agent_name
                    owner.registered_agent_address = agent_address
                    if owner.owner_type != "LLC":
                        owner.owner_type = "LLC"
                    stats["enriched"] += 1
                else:
                    logger.info(
                        f"[Sunbiz] DRY RUN — would write agent for owner_id={owner.id}"
                    )
                    stats["enriched"] += 1

        finally:
            await context.close()
            await browser.close()


# ---------------------------------------------------------------------------
# AI fallback (browser-use) — only used when Playwright raises
# ---------------------------------------------------------------------------

async def _ai_fallback(company_name: str) -> Tuple[Optional[str], Optional[str]]:
    from browser_use import Agent, Browser, ChatAnthropic
    from src.utils.http_helpers import get_browser_use_proxy

    task = (
        f"Go to https://search.sunbiz.org/Inquiry/CorporationSearch/ByName, "
        f"search for the company named '{company_name}', find the exact matching result, "
        f"open its detail page, locate the section titled 'Registered Agent Name & Address', "
        f"and return ONLY a JSON object with keys 'agent_name' and 'agent_address'. "
        f"If no exact match or no registered agent section exists, return "
        f"{{\"agent_name\": null, \"agent_address\": null}}."
    )

    proxy = get_browser_use_proxy()
    browser = Browser(headless=True, disable_security=True, proxy=proxy)
    llm = ChatAnthropic(model="claude-sonnet-4-6")

    agent = Agent(task=task, llm=llm, browser=browser)
    result = await agent.run()

    # Parse JSON from agent output
    import json
    text = str(result).strip()
    match = re.search(r"\{[^}]+\}", text)
    if match:
        try:
            data = json.loads(match.group())
            return data.get("agent_name"), data.get("agent_address")
        except json.JSONDecodeError:
            pass

    return None, None


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

    stats = {"processed": 0, "enriched": 0, "skipped": 0, "failed": 0}

    llc_keywords = ["%LLC%", "%INC%", "%CORP%", "%LLP%", "%PLLC%", "%LTD%"]

    q = (
        session.query(Owner)
        .join(Property, Property.id == Owner.property_id)
        .join(DistressScore, DistressScore.property_id == Property.id)
        .filter(
            or_(*[Owner.owner_name.ilike(kw) for kw in llc_keywords]),
            Owner.registered_agent_name.is_(None),
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
            record_scraper_stats(
                source_type="sunbiz",
                total_scraped=stats["processed"],
                matched=stats["enriched"],
                unmatched=stats["skipped"] + stats["failed"],
                skipped=0,
                run_success=True,
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
