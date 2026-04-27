"""
Florida Sunbiz (FL Division of Corporations) LLC Contact Enrichment

Searches the FL DOS Sunbiz database for LLC-owned properties in our system and
extracts the registered agent's phone and email address. These are written back
to the Owner record (phone_1 / email_1) so they appear in GHL CRM on the next
rescore/push.

Why Sunbiz?
  - FL Secretary of State maintains a public, searchable database of all Florida
    LLCs at search.sunbiz.org.
  - Each LLC filing lists a registered agent with an address, phone, and sometimes
    an email.
  - ~30-50% of LLC-owned investment properties have a reachable registered agent
    contact — free, no API key required.

Usage:
    python -m src.scrappers.sunbiz.sunbiz_engine
    python -m src.scrappers.sunbiz.sunbiz_engine --limit 100
    python -m src.scrappers.sunbiz.sunbiz_engine --dry-run
    python -m src.scrappers.sunbiz.sunbiz_engine --rescore

The scraper is intentionally slow (1-2 s between requests) to be polite to the
state's servers and avoid IP blocks.
"""

import argparse
import re
import time
import logging
from typing import Optional, Tuple
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup
from sqlalchemy.orm import Session

from src.core.database import get_db_context
from src.core.models import Owner, Property
from src.utils.logger import setup_logging, get_logger
from src.utils.http_helpers import requests_get_with_retry
from config.constants import DEFAULT_USER_AGENT

setup_logging()
logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Sunbiz search / detail URLs
# ---------------------------------------------------------------------------
SUNBIZ_SEARCH_URL = "https://search.sunbiz.org/Inquiry/CorporationSearch/SearchResults"
SUNBIZ_DETAIL_BASE = "https://search.sunbiz.org"

_HEADERS = {
    "User-Agent": DEFAULT_USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

_DELAY_BETWEEN_REQUESTS = 1.5  # seconds — polite crawl rate


# ---------------------------------------------------------------------------
# Core scraping functions
# ---------------------------------------------------------------------------

def _search_sunbiz(company_name: str) -> Optional[str]:
    """
    Search Sunbiz for a company name and return the detail page URL of the
    first exact (or best) result, or None if not found.
    """
    # Strip common legal suffixes for a cleaner search
    clean_name = re.sub(
        r"\b(LLC|INC|CORP|LTD|LP|LLP|PLLC|CO|COMPANY|INCORPORATED|LIMITED)\b\.?",
        "",
        company_name,
        flags=re.IGNORECASE,
    ).strip().strip(",").strip()

    if not clean_name:
        return None

    params = {
        "SearchTerm": clean_name,
        "SearchType": "EntityName",
    }

    try:
        resp = requests_get_with_retry(
            SUNBIZ_SEARCH_URL,
            headers=_HEADERS,
            params=params,
            timeout=15,
        )
        resp.raise_for_status()
    except Exception as e:
        logger.debug(f"[Sunbiz] Search request failed for '{clean_name}': {e}")
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    # Results are in a table with class "searchResultTable"
    table = soup.find("table", class_="searchResultTable") or soup.find("table")
    if not table:
        logger.debug(f"[Sunbiz] No results table found for '{clean_name}'")
        return None

    rows = table.find_all("tr")
    for row in rows[1:]:  # skip header row
        cols = row.find_all("td")
        if not cols:
            continue
        link_tag = cols[0].find("a", href=True)
        if not link_tag:
            continue

        result_name = link_tag.get_text(strip=True).upper()
        search_upper = company_name.upper()

        # Accept if the result name contains the search name (handles LLC / Inc variations)
        if clean_name.upper() in result_name or result_name in search_upper:
            href = link_tag["href"]
            if not href.startswith("http"):
                href = SUNBIZ_DETAIL_BASE + href
            logger.debug(f"[Sunbiz] Matched '{result_name}' → {href}")
            return href

    logger.debug(f"[Sunbiz] No match found for '{company_name}'")
    return None


def _extract_contact_from_detail(detail_url: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Fetch the Sunbiz entity detail page and extract phone and email from the
    registered agent section.

    Returns:
        (phone, email) — either may be None if not found on the page.
    """
    try:
        resp = requests_get_with_retry(
            detail_url,
            headers=_HEADERS,
            timeout=15,
        )
        resp.raise_for_status()
    except Exception as e:
        logger.debug(f"[Sunbiz] Detail fetch failed for {detail_url}: {e}")
        return None, None

    soup = BeautifulSoup(resp.text, "html.parser")

    phone: Optional[str] = None
    email: Optional[str] = None

    # ── Phone ────────────────────────────────────────────────────────────────
    # Sunbiz renders phone as plain text near "Registered Agent" section.
    # Also check principal address / officer sections.
    page_text = soup.get_text(" ", strip=True)

    phone_matches = re.findall(
        r"\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4}",
        page_text,
    )
    if phone_matches:
        # Prefer the first valid 10-digit number
        for raw in phone_matches:
            digits = re.sub(r"\D", "", raw)
            if len(digits) == 10 and digits[0] not in ("0", "1"):
                phone = f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
                break

    # ── Email ────────────────────────────────────────────────────────────────
    email_matches = re.findall(
        r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
        page_text,
    )
    if email_matches:
        # Filter out Sunbiz system emails
        for candidate in email_matches:
            if "sunbiz" not in candidate.lower() and "dos.myflorida" not in candidate.lower():
                email = candidate.lower()
                break

    return phone, email


# ---------------------------------------------------------------------------
# Main enrichment loop
# ---------------------------------------------------------------------------

def enrich_llc_owners(
    session: Session,
    limit: int = 500,
    dry_run: bool = False,
    urgency_tiers: list = None,
    county_id: str = "hillsborough",
) -> dict:
    """
    Query LLC-named owners of high-priority scored leads with no phone/email,
    search Sunbiz for each, and write contact data back to the Owner record.

    Priority order: Immediate → High → (others if urgency_tiers not set)

    Args:
        session:       SQLAlchemy session
        limit:         Max number of owners to process in this run
        dry_run:       If True, print results without writing to DB
        urgency_tiers: List of urgency levels to filter (default: Immediate + High)

    Returns:
        dict with keys: processed, enriched, skipped, failed
    """
    from sqlalchemy import or_, desc
    from src.core.models import DistressScore

    stats = {"processed": 0, "enriched": 0, "skipped": 0, "failed": 0}

    if urgency_tiers is None:
        urgency_tiers = ["Immediate", "High"]

    llc_keywords = ["%LLC%", "%INC%", "%CORP%", "%LLP%", "%PLLC%", "%LTD%"]

    # Join Owner → Property → DistressScore to get only high-priority scored leads
    # Order by score descending so highest-value leads are enriched first
    owners = (
        session.query(Owner)
        .join(Property, Property.id == Owner.property_id)
        .join(DistressScore, DistressScore.property_id == Property.id)
        .filter(
            or_(*[Owner.owner_name.ilike(kw) for kw in llc_keywords]),
            Owner.phone_1.is_(None),
            Owner.owner_name.isnot(None),
            DistressScore.urgency_level.in_(urgency_tiers),
            Property.county_id == county_id,
        )
        .order_by(desc(DistressScore.final_cds_score))
        .limit(limit)
        .all()
    )

    total = len(owners)
    logger.info(
        f"[Sunbiz] Found {total} LLC owners in {urgency_tiers} tier(s) without phone numbers"
    )

    for idx, owner in enumerate(owners, 1):
        stats["processed"] += 1
        name = owner.owner_name.strip()

        # Skip state-assessed entities, trusts, estates — not searchable on Sunbiz
        skip_patterns = ["ASSESSED BY DEPT", "ASSESSED BY STATE", "STATE OF FL",
                         "TRUSTEE", " TRUST", "ESTATE OF"]
        if any(p in name.upper() for p in skip_patterns):
            logger.debug(f"[Sunbiz] Skipping non-LLC entity: {name}")
            stats["skipped"] += 1
            continue

        logger.info(f"[Sunbiz] [{idx}/{total}] Searching: {name}")

        # Rate-limit
        time.sleep(_DELAY_BETWEEN_REQUESTS)

        detail_url = _search_sunbiz(name)
        if not detail_url:
            stats["skipped"] += 1
            continue

        time.sleep(_DELAY_BETWEEN_REQUESTS)

        try:
            phone, email = _extract_contact_from_detail(detail_url)
        except Exception as e:
            logger.warning(f"[Sunbiz] Failed to extract contact for '{name}': {e}")
            stats["failed"] += 1
            continue

        if not phone and not email:
            logger.debug(f"[Sunbiz] No contact data found for '{name}'")
            stats["skipped"] += 1
            continue

        logger.info(
            f"[Sunbiz] Found — {name} | phone={phone} | email={email}"
        )

        if not dry_run:
            if phone:
                owner.phone_1 = phone
            if email:
                owner.email_1 = email
            owner.skip_trace_success = True
            # Fix misclassified owner_type
            if owner.owner_type != "LLC":
                owner.owner_type = "LLC"
            stats["enriched"] += 1
        else:
            logger.info(f"[Sunbiz] DRY RUN — would update owner_id={owner.id}")
            stats["enriched"] += 1

    if not dry_run:
        session.commit()
        logger.info(f"[Sunbiz] Committed {stats['enriched']} owner updates to DB")

    return stats


def run_sunbiz_pipeline(
    limit: int = 500,
    dry_run: bool = False,
    rescore: bool = False,
    urgency_tiers: list = None,
    county_id: str = "hillsborough",
):
    """
    Run the full Sunbiz enrichment pipeline.

    Args:
        limit:         Max owners to process
        dry_run:       Print results without writing to DB
        rescore:       Trigger CDS rescore for enriched properties after enrichment
        urgency_tiers: Urgency levels to target (default: Immediate + High)
    """
    if urgency_tiers is None:
        urgency_tiers = ["Immediate", "High"]

    logger.info("=" * 60)
    logger.info("SUNBIZ LLC CONTACT ENRICHMENT")
    logger.info(f"Targeting urgency tiers: {urgency_tiers}")
    logger.info("=" * 60)
    if dry_run:
        logger.info("DRY RUN mode — no DB writes")

    with get_db_context() as session:
        stats = enrich_llc_owners(
            session, limit=limit, dry_run=dry_run, urgency_tiers=urgency_tiers, county_id=county_id
        )

    logger.info("=" * 60)
    logger.info("SUNBIZ ENRICHMENT COMPLETE")
    logger.info(f"  Processed : {stats['processed']}")
    logger.info(f"  Enriched  : {stats['enriched']}")
    logger.info(f"  Skipped   : {stats['skipped']}  (no Sunbiz match)")
    logger.info(f"  Failed    : {stats['failed']}")
    logger.info("=" * 60)

    if rescore and stats["enriched"] > 0 and not dry_run:
        logger.info("[Sunbiz] Triggering CDS rescore for enriched LLC properties...")
        try:
            from src.services.cds_engine import MultiVerticalScorer
            with get_db_context() as score_session:
                # Re-query enriched owner property IDs
                property_ids = (
                    score_session.query(Owner.property_id)
                    .filter(
                        Owner.owner_type == "LLC",
                        Owner.skip_trace_success == True,
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
        except Exception as _se:
            logger.warning("[Sunbiz] Could not record scraper stats: %s", _se)

    return stats


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Enrich LLC owner contacts from Florida Sunbiz"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=500,
        help="Max number of LLC owners to process (default: 500)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Search Sunbiz and print results without writing to DB",
    )
    parser.add_argument(
        "--rescore",
        action="store_true",
        help="Trigger CDS rescore for enriched properties after enrichment",
    )
    parser.add_argument(
        "--tiers",
        nargs="+",
        default=["Immediate", "High"],
        choices=["Immediate", "High", "Medium", "Low"],
        help="Urgency tiers to target (default: Immediate High)",
    )
    parser.add_argument(
        "--county-id",
        dest="county_id",
        default="hillsborough",
        help="County identifier (default: hillsborough)",
    )
    args = parser.parse_args()

    run_sunbiz_pipeline(
        limit=args.limit,
        dry_run=args.dry_run,
        rescore=args.rescore,
        urgency_tiers=args.tiers,
        county_id=args.county_id,
    )
