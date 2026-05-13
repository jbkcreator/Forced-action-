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
from typing import Optional, Tuple

import requests
from bs4 import BeautifulSoup
from sqlalchemy.orm import Session

from src.core.database import get_db_context
from src.core.models import Owner, Property
from src.utils.logger import setup_logging, get_logger
from src.utils.http_helpers import get_requests_proxies
from config.constants import DEFAULT_USER_AGENT

setup_logging()
logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Sunbiz search / detail URLs
# ---------------------------------------------------------------------------
SUNBIZ_SEARCH_URL = "https://search.sunbiz.org/Inquiry/CorporationSearch/SearchResults"
SUNBIZ_DETAIL_BASE = "https://search.sunbiz.org"
SUNBIZ_HOME_URL = "https://search.sunbiz.org/Inquiry/CorporationSearch/ByName"

_HEADERS = {
    "User-Agent": DEFAULT_USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

_DELAY_BETWEEN_REQUESTS = 1.5  # seconds — polite crawl rate


# ---------------------------------------------------------------------------
# Session management
# ---------------------------------------------------------------------------

def _build_session() -> requests.Session:
    """
    Create a requests.Session with proxy and browser-like headers, then warm
    it up against the Sunbiz search homepage so session cookies are set before
    any search requests are made.
    """
    session = requests.Session()
    session.headers.update(_HEADERS)

    proxies = get_requests_proxies()
    if proxies:
        session.proxies.update(proxies)
        logger.info("[Sunbiz] Session using Oxylabs proxy")
    else:
        logger.warning("[Sunbiz] No proxy configured — requests will be sent direct")

    # Warm up: visit the search landing page so the server sets session cookies
    try:
        resp = session.get(SUNBIZ_HOME_URL, timeout=15)
        resp.raise_for_status()
        logger.info(f"[Sunbiz] Session warmed up (status {resp.status_code}, cookies: {list(session.cookies.keys())})")
    except Exception as e:
        logger.warning(f"[Sunbiz] Warm-up request failed (continuing anyway): {e}")

    return session


# ---------------------------------------------------------------------------
# Core scraping functions
# ---------------------------------------------------------------------------

def _search_sunbiz(session: requests.Session, company_name: str) -> Optional[str]:
    """
    Search Sunbiz for a company name and return the detail page URL of the
    first exact (or best) match, or None if not found.
    """
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
        resp = session.get(
            SUNBIZ_SEARCH_URL,
            params=params,
            timeout=15,
            headers={**_HEADERS, "Referer": SUNBIZ_HOME_URL},
        )
        resp.raise_for_status()
    except Exception as e:
        logger.debug(f"[Sunbiz] Search request failed for '{clean_name}': {e}")
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

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

        if clean_name.upper() in result_name or result_name in search_upper:
            href = link_tag["href"]
            if not href.startswith("http"):
                href = SUNBIZ_DETAIL_BASE + href
            logger.debug(f"[Sunbiz] Matched '{result_name}' → {href}")
            return href

    logger.debug(f"[Sunbiz] No match found for '{company_name}'")
    return None


# Officer titles in priority order — MGR/MGRM are LLC-specific, PRES/VP/DIR for corps
_MANAGER_TITLE_PRIORITY = ["MGR", "MGRM", "PRES", "VP", "DIR"]


def _parse_officers_section(soup: BeautifulSoup) -> Optional[dict]:
    """
    Parse the Officers/Directors section of a Sunbiz detail page.

    Sunbiz renders each section as a <div class="detailSection"> with a
    <span class="detailSectionHeader"> label, followed by <span class="label">
    / <span class="info"> pairs for each field of each officer.

    Returns the best manager as {"name": str, "title": str}, preferring
    MGR > MGRM > PRES > VP > DIR > first-available. Returns None if the
    section is absent or empty.
    """
    officers = []

    for section in soup.find_all("div", class_="detailSection"):
        header = section.find("span", class_="detailSectionHeader")
        if not header or "officer" not in header.get_text(separator=" ", strip=True).lower():
            continue

        # Walk label/info pairs within this section
        labels = section.find_all("span", class_="label")
        infos = section.find_all("span", class_="info")

        current: dict = {}
        for label_tag, info_tag in zip(labels, infos):
            key = label_tag.get_text(strip=True).lower().rstrip(":")
            val = info_tag.get_text(strip=True)
            if not val:
                continue
            if key == "title":
                if current:
                    officers.append(current)
                current = {"title": val.upper()}
            elif key == "name" and "title" in current:
                current["name"] = val.upper()
        if current and "name" in current:
            officers.append(current)

        break  # only need the first Officers section

    if not officers:
        return None

    # Skip pure registered-agent entries — those are handled separately
    candidates = [o for o in officers if o.get("title") != "RAGT" and o.get("name")]
    if not candidates:
        return None

    # Return highest-priority title; fall back to first candidate
    for preferred in _MANAGER_TITLE_PRIORITY:
        for officer in candidates:
            if officer.get("title") == preferred:
                return officer
    return candidates[0]


def _extract_contact_from_detail(
    session: requests.Session, detail_url: str
) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """
    Fetch the Sunbiz entity detail page and extract:
      - phone and email from the registered agent / page text
      - manager name and title from the Officers/Directors section

    Returns:
        (phone, email, manager_name, manager_title)
    """
    try:
        resp = session.get(detail_url, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        logger.debug(f"[Sunbiz] Detail fetch failed for {detail_url}: {e}")
        return None, None, None, None

    soup = BeautifulSoup(resp.text, "html.parser")

    phone: Optional[str] = None
    email: Optional[str] = None

    # ── Phone ────────────────────────────────────────────────────────────────
    page_text = soup.get_text(" ", strip=True)

    phone_matches = re.findall(
        r"\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4}",
        page_text,
    )
    if phone_matches:
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
        for candidate in email_matches:
            if "sunbiz" not in candidate.lower() and "dos.myflorida" not in candidate.lower():
                email = candidate.lower()
                break

    # ── Manager (Officers/Directors section) ─────────────────────────────────
    manager_name: Optional[str] = None
    manager_title: Optional[str] = None

    officer = _parse_officers_section(soup)
    if officer:
        manager_name = officer.get("name")
        manager_title = officer.get("title")

    return phone, email, manager_name, manager_title


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

    stats = {"processed": 0, "enriched": 0, "manager_found": 0, "skipped": 0, "failed": 0}

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

    http_session = _build_session()

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

        detail_url = _search_sunbiz(http_session, name)
        if not detail_url:
            stats["skipped"] += 1
            continue

        time.sleep(_DELAY_BETWEEN_REQUESTS)

        try:
            phone, email, manager_name, manager_title = _extract_contact_from_detail(http_session, detail_url)
        except Exception as e:
            logger.warning(f"[Sunbiz] Failed to extract contact for '{name}': {e}")
            stats["failed"] += 1
            continue

        if not phone and not email and not manager_name:
            logger.debug(f"[Sunbiz] No data found for '{name}'")
            stats["skipped"] += 1
            continue

        if manager_name:
            logger.info(f"[Sunbiz] Manager: {manager_name} ({manager_title}) for '{name}'")
        if phone or email:
            logger.info(f"[Sunbiz] Contact: {name} | phone={phone} | email={email}")

        if not dry_run:
            if phone:
                owner.phone_1 = phone
            if email:
                owner.email_1 = email
            if phone or email:
                owner.skip_trace_success = True
            if manager_name:
                owner.manager_name = manager_name
                owner.manager_title = manager_title
                stats["manager_found"] += 1
            if owner.owner_type != "LLC":
                owner.owner_type = "LLC"
            if phone or email:
                stats["enriched"] += 1
        else:
            if phone or email:
                logger.info(f"[Sunbiz] DRY RUN — would write contact for owner_id={owner.id}")
                stats["enriched"] += 1
            if manager_name:
                logger.info(f"[Sunbiz] DRY RUN — would write manager for owner_id={owner.id}")
                stats["manager_found"] += 1

    if not dry_run:
        session.commit()
        logger.info(f"[Sunbiz] Committed {stats['enriched']} contact + {stats['manager_found']} manager updates to DB")

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
    logger.info(f"  Processed      : {stats['processed']}")
    logger.info(f"  Enriched       : {stats['enriched']}  (phone or email found)")
    logger.info(f"  Manager found  : {stats['manager_found']}  (name+title stored, no phone yet)")
    logger.info(f"  Skipped        : {stats['skipped']}  (no Sunbiz match)")
    logger.info(f"  Failed         : {stats['failed']}")
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
