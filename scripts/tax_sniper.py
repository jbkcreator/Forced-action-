"""
Tax Delinquency Firecrawl Sniper
=================================
Targeted enrichment: fetches total_amount_due and years_delinquent from Firecrawl
for tax delinquency records that are NULL on those fields, but only for properties
that already have at least one OTHER qualifying signal (multi-signal subset).

Why targeted:
  Running Firecrawl against all 16k+ null records is expensive and most have no
  other signals — they won't reach a qualifying tier regardless of the amount.
  Restricting to the multi-signal subset keeps API costs low while maximising
  the scoring impact of each enriched record.

After enrichment, affected properties are immediately rescored so the updated
amounts flow through to distress_scores and GHL summaries on the next sync.

Usage:
    python scripts/tax_sniper.py --dry-run                    # count targets only
    python scripts/tax_sniper.py --limit 50                   # smoke test
    python scripts/tax_sniper.py --county-id hillsborough     # full run
    python scripts/tax_sniper.py --limit 200 --county-id hillsborough
"""

import argparse
import asyncio
import logging
import random
import sys
from pathlib import Path

# Allow running as a script from the project root
_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from config.constants import PARCEL_LOOKUP_URL, REQUEST_DELAY_RANGE
from config.settings import settings
from src.core.database import get_db_context
from src.core.models import (
    BuildingPermit, CodeViolation, Deed, Foreclosure,
    LegalAndLien, LegalProceeding, Property, TaxDelinquency,
)
from src.scrappers.deliquencies.tax_delinquent_engine import _scrape_parcel_with_firecrawl
from src.services.cds_engine import MultiVerticalScorer
from src.utils.county_config import get_county
from src.utils.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Query
# ---------------------------------------------------------------------------

_MULTI_SIGNAL_SQL = """
    SELECT DISTINCT
        p.id          AS property_id,
        p.parcel_id,
        td.id         AS td_id,
        td.tax_year
    FROM properties p
    JOIN tax_delinquencies td
        ON td.property_id = p.id
        AND td.total_amount_due IS NULL
    WHERE p.county_id = :county_id
      AND (
          EXISTS (SELECT 1 FROM code_violations     WHERE property_id = p.id)
          OR EXISTS (SELECT 1 FROM legal_and_liens  WHERE property_id = p.id)
          OR EXISTS (SELECT 1 FROM legal_proceedings WHERE property_id = p.id)
          OR EXISTS (SELECT 1 FROM foreclosures     WHERE property_id = p.id)
          OR EXISTS (
              SELECT 1 FROM building_permits
              WHERE property_id = p.id AND is_enforcement_permit = TRUE
          )
          OR EXISTS (
              SELECT 1 FROM deeds
              WHERE property_id = p.id
                AND record_date >= CURRENT_DATE - INTERVAL '2 years'
          )
      )
    ORDER BY p.id
"""


def _query_targets(session, county_id: str) -> list:
    from sqlalchemy import text
    rows = session.execute(
        text(_MULTI_SIGNAL_SQL), {"county_id": county_id}
    ).fetchall()
    return [(r.property_id, r.parcel_id, r.td_id, r.tax_year) for r in rows]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main():
    parser = argparse.ArgumentParser(
        description="Firecrawl SNIPER — enrich null tax delinquency amounts for multi-signal leads"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Count targets and print a sample — no Firecrawl calls made")
    parser.add_argument("--limit", type=int, default=None,
                        help="Max records to enrich per run")
    parser.add_argument("--county-id", default="hillsborough",
                        help="County ID to process (default: hillsborough)")
    args = parser.parse_args()

    county_id = args.county_id

    # Resolve parcel lookup URL for this county
    try:
        parcel_lookup_url = get_county(county_id)["portals"]["parcel_lookup_url"]
    except (KeyError, TypeError):
        logger.warning("parcel_lookup_url not found in county config — using constants fallback")
        parcel_lookup_url = PARCEL_LOOKUP_URL

    logger.info("[Sniper] Starting — county=%s dry_run=%s limit=%s", county_id, args.dry_run, args.limit)

    with get_db_context() as session:
        targets = _query_targets(session, county_id)
        total_targeted = len(targets)

        logger.info("[Sniper] Multi-signal subset: %d properties with null tax amounts", total_targeted)

        if args.dry_run:
            print(f"\nDRY RUN — {total_targeted:,} properties targeted for enrichment")
            print(f"Parcel lookup URL: {parcel_lookup_url}")
            if targets:
                print("\nSample (first 5):")
                for pid, parcel_id, td_id, tax_year in targets[:5]:
                    print(f"  property_id={pid}  parcel_id={parcel_id}  tax_year={tax_year}")
            print()
            return

        if args.limit:
            targets = targets[:args.limit]
            logger.info("[Sniper] Capped to %d targets via --limit", len(targets))

        firecrawl_client = __import__("firecrawl").FirecrawlApp(
            api_key=settings.firecrawl_api_key.get_secret_value()
        )

        enriched_property_ids: set = set()
        stats = {"enriched": 0, "no_data": 0, "errors": 0}

        for idx, (property_id, parcel_id, td_id, tax_year) in enumerate(targets, 1):
            account_number = "A" + str(parcel_id)
            logger.info(
                "[Sniper] [%d/%d] Fetching %s (property_id=%d tax_year=%s)",
                idx, len(targets), account_number, property_id, tax_year,
            )

            try:
                result = await _scrape_parcel_with_firecrawl(
                    firecrawl_client, account_number, parcel_lookup_url
                )
            except Exception as exc:
                logger.warning("[Sniper] Firecrawl error for %s: %s", account_number, exc)
                stats["errors"] += 1
                continue

            amount = result.get("total_amount_due")
            years  = result.get("years_delinquent")

            if amount is None and years is None:
                logger.debug("[Sniper] No data returned for %s", account_number)
                stats["no_data"] += 1
            else:
                td_row = session.query(TaxDelinquency).filter(
                    TaxDelinquency.id == td_id
                ).first()
                if td_row:
                    if amount is not None:
                        # Amount may be a string like "$1,234.56" — parse it
                        if isinstance(amount, str):
                            try:
                                amount = float(amount.replace("$", "").replace(",", "").strip())
                            except ValueError:
                                amount = None
                        td_row.total_amount_due = amount
                    if years is not None:
                        try:
                            td_row.years_delinquent = int(years)
                        except (ValueError, TypeError):
                            pass
                    session.flush()
                    enriched_property_ids.add(property_id)
                    stats["enriched"] += 1
                    logger.info(
                        "[Sniper] Enriched %s — amount=%s years=%s",
                        account_number, amount, years,
                    )
                else:
                    logger.warning("[Sniper] TaxDelinquency row %d not found", td_id)
                    stats["errors"] += 1

            # Rate-limit between calls (skip delay after last record)
            if idx < len(targets):
                delay = random.uniform(*REQUEST_DELAY_RANGE)
                logger.debug("[Sniper] Waiting %.1fs before next account", delay)
                await asyncio.sleep(delay)

        # get_db_context() commits on exit — session auto-commits here

        # Rescore affected properties inside the same session so scorer sees updated amounts
        if enriched_property_ids:
            logger.info("[Sniper] Rescoring %d enriched properties...", len(enriched_property_ids))
            scorer = MultiVerticalScorer(session)
            scorer.score_properties_by_ids(list(enriched_property_ids))
            logger.info("[Sniper] Rescore complete")
        else:
            logger.info("[Sniper] No properties enriched — skipping rescore")

    print(
        f"\n[Tax Sniper] Complete\n"
        f"  Targeted   : {len(targets):,}\n"
        f"  Enriched   : {stats['enriched']:,}\n"
        f"  No data    : {stats['no_data']:,}\n"
        f"  Errors     : {stats['errors']:,}\n"
        f"  Rescored   : {len(enriched_property_ids):,}\n"
    )


if __name__ == "__main__":
    asyncio.run(main())
