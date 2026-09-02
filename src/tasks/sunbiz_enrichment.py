"""
Daily Sunbiz enrichment task — tiered staleness.

Targets LLC / Corporate owner rows that need a Sunbiz fa031 piercing scrape:

  Tier A (active-lead, hot)    : sunbiz_status='pending' OR enriched > 30d ago
                                  AND owning property is on a paying-subscriber
                                  feed with score >= Gold.
  Tier B (linked, cold)        : sunbiz_status='pending' OR enriched > 180d ago,
                                  any LLC owner with a property row.
  Skipped                      : everything else (no auto-refresh).

The cron entry runs Tier A first every day; Tier B is rolled into the same run
but order-by-tier ensures the cheap-most-valuable rows go first under any
daily budget cap.

Usage:
  python -m src.tasks.sunbiz_enrichment [--county hillsborough] [--limit N] [--dry-run]
  python -m src.tasks.sunbiz_enrichment --tier b   # cold tier only

Cron (daily 07:40 UTC — after assessor load and before CDS scoring):
  40 7 * * * cd /path/to/app && python -m src.tasks.sunbiz_enrichment
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

from sqlalchemy import and_, or_, select
from sqlalchemy.orm import Session

from src.core.database import get_db_context
from src.core.models import DistressScore, Owner, Property
from src.scrappers.sunbiz.sunbiz_engine import _run_playwright_batch  # internal API
from src.utils.logger import get_logger, setup_logging

setup_logging()
logger = get_logger(__name__)

# Staleness windows — keep in sync with grill doc Q9.
TIER_A_DAYS = 30
TIER_B_DAYS = 180

# Lead-tier threshold for Tier A. Mirrors `lead_tier` values produced by the
# CDS engine; Gold+ is the paying-subscriber-visible band.
ACTIVE_LEAD_TIERS = ("Gold", "Platinum", "Ultra Platinum")


def _select_tier_a(db: Session, county_id: str, limit: int) -> list[Owner]:
    """High-value: pending OR stale-30d, on an active high-tier lead."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=TIER_A_DAYS)
    rows = db.execute(
        select(Owner)
        .join(Property, Property.id == Owner.property_id)
        .join(DistressScore, DistressScore.property_id == Property.id)
        .where(
            Property.county_id == county_id,
            DistressScore.lead_tier.in_(ACTIVE_LEAD_TIERS),
            Owner.owner_name.isnot(None),
            Owner.owner_type.in_(("LLC", "Corporate")),
            or_(
                Owner.sunbiz_status.in_(("pending", "parser_failed")),
                and_(
                    Owner.sunbiz_status == "matched",
                    Owner.sunbiz_enriched_at < cutoff,
                ),
            ),
        )
        .order_by(DistressScore.final_cds_score.desc())
        .limit(limit)
    ).scalars().unique().all()
    return list(rows)


def _select_tier_b(db: Session, county_id: str, limit: int) -> list[Owner]:
    """Cold: pending OR parser_failed OR stale-180d, any LLC owner with a property."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=TIER_B_DAYS)
    rows = db.execute(
        select(Owner)
        .join(Property, Property.id == Owner.property_id)
        .where(
            Property.county_id == county_id,
            Owner.owner_name.isnot(None),
            Owner.owner_type.in_(("LLC", "Corporate")),
            or_(
                Owner.sunbiz_status.in_(("pending", "parser_failed")),
                and_(
                    Owner.sunbiz_status == "matched",
                    Owner.sunbiz_enriched_at < cutoff,
                ),
            ),
        )
        .order_by(Owner.sunbiz_enriched_at.asc().nulls_first())
        .limit(limit)
    ).scalars().unique().all()
    return list(rows)


def run(
    *,
    county_id: str = "hillsborough",
    limit: int = 200,
    tier: str = "both",
    dry_run: bool = False,
    headless: bool = True,
) -> dict:
    """
    Run the enrichment for `limit` rows, preferring Tier A. Returns stats dict.
    """
    import asyncio

    stats = {"processed": 0, "enriched": 0, "skipped": 0, "failed": 0, "tier_a": 0, "tier_b": 0}

    with get_db_context() as db:
        owners: list[Owner] = []
        if tier in ("a", "both"):
            tier_a = _select_tier_a(db, county_id, limit)
            stats["tier_a"] = len(tier_a)
            owners.extend(tier_a)
        if tier in ("b", "both") and len(owners) < limit:
            remaining = limit - len(owners)
            tier_b = _select_tier_b(db, county_id, remaining)
            seen = {o.id for o in owners}
            tier_b = [o for o in tier_b if o.id not in seen]
            stats["tier_b"] = len(tier_b)
            owners.extend(tier_b)

        logger.info(
            f"[sunbiz_enrichment] queued tier_a={stats['tier_a']} "
            f"tier_b={stats['tier_b']} county={county_id} dry_run={dry_run}"
        )
        if not owners:
            if not dry_run:
                from src.utils.scraper_db_helper import record_scraper_stats
                record_scraper_stats(
                    source_type="sunbiz",
                    total_scraped=0,
                    matched=0,
                    unmatched=0,
                    skipped=0,
                    run_success=True,
                    error_type="no_data",
                    county_id=county_id,
                )
            return stats

        asyncio.run(_run_playwright_batch(owners, dry_run, stats, db, headless=headless))

        if not dry_run:
            db.commit()
            logger.info(
                f"[sunbiz_enrichment] committed: enriched={stats['enriched']} "
                f"skipped={stats['skipped']} failed={stats['failed']}"
            )
            from src.utils.scraper_db_helper import record_scraper_stats
            from src.scrappers.sunbiz.sunbiz_engine import sunbiz_run_verdict

            # Shared verdict with the standalone engine — tolerant of the routine
            # 1-2 no-match/one-off Playwright failures at 200 owners/day. The old
            # zero-tolerance `stats["failed"] == 0` rule false-alarmed daily.
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

    return stats


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Daily Sunbiz enrichment with tiered staleness")
    p.add_argument("--county", default="hillsborough")
    p.add_argument("--limit", type=int, default=200)
    p.add_argument("--tier", choices=("a", "b", "both"), default="both")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--headful", action="store_true")
    args = p.parse_args()
    run(
        county_id=args.county,
        limit=args.limit,
        tier=args.tier,
        dry_run=args.dry_run,
        headless=not args.headful,
    )
