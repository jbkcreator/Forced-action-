"""
One-off Sunbiz piercing backfill.

Goal: enrich every distinct LLC / Corporate owner_name in `owners` with the
fa031 piercing fields (doc number, principal address, managing members, etc.)
without scraping the same name once per property — naive iteration would scrape
"ACME HOLDINGS LLC" ten times if that LLC owns ten parcels.

Strategy:
  1. Group all pending LLC owners by normalized owner_name. Active-lead names
     (owners on a Gold+ property in a paying ZIP) bubble to the front.
  2. Pick a single representative `Owner` row per name and scrape it. The
     engine's `_persist_snapshot_and_owner` writes back to that row only.
  3. After the scrape commits, fan the result out to every sibling Owner row
     sharing the same normalized name: copy the populated columns + set
     `sunbiz_status='matched'`. One scrape, N row updates.
  4. Resumable: only touches rows with `sunbiz_status='pending'`. Re-running
     skips everything already processed.

Usage:
  python -m scripts.backfill_sunbiz                          # active-lead first
  python -m scripts.backfill_sunbiz --limit 500              # cap names per run
  python -m scripts.backfill_sunbiz --county hillsborough --dry-run
"""

from __future__ import annotations

import argparse
import asyncio
import re
from collections import defaultdict
from datetime import datetime, timezone

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from src.core.database import get_db_context
from src.core.models import DistressScore, Owner, Property
from src.scrappers.sunbiz.sunbiz_engine import _run_playwright_batch
from src.services.owner_lookup import _normalize  # internal — reuse the same algo
from src.utils.logger import get_logger, setup_logging

setup_logging()
logger = get_logger(__name__)

ACTIVE_LEAD_TIERS = ("Gold", "Platinum", "Ultra Platinum")


def _group_pending_by_normalized_name(
    db: Session, county_id: str
) -> dict[str, list[Owner]]:
    """Return {normalized_name: [Owner, ...]} for every pending LLC owner."""
    rows = db.execute(
        select(Owner)
        .join(Property, Property.id == Owner.property_id)
        .where(
            Property.county_id == county_id,
            Owner.owner_type.in_(("LLC", "Corporate")),
            Owner.owner_name.isnot(None),
            Owner.sunbiz_status == "pending",
        )
    ).scalars().unique().all()

    buckets: dict[str, list[Owner]] = defaultdict(list)
    for o in rows:
        norm = _normalize(o.owner_name)
        if not norm:
            continue
        buckets[norm].append(o)
    return buckets


def _active_lead_names(db: Session, county_id: str) -> set[str]:
    """Return {normalized_name} for owners that sit on an active high-tier lead."""
    rows = db.execute(
        select(Owner.owner_name)
        .join(Property, Property.id == Owner.property_id)
        .join(DistressScore, DistressScore.property_id == Property.id)
        .where(
            Property.county_id == county_id,
            DistressScore.lead_tier.in_(ACTIVE_LEAD_TIERS),
            Owner.owner_name.isnot(None),
        )
    ).scalars().unique().all()
    return {_normalize(name) for name in rows if name}


def _fanout_to_siblings(db: Session, scraped: Owner, siblings: list[Owner]) -> int:
    """
    Copy populated Sunbiz columns from the scraped representative onto every
    other Owner row sharing the same normalized name. Returns count touched
    (excludes the scraped row itself).
    """
    if not scraped.sunbiz_status or scraped.sunbiz_status == "pending":
        return 0
    touched = 0
    now = datetime.now(timezone.utc)
    for sib in siblings:
        if sib.id == scraped.id:
            continue
        sib.sunbiz_doc_number       = scraped.sunbiz_doc_number
        sib.principal_address       = scraped.principal_address
        sib.registered_agent_name   = scraped.registered_agent_name
        sib.registered_agent_address = scraped.registered_agent_address
        sib.registered_agent_email  = scraped.registered_agent_email
        sib.entity_status           = scraped.entity_status
        sib.formation_date          = scraped.formation_date
        sib.managing_members        = scraped.managing_members
        sib.sunbiz_status           = scraped.sunbiz_status
        sib.sunbiz_enriched_at      = now
        if scraped.owner_type in ("LLC", "Corporate") and sib.owner_type not in ("LLC", "Corporate"):
            sib.owner_type = scraped.owner_type
        touched += 1
    return touched


def run(
    *,
    county_id: str = "hillsborough",
    limit: int = 0,
    dry_run: bool = False,
    headless: bool = True,
) -> dict:
    stats = {
        "unique_names": 0, "processed": 0, "enriched": 0, "skipped": 0,
        "failed": 0, "siblings_fanned_out": 0,
    }

    with get_db_context() as db:
        buckets = _group_pending_by_normalized_name(db, county_id)
        active = _active_lead_names(db, county_id)
        stats["unique_names"] = len(buckets)

        ordered = sorted(
            buckets.items(),
            key=lambda kv: (kv[0] not in active, -len(kv[1])),
        )
        if limit:
            ordered = ordered[:limit]

        logger.info(
            f"[backfill_sunbiz] queued unique_names={len(ordered)} "
            f"(active={sum(1 for n,_ in ordered if n in active)}) county={county_id}"
        )
        if not ordered:
            return stats

        representatives = [siblings[0] for _, siblings in ordered]
        asyncio.run(_run_playwright_batch(representatives, dry_run, stats, db, headless=headless))

        if not dry_run:
            for rep, (_, siblings) in zip(representatives, ordered):
                stats["siblings_fanned_out"] += _fanout_to_siblings(db, rep, siblings)
            db.commit()
            logger.info(
                f"[backfill_sunbiz] committed: enriched={stats['enriched']} "
                f"fanout={stats['siblings_fanned_out']} failed={stats['failed']}"
            )

    return stats


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="One-off resumable Sunbiz backfill")
    p.add_argument("--county", default="hillsborough")
    p.add_argument("--limit", type=int, default=0, help="Max unique names per run (0=all)")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--headful", action="store_true")
    args = p.parse_args()
    run(
        county_id=args.county,
        limit=args.limit,
        dry_run=args.dry_run,
        headless=not args.headful,
    )
