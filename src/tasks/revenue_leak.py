"""
Revenue Leak Score Engine (Sprint S5).

Nightly per-county job that finds Gold+ leads scored >48 hours ago with no
subsequent SentLead delivery, and estimates the platform's revenue loss using
lead pack pricing: every 5 undelivered leads = one $99 lead pack that wasn't sold.

Formula:
    estimated_revenue_loss = floor(leaked_leads / 5) * LEAD_PACK_PRICE
    (partial packs of <5 are not counted — you can't sell an incomplete pack)

Cron (crontab.txt):
    38 7 * * 1-6  src.tasks.revenue_leak hillsborough
    39 7 * * 1-6  src.tasks.revenue_leak pinellas

Usage:
    python -m src.tasks.revenue_leak hillsborough
    python -m src.tasks.revenue_leak hillsborough --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
from collections import defaultdict
from datetime import date
from decimal import Decimal

from sqlalchemy import text

from src.core.database import get_db_context

logger = logging.getLogger(__name__)

# Lead pack sell price — one pack = 5 leads, $99 one-time payment.
# Platform revenue lost = floor(leaked_leads / 5) * LEAD_PACK_PRICE
LEAD_PACK_PRICE = Decimal("99.00")
LEADS_PER_PACK = 5


def _estimate_platform_revenue_loss(leaked_lead_count: int) -> Decimal:
    """
    Estimate platform revenue loss using lead pack pricing.
    Only complete packs of 5 are counted — partial packs cannot be sold.
    """
    full_packs = leaked_lead_count // LEADS_PER_PACK
    return Decimal(full_packs) * LEAD_PACK_PRICE


def run(county_id: str, dry_run: bool = False) -> dict:
    """
    Compute the revenue leak for one county and upsert a revenue_leak_log row.
    Returns a summary dict for logging/testing.
    """
    today = date.today()
    summary: dict = {
        "county_id": county_id,
        "log_date": str(today),
        "dry_run": dry_run,
        "total_leads_leaked": 0,
        "estimated_revenue_loss": 0.0,
        "full_packs_missed": 0,
        "vertical_breakdown": {},
        "pricing_basis": f"{LEADS_PER_PACK} leads @ ${LEAD_PACK_PRICE} per pack",
    }

    with get_db_context() as db:
        # ── Find latest Gold+ score per property with no SentLead ────────────
        # DISTINCT ON keeps the most-recent score row per property.
        # Tier strings match CheckConstraint exactly (capitalised, with spaces).
        rows = db.execute(
            text("""
                WITH latest AS (
                    SELECT DISTINCT ON (ds.property_id)
                           ds.property_id,
                           ds.score_date,
                           ds.vertical_scores
                    FROM distress_scores ds
                    WHERE ds.county_id  = :county
                      AND ds.lead_tier IN ('Ultra Platinum', 'Platinum', 'Gold')
                    ORDER BY ds.property_id, ds.score_date DESC
                )
                SELECT l.property_id,
                       l.vertical_scores
                FROM latest l
                WHERE l.score_date < (NOW() - INTERVAL '48 hours')
                  AND NOT EXISTS (
                    SELECT 1 FROM sent_leads s
                    WHERE s.property_id = l.property_id
                      AND s.sent_at     > l.score_date
                  )
            """),
            {"county": county_id},
        ).fetchall()

        if not rows:
            logger.info("[RevenueLeak] county=%s — no leaked leads", county_id)
            if not dry_run:
                _upsert(db, today, county_id, 0, Decimal("0"), {})
                db.commit()
            return summary

        # ── Count by vertical for breakdown ──────────────────────────────────
        vertical_buckets: dict[str, int] = defaultdict(int)
        for _prop_id, vertical_scores in rows:
            if vertical_scores:
                top_v = max(vertical_scores, key=lambda k: vertical_scores[k])
            else:
                top_v = "unknown"
            vertical_buckets[top_v] += 1

        total_leads = len(rows)
        full_packs = total_leads // LEADS_PER_PACK
        estimated_loss = _estimate_platform_revenue_loss(total_leads)

        # Build vertical breakdown with lead counts and pack-equivalent revenue
        vertical_breakdown = {
            v: {
                "leads": count,
                "full_packs_missed": count // LEADS_PER_PACK,
                "estimated_loss_usd": float(
                    Decimal(count // LEADS_PER_PACK) * LEAD_PACK_PRICE
                ),
            }
            for v, count in vertical_buckets.items()
        }

        summary["total_leads_leaked"] = total_leads
        summary["full_packs_missed"] = full_packs
        summary["estimated_revenue_loss"] = float(estimated_loss)
        summary["vertical_breakdown"] = vertical_breakdown

        logger.info(
            "[RevenueLeak] county=%s date=%s leads=%d packs_missed=%d "
            "platform_revenue_loss=$%.2f (basis: %d leads @ $%s/pack)",
            county_id, today, total_leads, full_packs,
            estimated_loss, LEADS_PER_PACK, LEAD_PACK_PRICE,
        )

        if not dry_run:
            _upsert(
                db,
                today,
                county_id,
                total_leads,
                estimated_loss,
                vertical_breakdown,
            )
            db.commit()
        else:
            logger.info("[RevenueLeak] dry-run — no writes")

    return summary


def _upsert(db, log_date: date, county_id: str, total_leads: int,
            estimated_value: Decimal, vertical_breakdown: dict) -> None:
    db.execute(
        text("""
            INSERT INTO revenue_leak_log
                (log_date, county_id, total_leads_leaked,
                 estimated_dollar_value, vertical_breakdown, created_at)
            VALUES
                (:log_date, :county, :total_leads,
                 :value, CAST(:breakdown AS jsonb), NOW())
            ON CONFLICT (log_date, county_id)
            DO UPDATE SET
                total_leads_leaked     = EXCLUDED.total_leads_leaked,
                estimated_dollar_value = EXCLUDED.estimated_dollar_value,
                vertical_breakdown     = EXCLUDED.vertical_breakdown
        """),
        {
            "log_date": log_date,
            "county": county_id,
            "total_leads": total_leads,
            "value": estimated_value,
            "breakdown": json.dumps(vertical_breakdown),
        },
    )


def main(argv=None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    parser = argparse.ArgumentParser(description="Revenue Leak Score Engine")
    parser.add_argument(
        "county_id", nargs="?", default="hillsborough",
        help="County to evaluate (default: hillsborough)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Compute but do not write to the database",
    )
    args = parser.parse_args(argv)

    result = run(args.county_id, dry_run=args.dry_run)
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
