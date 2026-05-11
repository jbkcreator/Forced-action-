"""
Daily phone-deliverability sampler.

Samples N Gold+ leads, classifies each contact's primary phone as
mobile / voip / landline / unknown, records a snapshot row in
phone_deliverability_snapshots, and alerts ops if the mobile share
drops below the configured threshold.

Cached `Owner.phone_metadata` is used when present (free); only phones
that have no metadata are looked up via Telnyx (~$0.004 per call). New
lookups are written back to `Owner.phone_metadata` so subsequent runs
hit the cache.

Run manually:
    python -m src.tasks.phone_deliverability_sampler
    python -m src.tasks.phone_deliverability_sampler --county-id pinellas --sample 100

Run in cron (recommended after the daily CDS rescore):
    15 08 * * *  python -m src.tasks.phone_deliverability_sampler
"""

from __future__ import annotations

import argparse
import logging
from collections import Counter
from datetime import date, datetime, timezone
from typing import Optional

from sqlalchemy import func, text
from sqlalchemy.orm import Session

from src.core.database import get_db_context
from src.core.models import (
    DistressScore,
    Owner,
    PhoneDeliverabilitySnapshot,
    Property,
)
from src.services.telnyx_lookup import TelnyxLookupError, lookup_phone
from src.utils.logger import get_logger, setup_logging

setup_logging()
logger = get_logger(__name__)

GOLD_PLUS_TIERS = ("Gold", "Platinum", "Ultra Platinum")
TELNYX_LOOKUP_COST_CENTS = 0.4  # ~$0.004 per carrier lookup


# ---------------------------------------------------------------------------
# Sampling helpers
# ---------------------------------------------------------------------------

def _sample_gold_plus(
    session: Session,
    county_id: Optional[str],
    sample_size: int,
) -> list[tuple[int, Optional[str], Optional[dict]]]:
    """
    Return up to `sample_size` random (property_id, phone_1, phone_metadata)
    tuples for Gold+ properties whose Owner has a phone on file.

    Contacts with no phone are excluded from the sample on purpose: their
    upstream-drop count is already tracked by the daily ops report's
    PHONE COVERAGE section. Sampling them here would waste the row on a
    metric we measure elsewhere — and a Telnyx lookup can't run on an
    empty string anyway. This keeps the deliverability % a meaningful
    measure of "of the phones we have, how many are SMS-deliverable?"
    """
    q = (
        session.query(
            Property.id,
            Owner.phone_1,
            Owner.phone_metadata,
        )
        .join(Owner, Owner.property_id == Property.id)
        .join(DistressScore, DistressScore.property_id == Property.id)
        .filter(DistressScore.lead_tier.in_(GOLD_PLUS_TIERS))
        .filter(DistressScore.score_date >= func.current_date() - 7)
        # Only sample contacts that actually have a phone — empty/null
        # phones are tracked by the daily report's PHONE COVERAGE section.
        .filter(Owner.phone_1.isnot(None))
        .filter(func.length(func.trim(Owner.phone_1)) > 0)
    )
    if county_id:
        q = q.filter(Property.county_id == county_id)

    q = q.order_by(func.random()).limit(sample_size)
    return [(pid, phone, meta) for pid, phone, meta in q.all()]


def _cached_line_type(phone_metadata: Optional[dict], slot: str = "phone_1") -> Optional[str]:
    """Pull the cached line type for a phone slot, if metadata has it."""
    if not phone_metadata:
        return None
    entry = phone_metadata.get(slot) if isinstance(phone_metadata, dict) else None
    if not isinstance(entry, dict):
        return None
    t = (entry.get("type") or "").lower()
    if t in ("mobile", "voip", "landline"):
        return t
    return None


# ---------------------------------------------------------------------------
# Main task
# ---------------------------------------------------------------------------

def run_sample(
    sample_size: int = 200,
    county_id: Optional[str] = None,
    tier_filter: str = "gold_plus",
) -> dict:
    """
    Sample Gold+ contacts and record a phone-deliverability snapshot.

    Returns a dict summary of the run for logging / testing.
    """
    snap_date = date.today()
    counts: Counter = Counter()
    lookups_cached = 0
    lookups_attempted = 0
    lookups_succeeded = 0
    sampled = 0

    with get_db_context() as session:
        rows = _sample_gold_plus(session, county_id, sample_size)
        sampled = len(rows)

        for property_id, phone_1, phone_metadata in rows:
            if not phone_1:
                counts["no_phone"] += 1
                continue

            # Try cache first
            cached = _cached_line_type(phone_metadata, "phone_1")
            if cached:
                counts[cached] += 1
                lookups_cached += 1
                continue

            # Fall back to Telnyx
            lookups_attempted += 1
            try:
                result = lookup_phone(phone_1)
                lookups_succeeded += 1
            except TelnyxLookupError as exc:
                logger.warning(
                    "[PhoneSampler] Telnyx failed for property_id=%s: %s",
                    property_id, exc,
                )
                counts["unknown"] += 1
                continue

            ltype = result["type"]
            counts[ltype if ltype in ("mobile", "voip", "landline") else "unknown"] += 1

            # Backfill onto Owner.phone_metadata so future runs use cache.
            try:
                owner = (
                    session.query(Owner).filter(Owner.property_id == property_id).first()
                )
                if owner is not None:
                    meta = dict(owner.phone_metadata or {})
                    meta["phone_1"] = result
                    owner.phone_metadata = meta
            except Exception as exc:
                logger.warning(
                    "[PhoneSampler] Could not backfill phone_metadata for "
                    "property_id=%s: %s", property_id, exc,
                )

        # Compute headline metric
        mobile_pct: Optional[float] = None
        deliverable_universe = (
            counts["mobile"] + counts["voip"] + counts["landline"] + counts["unknown"]
        )
        if deliverable_universe > 0:
            mobile_pct = round(100.0 * counts["mobile"] / deliverable_universe, 2)

        # Cost estimate (Telnyx lookups only)
        cost_cents = round(lookups_attempted * TELNYX_LOOKUP_COST_CENTS)

        # Persist snapshot — upsert on (snapshot_date, county_id, tier_filter)
        snap_county = county_id or "all"
        existing = (
            session.query(PhoneDeliverabilitySnapshot)
            .filter_by(
                snapshot_date=snap_date,
                county_id=snap_county,
                tier_filter=tier_filter,
            )
            .first()
        )
        if existing:
            existing.sample_size = sampled
            existing.lookups_cached = lookups_cached
            existing.lookups_attempted = lookups_attempted
            existing.lookups_succeeded = lookups_succeeded
            existing.mobile_count   = counts["mobile"]
            existing.voip_count     = counts["voip"]
            existing.landline_count = counts["landline"]
            existing.unknown_count  = counts["unknown"]
            existing.no_phone_count = counts["no_phone"]
            existing.mobile_pct = mobile_pct
            existing.vendor = "telnyx"
            existing.cost_cents = cost_cents
            snapshot = existing
        else:
            snapshot = PhoneDeliverabilitySnapshot(
                snapshot_date=snap_date,
                county_id=snap_county,
                tier_filter=tier_filter,
                sample_size=sampled,
                lookups_cached=lookups_cached,
                lookups_attempted=lookups_attempted,
                lookups_succeeded=lookups_succeeded,
                mobile_count=counts["mobile"],
                voip_count=counts["voip"],
                landline_count=counts["landline"],
                unknown_count=counts["unknown"],
                no_phone_count=counts["no_phone"],
                mobile_pct=mobile_pct,
                vendor="telnyx",
                cost_cents=cost_cents,
            )
            session.add(snapshot)

        session.commit()

    summary = {
        "snapshot_date":     snap_date.isoformat(),
        "county_id":         county_id or "all",
        "sample_size":       sampled,
        "lookups_cached":    lookups_cached,
        "lookups_attempted": lookups_attempted,
        "lookups_succeeded": lookups_succeeded,
        "mobile_pct":        mobile_pct,
        "counts":            dict(counts),
        "cost_cents":        cost_cents,
    }
    logger.info("[PhoneSampler] %s", summary)
    _maybe_alert(summary)
    return summary


def _maybe_alert(summary: dict) -> None:
    """If mobile_pct fell below the alert threshold today and yesterday, alert ops."""
    from config.settings import get_settings
    settings = get_settings()
    threshold = settings.phone_sample_mobile_alert_pct

    today_pct = summary.get("mobile_pct")
    if today_pct is None or today_pct >= threshold:
        return

    # Two-day trend check before alerting
    with get_db_context() as session:
        rows = (
            session.query(PhoneDeliverabilitySnapshot.mobile_pct)
            .filter(PhoneDeliverabilitySnapshot.county_id == summary["county_id"])
            .order_by(PhoneDeliverabilitySnapshot.snapshot_date.desc())
            .limit(2)
            .all()
        )
        recent_pcts = [float(r[0]) for r in rows if r[0] is not None]

    if len(recent_pcts) < 2 or any(p >= threshold for p in recent_pcts):
        return  # not two consecutive bad days yet

    logger.warning(
        "[PhoneSampler][ALERT] mobile_pct=%s%% for %s — below threshold %s%% for 2 days",
        today_pct, summary["county_id"], threshold,
    )
    try:
        from src.services.email import send_alert
        send_alert(
            subject=f"[FA] Phone deliverability degraded: {today_pct}% mobile",
            body=(
                f"Phone deliverability sampler is reporting only {today_pct}% mobile "
                f"share across Gold+ leads in {summary['county_id']} for 2 consecutive "
                f"days (threshold {threshold}%).\n\nRecent: {recent_pcts}\n"
                f"Full run: {summary}"
            ),
        )
    except Exception as exc:
        logger.warning("[PhoneSampler] Alert email send failed: %s", exc)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Sample Gold+ leads for phone deliverability (mobile/landline/voip)."
    )
    parser.add_argument("--county-id", dest="county_id", default=None,
                        help="Restrict sample to a single county")
    parser.add_argument("--sample", type=int, default=None,
                        help="Sample size (default: PHONE_SAMPLE_SIZE env var or 200)")
    parser.add_argument("--tier-filter", default="gold_plus")
    args = parser.parse_args()

    from config.settings import get_settings
    settings = get_settings()
    size = args.sample or settings.phone_sample_size

    run_sample(
        sample_size=size,
        county_id=args.county_id,
        tier_filter=args.tier_filter,
    )


if __name__ == "__main__":
    main()
