"""Daily contact freshness sweep for aged skip-traced phones."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import and_, func, or_
from sqlalchemy.orm import Session

from src.core.database import get_db_context
from src.core.models import DistressScore, EnrichedContact, Owner, Property, SmsDeadLetter, SmsSendLog
from src.services.contact_freshness import (
    CONFIDENCE_LOW,
    CONFIDENCE_STALE,
    apply_contact_freshness,
    compute_contact_freshness,
)
from src.utils.logger import get_logger, setup_logging

setup_logging()
logger = get_logger(__name__)

GOLD_PLUS_TIERS = ("Gold", "Platinum", "Ultra Platinum")
REFRESHABLE_LEVELS = (CONFIDENCE_LOW, CONFIDENCE_STALE)


@dataclass
class SweepStats:
    scanned: int = 0
    high: int = 0
    medium: int = 0
    low: int = 0
    stale: int = 0
    queued: int = 0
    refreshed: int = 0
    failed: int = 0

    def as_dict(self) -> dict:
        return {
            "scanned": self.scanned,
            "high": self.high,
            "medium": self.medium,
            "low": self.low,
            "stale": self.stale,
            "queued": self.queued,
            "refreshed": self.refreshed,
            "failed": self.failed,
        }


def _latest_contact(session: Session, property_id: int) -> Optional[EnrichedContact]:
    return (
        session.query(EnrichedContact)
        .filter(
            EnrichedContact.property_id == property_id,
            EnrichedContact.match_success.is_(True),
            EnrichedContact.superseded_at.is_(None),
        )
        .order_by(EnrichedContact.enriched_at.desc())
        .first()
    )


def _last_sms_failed_at(session: Session, owner: Owner) -> Optional[datetime]:
    phones = [p for p in (owner.phone_1, owner.phone_2, owner.phone_3) if p]
    if not phones:
        return None

    send_failed_at = (
        session.query(func.max(SmsSendLog.created_at))
        .filter(SmsSendLog.phone.in_(phones), SmsSendLog.outcome == "failed")
        .scalar()
    )
    dlq_failed_at = (
        session.query(func.max(SmsDeadLetter.created_at))
        .filter(
            SmsDeadLetter.phone.in_(phones),
            SmsDeadLetter.reason.in_(("delivery_failed", "error", "unresolvable")),
        )
        .scalar()
    )
    return max([dt for dt in (send_failed_at, dlq_failed_at) if dt], default=None)


def _active_owner_query(session: Session, county_id: str):
    ds_latest = (
        session.query(
            DistressScore.property_id,
            func.max(DistressScore.score_date).label("max_date"),
        )
        .group_by(DistressScore.property_id)
        .subquery()
    )
    ds_current = (
        session.query(DistressScore.property_id, DistressScore.lead_tier, DistressScore.score_date)
        .join(
            ds_latest,
            and_(
                DistressScore.property_id == ds_latest.c.property_id,
                DistressScore.score_date == ds_latest.c.max_date,
            ),
        )
        .filter(DistressScore.lead_tier.in_(GOLD_PLUS_TIERS))
        .subquery()
    )

    return (
        session.query(Owner)
        .join(Property, Owner.property_id == Property.id)
        .join(ds_current, ds_current.c.property_id == Property.id)
        .filter(Owner.county_id == county_id)
        .filter(
            or_(
                Owner.phone_1.isnot(None),
                Owner.phone_2.isnot(None),
                Owner.phone_3.isnot(None),
                Owner.skip_trace_success.is_(True),
            )
        )
        .order_by(ds_current.c.score_date.desc(), Owner.id.asc())
    )


def _refresh_owner(owner_id: int, county_id: str, run_skip_trace_fn=None) -> bool:
    if run_skip_trace_fn is None:
        from src.services.skip_trace import run_skip_trace as run_skip_trace_fn

    stats = run_skip_trace_fn(
        owner_ids=[owner_id],
        county_id=county_id,
        today_only=False,
        refresh_stale=True,
        refresh_stale_after_days=0,
        limit=1,
    )
    return bool(stats.get("success") or stats.get("retraced"))


def run_sweep(
    county_id: str = "hillsborough",
    limit: int = 200,
    refresh_limit: int = 50,
    refresh: bool = True,
    now: Optional[datetime] = None,
) -> dict:
    """Recompute contact confidence and optionally refresh stale active leads."""
    now = now or datetime.now(timezone.utc)
    stats = SweepStats()
    refresh_ids: list[int] = []

    with get_db_context() as session:
        owners = _active_owner_query(session, county_id).limit(limit).all()
        for owner in owners:
            latest = _latest_contact(session, owner.property_id)
            failed_at = _last_sms_failed_at(session, owner)
            freshness = compute_contact_freshness(owner, latest, now=now, last_sms_failed_at=failed_at)
            apply_contact_freshness(owner, freshness)

            stats.scanned += 1
            setattr(stats, freshness.level, getattr(stats, freshness.level) + 1)

            if freshness.level in REFRESHABLE_LEVELS and len(refresh_ids) < refresh_limit:
                owner.contact_refresh_status = "queued"
                refresh_ids.append(owner.id)
                stats.queued += 1

        session.commit()

    if not refresh:
        logger.info("[ContactFreshness] dry scan county=%s stats=%s", county_id, stats.as_dict())
        return stats.as_dict()

    for owner_id in refresh_ids:
        try:
            if _refresh_owner(owner_id, county_id):
                stats.refreshed += 1
            else:
                stats.failed += 1
        except Exception as exc:
            stats.failed += 1
            logger.warning("[ContactFreshness] refresh failed owner_id=%s: %s", owner_id, exc)

    if refresh_ids:
        with get_db_context() as session:
            refreshed_owners = session.query(Owner).filter(Owner.id.in_(refresh_ids)).all()
            for owner in refreshed_owners:
                latest = _latest_contact(session, owner.property_id)
                failed_at = _last_sms_failed_at(session, owner)
                freshness = compute_contact_freshness(
                    owner,
                    latest,
                    now=datetime.now(timezone.utc),
                    last_sms_failed_at=failed_at,
                )
                apply_contact_freshness(owner, freshness)
                owner.contact_refresh_status = (
                    "refreshed" if freshness.level not in REFRESHABLE_LEVELS else "failed"
                )
            session.commit()

    logger.info("[ContactFreshness] county=%s stats=%s", county_id, stats.as_dict())
    return stats.as_dict()


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh stale owner contact confidence.")
    parser.add_argument("--county-id", default="hillsborough")
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--refresh-limit", type=int, default=50)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    stats = run_sweep(
        county_id=args.county_id,
        limit=args.limit,
        refresh_limit=args.refresh_limit,
        refresh=not args.dry_run,
    )
    print(stats)


if __name__ == "__main__":
    main()
