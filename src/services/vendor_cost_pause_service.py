"""
Service layer for VendorCostPause lifecycle.

All pause reads, creates, extends, auto-resume, and manual resume
go through this service to maintain consistent semantics.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from src.core.models import VendorCostPause

logger = logging.getLogger(__name__)

# Cache TTL for active pause lookups (seconds)
PAUSE_CACHE_TTL = 60

# Auto-resume after this many hours
AUTO_RESUME_HOURS = 24

# In-memory cache: { (vendor, pause_target): (pause_id, resumed_at, cached_at) }
_cache: dict[tuple[str, str], tuple[int | None, datetime | None, float]] = {}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _cache_key(vendor: str, pause_target: str) -> tuple[str, str]:
    return (vendor, pause_target)


def get_active_pause(
    db: Session,
    vendor: str,
    pause_target: str,
    use_cache: bool = True,
) -> Optional[VendorCostPause]:
    """
    Return the active VendorCostPause for (vendor, pause_target) if one exists.
    Respects in-memory TTL cache when use_cache=True.
    """
    key = _cache_key(vendor, pause_target)

    if use_cache and key in _cache:
        pause_id, resumed_at, cached_at = _cache[key]
        if (datetime.now(timezone.utc).timestamp() - cached_at) < PAUSE_CACHE_TTL:
            if pause_id is None:
                return None
            row = db.get(VendorCostPause, pause_id)
            if row and row.status == "active":
                return row
            # Stale cache entry, fall through to DB query
        # Cache expired, fall through

    row = db.execute(
        select(VendorCostPause).where(
            VendorCostPause.vendor == vendor,
            VendorCostPause.pause_target == pause_target,
            VendorCostPause.status == "active",
        )
    ).scalar_one_or_none()

    _cache[key] = (
        row.id if row else None,
        _now(),
        datetime.now(timezone.utc).timestamp(),
    )
    return row


def list_active_pauses(db: Session) -> list[VendorCostPause]:
    """Return all active VendorCostPause rows."""
    return list(
        db.execute(
            select(VendorCostPause)
            .where(VendorCostPause.status == "active")
            .order_by(VendorCostPause.paused_at.desc())
        ).scalars().all()
    )


def list_recent_pauses(
    db: Session,
    limit: int = 50,
    offset: int = 0,
) -> list[VendorCostPause]:
    """Return recent pauses ordered by creation time (desc)."""
    return list(
        db.execute(
            select(VendorCostPause)
            .order_by(VendorCostPause.created_at.desc())
            .limit(limit)
            .offset(offset)
        ).scalars().all()
    )


def create_pause(
    db: Session,
    vendor: str,
    pause_target: str,
    reason: str,
    anomaly_score: Optional[float] = None,
    today_cost_usd: Optional[float] = None,
    baseline_avg_usd: Optional[float] = None,
    baseline_stddev_usd: Optional[float] = None,
    threshold_usd: Optional[float] = None,
    sample_n: Optional[int] = None,
    window_days: int = 14,
    source_table: Optional[str] = None,
    source_key: Optional[str] = None,
    metadata_json: Optional[dict] = None,
) -> VendorCostPause:
    """
    Create a new active pause. Auto-resume is set to 24 hours from now.
    If an active pause already exists for (vendor, pause_target), extend it instead.
    """
    existing = get_active_pause(db, vendor, pause_target, use_cache=False)
    if existing:
        return extend_pause(db, existing, today_cost_usd=today_cost_usd,
                            baseline_avg_usd=baseline_avg_usd,
                            baseline_stddev_usd=baseline_stddev_usd,
                            threshold_usd=threshold_usd,
                            sample_n=sample_n,
                            anomaly_score=anomaly_score)

    pause = VendorCostPause(
        vendor=vendor,
        pause_target=pause_target,
        reason=reason,
        anomaly_score=anomaly_score,
        today_cost_usd=today_cost_usd,
        baseline_avg_usd=baseline_avg_usd,
        baseline_stddev_usd=baseline_stddev_usd,
        threshold_usd=threshold_usd,
        sample_n=sample_n,
        window_days=window_days,
        paused_at=_now(),
        auto_resume_at=_now() + timedelta(hours=AUTO_RESUME_HOURS),
        status="active",
        created_by="cost_monitor",
        source_table=source_table,
        source_key=source_key,
        metadata_json=metadata_json or {},
    )
    db.add(pause)
    db.flush()

    key = _cache_key(vendor, pause_target)
    _cache[key] = (pause.id, _now(), datetime.now(timezone.utc).timestamp())

    logger.info(
        "vendor_cost_pause: CREATED vendor=%s target=%s reason=%s cost=%.4f threshold=%.4f",
        vendor, pause_target, reason, today_cost_usd or 0, threshold_usd or 0,
    )
    return pause


def extend_pause(
    db: Session,
    pause: VendorCostPause,
    anomaly_score: Optional[float] = None,
    today_cost_usd: Optional[float] = None,
    baseline_avg_usd: Optional[float] = None,
    baseline_stddev_usd: Optional[float] = None,
    threshold_usd: Optional[float] = None,
    sample_n: Optional[int] = None,
) -> VendorCostPause:
    """
    Extend an existing active pause: refresh auto_resume_at from now,
    update anomaly snapshot fields, and increment the repeat counter.
    """
    now = _now()

    # Increment a repeat counter in metadata
    meta = pause.metadata_json or {}
    meta["repeat_count"] = meta.get("repeat_count", 0) + 1
    meta["last_extended_at"] = now.isoformat()

    pause.auto_resume_at = now + timedelta(hours=AUTO_RESUME_HOURS)
    pause.updated_at = now
    pause.metadata_json = meta

    if anomaly_score is not None:
        pause.anomaly_score = anomaly_score
    if today_cost_usd is not None:
        pause.today_cost_usd = today_cost_usd
    if baseline_avg_usd is not None:
        pause.baseline_avg_usd = baseline_avg_usd
    if baseline_stddev_usd is not None:
        pause.baseline_stddev_usd = baseline_stddev_usd
    if threshold_usd is not None:
        pause.threshold_usd = threshold_usd
    if sample_n is not None:
        pause.sample_n = sample_n

    db.flush()

    key = _cache_key(pause.vendor, pause.pause_target)
    _cache[key] = (pause.id, now, datetime.now(timezone.utc).timestamp())

    logger.info(
        "vendor_cost_pause: EXTENDED vendor=%s target=%s repeat=%d",
        pause.vendor, pause.pause_target, meta.get("repeat_count"),
    )
    return pause


def auto_resume_expired(db: Session) -> int:
    """
    Find all active pauses whose auto_resume_at has passed and mark them as
    auto_resumed. Returns the number of pauses resumed.
    """
    now = _now()
    expired = list(
        db.execute(
            select(VendorCostPause).where(
                VendorCostPause.status == "active",
                VendorCostPause.auto_resume_at <= now,
            )
        ).scalars().all()
    )

    count = 0
    for pause in expired:
        pause.status = "auto_resumed"
        pause.resumed_at = now
        pause.updated_at = now
        # Invalidate cache
        key = _cache_key(pause.vendor, pause.pause_target)
        _cache.pop(key, None)
        count += 1
        logger.info(
            "vendor_cost_pause: AUTO_RESUMED vendor=%s target=%s",
            pause.vendor, pause.pause_target,
        )

    if count:
        db.flush()
        logger.info("vendor_cost_pause: auto-resumed %d expired pauses", count)
    return count


def manual_resume(
    db: Session,
    pause: VendorCostPause,
    resumed_by: str,
    reason: str,
) -> VendorCostPause:
    """
    Manually resume an active pause. Requires a reason.
    The next daily monitor may re-pause if the anomaly condition still holds.
    """
    if pause.status != "active":
        logger.warning(
            "vendor_cost_pause: cannot resume pause id=%s status=%s",
            pause.id, pause.status,
        )
        return pause

    now = _now()
    pause.status = "manually_resumed"
    pause.resumed_at = now
    pause.resumed_by = resumed_by
    pause.updated_at = now

    # Store resume reason in metadata
    meta = pause.metadata_json or {}
    meta["manual_resume_reason"] = reason
    meta["manually_resumed_at"] = now.isoformat()
    pause.metadata_json = meta

    db.flush()

    key = _cache_key(pause.vendor, pause.pause_target)
    _cache.pop(key, None)

    logger.info(
        "vendor_cost_pause: MANUALLY_RESUMED vendor=%s target=%s by=%s reason=%s",
        pause.vendor, pause.pause_target, resumed_by, reason,
    )
    return pause


def count_skipped_actions(
    db: Session,
    vendor: str,
    pause_target: str,
    since: Optional[datetime] = None,
) -> int:
    """
    Count how many api_usage_logs rows were blocked_by_pause for
    (vendor, pause_target), optionally since a given timestamp.
    Used by daily reporting.
    """
    from src.core.models import ApiUsageLog

    stmt = select(func.count(ApiUsageLog.id)).where(
        ApiUsageLog.service == vendor,
        ApiUsageLog.pause_target == pause_target,
        ApiUsageLog.blocked_by_pause == True,
    )
    if since:
        stmt = stmt.where(ApiUsageLog.created_at >= since)

    return db.execute(stmt).scalar() or 0


def invalidate_cache(vendor: str, pause_target: str) -> None:
    """Force cache invalidation for a specific (vendor, pause_target) pair."""
    key = _cache_key(vendor, pause_target)
    _cache.pop(key, None)