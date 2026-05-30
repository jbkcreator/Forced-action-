"""
Churn signal extraction — Phase 2 (fa051).

Pure functions; no side effects; no DB writes.
All functions accept an optional `now` datetime for deterministic testing.

Each returns:
    {"score": float 0-1, "raw": {...raw values for features JSONB...}}

Score conventions:
    inactivity_trajectory : 0 = active/on-cadence, 1 = well past personal baseline
    usage_slope           : 0 = stable or growing, 1 = steep decline
    payment_stress        : 0 = no stress, 1 = multiple overlapping stress signals
    engagement_dampener   : 0 = no recent engagement, 1 = strong recent engagement
                            (used as a *reducer* by the scorer — high = reduces risk)
"""
from __future__ import annotations

import statistics
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.core.models import MessageOutcome, Subscriber, WalletTransaction

# ── Calibration constants ──────────────────────────────────────────────────

_ONSET_DAYS = 5           # proactive-save inactivity window (must match _INACTIVE_MIN)
_MIN_HISTORY_DEBITS = 3   # minimum debits for a personalized baseline
_NEW_ACCOUNT_DAYS = 21    # accounts younger than this fall back to global threshold
_DEBIT_HISTORY_CAP = 60   # max rows fetched for median-interval computation
_SLOPE_WINDOW_DAYS = 7    # usage slope look-back window (each side)
_ENGAGEMENT_WINDOW_DAYS = 7  # look-back for engagement dampener


def _now(clock: Optional[datetime]) -> datetime:
    return clock if clock is not None else datetime.now(timezone.utc)


def _ensure_tz(dt: datetime) -> datetime:
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt


# ── Public signal functions ────────────────────────────────────────────────


def inactivity_trajectory(
    sub_id: int,
    db: Session,
    now: Optional[datetime] = None,
) -> dict:
    """Days-since-last-debit relative to subscriber's own median inter-debit interval.

    Personalized baseline: a subscriber whose normal cadence is every 14 days
    and who has been silent for 3 days scores near 0 (not at risk). A daily
    buyer silent for 3 days scores much higher.

    Falls back to flat days_since / _ONSET_DAYS for new accounts or thin history.

    Returns {"score": 0-1, "raw": {...}}.
    """
    reference = _now(now)

    debit_times = db.execute(
        select(WalletTransaction.created_at)
        .where(
            WalletTransaction.subscriber_id == sub_id,
            WalletTransaction.txn_type == "debit",
        )
        .order_by(WalletTransaction.created_at.desc())
        .limit(_DEBIT_HISTORY_CAP)
    ).scalars().all()

    debit_times = [_ensure_tz(d) for d in debit_times]

    last_debit_at = debit_times[0] if debit_times else None
    if last_debit_at:
        days_since = (reference - last_debit_at).total_seconds() / 86400
    else:
        days_since = 999.0

    # Account age check for personalized vs global fallback
    sub_created = db.execute(
        select(Subscriber.created_at).where(Subscriber.id == sub_id)
    ).scalar_one_or_none()

    account_age_days: Optional[float] = None
    if sub_created:
        account_age_days = (reference - _ensure_tz(sub_created)).total_seconds() / 86400

    use_personalized = (
        len(debit_times) >= _MIN_HISTORY_DEBITS
        and account_age_days is not None
        and account_age_days >= _NEW_ACCOUNT_DAYS
    )

    if use_personalized:
        intervals = [
            (debit_times[i] - debit_times[i + 1]).total_seconds() / 86400
            for i in range(len(debit_times) - 1)
        ]
        median_interval = statistics.median(intervals)
        # Ramp: 0 at median_interval, 1 at 2× median (or at least _ONSET_DAYS)
        horizon = max(_ONSET_DAYS, median_interval * 2.0)
        denom = max(1.0, horizon - median_interval)
        score = max(0.0, min(1.0, (days_since - median_interval) / denom))
    else:
        median_interval = None
        score = max(0.0, min(1.0, days_since / _ONSET_DAYS))

    return {
        "score": score,
        "raw": {
            "days_since_last_debit": round(days_since, 2),
            "median_interval_days": round(median_interval, 2) if median_interval is not None else None,
            "debit_count": len(debit_times),
            "personalized": use_personalized,
        },
    }


def usage_slope(
    sub_id: int,
    db: Session,
    now: Optional[datetime] = None,
) -> dict:
    """Slope of wallet debit activity: last-7d vs prior-7d count.

    score 0 = stable or growing; score 1 = activity dropped to zero this window.

    Returns {"score": 0-1, "raw": {...}}.
    """
    reference = _now(now)
    window = timedelta(days=_SLOPE_WINDOW_DAYS)
    recent_start = reference - window
    prior_start = recent_start - window

    def _fetch(start: datetime, end: datetime) -> tuple[int, int]:
        rows = db.execute(
            select(WalletTransaction.amount)
            .where(
                WalletTransaction.subscriber_id == sub_id,
                WalletTransaction.txn_type == "debit",
                WalletTransaction.created_at >= start,
                WalletTransaction.created_at < end,
            )
        ).scalars().all()
        return len(rows), sum(abs(a) for a in rows)

    recent_count, recent_volume = _fetch(recent_start, reference)
    prior_count, prior_volume = _fetch(prior_start, recent_start)

    # Slope: 0 = unchanged; -1 = lost all; +1 = doubled
    count_slope = (recent_count - prior_count) / max(prior_count, 1)
    # Normalize to 0-1 risk: slope <= -1 → score 1.0; slope >= 0 → score 0.0
    score = max(0.0, min(1.0, -count_slope))

    return {
        "score": score,
        "raw": {
            "recent_7d_count": recent_count,
            "prior_7d_count": prior_count,
            "recent_7d_volume": recent_volume,
            "prior_7d_volume": prior_volume,
            "count_slope": round(count_slope, 4),
        },
    }


def payment_stress(
    sub: Subscriber,
    db: Session,  # noqa: ARG001 — reserved for future queries (disputed charges, etc.)
) -> dict:
    """Composite payment distress score from grace + recovery cadence + deficits.

    Returns {"score": 0-1, "raw": {...}}.
    """
    raw: dict = {}
    score = 0.0

    in_grace = sub.status == "grace"
    if in_grace:
        score += 0.40
    raw["in_grace"] = in_grace

    recovery_day3 = bool(getattr(sub, "recovery_day3_sent", False))
    recovery_day5 = bool(getattr(sub, "recovery_day5_sent", False))
    if recovery_day5:
        score += 0.25
    elif recovery_day3:
        score += 0.15
    raw["recovery_day3_sent"] = recovery_day3
    raw["recovery_day5_sent"] = recovery_day5

    missed = int(getattr(sub, "missed_lead_count", 0) or 0)
    disputed = int(getattr(sub, "disputed_count", 0) or 0)
    if missed > 2:
        score += 0.20
    if disputed > 0:
        score += 0.15
    raw["missed_lead_count"] = missed
    raw["disputed_count"] = disputed

    score = max(0.0, min(1.0, score))
    return {"score": score, "raw": raw}


def engagement_dampener(
    sub_id: int,
    db: Session,
    now: Optional[datetime] = None,
) -> dict:
    """Recent reply/click score — high value indicates strong engagement.

    Returned score is used by the scorer as a *reducer*: high engagement
    lowers the final Churn Risk even when buying signals are weak.
    A subscriber who buys nothing but actively replies is not silently churning.

    Returns {"score": 0-1, "raw": {...}} where 1 = strong recent engagement.
    """
    reference = _now(now)
    cutoff = reference - timedelta(days=_ENGAGEMENT_WINDOW_DAYS)

    outcomes = db.execute(
        select(MessageOutcome.replied_at, MessageOutcome.clicked_at)
        .where(
            MessageOutcome.subscriber_id == sub_id,
            MessageOutcome.sent_at >= cutoff,
        )
        .limit(20)
    ).fetchall()

    replies = sum(1 for row in outcomes if row.replied_at is not None)
    clicks = sum(1 for row in outcomes if row.clicked_at is not None)

    if replies >= 2:
        score = 1.0
    elif replies == 1:
        score = 0.75
    elif clicks >= 2:
        score = 0.50
    elif clicks == 1:
        score = 0.30
    else:
        score = 0.0

    return {
        "score": score,
        "raw": {
            "recent_replies": replies,
            "recent_clicks": clicks,
            "window_days": _ENGAGEMENT_WINDOW_DAYS,
        },
    }
