"""Contact freshness scoring for skip-traced owner data."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional

from config.triangulation import (
    STRONG_AGE_DECAY_FACTOR,
    STRONG_BASE_BOOST,
    WEAK_BASE_BOOST,
    WEAK_CAP_LABEL,
)

CONFIDENCE_HIGH = "high"
CONFIDENCE_MEDIUM = "medium"
CONFIDENCE_LOW = "low"
CONFIDENCE_STALE = "stale"


@dataclass(frozen=True)
class ContactFreshness:
    level: str
    score: float
    reason: str
    last_verified_at: Optional[datetime]
    next_refresh_at: Optional[datetime]
    refresh_status: str


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _as_float(value: object, default: float = 0.0) -> float:
    if value is None:
        return default
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _aware(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _phone_meta(owner) -> dict:
    meta = getattr(owner, "phone_metadata", None)
    if not isinstance(meta, dict):
        return {}
    slot_meta = meta.get("phone_1")
    return slot_meta if isinstance(slot_meta, dict) else {}


def _has_phone(owner) -> bool:
    return any(
        bool((getattr(owner, slot, None) or "").strip())
        for slot in ("phone_1", "phone_2", "phone_3")
    )


def _has_recent_sms_failure(last_sms_failed_at: Optional[datetime], now: datetime) -> bool:
    failed_at = _aware(last_sms_failed_at)
    return bool(failed_at and failed_at >= now - timedelta(days=90))


def compute_contact_freshness(
    owner,
    latest_contact=None,
    *,
    now: Optional[datetime] = None,
    last_sms_failed_at: Optional[datetime] = None,
    corroboration: Optional[str] = None,
) -> ContactFreshness:
    """
    Convert contact age, provider confidence, and phone metadata into a label.

    The labels are conservative because stale contact data creates wasted SMS
    sends and poor lead-sale experiences.

    corroboration (ADR 0015): cross-source triangulation verdict from
    contact_triangulation.compute_corroboration. "strong" boosts the base
    score and relaxes age decay (a number independent sources keep confirming
    decays slower); "weak" boosts slightly but caps the label at
    WEAK_CAP_LABEL. None/"none" leaves behavior byte-identical to pre-ADR-0015.
    Hard overrides (missing phone, invalid verification, recent SMS failure)
    always win — corroboration never resurrects a bounced number.
    """
    now = _aware(now) or _utcnow()
    latest_at = _aware(getattr(latest_contact, "enriched_at", None))
    contact_confidence = _as_float(getattr(latest_contact, "confidence", None), default=0.0)
    meta = _phone_meta(owner)
    meta_score = _as_float(meta.get("score"), default=0.0) / 100.0
    reachable = meta.get("reachable")
    line_type = (meta.get("type") or "").lower()
    verification_status = (getattr(latest_contact, "verification_status", None) or "").lower()

    if not _has_phone(owner):
        return ContactFreshness(
            level=CONFIDENCE_STALE,
            score=0.0,
            reason="missing_phone",
            last_verified_at=latest_at,
            next_refresh_at=now,
            refresh_status="due",
        )

    if verification_status == "invalid":
        return ContactFreshness(
            level=CONFIDENCE_STALE,
            score=0.0,
            reason="verification_invalid",
            last_verified_at=latest_at,
            next_refresh_at=now,
            refresh_status="due",
        )

    if _has_recent_sms_failure(last_sms_failed_at, now):
        return ContactFreshness(
            level=CONFIDENCE_STALE,
            score=min(0.30, max(contact_confidence, meta_score)),
            reason="recent_sms_failure",
            last_verified_at=latest_at,
            next_refresh_at=now,
            refresh_status="due",
        )

    age_days = 9999 if latest_at is None else max(0, (now - latest_at).days)
    base = max(contact_confidence, meta_score)
    if base <= 0:
        base = 0.55
    if reachable is True:
        base += 0.10
    elif reachable is False:
        base -= 0.10
    if line_type == "mobile":
        base += 0.10
    elif line_type == "landline":
        base -= 0.10
    elif line_type == "unknown":
        base -= 0.05

    if corroboration == "strong":
        base += STRONG_BASE_BOOST
    elif corroboration == "weak":
        base += WEAK_BASE_BOOST

    # Strong corroboration slows the clock: both the score penalty and the
    # label ladder see the discounted age, so a voter-confirmed number takes
    # twice as long to decay through medium -> low -> stale.
    eff_age_days = (
        age_days * STRONG_AGE_DECAY_FACTOR if corroboration == "strong" else age_days
    )

    if eff_age_days > 270:
        age_penalty = 0.60
    elif eff_age_days > 180:
        age_penalty = 0.35
    elif eff_age_days > 90:
        age_penalty = 0.15
    else:
        age_penalty = 0.0

    score = round(max(0.0, min(1.0, base - age_penalty)), 3)

    if eff_age_days > 270 or score < 0.40:
        level = CONFIDENCE_STALE
        refresh_after = now
        status = "due"
    elif eff_age_days > 180 or score < 0.60:
        level = CONFIDENCE_LOW
        refresh_after = now
        status = "due"
    elif eff_age_days > 90 or score < 0.80:
        level = CONFIDENCE_MEDIUM
        refresh_after = (latest_at or now) + timedelta(days=180)
        status = "fresh"
    else:
        level = CONFIDENCE_HIGH
        refresh_after = (latest_at or now) + timedelta(days=180)
        status = "fresh"

    # Weak corroboration (phone matched but name didn't, historical voter
    # phone, inactive voter) never mints the top label.
    if corroboration == "weak" and level == CONFIDENCE_HIGH:
        level = WEAK_CAP_LABEL

    return ContactFreshness(
        level=level,
        score=score,
        reason=f"age_{age_days}_days",
        last_verified_at=latest_at,
        next_refresh_at=refresh_after,
        refresh_status=status,
    )


def apply_contact_freshness(owner, freshness: ContactFreshness) -> None:
    owner.contact_info_confidence = freshness.level
    owner.contact_info_confidence_score = freshness.score
    owner.contact_last_verified_at = freshness.last_verified_at
    owner.contact_next_refresh_at = freshness.next_refresh_at
    owner.contact_refresh_status = freshness.refresh_status
    owner.contact_refresh_reason = freshness.reason
