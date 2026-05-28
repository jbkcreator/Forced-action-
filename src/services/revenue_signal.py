"""
Revenue Signal Score — Balanced RFM (0–100).

Weights:
    spend_velocity       25%
    engagement_recency   25%
    wallet_lock_status   20%
    lead_interaction_rate 20%
    zip_competition      10%

fa037 — completes the per-subscriber feature:
    - `compute_score_detail()` returns the full {score, band, breakdown,
      reasons, ...} dict so the admin endpoint and Cora can explain "why".
    - `update_revenue_signal_score()` is the canonical write path — it
      persists all five user_segments freshness/explainability columns
      and appends one row to `revenue_signal_score_events` (audit trail).
    - `get_revenue_signal_score()` is the read-only accessor with a safe
      default for subscribers with no UserSegment row yet.

`compute_score()` is preserved as a back-compat shim that returns the
int score; the 9 existing callsites continue to work unchanged.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import func, select, text as sa_text
from sqlalchemy.orm import Session

from src.core.models import Subscriber, UserSegment, WalletBalance, WalletTransaction

logger = logging.getLogger(__name__)

# ── Weights + band thresholds ──────────────────────────────────────────────

WEIGHTS = {
    "spend_velocity": 0.25,
    "engagement_recency": 0.25,
    "wallet_lock_status": 0.20,
    "lead_interaction_rate": 0.20,
    "zip_competition": 0.10,
}

# (min, max, label) — inclusive on both ends. min must be ascending.
BANDS = [
    (0, 29, "low"),
    (30, 59, "medium"),
    (60, 79, "high"),
    (80, 100, "very_high"),
]


# ── Action labels (passed by callers into update_revenue_signal_score) ────
#
# Stable identifiers — referenced from the admin UI and audit table, so
# changing a value silently would break historical analysis. Add new
# entries; do not edit existing ones.

ACTION_CHECKOUT_COMPLETED = "checkout_completed"           # _on_checkout_completed
ACTION_INVOICE_PAID = "invoice_paid"                       # _on_payment_succeeded
ACTION_PAYMENT_INTENT_SUCCEEDED = "payment_intent_succeeded"  # _on_payment_intent_succeeded
ACTION_LEAD_UNLOCK_PAID = "lead_unlock_paid"               # _on_lead_unlock_payment
ACTION_WALLET_SUBSCRIPTION_RENEWED = "wallet_subscription_renewed"  # _on_wallet_subscription_invoice
ACTION_SUBSCRIPTION_DELETED = "subscription_deleted"       # fa037 — churn hook
ACTION_WALLET_TXN = "wallet_txn"                           # wallet_engine debit/credit
ACTION_ZIP_LOCK_ACQUIRED = "zip_lock_acquired"             # wallet_to_lock
ACTION_SMS_COMMAND_REPLY = "sms_command_reply"             # sms_commands inbound
ACTION_AUTO_MODE_SMS_SENT = "auto_mode_sms_sent"           # auto_mode_followup
ACTION_SMS_OPT_OUT = "sms_opt_out"                         # fa037 — STOP / opt-out hook


def band_for(score: int) -> str:
    """Map a 0–100 score to a band label. Clips out-of-range input."""
    s = max(0, min(100, int(score)))
    for lo, hi, label in BANDS:
        if lo <= s <= hi:
            return label
    return "low"  # unreachable given BANDS coverage


# ──────────────────────────────────────────────────────────────────────────
# Pure computation (no writes)
# ──────────────────────────────────────────────────────────────────────────


def compute_score_detail(subscriber_id: int, db: Session) -> dict:
    """Compute the full Revenue Signal Score breakdown for a subscriber.

    Pure function — does NOT write to the DB. The breakdown values are
    the weighted point contributions, so sum(breakdown.values()) equals
    the score modulo rounding error (≤ ±2 across 5 components).

    Returns a dict:
        {
          "score": int 0-100,
          "band": str in {low, medium, high, very_high},
          "breakdown": {component_name: int contribution},
          "reasons": list[str]  # up to 3 plain-English drivers
        }

    A subscriber that does not exist returns the safe zero shape.
    """
    sub = db.get(Subscriber, subscriber_id)
    if not sub:
        return {
            "score": 0, "band": "low",
            "breakdown": {k: 0 for k in WEIGHTS},
            "reasons": [],
        }

    wallet = db.execute(
        select(WalletBalance).where(WalletBalance.subscriber_id == subscriber_id)
    ).scalar_one_or_none()

    # Each helper returns a 0..1 normalized signal.
    normalized = {
        "spend_velocity": _spend_velocity(subscriber_id, db),
        "engagement_recency": _engagement_recency(subscriber_id, db),
        "wallet_lock_status": _wallet_lock_status(sub, wallet),
        "lead_interaction_rate": _lead_interaction_rate(subscriber_id, db),
        "zip_competition": _zip_competition(sub, db),
    }

    # Weighted contribution in points (0..100 total).
    breakdown = {
        name: int(round(value * WEIGHTS[name] * 100))
        for name, value in normalized.items()
    }
    score = max(0, min(100, sum(breakdown.values())))

    return {
        "score": score,
        "band": band_for(score),
        "breakdown": breakdown,
        "reasons": _build_reasons(normalized, breakdown, sub, wallet),
    }


def compute_score(subscriber_id: int, db: Session) -> int:
    """Back-compat shim — returns just the int score.

    Existing callers (segmentation_engine.reclassify_safe, etc.) keep
    working unchanged. New code should call `compute_score_detail`
    instead so the breakdown is observable.

    Side effect (preserved from pre-fa037 behaviour): if a UserSegment
    row exists, this writes the int score back to it. Removing this
    would break any legacy paths that rely on the implicit persist.
    """
    detail = compute_score_detail(subscriber_id, db)
    score = detail["score"]

    seg = db.execute(
        select(UserSegment).where(UserSegment.subscriber_id == subscriber_id)
    ).scalar_one_or_none()
    if seg:
        seg.revenue_signal_score = score
        db.flush()

    return score


# Alias expected by segmentation_engine.reclassify_safe (pre-fa037 contract).
recompute = compute_score


# ──────────────────────────────────────────────────────────────────────────
# Canonical write path (fa037)
# ──────────────────────────────────────────────────────────────────────────


def update_revenue_signal_score(
    subscriber_id: int,
    action_type: Optional[str] = None,
    metadata: Optional[dict] = None,
    db: Optional[Session] = None,
) -> dict:
    """Recompute the Revenue Signal Score and persist everything.

    This is the canonical write path for the fa037 feature. It:

      1. Reads the prior score from user_segments (None if no row yet).
      2. Computes the new score + band + breakdown + reasons.
      3. Upserts the user_segments row with the 5 explainability /
         freshness columns populated (revenue_signal_band, _breakdown,
         _updated_at, last_significant_action_at, _last_action).
      4. INSERTs one row into revenue_signal_score_events (append-only
         audit). Captures old/new/delta + action_type + metadata.
      5. Returns the full detail dict.

    `db` is required (the helper is always called from inside a session).
    Caller owns the commit boundary — same convention as the rest of the
    services layer.
    """
    if db is None:
        raise ValueError("db session is required")

    sub = db.get(Subscriber, subscriber_id)
    if not sub:
        # Honest no-op: nothing to update, no row to audit. Callers that
        # invoke us from a webhook with a stale subscriber_id (e.g., a
        # Stripe replay after deletion) just get the safe-default shape.
        return {
            "score": 0, "band": "low",
            "breakdown": {k: 0 for k in WEIGHTS},
            "reasons": [],
            "updated_at": None,
            "last_significant_action_at": None,
            "last_action": None,
        }

    old_score_row = db.execute(sa_text("""
        SELECT revenue_signal_score FROM user_segments
        WHERE subscriber_id = :sid
    """), {"sid": subscriber_id}).first()
    old_score = old_score_row.revenue_signal_score if old_score_row else None

    detail = compute_score_detail(subscriber_id, db)
    score = detail["score"]
    band = detail["band"]
    breakdown = detail["breakdown"]

    now = datetime.now(timezone.utc)

    # UPSERT the user_segments row. If no row exists yet, create one with
    # segment='browsing' as the safe default (matches segmentation_engine
    # fallback). The subsequent classify() call from reclassify_safe will
    # overwrite the segment if it should be something else.
    db.execute(sa_text("""
        INSERT INTO user_segments (
            subscriber_id, segment, revenue_signal_score,
            revenue_signal_band, revenue_signal_breakdown,
            revenue_signal_updated_at, last_significant_action_at,
            revenue_signal_last_action,
            last_classified_at, created_at, updated_at
        ) VALUES (
            :sid, 'browsing', :score,
            :band, CAST(:breakdown AS jsonb),
            :now, :now,
            :action,
            :now, :now, :now
        )
        ON CONFLICT (subscriber_id) DO UPDATE SET
            revenue_signal_score        = EXCLUDED.revenue_signal_score,
            revenue_signal_band         = EXCLUDED.revenue_signal_band,
            revenue_signal_breakdown    = EXCLUDED.revenue_signal_breakdown,
            revenue_signal_updated_at   = EXCLUDED.revenue_signal_updated_at,
            last_significant_action_at  = EXCLUDED.last_significant_action_at,
            revenue_signal_last_action  = EXCLUDED.revenue_signal_last_action,
            updated_at                  = EXCLUDED.updated_at
    """), {
        "sid": subscriber_id,
        "score": score,
        "band": band,
        "breakdown": json.dumps(breakdown),
        "now": now,
        "action": action_type,
    })

    # Append-only audit row.
    delta = score - (old_score or 0)
    db.execute(sa_text("""
        INSERT INTO revenue_signal_score_events (
            subscriber_id, action_type, old_score, new_score, delta,
            band, breakdown, metadata, created_at
        ) VALUES (
            :sid, :action, :old, :new, :delta,
            :band, CAST(:breakdown AS jsonb), CAST(:meta AS jsonb), :now
        )
    """), {
        "sid": subscriber_id,
        "action": action_type,
        "old": old_score,
        "new": score,
        "delta": delta,
        "band": band,
        "breakdown": json.dumps(breakdown),
        "meta": json.dumps(metadata) if metadata is not None else None,
        "now": now,
    })

    return {
        **detail,
        "updated_at": now.isoformat(),
        "last_significant_action_at": now.isoformat(),
        "last_action": action_type,
    }


def get_revenue_signal_score(subscriber_id: int, db: Session) -> dict:
    """Read the live Revenue Signal Score state for a subscriber.

    Returns the same shape as `update_revenue_signal_score`. If no
    UserSegment row exists yet (brand-new subscriber, or pre-fa037
    backfill miss), returns a safe default — never raises.

    Reads only — no writes, no audit row.
    """
    row = db.execute(sa_text("""
        SELECT revenue_signal_score, revenue_signal_band,
               revenue_signal_breakdown, revenue_signal_updated_at,
               last_significant_action_at, revenue_signal_last_action
        FROM user_segments
        WHERE subscriber_id = :sid
    """), {"sid": subscriber_id}).first()

    if row is None:
        # No user_segments row — fall back to Stage 8 attribution score stored
        # directly on the subscribers table (written by attribution_service).
        sub_row = db.execute(sa_text("""
            SELECT revenue_signal_score, revenue_signal_band,
                   revenue_signal_breakdown, revenue_signal_updated_at
            FROM subscribers
            WHERE id = :sid
        """), {"sid": subscriber_id}).first()

        if sub_row and sub_row.revenue_signal_score:
            score = int(sub_row.revenue_signal_score)
            band  = sub_row.revenue_signal_band or band_for(score)
            breakdown = sub_row.revenue_signal_breakdown or {}
            reasons = breakdown.get("reasons", []) if isinstance(breakdown, dict) else []
            return {
                "score":    score,
                "band":     band,
                "breakdown": {k: 0 for k in WEIGHTS},
                "reasons":  reasons if reasons else [f"attribution score {score}"],
                "updated_at": sub_row.revenue_signal_updated_at.isoformat()
                    if sub_row.revenue_signal_updated_at else None,
                "last_significant_action_at": None,
                "last_action": None,
            }

        return {
            "score": 0, "band": "low",
            "breakdown": {k: 0 for k in WEIGHTS},
            "reasons": ["no significant signals yet"],
            "updated_at": None,
            "last_significant_action_at": None,
            "last_action": None,
        }

    score = int(row.revenue_signal_score or 0)
    band = row.revenue_signal_band or band_for(score)
    breakdown = row.revenue_signal_breakdown or {k: 0 for k in WEIGHTS}

    # Reasons aren't persisted — they're a derivation. Rebuild from the
    # stored breakdown so the admin payload stays explainable without a
    # full recompute. (We trade a tiny bit of staleness for one less SQL
    # round-trip per detail view.)
    reasons = _reasons_from_breakdown(breakdown)

    return {
        "score": score,
        "band": band,
        "breakdown": dict(breakdown),
        "reasons": reasons,
        "updated_at": row.revenue_signal_updated_at.isoformat()
            if row.revenue_signal_updated_at else None,
        "last_significant_action_at": row.last_significant_action_at.isoformat()
            if row.last_significant_action_at else None,
        "last_action": row.revenue_signal_last_action,
    }


# ──────────────────────────────────────────────────────────────────────────
# Component helpers (normalized 0..1 — unchanged from pre-fa037)
# ──────────────────────────────────────────────────────────────────────────


def _spend_velocity(subscriber_id: int, db: Session) -> float:
    cutoff = datetime.now(timezone.utc) - timedelta(days=30)
    total_debits = db.execute(
        select(func.sum(WalletTransaction.amount)).where(
            WalletTransaction.subscriber_id == subscriber_id,
            WalletTransaction.txn_type == "debit",
            WalletTransaction.created_at >= cutoff,
        )
    ).scalar() or 0
    spent = abs(total_debits)
    # Normalize: 20+ credits/month = 1.0
    return min(1.0, spent / 20.0)


def _engagement_recency(subscriber_id: int, db: Session) -> float:
    sub = db.get(Subscriber, subscriber_id)
    if not sub or not sub.updated_at:
        return 0.0
    updated_at = sub.updated_at
    if updated_at.tzinfo is None:
        updated_at = updated_at.replace(tzinfo=timezone.utc)
    days_since = (datetime.now(timezone.utc) - updated_at).days
    if days_since <= 1:
        return 1.0
    if days_since <= 7:
        return 0.7
    if days_since <= 14:
        return 0.4
    if days_since <= 30:
        return 0.2
    return 0.0


def _wallet_lock_status(sub: Subscriber, wallet: Optional[WalletBalance]) -> float:
    if wallet is None:
        return 0.0
    if wallet.wallet_tier == "power":
        return 1.0
    if wallet.wallet_tier == "growth":
        return 0.7
    if wallet.wallet_tier == "starter_wallet":
        return 0.4
    return 0.0


def _lead_interaction_rate(subscriber_id: int, db: Session) -> float:
    cutoff = datetime.now(timezone.utc) - timedelta(days=30)
    interactions = db.execute(
        select(func.count()).select_from(WalletTransaction).where(
            WalletTransaction.subscriber_id == subscriber_id,
            WalletTransaction.txn_type == "debit",
            WalletTransaction.created_at >= cutoff,
        )
    ).scalar() or 0
    # Normalize: 10+ interactions/month = 1.0
    return min(1.0, interactions / 10.0)


def _zip_competition(sub: Subscriber, db: Session) -> float:
    from src.core.redis_client import redis_available
    if not redis_available():
        return 0.5  # neutral default without Redis
    # Placeholder: would query ZIP activity counters from Redis sorted set
    return 0.5


# ──────────────────────────────────────────────────────────────────────────
# Reason synthesis — short plain-English explanations
# ──────────────────────────────────────────────────────────────────────────


_REASON_TEXT = {
    "spend_velocity":        "spending recently",
    "engagement_recency":    "recently active",
    "wallet_lock_status":    "wallet tier",
    "lead_interaction_rate": "interacting with leads",
    "zip_competition":       "ZIP competition signal",
}


def _build_reasons(
    normalized: dict, breakdown: dict, sub: Subscriber, wallet: Optional[WalletBalance]
) -> list[str]:
    """Top 1–3 reasons the score is what it is.

    Highest-contribution components first, with concrete data when we
    have it (wallet tier, engagement window). Falls back to a generic
    "no signals yet" reason if every component is zero.
    """
    reasons: list[str] = []

    # Pull the wallet tier in plain English if it's the top contributor.
    if wallet and breakdown.get("wallet_lock_status", 0) > 0:
        reasons.append(f"wallet tier: {wallet.wallet_tier}")

    # Engagement-recency: produce a specific window when we can.
    if normalized.get("engagement_recency", 0) >= 0.7:
        reasons.append("active within last 7 days")
    elif normalized.get("engagement_recency", 0) >= 0.4:
        reasons.append("active within last 14 days")

    # Highest-contribution remaining component (if not already named).
    if len(reasons) < 3:
        ranked = sorted(breakdown.items(), key=lambda kv: kv[1], reverse=True)
        for name, pts in ranked:
            if pts <= 0:
                continue
            label = _REASON_TEXT.get(name, name)
            already_covered = any(label in r or name in r for r in reasons)
            if already_covered:
                continue
            reasons.append(f"{label} ({pts} pts)")
            if len(reasons) >= 3:
                break

    if not reasons:
        reasons.append("no significant signals yet")
    return reasons[:3]


def _reasons_from_breakdown(breakdown: dict) -> list[str]:
    """Cheap reason rebuild from a stored breakdown — used by the read
    path so admin lookups don't need a full recompute."""
    if not breakdown:
        return ["no significant signals yet"]
    ranked = sorted(
        ((k, int(v or 0)) for k, v in breakdown.items()),
        key=lambda kv: kv[1], reverse=True,
    )
    out: list[str] = []
    for name, pts in ranked:
        if pts <= 0:
            continue
        out.append(f"{_REASON_TEXT.get(name, name)} ({pts} pts)")
        if len(out) >= 3:
            break
    if not out:
        out.append("no significant signals yet")
    return out
