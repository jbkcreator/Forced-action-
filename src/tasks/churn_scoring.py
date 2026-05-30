"""
Churn Scoring — nightly job (fa051).

Scores every active/grace subscriber for churn risk, writes a snapshot to
user_segments, and appends one history row to churn_predictions.

Sends NOTHING. Makes NO Claude calls. Has NO vendor-cost-pause guard.

Cron: 0 13 * * *  (must finish before proactive_save at 15:00 — see CLAUDE.md)

Stable holdout: sha256("churn_holdout:{subscriber_id}") % 100 < HOLDOUT_PCT
  → deterministic per subscriber, never re-rolled, mirrors ADR 0005 pattern.

Usage:
    python -m src.tasks.churn_scoring [--dry-run]
"""
from __future__ import annotations

import hashlib
import logging
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from config.churn import HOLDOUT_PCT
from src.core.database import get_db_context
from src.core.models import ChurnPrediction, Subscriber, UserSegment
from src.services.churn_risk import compute_churn_risk

logger = logging.getLogger(__name__)

_EXCLUDED_TIERS = frozenset({"free", "data_only"})


# ── Holdout ────────────────────────────────────────────────────────────────


def is_in_holdout(subscriber_id: int) -> bool:
    """Stable hash-based holdout — never re-rolled between runs."""
    digest = hashlib.sha256(f"churn_holdout:{subscriber_id}".encode()).hexdigest()
    return int(digest, 16) % 100 < HOLDOUT_PCT


# ── Backfill pass ──────────────────────────────────────────────────────────


def _backfill_outcomes(db: Session, now: datetime) -> int:
    """Set realized_inactive_at + was_correct on predictions from ~5–7 days ago.

    A prediction is 'correct' if the subscriber actually had no wallet debit
    in the 5-day window that was predicted.
    """
    from src.core.models import WalletTransaction

    window_start = now - timedelta(days=7)
    window_end = now - timedelta(days=5)

    rows = db.execute(
        select(ChurnPrediction)
        .where(
            ChurnPrediction.predicted_at >= window_start,
            ChurnPrediction.predicted_at <= window_end,
            ChurnPrediction.realized_inactive_at.is_(None),
        )
    ).scalars().all()

    updated = 0
    for pred in rows:
        last_debit = db.execute(
            select(WalletTransaction.created_at)
            .where(
                WalletTransaction.subscriber_id == pred.subscriber_id,
                WalletTransaction.txn_type == "debit",
                WalletTransaction.created_at >= pred.predicted_at,
            )
            .order_by(WalletTransaction.created_at.asc())
            .limit(1)
        ).scalar_one_or_none()

        if last_debit is None:
            # No debit since prediction → subscriber went inactive
            pred.realized_inactive_at = now
            pred.was_correct = bool(pred.predicted_inactivity_at is not None)
        else:
            # Debit found → subscriber stayed active
            pred.realized_inactive_at = last_debit
            pred.was_correct = pred.predicted_inactivity_at is None
        updated += 1

    return updated


# ── Snapshot write ─────────────────────────────────────────────────────────


def _upsert_segment_snapshot(
    db: Session,
    subscriber_id: int,
    score: int,
    band: str,
    predicted_inactivity_at: Optional[datetime],
    reason: str,
    now: datetime,
) -> None:
    """Write the 5 Churn Risk columns onto an existing user_segments row.

    UPDATE-only by design: this is a denormalized DISPLAY mirror for the admin
    surface, not the source of truth. Subscribers without a user_segments row
    (segmentation_engine owns row creation) simply have no mirror — that is fine
    because consumers (proactive_save, retention_event_producer) read the churn
    state from churn_predictions, which IS written for every scored subscriber.
    We deliberately do not INSERT here: fabricating a behavioral `segment` value
    would corrupt data that segmentation_engine owns.
    """
    db.execute(
        update(UserSegment)
        .where(UserSegment.subscriber_id == subscriber_id)
        .values(
            churn_risk_score=score,
            churn_risk_band=band,
            predicted_inactivity_at=predicted_inactivity_at,
            churn_risk_reason=reason[:255],
            churn_risk_updated_at=now,
        )
    )


# ── Main run ───────────────────────────────────────────────────────────────


def run(dry_run: bool = False) -> dict:
    """Score all active/grace subscribers. Returns summary dict."""
    results = {
        "checked": 0,
        "at_risk": 0,
        "holdout": 0,
        "backfilled": 0,
        "errors": 0,
    }
    now = datetime.now(timezone.utc)

    with get_db_context() as db:
        # 1. Backfill outcomes from previous predictions first
        if not dry_run:
            try:
                results["backfilled"] = _backfill_outcomes(db, now)
            except Exception as exc:
                logger.error("[ChurnScoring] Backfill error: %s", exc)

        # 2. Score all active/grace subscribers (excluding free + data_only)
        subs = db.execute(
            select(Subscriber).where(
                Subscriber.status.in_(["active", "grace"]),
                Subscriber.tier.notin_(_EXCLUDED_TIERS),
            )
        ).scalars().all()

        for sub in subs:
            results["checked"] += 1
            try:
                risk = compute_churn_risk(sub.id, db, now=now)
                in_holdout = is_in_holdout(sub.id)

                if risk["band"] in ("high", "very_high"):
                    results["at_risk"] += 1
                if in_holdout:
                    results["holdout"] += 1

                if not dry_run:
                    # Snapshot on user_segments
                    _upsert_segment_snapshot(
                        db,
                        subscriber_id=sub.id,
                        score=risk["score"],
                        band=risk["band"],
                        predicted_inactivity_at=risk["predicted_inactivity_at"],
                        reason=risk["reason"],
                        now=now,
                    )
                    # Append history row
                    pred = ChurnPrediction(
                        subscriber_id=sub.id,
                        predicted_at=now,
                        churn_risk_score=risk["score"],
                        churn_risk_band=risk["band"],
                        predicted_inactivity_at=risk["predicted_inactivity_at"],
                        features=risk["features"],
                        in_holdout=in_holdout,
                    )
                    db.add(pred)

            except Exception as exc:
                logger.error(
                    "[ChurnScoring] Failed subscriber=%d: %s", sub.id, exc, exc_info=True
                )
                results["errors"] += 1

    logger.info(
        "[ChurnScoring] checked=%d at_risk=%d holdout=%d backfilled=%d errors=%d dry_run=%s",
        results["checked"],
        results["at_risk"],
        results["holdout"],
        results["backfilled"],
        results["errors"],
        dry_run,
    )
    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    dry = "--dry-run" in sys.argv
    print(run(dry_run=dry))
