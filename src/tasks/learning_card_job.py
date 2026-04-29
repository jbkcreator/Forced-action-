"""
Learning Card Job — Sunday midnight cron.

Generates weekly performance summary cards for Cora's decision context.
Cron: 0 0 * * 0 (Sunday midnight UTC)

Usage:
    python src/tasks/learning_card_job.py [--dry-run]
"""

import logging
import sys
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from src.core.models import AbTest, AbAssignment, DealOutcome, LearningCard, MessageOutcome, UserSegment

logger = logging.getLogger(__name__)


def run(dry_run: bool = False) -> dict:
    from src.core.database import get_db_context
    written = 0
    with get_db_context() as db:
        today = date.today()
        for generator in (_message_perf_card, _deal_pattern_card, _ab_result_card, _churn_signal_card):
            card = generator(db)
            if card and not dry_run:
                _upsert_card(
                    card_date=today,
                    card_type=card["card_type"],
                    summary=card["summary"],
                    data=card["data"],
                    action=card.get("action", ""),
                    db=db,
                )
                written += 1
            elif card and dry_run:
                logger.info("[DRY RUN] Would write card: %s — %s", card["card_type"], card["summary"])
                written += 1
    logger.info("Learning card job complete: %d cards written (dry_run=%s)", written, dry_run)
    return {"cards_written": written}


def _message_perf_card(db: Session) -> Optional[dict]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    total = db.execute(
        select(func.count()).select_from(MessageOutcome).where(MessageOutcome.sent_at >= cutoff)
    ).scalar() or 0
    if total < 10:
        return None

    delivered = db.execute(
        select(func.count()).select_from(MessageOutcome).where(
            MessageOutcome.sent_at >= cutoff,
            MessageOutcome.delivered_at.isnot(None),
        )
    ).scalar() or 0
    converted_4h = db.execute(
        select(func.count()).select_from(MessageOutcome).where(
            MessageOutcome.sent_at >= cutoff,
            MessageOutcome.conversion_within_4h == True,  # noqa: E712
        )
    ).scalar() or 0

    delivery_rate = round(delivered / total * 100, 1) if total else 0
    conv_rate = round(converted_4h / total * 100, 1) if total else 0

    return {
        "card_type": "message_perf",
        "summary": f"Last 7d: {total} messages, {delivery_rate}% delivered, {conv_rate}% converted (4h)",
        "data": {"total": total, "delivered": delivered, "delivery_rate": delivery_rate, "conv_rate_4h": conv_rate},
        "action": "review_low_delivery" if delivery_rate < 80 else "",
    }


def _deal_pattern_card(db: Session) -> Optional[dict]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    deals = db.execute(
        select(DealOutcome).where(DealOutcome.created_at >= cutoff)
    ).scalars().all()
    if not deals:
        return None

    buckets: dict[str, int] = {}
    days_list = []
    for d in deals:
        bucket = d.deal_size_bucket or "unknown"
        buckets[bucket] = buckets.get(bucket, 0) + 1
        if d.days_to_close:
            days_list.append(d.days_to_close)

    top_bucket = max(buckets, key=buckets.get) if buckets else "unknown"
    avg_days = round(sum(days_list) / len(days_list)) if days_list else None

    return {
        "card_type": "deal_pattern",
        "summary": f"Last 7d: {len(deals)} deals, top bucket={top_bucket}, avg_close={avg_days}d",
        "data": {"deals": len(deals), "buckets": buckets, "avg_days_to_close": avg_days},
        "action": "",
    }


def _ab_result_card(db: Session) -> Optional[dict]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    tests = db.execute(
        select(AbTest).where(AbTest.status == "active", AbTest.started_at <= cutoff)
    ).scalars().all()
    if not tests:
        return None

    results = []
    for test in tests:
        assignments = db.execute(
            select(AbAssignment).where(AbAssignment.test_id == test.id)
        ).scalars().all()
        a_conv = sum(1 for a in assignments if a.variant == "a" and a.outcome == "converted")
        b_conv = sum(1 for a in assignments if a.variant == "b" and a.outcome == "converted")
        a_total = sum(1 for a in assignments if a.variant == "a") or 1
        b_total = sum(1 for a in assignments if a.variant == "b") or 1
        results.append({
            "test": test.test_name,
            "a_rate": round(a_conv / a_total * 100, 1),
            "b_rate": round(b_conv / b_total * 100, 1),
            "n": len(assignments),
        })

    return {
        "card_type": "ab_result",
        "summary": f"{len(tests)} active A/B tests with ≥7d data",
        "data": {"tests": results},
        "action": "check_rollback" if any(r["b_rate"] > r["a_rate"] + 5 for r in results) else "",
    }


def _churn_signal_card(db: Session) -> Optional[dict]:
    at_risk = db.execute(
        select(func.count()).select_from(UserSegment).where(UserSegment.segment == "at_risk")
    ).scalar() or 0
    if at_risk == 0:
        return None

    avg_score = db.execute(
        select(func.avg(UserSegment.revenue_signal_score)).where(UserSegment.segment == "at_risk")
    ).scalar() or 0

    return {
        "card_type": "churn_signal",
        "summary": f"{at_risk} at-risk subscribers, avg revenue signal score={round(avg_score, 1)}",
        "data": {"at_risk_count": at_risk, "avg_score": round(float(avg_score), 1)},
        "action": "trigger_save_sequence" if at_risk > 5 else "",
    }


def _upsert_card(
    card_date: date,
    card_type: str,
    summary: str,
    data: dict,
    action: str,
    db: Session,
) -> LearningCard:
    existing = db.execute(
        select(LearningCard).where(
            LearningCard.card_date == card_date,
            LearningCard.card_type == card_type,
        )
    ).scalar_one_or_none()
    if existing:
        existing.summary_text = summary
        existing.data_json = data
        existing.action_taken = action
        db.flush()
        return existing
    card = LearningCard(
        card_date=card_date,
        card_type=card_type,
        summary_text=summary,
        data_json=data,
        action_taken=action,
    )
    db.add(card)
    db.flush()
    return card


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = run(dry_run="--dry-run" in sys.argv)
    print(result)
