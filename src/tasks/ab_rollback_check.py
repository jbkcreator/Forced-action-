"""
A/B auto-rollback monitor — fa008+ (2026-05-04).

Daily cron that walks every active AbTest and applies the auto-rollback
math from `ab_engine.should_rollback`. The math has existed since Stage 5;
this job is what actually fires it.

Per spec: rollback when variant A is losing by > 2 std devs after 200+
total assignments (with at least 10 in each arm). When triggered:

    1. complete_test(test_name, winner='b') — flips status='completed'
    2. Founder SMS alert via _send_founder_alert
    3. Append a learning_cards row (card_type='ab_result') so future
       Cora decisions see the lesson learned.

Run via: python -m src.tasks.ab_rollback_check [--dry-run]
Cron:    0 9 * * *   (daily 09:00 UTC, before the Revenue Pulse Monday SMS)
"""

from __future__ import annotations

import logging
import sys
from datetime import date, datetime, timezone
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.core.database import get_db_context
from src.core.models import AbAssignment, AbTest, LearningCard
from src.services.ab_engine import complete_test, should_rollback

logger = logging.getLogger(__name__)


def _summary_stats(test: AbTest, db: Session) -> dict:
    """Pull the numbers we'll show in the founder alert + learning card."""
    assignments = db.execute(
        select(AbAssignment).where(AbAssignment.test_id == test.id)
    ).scalars().all()
    a_total = sum(1 for a in assignments if a.variant == "a")
    b_total = sum(1 for a in assignments if a.variant == "b")
    a_conv = sum(1 for a in assignments if a.variant == "a" and a.outcome == "converted")
    b_conv = sum(1 for a in assignments if a.variant == "b" and a.outcome == "converted")
    a_rate = (a_conv / a_total) if a_total else 0.0
    b_rate = (b_conv / b_total) if b_total else 0.0
    lift_pct = round(100 * (b_rate - a_rate) / a_rate, 1) if a_rate > 0 else None
    return {
        "n_total": len(assignments),
        "a_total": a_total,
        "b_total": b_total,
        "a_conv": a_conv,
        "b_conv": b_conv,
        "a_rate_pct": round(a_rate * 100, 2),
        "b_rate_pct": round(b_rate * 100, 2),
        "b_lift_pct": lift_pct,
    }


def _write_learning_card(test: AbTest, stats: dict, db: Session) -> None:
    """Upsert today's ab_result learning card. (card_date, card_type) is unique."""
    today = date.today()
    existing = db.execute(
        select(LearningCard).where(
            LearningCard.card_date == today,
            LearningCard.card_type == "ab_result",
        )
    ).scalar_one_or_none()
    summary = (
        f"AB rollback: {test.test_name} — variant_a retired "
        f"({stats['a_rate_pct']}% conv) in favour of variant_b "
        f"({stats['b_rate_pct']}% conv, lift {stats['b_lift_pct']}%) "
        f"after n={stats['n_total']}."
    )
    payload = {"test_name": test.test_name, **stats, "winner": "b"}
    if existing:
        existing.summary_text = summary
        existing.data_json = payload
        existing.action_taken = f"auto_rollback:{test.test_name}"
    else:
        db.add(LearningCard(
            card_date=today,
            card_type="ab_result",
            summary_text=summary,
            data_json=payload,
            action_taken=f"auto_rollback:{test.test_name}",
        ))
    db.flush()


def run(dry_run: bool = False) -> dict:
    stats_out = {"checked": 0, "rolled_back": 0, "errors": 0}

    with get_db_context() as db:
        active_tests = db.execute(
            select(AbTest).where(AbTest.status == "active")
        ).scalars().all()

        for test in active_tests:
            stats_out["checked"] += 1
            try:
                if not should_rollback(test.test_name, db):
                    continue

                stats = _summary_stats(test, db)
                msg = (
                    f"AB ROLLBACK: {test.test_name} — variant_b won, "
                    f"variant_a retired (n={stats['n_total']}, "
                    f"a={stats['a_rate_pct']}% b={stats['b_rate_pct']}% "
                    f"lift={stats['b_lift_pct']}%)"
                )
                if dry_run:
                    logger.info("[ABRollback] DRY-RUN would rollback: %s", msg)
                    continue

                complete_test(test.test_name, winner="b", db=db)
                _write_learning_card(test, stats, db)

                # Founder alert via the existing Revenue Pulse SMS path.
                try:
                    from src.services.stripe_webhooks import _send_founder_alert
                    _send_founder_alert(msg)
                except Exception as exc:
                    logger.warning("[ABRollback] founder alert failed: %s", exc)

                stats_out["rolled_back"] += 1
                logger.info("[ABRollback] %s", msg)
            except Exception as exc:
                logger.error(
                    "[ABRollback] error checking test %s: %s",
                    test.test_name, exc, exc_info=True,
                )
                stats_out["errors"] += 1

    logger.info("[ABRollback] %s", stats_out)
    return stats_out


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    dry = "--dry-run" in sys.argv
    print(run(dry_run=dry))
