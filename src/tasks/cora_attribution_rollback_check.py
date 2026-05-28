"""
Cora Attribution Rollout — auto-rollback monitor.

Runs every 120-180 seconds. Evaluates whether the 'variant' arm of the
cora_attribution_v1 rollout test is losing vs 'control' by >2σ over the
rolling 48h window (requires ≥30 per arm). On trigger:

  1. Sets AbTest.status='rolled_back' via rollback_rollout().
  2. Sends founder SMS alert.
  3. Writes a learning card (card_type='ab_result') for future Cora decisions.

Fail-safe: if the floor isn't met or the z-test is indeterminate, no action
is taken. The variant keeps running until a verdict is possible.

Run manually: python -m src.tasks.cora_attribution_rollback_check [--dry-run]
Cron: */2 * * * * (every 2 minutes — ensures ≤5-min detection lag)
"""

from __future__ import annotations

import logging
import sys
from datetime import date, datetime, timezone

from sqlalchemy import select

from src.core.database import get_db_context
from src.core.models import AbAssignment, AbTest, LearningCard
from src.services.ab_engine import (
    ATTRIBUTION_ROLLOUT_TEST_NAME,
    rollback_rollout,
    should_rollback_rollout,
)

logger = logging.getLogger(__name__)


def _summary_stats(test: AbTest, db) -> dict:
    from datetime import timedelta
    cutoff = datetime.now(timezone.utc) - timedelta(hours=48)
    assignments = db.execute(
        select(AbAssignment).where(
            AbAssignment.test_id == test.id,
            AbAssignment.created_at >= cutoff,
        )
    ).scalars().all()

    ctrl = [a for a in assignments if a.variant == "control"]
    var = [a for a in assignments if a.variant == "variant"]
    ctrl_conv = sum(1 for a in ctrl if a.outcome == "converted")
    var_conv = sum(1 for a in var if a.outcome == "converted")
    n_ctrl, n_var = len(ctrl), len(var)

    ctrl_rate = round(ctrl_conv / n_ctrl * 100, 2) if n_ctrl else 0.0
    var_rate = round(var_conv / n_var * 100, 2) if n_var else 0.0
    return {
        "n_ctrl": n_ctrl,
        "n_var": n_var,
        "ctrl_conv": ctrl_conv,
        "var_conv": var_conv,
        "ctrl_rate_pct": ctrl_rate,
        "var_rate_pct": var_rate,
    }


def _write_learning_card(test: AbTest, stats: dict, db) -> None:
    today = date.today()
    existing = db.execute(
        select(LearningCard).where(
            LearningCard.card_date == today,
            LearningCard.card_type == "ab_result",
        )
    ).scalar_one_or_none()
    summary = (
        f"Attribution rollback: {test.test_name} — variant retired "
        f"({stats['var_rate_pct']}% conv) vs control "
        f"({stats['ctrl_rate_pct']}% conv) "
        f"after n_var={stats['n_var']} n_ctrl={stats['n_ctrl']} in 48h window."
    )
    payload = {"test_name": test.test_name, **stats, "trigger": "rollout_rollback"}
    if existing:
        existing.summary_text = summary
        existing.data_json = payload
        existing.action_taken = f"rollout_rollback:{test.test_name}"
    else:
        db.add(LearningCard(
            card_date=today,
            card_type="ab_result",
            summary_text=summary,
            data_json=payload,
            action_taken=f"rollout_rollback:{test.test_name}",
        ))
    db.flush()


def run(dry_run: bool = False) -> dict:
    result = {"checked": 0, "rolled_back": 0, "errors": 0}

    with get_db_context() as db:
        test = db.execute(
            select(AbTest).where(
                AbTest.test_name == ATTRIBUTION_ROLLOUT_TEST_NAME,
                AbTest.status == "active",
            )
        ).scalar_one_or_none()

        if test is None:
            logger.debug("[AttrRollback] no active %s test", ATTRIBUTION_ROLLOUT_TEST_NAME)
            return result

        result["checked"] = 1

        try:
            if not should_rollback_rollout(ATTRIBUTION_ROLLOUT_TEST_NAME, db):
                return result

            stats = _summary_stats(test, db)
            msg = (
                f"ATTRIBUTION ROLLBACK: {test.test_name} — variant arm retired. "
                f"variant={stats['var_rate_pct']}% ctrl={stats['ctrl_rate_pct']}% "
                f"(n_var={stats['n_var']} n_ctrl={stats['n_ctrl']}, 48h window)"
            )

            if dry_run:
                logger.info("[AttrRollback] DRY-RUN would rollback: %s", msg)
                return result

            rollback_rollout(ATTRIBUTION_ROLLOUT_TEST_NAME, db)
            _write_learning_card(test, stats, db)

            try:
                from src.services.stripe_webhooks import _send_founder_alert
                _send_founder_alert(msg)
            except Exception as exc:
                logger.warning("[AttrRollback] founder alert failed: %s", exc)

            result["rolled_back"] = 1
            logger.warning("[AttrRollback] %s", msg)

        except Exception as exc:
            logger.error("[AttrRollback] error: %s", exc, exc_info=True)
            result["errors"] = 1

    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    dry = "--dry-run" in sys.argv
    print(run(dry_run=dry))
