"""
Task 4.1 — Frozen control holdout surfacing monitor.

Daily-ish cron that walks every active `*_holdout` AbTest, applies
ab_engine.holdout_verdict, and on a 'proven' verdict (variant beats the
frozen control by >2σ, both arms >=30 assignments) writes:

    1. A cora_playbook recommendation (upsert_recommendation, idempotent by
       source_key) — status stays 'recommended' until a human adopts it via
       the existing admin endpoint. No auto-promotion: the holdout only
       proves the copy beats baseline, adoption is still a human call.
    2. A LearningCard (card_type='holdout_result' — its own constrained
       value, migrations/apply_learning_card_holdout_result.py; NOT
       'ab_result', which ab_rollback_check.py/cora_attribution_rollback_
       check.py already write daily — LearningCard has a table-wide
       UNIQUE(card_date, card_type), so sharing a type risks two unrelated
       jobs' results silently overwriting each other the same day) so
       future Cora decisions see the result.

Never mutates the AbTest itself — that's the whole point of a holdout;
promotion/rollback of the underlying a/b test stays with ab_rollback_check.

Run manually: python -m src.tasks.cora_holdout_check [--dry-run]
Cron: 0 9 * * *  (daily 09:00 UTC, alongside ab_rollback_check)
"""

from __future__ import annotations

import logging
import sys
from datetime import date

from sqlalchemy import select

from src.agents.prompts.loader import get_holdout_config_by_test_name
from src.core.database import get_db_context
from src.core.models import AbTest, LearningCard
from src.services.ab_engine import holdout_verdict
from src.services.playbook_writer import upsert_recommendation

logger = logging.getLogger(__name__)


def _write_learning_card(test_name: str, verdict: dict, db) -> None:
    """Upsert today's holdout_result learning card. (card_date, card_type) is
    UNIQUE table-wide — matches ab_rollback_check.py/cora_attribution_
    rollback_check.py's convention exactly: if more than one holdout test
    proves on the same day, the latest overwrites the day's card (last write
    wins), it does not error. A per-test action_taken filter here would
    instead try to INSERT a second row for the same (card_date, card_type)
    and hit the UNIQUE constraint."""
    today = date.today()
    existing = db.execute(
        select(LearningCard).where(
            LearningCard.card_date == today,
            LearningCard.card_type == "holdout_result",
        )
    ).scalar_one_or_none()
    summary = (
        f"Holdout PROVEN: {test_name} — variant beats frozen control "
        f"({verdict['variant_rate_pct']}% vs {verdict['control_rate_pct']}%, "
        f"z={verdict['z_score']}, n_var={verdict['n_var']} n_ctrl={verdict['n_ctrl']})."
    )
    payload = {"test_name": test_name, **verdict}
    if existing:
        existing.summary_text = summary
        existing.data_json = payload
    else:
        db.add(LearningCard(
            card_date=today,
            card_type="holdout_result",
            summary_text=summary,
            data_json=payload,
            action_taken=f"holdout_proven:{test_name}",
        ))
    db.flush()


def run(dry_run: bool = False) -> dict:
    stats = {"checked": 0, "proven": 0, "errors": 0}

    with get_db_context() as db:
        holdout_tests = db.execute(
            select(AbTest).where(
                AbTest.status == "active",
                AbTest.test_name.like("%_holdout"),
            )
        ).scalars().all()

        for test in holdout_tests:
            stats["checked"] += 1
            try:
                # conversion_window_days is registry-driven (config/cora_holdout_
                # tests.yaml) — only retention_v1-style "any paid action within
                # N days" sequences set it; a missing/disabled YAML entry
                # falls back to holdout_verdict's unbounded default.
                holdout_cfg = get_holdout_config_by_test_name(test.test_name)
                conversion_window_days = (holdout_cfg or {}).get("conversion_window_days")

                verdict = holdout_verdict(
                    test.test_name, db, conversion_window_days=conversion_window_days,
                )
                if verdict["status"] != "proven":
                    continue

                if dry_run:
                    logger.info("[HoldoutCheck] DRY-RUN would surface proven: %s %s",
                                test.test_name, verdict)
                    continue

                upsert_recommendation(
                    db,
                    name=f"holdout_winner:{test.test_name}",
                    description=(
                        f"Holdout {test.test_name} — variant proved it beats the "
                        f"frozen control (z={verdict['z_score']}); recommend promoting."
                    ),
                    pattern={"test_name": test.test_name, **verdict},
                    source_type="holdout_test",
                    source_id=test.test_name,
                    authored_by="cora",
                )
                _write_learning_card(test.test_name, verdict, db)

                stats["proven"] += 1
                logger.info("[HoldoutCheck] proven: %s %s", test.test_name, verdict)
            except Exception as exc:
                logger.error(
                    "[HoldoutCheck] error checking test %s: %s",
                    test.test_name, exc, exc_info=True,
                )
                stats["errors"] += 1

    logger.info("[HoldoutCheck] %s", stats)
    return stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    dry = "--dry-run" in sys.argv
    logger.info("cora_holdout_check result: %s", run(dry_run=dry))
