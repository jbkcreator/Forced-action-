"""
Stage 10 — Variant Mutation Job (fa055).

Scheduled task that runs the full A/B mutation + pricing cohort check cycle:

  1. For every active message_variant_test:
     a. check_sigma_rollback  — auto-pause any slot >2σ below best
     b. check_and_retire      — retire lowest slot after 200 sends, install Haiku replacement
     c. check_replacement_performance — after 200 sends of replacement, promote or revert

  2. evaluate_all_cohorts   — check rollback triggers on all active pricing cohorts

Rate limits (inherited from cora_guardrails CORA_SELF_HEALING):
  - max_actions_per_run: 3 (shared; this job counts against the same budget)

Usage:
    python -m src.tasks.variant_mutation_job
    python -m src.tasks.variant_mutation_job --dry-run

Schedule: run hourly (after cora_self_healing, before revenue_pulse).
Gate: CORA_SELF_HEALING_ENABLED env var (reuses the same flag as self-healing).
"""

from __future__ import annotations

import json
import logging
import sys
from typing import Optional

from sqlalchemy import text as sa_text

from config.cora_guardrails import CORA_SELF_HEALING
from config.settings import get_settings
from src.core.database import get_db_context
from src.services.pricing_cohort_engine import evaluate_all_cohorts
from src.services.variant_engine import (
    check_and_retire,
    check_replacement_performance,
    check_sigma_rollback,
)

logger = logging.getLogger(__name__)


def _all_active_sequences(db) -> list[str]:
    rows = db.execute(sa_text("""
        SELECT sequence_name FROM message_variant_tests WHERE status = 'active'
    """)).fetchall()
    return [r.sequence_name for r in rows]


def run_variant_mutation(dry_run: bool = False) -> dict:
    """One pass of the variant mutation + pricing cohort evaluation loop.

    dry_run=True: reads state, computes what WOULD happen, but writes nothing.
    Returns a summary dict.
    """
    actions_taken = 0
    max_actions = CORA_SELF_HEALING["max_actions_per_run"]
    per_sequence: dict = {}
    cohort_results: list = []

    with get_db_context() as db:
        sequences = _all_active_sequences(db)

        for seq in sequences:
            seq_results: dict = {}

            # Step 1 — sigma rollback check (always run; writes only if sigma exceeded).
            if not dry_run and actions_taken < max_actions:
                sigma_result = check_sigma_rollback(seq, db)
                seq_results["sigma_rollback"] = sigma_result
                if sigma_result.get("paused"):
                    actions_taken += len(sigma_result["paused"])
            else:
                seq_results["sigma_rollback"] = {"status": "skipped_dry_run_or_rate_limit"}

            # Step 2 — proving cycle check (promote or revert replacement).
            if not dry_run and actions_taken < max_actions:
                prove_result = check_replacement_performance(seq, db)
                seq_results["proving_cycle"] = prove_result
                if prove_result.get("status") in ("promoted", "reverted"):
                    actions_taken += 1
            else:
                seq_results["proving_cycle"] = {"status": "skipped_dry_run_or_rate_limit"}

            # Step 3 — retirement check (retire loser + generate Haiku replacement).
            if not dry_run and actions_taken < max_actions:
                retire_result = check_and_retire(seq, db)
                seq_results["retirement"] = retire_result
                if retire_result.get("status") == "retired":
                    actions_taken += 1
            else:
                seq_results["retirement"] = {"status": "skipped_dry_run_or_rate_limit"}

            per_sequence[seq] = seq_results

        # Step 4 — pricing cohort rollback triggers.
        if not dry_run and actions_taken < max_actions:
            cohort_results = evaluate_all_cohorts(db)
            rolled_back = sum(1 for r in cohort_results if r.get("action") == "rolled_back")
            actions_taken += rolled_back
        else:
            cohort_results = [{"action": "skipped_dry_run_or_rate_limit"}]

    summary = {
        "sequences_processed": len(sequences),
        "actions_taken": actions_taken,
        "dry_run": dry_run,
        "per_sequence": per_sequence,
        "cohort_results": cohort_results,
    }
    logger.info("[variant-mutation-job] %s", json.dumps(summary, default=str))
    return summary


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    args = set(argv or sys.argv[1:])
    dry_run = "--dry-run" in args

    settings = get_settings()
    if not settings.cora_self_healing_enabled:
        logger.info("[variant-mutation-job] disabled via CORA_SELF_HEALING_ENABLED — exiting")
        return 0

    summary = run_variant_mutation(dry_run=dry_run)
    if dry_run:
        print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
