"""
Wallet-to-Lock sweep task.

Runs daily at 0 9 * * * (9 AM UTC).
Finds wallet subscribers with >= 40 credits in a single ZIP over 30 days
and emits Cora events for Territory Lock close.

Usage:
    python -m src.tasks.wallet_to_lock_sweep [--dry-run]
"""
import logging
import sys

from src.core.database import get_db_context
from src.services.wallet_to_lock import (
    emit_event,
    find_candidates,
    is_zip_locked,
    mark_lock_candidate,
)

logger = logging.getLogger(__name__)


def run_sweep(dry_run: bool = False) -> dict:
    results = {
        "candidates_found": 0,
        "skipped_zip_locked": 0,
        "events_emitted": 0,
        "errors": 0,
    }

    with get_db_context() as db:
        candidates = find_candidates(db)

        for cand in candidates:
            try:
                if is_zip_locked(db, cand.zip_code, cand.vertical, cand.county_id):
                    results["skipped_zip_locked"] += 1
                    logger.debug(
                        "wallet_to_lock: ZIP %s already locked, skipping sub %s",
                        cand.zip_code, cand.subscriber_id,
                    )
                    continue
                logger.info(
                    "wallet_to_lock: candidate sub=%s zip=%s credits=%d",
                    cand.subscriber_id, cand.zip_code, cand.credits_used,
                )

                results["candidates_found"] += 1
                if not dry_run:
                    mark_lock_candidate(db, cand.subscriber_id, cand.zip_code)
                    emit_event(
                        cand.subscriber_id,
                        cand.zip_code,
                        cand.credits_used,
                        cand.vertical,
                        uncontacted_count=cand.uncontacted_count,
                        tier_breakdown=cand.tier_breakdown,
                    )
                    results["events_emitted"] += 1

            except Exception as exc:
                logger.error(
                    "wallet_to_lock_sweep error sub=%s zip=%s: %s",
                    cand.subscriber_id, cand.zip_code, exc,
                )
                results["errors"] += 1

    logger.info(
        "[WalletToLockSweep] total=%d skipped_locked=%d emitted=%d errors=%d dry_run=%s",
        results["candidates_found"],
        results["skipped_zip_locked"],
        results["events_emitted"],
        results["errors"],
        dry_run,
    )
    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    dry = "--dry-run" in sys.argv
    print(run_sweep(dry_run=dry))
