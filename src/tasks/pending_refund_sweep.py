"""Pending hold-refund sweep — durably retries hold-deposit refunds (bug #2).

On conversion, the webhook records refund_status='pending' in its committed
transaction, then attempts the Stripe refund best-effort in a post-commit
BackgroundTask. If that task never runs (crash/deploy) or fails, the deposit is
still owed. This sweep re-attempts every deal_room stuck in
'pending'/'refund_failed'. The refund carries a PI-derived idempotency_key, so a
refund already issued is never double-refunded.

    python -m src.tasks.pending_refund_sweep

Cron: */15 * * * *  (every 15 minutes)
"""
from __future__ import annotations

import logging
import sys

import stripe

from config.settings import get_settings
from src.core.database import get_db_context
from src.services.hold_lifecycle_service import sweep_pending_refunds

logger = logging.getLogger(__name__)


def run() -> int:
    """Run the pending-refund sweep. Returns the count of rows processed."""
    settings = get_settings()
    stripe.api_key = settings.active_stripe_secret_key.get_secret_value()

    with get_db_context() as db:
        count = sweep_pending_refunds(db, stripe)
        db.commit()

    logger.info("[PendingRefundSweep] %d pending refund(s) processed", count)
    return count


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
    )
    sys.exit(0 if run() >= 0 else 1)
