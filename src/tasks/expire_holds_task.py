"""
Expiry sweep — releases held ZIP territories back to 'available' after 48 h.

Calls expire_holds() from hold_lifecycle_service. Deposit is forfeited ($97);
no refund is issued. A Slack alert fires per expired hold (inside the service).

    python -m src.tasks.expire_holds_task

Cron: */15 * * * *  (every 15 minutes, all days)
"""
from __future__ import annotations

import logging
import sys

import stripe

from config.settings import get_settings
from src.core.database import get_db_context
from src.services.hold_lifecycle_service import expire_holds

logger = logging.getLogger(__name__)


def run() -> int:
    """Run the expiry sweep. Returns the count of holds released."""
    settings = get_settings()
    stripe.api_key = settings.active_stripe_secret_key.get_secret_value()

    with get_db_context() as db:
        count = expire_holds(db, stripe)
        db.commit()

    logger.info("[ExpireHolds] %d expired hold(s) released", count)
    return count


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
    )
    sys.exit(0 if run() >= 0 else 1)
