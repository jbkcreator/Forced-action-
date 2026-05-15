"""
Bundle expiry sweep — runs hourly via cron.

Marks stale BundlePurchases as 'expired' and releases their Redis
lead holds so the properties re-enter the regular subscriber feed.
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone

from sqlalchemy import select

from src.core.database import get_db_context
from src.core.models import BundlePurchase
from src.services import bundle_engine

logger = logging.getLogger(__name__)


def run() -> dict:
    stats = {"expired": 0, "holds_released": 0}

    with get_db_context() as db:
        now = datetime.now(timezone.utc)

        # Snapshot active purchases that are about to expire so we can release
        # their Redis holds after expire_stale() marks them done.
        active_expiring = db.execute(
            select(BundlePurchase).where(
                BundlePurchase.expires_at < now,
                BundlePurchase.status == "active",
            )
        ).scalars().all()

        expired_count = bundle_engine.expire_stale(db)
        stats["expired"] = expired_count

        if active_expiring:
            from src.services.lead_hold import release_many
            for bp in active_expiring:
                if bp.lead_ids:
                    released = release_many(bp.lead_ids, bp.subscriber_id)
                    stats["holds_released"] += released

    logger.info("[BundleExpiry] %s", stats)
    return stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(run())
