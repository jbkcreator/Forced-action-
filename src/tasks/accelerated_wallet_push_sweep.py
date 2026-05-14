"""
Accelerated Wallet Push sweep task (fa016).

Periodic safety net for the in-line detector calls in wallet_engine.debit(),
stripe_webhooks._on_card_saved, and stripe_webhooks._on_premium_payment. Catches
subscribers who became eligible but whose event was lost (process crash,
Redis flap, race with the webhook ack).

Selects:
  - Subscribers with has_saved_card=True
  - AND wallet_opt_out=False
  - AND no WalletBalance row
  - AND at least one paid action (WalletTransaction debit OR PremiumPurchase
    paid_via='card' status in (pending,delivered)) in the lookback window.

Runs every 5 minutes via cron. Idempotency is the Redis per-day key the
detector itself sets — if the day key exists, accelerated_push_eligible
returns None and we skip.

Usage:
    python -m src.tasks.accelerated_wallet_push_sweep [--dry-run]
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta, timezone
from typing import List

from sqlalchemy import select

from config.settings import settings
from src.core.database import get_db_context
from src.core.models import (
    PremiumPurchase,
    Subscriber,
    WalletBalance,
    WalletTransaction,
)

logger = logging.getLogger(__name__)

# Lookback window — only candidates with recent paid activity. Keeps the
# sweep cheap and avoids re-pushing old saved-card users every 5 minutes.
LOOKBACK_HOURS = 72


def _candidate_subscriber_ids(db, since) -> List[int]:
    """Subscribers with paid activity in the lookback window that haven't
    enrolled in a wallet yet."""
    debit_subs = db.execute(
        select(WalletTransaction.subscriber_id).where(
            WalletTransaction.txn_type == "debit",
            WalletTransaction.created_at >= since,
        ).distinct()
    ).scalars().all()

    premium_subs = db.execute(
        select(PremiumPurchase.subscriber_id).where(
            PremiumPurchase.paid_via == "card",
            PremiumPurchase.status.in_(("pending", "delivered")),
            PremiumPurchase.created_at >= since,
        ).distinct()
    ).scalars().all()

    paid_ids = set(debit_subs) | set(premium_subs)
    if not paid_ids:
        return []

    return list(db.execute(
        select(Subscriber.id).where(
            Subscriber.id.in_(paid_ids),
            Subscriber.has_saved_card.is_(True),
            Subscriber.wallet_opt_out.is_(False),
            ~Subscriber.id.in_(select(WalletBalance.subscriber_id)),
        )
    ).scalars().all())


def run_sweep(dry_run: bool = False) -> dict:
    results = {"scanned": 0, "eligible": 0, "emitted": 0, "errors": 0, "skipped_disabled": False}

    if not getattr(settings, "accelerated_wallet_push_enabled", False):
        logger.info("[AccelWalletPushSweep] feature flag disabled — skipping")
        results["skipped_disabled"] = True
        return results

    since = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)

    from src.services import wallet_engine
    from src.agents.supervisor import dispatch_event

    with get_db_context() as db:
        sub_ids = _candidate_subscriber_ids(db, since)
        results["scanned"] = len(sub_ids)

        for sub_id in sub_ids:
            try:
                eligible = wallet_engine.accelerated_push_eligible(sub_id, db)
                if not eligible:
                    continue
                results["eligible"] += 1
                if dry_run:
                    logger.info("[AccelWalletPushSweep] dry-run eligible sub=%s payload=%s",
                                sub_id, eligible)
                    continue
                dispatch_event({
                    "event_type": "accelerated_wallet_push_eligible",
                    "subscriber_id": sub_id,
                    "payload": eligible,
                })
                results["emitted"] += 1
            except Exception as exc:
                logger.error("[AccelWalletPushSweep] error sub=%s: %s", sub_id, exc, exc_info=True)
                results["errors"] += 1

    logger.info(
        "[AccelWalletPushSweep] scanned=%d eligible=%d emitted=%d errors=%d dry_run=%s",
        results["scanned"], results["eligible"], results["emitted"], results["errors"], dry_run,
    )
    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    dry = "--dry-run" in sys.argv
    print(run_sweep(dry_run=dry))
