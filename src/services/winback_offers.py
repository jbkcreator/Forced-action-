"""
Win-back offer redemption — T-B12-07 (PR #172 review fix).

The reactivation graph used to grant the zip_released 5-credit bonus (and
imply, via message copy, the zip_held 50%-off discount) purely on a
successful message SEND. That means every eligible lapsed subscriber got
the benefit merely for the outbound message being dispatched — whether or
not they ever actually came back.

This module creates a one-time, expiring, subscriber-bound token when the
message is sent, and only grants the promised benefit when that token is
redeemed via a real checkout completion:
  - zip_held:     token validated + a Stripe coupon applied at checkout
                   session creation (src.services.stripe_service /
                   POST /api/checkout). Discount happens through Stripe
                   itself, not a manual credit.
  - zip_released: token validated in the checkout-completion webhook;
                   credits are granted there, not at send time.

Reusable idempotency: a subscriber+branch offer is reused (not duplicated)
while a prior one is still pending and unexpired, so retried scheduler runs
don't mint a fresh token — and thus don't invalidate a link the subscriber
may already have received.
"""
from __future__ import annotations

import logging
import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

OFFER_VALIDITY_DAYS = 14
TIER3_WINBACK_CREDIT_BONUS = 5
TIER3_WINBACK_CREDIT_REASON = "tier3_winback_reactivation"


def create_or_reuse_offer(subscriber_id: int, branch: str, db: Session) -> str:
    """
    Returns a token for this subscriber+branch, reusing an existing
    unredeemed/unexpired one instead of minting a duplicate so a retried
    scheduler run doesn't silently invalidate a link already sent out.
    """
    now = datetime.now(timezone.utc)
    existing = db.execute(
        text(
            """
            SELECT token FROM winback_offers
             WHERE subscriber_id = :sid AND branch = :branch
               AND redeemed_at IS NULL AND expires_at > :now
             ORDER BY created_at DESC
             LIMIT 1
            """
        ),
        {"sid": subscriber_id, "branch": branch, "now": now},
    ).first()
    if existing:
        return existing[0]

    token = secrets.token_urlsafe(32)
    db.execute(
        text(
            """
            INSERT INTO winback_offers (subscriber_id, branch, token, created_at, expires_at)
            VALUES (:sid, :branch, :token, :now, :expires)
            """
        ),
        {
            "sid": subscriber_id,
            "branch": branch,
            "token": token,
            "now": now,
            "expires": now + timedelta(days=OFFER_VALIDITY_DAYS),
        },
    )
    return token


def get_valid_offer(token: str, db: Session) -> Optional[dict]:
    """Returns {subscriber_id, branch} for a live, unredeemed, unexpired token, else None."""
    if not token:
        return None
    row = db.execute(
        text(
            """
            SELECT subscriber_id, branch FROM winback_offers
             WHERE token = :token AND redeemed_at IS NULL AND expires_at > :now
            """
        ),
        {"token": token, "now": datetime.now(timezone.utc)},
    ).first()
    if not row:
        return None
    return {"subscriber_id": row.subscriber_id, "branch": row.branch}


def redeem_offer(token: str, db: Session) -> Optional[dict]:
    """
    Marks a token redeemed (set-once — a second call is a no-op) and returns
    {subscriber_id, branch}, or None if the token is missing/expired/already
    redeemed. Caller (the checkout webhook) uses `branch` to decide whether
    a credit grant is owed (zip_released) — zip_held's discount already
    happened via the Stripe coupon at session creation, nothing further to do.
    """
    offer = get_valid_offer(token, db)
    if not offer:
        return None
    result = db.execute(
        text(
            """
            UPDATE winback_offers SET redeemed_at = :now
             WHERE token = :token AND redeemed_at IS NULL
             RETURNING subscriber_id, branch
            """
        ),
        {"token": token, "now": datetime.now(timezone.utc)},
    ).first()
    if not result:
        return None  # lost a race with a concurrent redemption — already handled
    return {"subscriber_id": result.subscriber_id, "branch": result.branch}


def grant_winback_credits(subscriber_id: int, db: Session, token: Optional[str] = None) -> bool:
    """
    Grants the 5-free-credit zip_released win-back bonus. Idempotent on the
    wallet-transaction description, so even a duplicate call (e.g. a retried
    webhook or the reconciliation sweep) never double-credits. Call ONLY
    after redeem_offer() confirms a real reactivation — never at
    message-send time.

    Returns True on success (including "already granted"), False on failure.

    Exceptions are caught here (a webhook-time failure must not break
    checkout) — but this is why `token`, when given, is used to stamp
    `winback_offers.credits_granted_at` separately from `redeemed_at`
    (PR #172 follow-up review): if this raises, redeemed_at is already set
    and stays set, but credits_granted_at stays NULL, so
    reconcile_pending_credit_grants() can find and retry exactly this row
    later instead of the benefit being silently lost forever.
    """
    from src.services.wallet_engine import add_bonus

    try:
        existing = db.execute(
            text(
                "SELECT 1 FROM wallet_transactions "
                "WHERE subscriber_id = :sid AND txn_type = 'bonus' AND description = :reason "
                "LIMIT 1"
            ),
            {"sid": subscriber_id, "reason": TIER3_WINBACK_CREDIT_REASON},
        ).first()
        if not existing:
            add_bonus(subscriber_id, TIER3_WINBACK_CREDIT_BONUS, TIER3_WINBACK_CREDIT_REASON, db)
            logger.info(
                "winback_offers: granted tier3 win-back credits sub_id=%s amount=%s",
                subscriber_id, TIER3_WINBACK_CREDIT_BONUS,
            )
        if token:
            db.execute(
                text(
                    "UPDATE winback_offers SET credits_granted_at = :now "
                    "WHERE token = :token AND credits_granted_at IS NULL"
                ),
                {"token": token, "now": datetime.now(timezone.utc)},
            )
        return True
    except Exception:
        logger.exception(
            "winback_offers: failed to grant tier3 win-back credits sub_id=%s token=%s "
            "— will be retried by reconcile_pending_credit_grants",
            subscriber_id, token,
        )
        return False


def reconcile_pending_credit_grants(db: Session, limit: int = 100) -> dict:
    """
    Periodic sweep (PR #172 follow-up review): finds zip_released offers that
    were successfully redeemed (proof of a real reactivation) but whose
    credit grant never completed — e.g. a transient wallet/DB failure inside
    the checkout webhook — and retries the grant for each. Safe to run
    repeatedly; grant_winback_credits is idempotent per subscriber+reason.
    """
    rows = db.execute(
        text(
            """
            SELECT token, subscriber_id FROM winback_offers
             WHERE branch = 'zip_released'
               AND redeemed_at IS NOT NULL
               AND credits_granted_at IS NULL
             ORDER BY redeemed_at ASC
             LIMIT :limit
            """
        ),
        {"limit": limit},
    ).all()

    result = {"checked": len(rows), "granted": 0, "failed": 0}
    for row in rows:
        ok = grant_winback_credits(row.subscriber_id, db, token=row.token)
        if ok:
            result["granted"] += 1
        else:
            result["failed"] += 1
    return result


def main() -> int:
    import argparse
    import sys

    from src.core.database import get_db_context

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
        stream=sys.stdout,
    )
    parser = argparse.ArgumentParser(description="Win-back offer maintenance")
    parser.add_argument(
        "--reconcile", action="store_true",
        help="Retry credit grants for redeemed-but-not-yet-credited zip_released offers",
    )
    args = parser.parse_args()

    if args.reconcile:
        with get_db_context() as db:
            result = reconcile_pending_credit_grants(db)
            db.commit()
        logger.info("winback_offers reconcile result: %s", result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
