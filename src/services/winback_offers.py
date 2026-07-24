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


def grant_winback_credits(subscriber_id: int, db: Session) -> None:
    """
    Grants the 5-free-credit zip_released win-back bonus. Idempotent on the
    wallet-transaction description, so even a duplicate call (e.g. a retried
    webhook) never double-credits. Call ONLY after redeem_offer() confirms
    a real reactivation — never at message-send time.
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
        if existing:
            return
        add_bonus(subscriber_id, TIER3_WINBACK_CREDIT_BONUS, TIER3_WINBACK_CREDIT_REASON, db)
        logger.info(
            "winback_offers: granted tier3 win-back credits sub_id=%s amount=%s",
            subscriber_id, TIER3_WINBACK_CREDIT_BONUS,
        )
    except Exception:
        logger.exception(
            "winback_offers: failed to grant tier3 win-back credits sub_id=%s", subscriber_id
        )
