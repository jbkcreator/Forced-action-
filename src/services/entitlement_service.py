"""
Cached tier resolution for CustomerAccount (fa B1-02 — centralized entitlement middleware).

customer_accounts.plan_tier is a FK to plans.plan_id (a plan slug), not the
tier bucket itself — the actual tier lives on plans.tier. This resolves and
caches that join so the gate (src/middleware/tier_gate.py) doesn't hit the DB
on every gated request.

Cache pattern mirrors src/services/kill_switch_service.py, but unlike a
metric cache this is an authorization check: a Redis miss or outage must fall
through to a direct DB read, never return an "unknown"/allow default.

Tier vocabulary note: the brief names target tiers STARTER/INVESTOR_PRO/
FOUNDER, but live `plans.tier` data today only has free_trial/starter/pro —
investor_pro and founder don't exist as real plans yet (tracked separately:
founder ties to the B0-04 founder-cohort prepay portal, investor_pro ties to
Block 5's investor lane). "investor_pro" is aliased onto the real "pro" plan
below so the gate is usable against live data now; once real investor_pro/
founder plans are seeded, this file is the only place that needs updating.
"""
from __future__ import annotations

import logging
from typing import Optional

from sqlalchemy import text as sa_text

from src.core.redis_client import redis_available, rget, rset

logger = logging.getLogger(__name__)

_REDIS_PREFIX = "fa:entitlement_tier:"
_REDIS_TTL_SECONDS = 300  # 5 min — short enough that a plan change propagates quickly

# Single source of truth for tier ordering. Add new tiers here only.
TIER_RANK: dict[str, int] = {
    "free_trial": 0,
    "starter": 10,
    "pro": 20,
    "investor_pro": 20,  # alias for the real "pro" plan until a dedicated plan exists
    "founder": 30,       # no live plan reaches this yet — fails closed until B0-04 seeds one
}


def _fetch_tier_from_db(db, account_id) -> Optional[str]:
    row = db.execute(
        sa_text("""
            SELECT p.tier
              FROM customer_accounts ca
              JOIN plans p ON p.plan_id = ca.plan_tier
             WHERE ca.account_id = :account_id
        """),
        {"account_id": account_id},
    ).fetchone()
    return row.tier if row is not None else None


def get_account_tier(db, account_id) -> Optional[str]:
    """Return the account's plan tier (e.g. "starter"), or None if it has no active plan.

    Reads Redis first; on cache miss or Redis unavailability, reads the DB
    directly and (on a hit) populates the cache. Never returns a cached
    "allow" default — a miss always re-checks the source of truth.
    """
    key = f"{_REDIS_PREFIX}{account_id}"
    redis_up = redis_available()

    if redis_up:
        cached = rget(key)
        if cached is not None:
            return cached
    else:
        logger.debug("Redis unavailable, bypassing cache for account_id=%s", account_id)

    tier = _fetch_tier_from_db(db, account_id)

    if tier is not None and redis_up:
        rset(key, tier, ttl_seconds=_REDIS_TTL_SECONDS)

    return tier
