"""
Centralized entitlement gate (fa B1-02).

One framework-level dependency enforcing per-tier access on non-delivery
surfaces (deal-intake, Lifecycle SLA, data scope, geographic boundary), reading
cached CustomerAccount entitlement data. Attach via Depends(require_tier(...))
on a route signature — never re-implement this check inline in a controller.

Does NOT gate lead delivery — src/services/lead_delivery.py already enforces
per-grade entitlement for that surface and is untouched by this gate.
"""
from __future__ import annotations

from fastapi import Depends, HTTPException

from src.core.database import get_db
from src.services.account_auth import get_current_account
from src.services.entitlement_service import TIER_RANK, get_account_tier


def require_tier(min_tier: str):
    """Return a FastAPI dependency that 403s any account below `min_tier`.

    Usage: @router.get("/some-investor-only-route")
           def route(account=Depends(require_tier("investor_pro"))): ...
    """
    if min_tier not in TIER_RANK:
        raise ValueError(f"Unknown tier {min_tier!r} — must be one of {sorted(TIER_RANK)}")

    def _gate(account=Depends(get_current_account), db=Depends(get_db)):
        tier = get_account_tier(db, account.account_id)
        if TIER_RANK.get(tier, -1) < TIER_RANK[min_tier]:
            raise HTTPException(
                status_code=403,
                detail="Your plan does not include access to this feature",
            )
        return account

    return _gate
