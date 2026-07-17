"""
Reference route for the centralized entitlement gate (fa B1-02).

Exists only to prove require_tier() end-to-end against a real HTTP route.
Real non-delivery surfaces (deal-intake, Cora SLA, data scope, geo boundary)
attach the same Depends(require_tier(...)) pattern on their own routers as
those are built — they do not extend this file.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends

from src.middleware.tier_gate import require_tier

router = APIRouter(prefix="/api/account", tags=["account"])


@router.get("/investor-pro-ping")
def investor_pro_ping(account=Depends(require_tier("investor_pro"))):
    return {"ok": True}
