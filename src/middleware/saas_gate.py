"""
SaaS tier gating (S3).

A reusable subscription-state guard for protected data endpoints. Consolidates
the status check that is otherwise copy-pasted inline in the feed endpoints
(`/api/feed/{feed_uuid}` and `/api/feed/{feed_uuid}/stats`) into one place so
the rule cannot drift between routes.

Usage — attach as a FastAPI dependency on any feed-scoped data route:

    from src.middleware.saas_gate import require_active_subscriber

    @app.get("/api/feed/{feed_uuid}/something")
    def something(sub: Subscriber = Depends(require_active_subscriber)):
        ...  # sub is guaranteed not churned/cancelled

It reuses `subscriber_auth.get_current_subscriber` for JWT + feed-ownership
auth (401/403/404), then blocks the resolved subscriber when their subscription
state is terminal. It does NOT re-implement token parsing.

Blocking policy (S3 decision): block only `churned` and `cancelled` — the two
terminal "expired" states. `active`, `grace` (the 48h soft-landing window after
cancellation), `disputed`, and `paused` are allowed through; the recovery/grace
dunning machinery owns those cases.
"""

from __future__ import annotations

import logging

from fastapi import Depends, HTTPException

from src.core.models import Subscriber
from src.services.subscriber_auth import get_current_subscriber

logger = logging.getLogger(__name__)


class SaasGate:
    """Callable FastAPI dependency that blocks terminal-state subscribers.

    Instances are dependencies: `Depends(require_active_subscriber)`. The set of
    blocked states is an instance attribute so a stricter gate can be composed
    without subclassing (e.g. `SaasGate(blocked={"churned", "cancelled", "paused"})`).
    """

    DEFAULT_BLOCKED: frozenset[str] = frozenset({"churned", "cancelled"})

    def __init__(self, blocked: frozenset[str] | None = None) -> None:
        self.blocked = blocked if blocked is not None else self.DEFAULT_BLOCKED

    def __call__(
        self,
        subscriber: Subscriber = Depends(get_current_subscriber),
    ) -> Subscriber:
        if subscriber.status in self.blocked:
            logger.warning(
                "saas_gate: blocking subscriber=%s status=%s",
                subscriber.id,
                subscriber.status,
            )
            raise HTTPException(
                status_code=403,
                detail={
                    "error": "subscription_inactive",
                    "message": "Subscription is not active",
                },
            )
        return subscriber


# Module-level singleton — import and attach this to protected routes.
require_active_subscriber = SaasGate()
