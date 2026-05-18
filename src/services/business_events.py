"""
Business event log — Phase 2B commercial-ladder audit trail.

Wraps `webhook_log.log_webhook_event` with a fixed `source='business'` (or
`source='frontend'` when called from the in-app endpoint) so signup /
unlock / payment / wallet events are queryable from one place:

    SELECT event_type, count(*) FROM webhook_events
     WHERE source IN ('business','frontend')
       AND processed_at > NOW() - INTERVAL '1 day'
     GROUP BY event_type;

Allowed event types are validated against `BUSINESS_EVENT_TYPES`. Unknown
types are logged at WARNING and DROPPED — we never want a typo'd event
name to silently pollute the analytics table.

The function is non-blocking by design: a failed audit write must never
break the request that triggered it.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from sqlalchemy.orm import Session

from src.services.webhook_log import log_webhook_event

logger = logging.getLogger(__name__)


# Allow-list. Anything not in this set is dropped + logged.
# Kept in sync with the spec under docs/phase_2b_v9_missing_flows_implementation_plan.md
# and the QA runbook acceptance criteria.
BUSINESS_EVENT_TYPES = frozenset({
    "LANDING_PAGE_VIEWED",
    "SIGNUP_STARTED",
    "SIGNUP_COMPLETED",
    "SIGNUP_SOURCE_ATTRIBUTED",
    "PROOF_MOMENT_VIEWED",
    "LEAD_UNLOCK_CLICKED",
    "PAYMENT_STARTED",
    "PAYMENT_SUCCEEDED",
    "PREMIUM_PURCHASE_COMPLETED",
    "CARD_SAVED",
    "ACCELERATED_WALLET_ELIGIBLE",
    "WALLET_OFFER_SENT",
    "WALLET_OFFER_SHOWN_IN_APP",
    "WALLET_DECLINED",
    "WALLET_ACTIVATED",
    "LEAD_PACK_PURCHASED",
    "FEED_REFRESHED",
    "SMS_SENT",
    "TOKEN_RESOLVED",
    "BUNDLE_CARD_VIEWED",
    "BUNDLE_CARD_CLICKED",
    "BUNDLE_CHECKOUT_STARTED",
    "BUNDLE_CHECKOUT_BLOCKED",
    "BUNDLE_CHECKOUT_SUCCEEDED",
})


def log_business_event(
    event_type: str,
    subscriber_id: Optional[int] = None,
    property_id: Optional[int] = None,
    payload: Optional[Dict[str, Any]] = None,
    source: str = "business",
    db: Optional[Session] = None,
) -> None:
    """Non-blocking audit write. Returns None.

    `source` should be 'business' for backend-emitted events and 'frontend'
    for the /api/business-event endpoint. The underlying WebhookEvent row
    has `status='processed'` by convention since the business action has
    already happened by the time this is called.
    """
    if event_type not in BUSINESS_EVENT_TYPES:
        logger.warning(
            "log_business_event: unknown event_type=%r — dropping (allowed: %s)",
            event_type, sorted(BUSINESS_EVENT_TYPES),
        )
        return
    try:
        log_webhook_event(
            source=source,
            event_type=event_type,
            direction="inbound",
            status="processed",
            subscriber_id=subscriber_id,
            property_id=property_id,
            payload=payload,
            payload_kind="generic",  # use generic sanitizer (allow-listed key set)
            db=db,
        )
    except Exception as exc:
        # Defensive — webhook_log already swallows, but belt-and-braces.
        logger.warning("log_business_event swallowed error: %s", exc)
