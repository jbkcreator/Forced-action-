"""
Instantly webhook receiver — immediate bounce/unsubscribe suppression
(WP-T2-1 go-live review, 2026-09).

Before this module existed, hard bounces and unsubscribes only reached
email_opt_outs via suppression_sync.sync_unsubscribes()'s poll (run on
sweep's ~30-min cadence). Between a bounce happening and the next poll, a
second send to the same dead address could still go out. Instantly's
documented webhook catalog (https://developer.instantly.ai/guides/webhook-events)
exposes `email_bounced` and `lead_unsubscribed` as real-time push events —
this module is the receiving side, wired at POST /webhooks/instantly in
src/api/main.py.

This does NOT add a second suppression path. It calls the exact same
src.services.email_suppression.suppress_contact() that the poll calls —
the poll remains in place unchanged as a backstop (a missed/failed webhook
delivery is still caught within ~30 minutes), so this is strictly an
additional, faster trigger into the one existing suppression sink, not a
new one.

Spam complaints are explicitly NOT handled here: Instantly's webhook
catalog has no complaint/FBL event type (see suppression_sync.py's
docstring for the fuller investigation). An unrecognized event_type is
logged and ignored, not treated as an error — Instantly's catalog may add
event types this account doesn't otherwise use.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from src.services.email_suppression import suppress_contact

logger = logging.getLogger(__name__)

# Instantly's documented event_type values that this module treats as an
# immediate suppression trigger. Matches suppression_sync._SUPPRESS_ON_STATUS's
# intent (bounced, unsubscribed) but keyed on the webhook's own vocabulary,
# which is not guaranteed to match the lead-status polling vocabulary.
#
# Values are short, fixed labels (not f"instantly_webhook:{event_type}") --
# email_opt_outs.source is VARCHAR(30), and "instantly_webhook:email_bounced"
# (32 chars) overflowed it, silently failing the suppression INSERT the
# first time this was exercised against a real event (caught by manual
# local verification, 2026-09). Every value here must stay under 30 chars.
_SUPPRESS_ON_EVENT = {
    "email_bounced": "instantly_webhook_bounce",
    "lead_unsubscribed": "instantly_webhook_unsub",
}


def _extract_email(payload: dict[str, Any]) -> Optional[str]:
    """Instantly's webhook envelopes nest the lead under different keys
    depending on event type (undocumented precisely enough to trust one
    shape) — check the plausible candidates rather than assuming one."""
    for key in ("email", "lead_email"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip().lower()
    lead = payload.get("lead")
    if isinstance(lead, dict):
        value = lead.get("email")
        if isinstance(value, str) and value.strip():
            return value.strip().lower()
    return None


def handle_event(db, payload: dict[str, Any]) -> bool:
    """Process one Instantly webhook delivery. Returns True if it caused a
    suppression write, False otherwise (unrecognized event, no email, or an
    event type this module doesn't act on). Never raises — a malformed or
    unexpected payload must not crash the webhook endpoint; the caller
    still returns 200 so Instantly doesn't retry a payload we understood
    and chose to ignore.
    """
    event_type = (payload.get("event_type") or payload.get("event") or "").strip().lower()
    source = _SUPPRESS_ON_EVENT.get(event_type)
    if source is None:
        logger.info("[Relay][InstantlyWebhook] ignoring event_type=%s", event_type or "<missing>")
        return False

    email = _extract_email(payload)
    if not email:
        logger.warning(
            "[Relay][InstantlyWebhook] event_type=%s had no extractable email — cannot suppress",
            event_type,
        )
        return False

    suppress_contact(db, email=email, source=source)
    logger.info("[Relay][InstantlyWebhook] suppressed contact via event_type=%s", event_type)
    return True
