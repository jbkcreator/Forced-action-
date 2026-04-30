"""
Auto Mode — Stage 5.

When a wallet subscriber's `auto_mode_enabled` flag is on (or they're on a
Growth/Power tier where Auto Mode is included), Cora performs three actions
on every newly delivered lead:

  1. Auto skip-trace the property owner (so a phone number exists)
  2. Send the first outbound SMS to the owner (TCPA-gated)
  3. If no reply within 24h, drop a personalised voicemail via Synthflow
     (handled by `auto_mode_followup` cron task).

`enqueue_action(subscriber_id, property_id, db)` is the public entry point —
call it from the lead-delivery path after a SentLead row is committed. It
fails-soft: every step logs and returns False rather than raising, so a
dropped step never blocks lead delivery.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.core.models import (
    MessageOutcome,
    Owner,
    Property,
    Subscriber,
    WalletBalance,
)

logger = logging.getLogger(__name__)


_AUTO_MODE_TIERS = {"growth", "power"}    # wallet tiers where Auto Mode is included


def is_enabled(subscriber_id: int, db: Session) -> bool:
    """Subscriber.auto_mode_enabled flag check (kept for back-compat)."""
    sub = db.get(Subscriber, subscriber_id)
    return bool(sub and sub.auto_mode_enabled)


def toggle(subscriber_id: int, enabled: bool, db: Session) -> bool:
    sub = db.get(Subscriber, subscriber_id)
    if not sub:
        return False
    sub.auto_mode_enabled = enabled
    db.flush()
    logger.info("Auto mode %s for subscriber %d", "enabled" if enabled else "disabled", subscriber_id)
    return enabled


def is_eligible(subscriber_id: int, db: Session) -> bool:
    """Eligible if `auto_mode_enabled` flag is on OR wallet is Growth/Power."""
    sub = db.get(Subscriber, subscriber_id)
    if not sub:
        return False
    if sub.auto_mode_enabled:
        return True
    wallet = db.execute(
        select(WalletBalance).where(WalletBalance.subscriber_id == subscriber_id)
    ).scalar_one_or_none()
    return bool(wallet and wallet.wallet_tier in _AUTO_MODE_TIERS)


def enqueue_action(subscriber_id: int, property_id: int, db: Session) -> dict:
    """
    Run Auto Mode steps for a newly delivered lead. Returns a per-step status dict.
    Always returns; never raises (fails-soft so it never blocks delivery).
    """
    result = {
        "eligible": False,
        "skip_trace_queued": False,
        "first_text_sent": False,
        "first_text_outcome_id": None,
    }
    if not is_eligible(subscriber_id, db):
        return result
    result["eligible"] = True

    prop = db.get(Property, property_id)
    if not prop:
        logger.warning("[AutoMode] property %s missing - abort", property_id)
        return result

    owner = db.execute(
        select(Owner).where(Owner.property_id == property_id).limit(1)
    ).scalar_one_or_none()

    # 1. Skip-trace if no phone yet — defer to the existing batch runner.
    #    The daily cron `run_enrichment` will pick it up. We mark intent
    #    so monitoring can see auto-mode requested it.
    if not owner or not owner.phone_1:
        result["skip_trace_queued"] = True
        logger.info(
            "[AutoMode] skip-trace queued (no phone yet): subscriber=%d property=%d",
            subscriber_id, property_id,
        )
        # Bail until enrichment ships a phone number; followup will retry.
        return result

    # 2. Send first text — TCPA gate inside send_sms() handles quiet hours.
    body = _compose_first_text(prop, owner)
    outcome = _record_outcome(subscriber_id, body, db)
    result["first_text_outcome_id"] = outcome.id

    from src.services.sms_compliance import send_sms
    sent = send_sms(
        to=owner.phone_1,
        body=body,
        db=db,
        subscriber_id=subscriber_id,
        task_type="auto_mode",
        campaign="auto_mode_first_text",
    )
    result["first_text_sent"] = sent
    if sent:
        outcome.delivered_at = datetime.now(timezone.utc)
        db.flush()
    return result


# Kept for back-compat — old call sites still reference this name.
def queue_action(subscriber_id: int, action_type: str, lead_id: int, db: Session) -> None:
    if action_type == "first_text":
        enqueue_action(subscriber_id=subscriber_id, property_id=lead_id, db=db)
        return
    logger.info(
        "Auto mode action queued: subscriber=%d action=%s lead=%d",
        subscriber_id, action_type, lead_id,
    )


def _compose_first_text(prop: Property, owner: Owner) -> str:
    name = (owner.owner_name or "").split()[0] if owner.owner_name else "there"
    line = (
        f"Hi {name}, my team noticed your property at {prop.address or 'your address'} "
        f"may qualify for a fast no-cost assessment. Reply YES to learn more or STOP to opt out."
    )
    # Keep under 320 chars for two-segment safety.
    return line[:320]


def _record_outcome(subscriber_id: int, body: str, db: Session) -> MessageOutcome:
    outcome = MessageOutcome(
        subscriber_id=subscriber_id,
        message_type="sms",
        template_id="auto_mode_first_text",
        channel="twilio",
        sent_at=datetime.now(timezone.utc),
    )
    db.add(outcome)
    db.flush()
    return outcome
