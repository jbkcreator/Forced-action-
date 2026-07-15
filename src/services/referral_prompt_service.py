"""
Proactive referral prompt — nudges a subscriber to share their referral link
right after a deal-win or lead-pack-delivery moment, instead of waiting for
them to visit their own /share/{code} page.

Reuses the existing weekly-cached, vertical-specific referral_forward_pack
copy (forward_pack_renderer.get_current_copy) rather than composing new copy,
and the existing SMS/email send primitives (sms_compliance.send_sms,
services/email.send_email). Funnel state (shown -> shared -> confirmed) is
tracked in referral_prompt_funnel via raw SQL only — see
migrations/apply_referral_prompt_funnel.py for the schema.

Called from two fail-soft hook sites: deal_outcome_effects.py (deal-win) and
lead_pack_fulfillment_sweep.py (lead-pack-delivery). Never raises — callers
already wrap this in their own try/except, but every external call here is
independently guarded too.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

PROMPT_COOLDOWN_DAYS = 30


def _within_cooldown(subscriber_id: int, db: Session) -> bool:
    cutoff = datetime.now(timezone.utc) - timedelta(days=PROMPT_COOLDOWN_DAYS)
    row = db.execute(
        text(
            "SELECT 1 FROM referral_prompt_funnel "
            "WHERE subscriber_id = :sid AND prompt_shown_at > :cutoff LIMIT 1"
        ),
        {"sid": subscriber_id, "cutoff": cutoff},
    ).first()
    return row is not None


def maybe_send_referral_prompt(
    subscriber,
    db: Session,
    *,
    trigger_type: str,
    trigger_source_table: str,
    trigger_source_id: int,
) -> bool:
    """
    Send the SMS+email referral prompt and record a 'shown' funnel row.

    No-op (returns False) if the subscriber has no vertical, no cached
    forward-pack copy yet, or is within the per-subscriber cooldown window.
    Idempotent against retries via the funnel table's UNIQUE(trigger_source_table,
    trigger_source_id) index.
    """
    if subscriber is None or not getattr(subscriber, "vertical", None):
        return False

    if _within_cooldown(subscriber.id, db):
        logger.info(
            "[ReferralPrompt] subscriber=%d within %dd cooldown — skipping %s prompt",
            subscriber.id, PROMPT_COOLDOWN_DAYS, trigger_type,
        )
        return False

    from src.services.referral_engine import ensure_referral_code
    referral_code = ensure_referral_code(subscriber.id, db)

    from src.services.forward_pack_renderer import get_current_copy
    body = get_current_copy(subscriber.vertical, db)
    if not body:
        logger.warning(
            "[ReferralPrompt] no forward-pack copy cached for vertical=%s — skipping subscriber=%d",
            subscriber.vertical, subscriber.id,
        )
        return False

    from config.settings import get_settings
    settings = get_settings()
    share_url = f"{settings.app_base_url}/share/{referral_code}"

    sms_sent = False
    email_sent = False

    phone = getattr(subscriber, "phone", None)
    if phone:
        try:
            from src.services.sms_compliance import send_sms
            sms_sent = send_sms(
                phone,
                f"{body} {share_url}",
                db,
                message_type="transactional",
                subscriber_id=subscriber.id,
                task_type=f"referral_prompt_{trigger_type}",
            )
        except Exception as exc:
            logger.warning("[ReferralPrompt] SMS send failed for subscriber=%d: %s", subscriber.id, exc)

    email = getattr(subscriber, "email", None)
    if email:
        try:
            from src.services.email import send_email
            email_sent = send_email(
                to=email,
                subject="Nice work — want to share the love?",
                body_text=f"{body}\n\n{share_url}",
                db=db,
            )
        except Exception as exc:
            logger.warning("[ReferralPrompt] email send failed for subscriber=%d: %s", subscriber.id, exc)

    try:
        db.execute(
            text(
                "INSERT INTO referral_prompt_funnel "
                "(subscriber_id, trigger_type, trigger_source_table, trigger_source_id, "
                " referral_code, sms_sent, email_sent) "
                "VALUES (:sid, :ttype, :ttable, :tid, :code, :sms, :email) "
                "ON CONFLICT (trigger_source_table, trigger_source_id) DO NOTHING"
            ),
            {
                "sid": subscriber.id,
                "ttype": trigger_type,
                "ttable": trigger_source_table,
                "tid": trigger_source_id,
                "code": referral_code,
                "sms": sms_sent,
                "email": email_sent,
            },
        )
        db.flush()
    except Exception as exc:
        logger.warning(
            "[ReferralPrompt] funnel row insert failed for subscriber=%d source=%s:%d: %s",
            subscriber.id, trigger_source_table, trigger_source_id, exc,
        )

    logger.info(
        "[ReferralPrompt] subscriber=%d trigger=%s sms_sent=%s email_sent=%s",
        subscriber.id, trigger_type, sms_sent, email_sent,
    )
    return sms_sent or email_sent


def mark_confirmed(referrer_subscriber_id: int, referral_event_id: int, db: Session) -> None:
    """
    Advance the referrer's most recent shown/shared funnel row to 'confirmed'.
    Best-effort — called from the confirm_purchase() call sites in
    stripe_webhooks.py, never from referral_engine.py itself.
    """
    db.execute(
        text(
            "UPDATE referral_prompt_funnel "
            "SET state = 'confirmed', confirmed_at = now(), confirmed_referral_event_id = :eid "
            "WHERE id = ("
            "  SELECT id FROM referral_prompt_funnel "
            "  WHERE subscriber_id = :rid AND state IN ('shown', 'shared') "
            "  ORDER BY prompt_shown_at DESC LIMIT 1"
            ")"
        ),
        {"eid": referral_event_id, "rid": referrer_subscriber_id},
    )
    db.flush()
