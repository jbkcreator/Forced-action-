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


def _reserve_funnel_row(
    subscriber_id: int,
    trigger_type: str,
    trigger_source_table: str,
    trigger_source_id: int,
    referral_code: str,
    db: Session,
) -> Optional[int]:
    """Atomically claim the funnel row for this trigger BEFORE any external
    send. Returns the new row id when this caller won the claim, or None when
    the row already exists (a concurrent execution or a retry) — in which case
    the caller must not send, because the winner already did (or will).

    The UNIQUE(trigger_source_table, trigger_source_id) index makes the INSERT
    the single point of serialization: a concurrent second INSERT blocks on the
    index until the first transaction resolves, then returns no row via
    ON CONFLICT DO NOTHING RETURNING.
    """
    row = db.execute(
        text(
            "INSERT INTO referral_prompt_funnel "
            "(subscriber_id, trigger_type, trigger_source_table, trigger_source_id, referral_code) "
            "VALUES (:sid, :ttype, :ttable, :tid, :code) "
            "ON CONFLICT (trigger_source_table, trigger_source_id) DO NOTHING "
            "RETURNING id"
        ),
        {
            "sid": subscriber_id,
            "ttype": trigger_type,
            "ttable": trigger_source_table,
            "tid": trigger_source_id,
            "code": referral_code,
        },
    ).first()
    db.flush()
    return row[0] if row else None


def _mark_send_status(funnel_id: int, sms_sent: bool, email_sent: bool, db: Session) -> None:
    """Record per-channel send outcome on an already-reserved funnel row."""
    db.execute(
        text(
            "UPDATE referral_prompt_funnel "
            "SET sms_sent = :sms, email_sent = :email "
            "WHERE id = :fid"
        ),
        {"sms": sms_sent, "email": email_sent, "fid": funnel_id},
    )
    db.flush()


def maybe_send_referral_prompt(
    subscriber,
    db: Session,
    *,
    trigger_type: str,
    trigger_source_table: str,
    trigger_source_id: int,
) -> bool:
    """
    Reserve a 'shown' funnel row, then send the SMS+email referral prompt.

    No-op (returns False) if the subscriber has no vertical, no cached
    forward-pack copy yet, or is within the per-subscriber cooldown window.

    Idempotency (fix): the funnel row is reserved via INSERT ... ON CONFLICT
    DO NOTHING RETURNING id *before* any external send. Only the caller that
    wins the reservation dispatches messages; a concurrent execution or a retry
    that loses the race short-circuits without re-sending. Per-channel send
    status is written back to the reserved row afterwards.

    The SMS goes out on the marketing path (fix): this is promotional copy, so
    it must respect SMS opt-in consent, the marketing frequency caps, and the
    free-tier allotment — all enforced inside send_sms for message_type
    "marketing".
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

    # Reserve the funnel row up front. If we don't win the claim, a concurrent
    # run or a retry already owns this trigger — never send twice.
    try:
        funnel_id = _reserve_funnel_row(
            subscriber.id, trigger_type, trigger_source_table, trigger_source_id,
            referral_code, db,
        )
    except Exception as exc:
        logger.warning(
            "[ReferralPrompt] funnel reservation failed for subscriber=%d source=%s:%d: %s — not sending",
            subscriber.id, trigger_source_table, trigger_source_id, exc,
        )
        return False

    if funnel_id is None:
        logger.info(
            "[ReferralPrompt] subscriber=%d source=%s:%d already reserved — skipping duplicate send",
            subscriber.id, trigger_source_table, trigger_source_id,
        )
        return False

    # Bind the share link to THIS funnel row so a later /share visit or signup
    # can be attributed to the exact prompt that drove it (not the newest one).
    from config.settings import get_settings
    settings = get_settings()
    from src.services.signed_links import encode_prompt_attribution_token
    token = encode_prompt_attribution_token(funnel_id)
    base_share = f"{settings.app_base_url}/share/{referral_code}"
    share_url = f"{base_share}?t={token}" if token else base_share

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
                message_type="marketing",
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
        _mark_send_status(funnel_id, sms_sent, email_sent, db)
    except Exception as exc:
        logger.warning(
            "[ReferralPrompt] send-status update failed for funnel=%d: %s",
            funnel_id, exc,
        )

    logger.info(
        "[ReferralPrompt] subscriber=%d trigger=%s funnel=%d sms_sent=%s email_sent=%s",
        subscriber.id, trigger_type, funnel_id, sms_sent, email_sent,
    )
    return sms_sent or email_sent


def mark_confirmed(
    referrer_subscriber_id: int,
    referral_event_id: int,
    db: Session,
    prompt_funnel_id: Optional[int] = None,
) -> None:
    """
    Advance the funnel row that actually drove this referral to 'confirmed'.
    Best-effort — called from the confirm_purchase() call sites in
    stripe_webhooks.py, never from referral_engine.py itself.

    Attribution (fix): prefer the exact funnel row carried through signup on the
    referral event (`prompt_funnel_id`). When that is absent (older links, or a
    referral that predates any prompt), only attribute when the referrer has
    exactly ONE open (shown/shared) row — an unambiguous match. If several are
    open we cannot know which prompt drove the conversion, so we leave it
    un-attributed rather than falsely crediting the newest one.
    """
    if prompt_funnel_id is not None:
        result = db.execute(
            text(
                "UPDATE referral_prompt_funnel "
                "SET state = 'confirmed', confirmed_at = now(), confirmed_referral_event_id = :eid "
                "WHERE id = :fid AND subscriber_id = :rid AND state IN ('shown', 'shared')"
            ),
            {"eid": referral_event_id, "fid": prompt_funnel_id, "rid": referrer_subscriber_id},
        )
        db.flush()
        if result.rowcount:
            return
        logger.info(
            "[ReferralPrompt] prompt_funnel_id=%s did not match an open row for referrer=%d — un-attributed",
            prompt_funnel_id, referrer_subscriber_id,
        )
        return

    # No explicit attribution — only advance when it is unambiguous.
    open_rows = db.execute(
        text(
            "SELECT id FROM referral_prompt_funnel "
            "WHERE subscriber_id = :rid AND state IN ('shown', 'shared') "
            "LIMIT 2"
        ),
        {"rid": referrer_subscriber_id},
    ).fetchall()
    if len(open_rows) != 1:
        logger.info(
            "[ReferralPrompt] referrer=%d has %d open funnel rows and no explicit "
            "attribution — leaving confirmed un-attributed",
            referrer_subscriber_id, len(open_rows),
        )
        return
    db.execute(
        text(
            "UPDATE referral_prompt_funnel "
            "SET state = 'confirmed', confirmed_at = now(), confirmed_referral_event_id = :eid "
            "WHERE id = :fid"
        ),
        {"eid": referral_event_id, "fid": open_rows[0][0]},
    )
    db.flush()
