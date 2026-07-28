from __future__ import annotations

import logging
from datetime import datetime, timezone

from config.settings import get_settings
from src.services.referral_prompt_service import (
    _mark_send_status,
    _reserve_funnel_row,
    _within_cooldown,
)

logger = logging.getLogger(__name__)


def _is_big_win(outcome) -> bool:
    return bool(
        (getattr(outcome, "deal_amount", None) and outcome.deal_amount >= 10000)
        or getattr(outcome, "deal_size_bucket", None) in ("10_25k", "25k_plus")
    )


def _is_go_live_eligible(outcome) -> bool:
    created_at = getattr(outcome, "created_at", None)
    if created_at is None:
        return False
    settings = get_settings()
    created_date = created_at.date() if hasattr(created_at, "date") else created_at
    return created_date >= settings.deal_win_testimonial_go_live_at


def maybe_send_deal_win_social_proof_prompt(subscriber, outcome, db) -> bool:
    if subscriber is None or outcome is None or not getattr(subscriber, "email", None):
        return False
    if not _is_big_win(outcome):
        return False
    if not _is_go_live_eligible(outcome):
        logger.info(
            "[DealWinSocialProof] outcome=%s predates go-live; skipping",
            getattr(outcome, "id", None),
        )
        return False
    if _within_cooldown(subscriber.id, db):
        logger.info(
            "[DealWinSocialProof] subscriber=%s within referral cooldown; skipping",
            subscriber.id,
        )
        return False

    from src.services.referral_engine import ensure_referral_code
    from src.services.signed_links import encode_prompt_attribution_token
    from src.services.email import send_email
    from src.services.transactional_email_tracking import log_transactional_email_send

    referral_code = ensure_referral_code(subscriber.id, db)
    funnel_id = _reserve_funnel_row(
        subscriber.id,
        "deal_win_social_proof",
        "deal_outcomes",
        outcome.id,
        referral_code,
        db,
    )
    if funnel_id is None:
        logger.info(
            "[DealWinSocialProof] subscriber=%s outcome=%s already reserved; skipping duplicate send",
            subscriber.id,
            outcome.id,
        )
        return False

    settings = get_settings()
    token = encode_prompt_attribution_token(funnel_id)
    base_share = f"{settings.app_base_url}/share/{referral_code}"
    share_url = f"{base_share}?t={token}" if token else base_share

    amount_text = "five figures"
    if getattr(outcome, "deal_amount", None):
        amount_text = f"${int(outcome.deal_amount):,}"

    subscriber_name = getattr(subscriber, "name", None) or "there"
    body_text = (
        f"Hi {subscriber_name},\n\n"
        f"Huge win on the {amount_text} close.\n\n"
        "Could you hit reply and send us two quick things?\n"
        "1. A short testimonial about what Forced Action helped you close.\n"
        "2. The approximate dollar amount you made on the deal.\n\n"
        "Also, if you know another operator who would want deals like this, "
        "you can share your referral link here:\n"
        f"{share_url}\n\n"
        "We read every reply.\n\n"
        "- Forced Action Team"
    )
    body_html = f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/></head>
<body style="margin:0;padding:0;background:#0f172a;font-family:Arial,sans-serif;color:#e2e8f0;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#0f172a;padding:40px 0;">
    <tr><td align="center">
      <table width="560" cellpadding="0" cellspacing="0"
             style="background:#1e293b;border:1px solid rgba(255,255,255,0.08);border-radius:16px;overflow:hidden;max-width:560px;width:100%;">
        <tr>
          <td style="padding:32px 40px;">
            <h1 style="margin:0 0 12px;font-size:28px;color:#ffffff;">Huge win.</h1>
            <p style="margin:0 0 20px;font-size:16px;color:#cbd5e1;">
              Congrats on the {amount_text} close.
            </p>
            <p style="margin:0 0 12px;font-size:15px;color:#e2e8f0;">
              Hit reply and send us:
            </p>
            <p style="margin:0 0 20px;font-size:15px;color:#e2e8f0;">
              1. A short testimonial about what Forced Action helped you close.<br/>
              2. The approximate dollar amount you made on the deal.
            </p>
            <p style="margin:0 0 16px;font-size:15px;color:#94a3b8;">
              If you know another operator who should see deals like this, share your referral link too:
            </p>
            <p style="margin:0 0 24px;">
              <a href="{share_url}" style="color:#fbbf24;text-decoration:none;font-weight:700;">{share_url}</a>
            </p>
            <p style="margin:0;font-size:13px;color:#64748b;">We read every reply.</p>
          </td>
        </tr>
      </table>
    </td></tr>
  </table>
</body>
</html>"""

    email_sent = False
    try:
        email_sent = send_email(
            to=subscriber.email,
            subject="Big win - can we feature it?",
            body_text=body_text,
            body_html=body_html,
            db=db,
        )
        if email_sent:
            log_transactional_email_send(
                db,
                recipient_email=subscriber.email,
                subscriber_id=subscriber.id,
                template_id="deal_win_social_proof_email",
                context_snapshot={
                    "deal_outcome_id": outcome.id,
                    "deal_amount": float(outcome.deal_amount) if getattr(outcome, "deal_amount", None) else None,
                    "deal_size_bucket": outcome.deal_size_bucket,
                    "share_url": share_url,
                },
            )
    finally:
        try:
            _mark_send_status(funnel_id, False, email_sent, db)
        except Exception as exc:
            logger.warning(
                "[DealWinSocialProof] send-status update failed for funnel=%s: %s",
                funnel_id,
                exc,
            )

    logger.info(
        "[DealWinSocialProof] subscriber=%s outcome=%s email_sent=%s",
        subscriber.id,
        outcome.id,
        email_sent,
    )
    return email_sent
