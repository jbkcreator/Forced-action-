"""
Shared email sending helper.

All transactional emails (payment receipts, payment failures, waitlist alerts,
match-rate ops alerts) go through send_email(). If SMTP is not configured the
call is a no-op — callers never need to guard against missing credentials.

Configure via environment variables (loaded into AppSettings):
    SMTP_HOST, SMTP_PORT (default 587), SMTP_USER, SMTP_PASS
    EMAIL_FROM  (optional, falls back to SMTP_USER)
"""

import logging
import mimetypes
import smtplib
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import encoders
from pathlib import Path
from typing import List, Optional, Union

from config.settings import get_settings
from src.services.email_shell import (
    ACCENT,
    lead_row,
    paragraph,
    render_email_shell,
)

logger = logging.getLogger(__name__)


def _create_message_outcome(db, *, to: str, tracking: dict):
    from datetime import datetime, timezone
    from src.core.models import MessageOutcome

    outcome = MessageOutcome(
        subscriber_id=tracking.get("subscriber_id"),
        message_type="email",
        template_id=tracking.get("template_id"),
        channel=tracking.get("channel") or "mandrill",
        recipient_email=to.strip().lower(),
        sent_at=datetime.now(timezone.utc),
        # Pre-send row; flips to 'sent' on SMTP accept, 'failed' on error.
        # Must be a value allowed by the check_mo_send_status constraint
        # ('pending' is NOT allowed — see apply_fa060).
        send_status="scheduled",
        context_snapshot=tracking.get("context_snapshot"),
    )
    db.add(outcome)
    db.flush()
    return outcome


def send_email(
    to: str,
    subject: str,
    body_text: str,
    body_html: Optional[str] = None,
    attachments: Optional[List[Union[str, Path]]] = None,
    cc: Optional[List[str]] = None,
    list_unsubscribe_url: Optional[str] = None,
    headers: Optional[dict[str, str]] = None,
    tracking: Optional[dict] = None,
    db=None,
) -> bool:
    """
    Send a transactional email via SMTP.

    Args:
        to:          Recipient address.
        subject:     Email subject line.
        body_text:   Plain-text body (always sent).
        body_html:   Optional HTML alternative body.
        attachments: Optional list of file paths to attach. Missing files are
                     skipped with a warning so a single bad path doesn't block
                     the whole send.

    Returns:
        True  — email accepted by SMTP server.
        False — SMTP not configured, or send failed (error logged).
    """
    settings = get_settings()

    if not all([settings.smtp_host, settings.smtp_user, settings.smtp_pass]):
        logger.debug("SMTP not configured — skipping email to %s", to)
        return False

    # Do-Not-Contact gate. Fail closed: if the suppression check itself errors
    # (DB blip, pool exhaustion) we do NOT send — sending to a possibly-opted-out
    # address is the compliance risk. Never let the gate's DB dependency crash
    # send_email(); it has always been a non-throwing bool. Reuse the caller's
    # session when given, else open a short-lived one.
    from src.services.email_suppression import is_email_suppressed

    try:
        if db is not None:
            suppressed = is_email_suppressed(db, to)
        else:
            from src.core.database import get_db_context
            with get_db_context() as _db:
                suppressed = is_email_suppressed(_db, to)
    except Exception as exc:
        logger.error("Suppression check failed for %s (%s) — not sending", to, exc)
        return False
    if suppressed:
        logger.info("Email to %s suppressed (opted out) — skipping send", to)
        return False

    from_addr = settings.email_from or settings.smtp_user
    password = settings.smtp_pass.get_secret_value()

    # Attribution: create the MessageOutcome BEFORE sending so its id can ride
    # along as Mandrill metadata (X-MC-Metadata). The webhook resolves events
    # back to this exact send by that id — never by "latest email for
    # recipient", which mis-attributes out-of-order bounce callbacks.
    outcome = None
    if tracking is not None and db is not None:
        try:
            from src.services.transactional_email_tracking import _email_tracking_columns_ready
            if _email_tracking_columns_ready(db):
                outcome = _create_message_outcome(db, to=to, tracking=tracking)
        except Exception as exc:
            logger.warning("Could not create MessageOutcome for %s: %s", to, exc)
            outcome = None

    try:
        # Use mixed multipart whenever attachments are present; alternative
        # body lives nested inside.
        if attachments:
            msg = MIMEMultipart("mixed")
            if body_html:
                alt = MIMEMultipart("alternative")
                alt.attach(MIMEText(body_text, "plain"))
                alt.attach(MIMEText(body_html, "html"))
                msg.attach(alt)
            else:
                msg.attach(MIMEText(body_text, "plain"))

            for att in attachments:
                path = Path(att)
                if not path.exists() or not path.is_file():
                    logger.warning("Skipping missing attachment: %s", path)
                    continue
                ctype, encoding = mimetypes.guess_type(str(path))
                if ctype is None or encoding is not None:
                    ctype = "application/octet-stream"
                maintype, subtype = ctype.split("/", 1)
                part = MIMEBase(maintype, subtype)
                part.set_payload(path.read_bytes())
                encoders.encode_base64(part)
                part.add_header(
                    "Content-Disposition",
                    f'attachment; filename="{path.name}"',
                )
                msg.attach(part)
        elif body_html:
            msg = MIMEMultipart("alternative")
            msg.attach(MIMEText(body_text, "plain"))
            msg.attach(MIMEText(body_html, "html"))
        else:
            msg = MIMEText(body_text, "plain")

        msg["Subject"] = subject
        msg["From"] = from_addr
        msg["To"] = to
        if list_unsubscribe_url:
            msg["List-Unsubscribe"] = f"<{list_unsubscribe_url}>"
            msg["List-Unsubscribe-Post"] = "List-Unsubscribe=One-Click"
        if cc:
            msg["Cc"] = ", ".join(cc)
            # Tell Mandrill to preserve original To/Cc headers for all recipients
            # instead of rewriting To: per-recipient (default ESP behaviour).
            msg["X-MC-PreserveRecipients"] = "true"

        # Mandrill echoes X-MC-Metadata back on every webhook event as
        # msg.metadata — this is how a bounce/open/click is matched to the
        # exact send that produced it.
        if outcome is not None:
            import json as _json
            msg["X-MC-Metadata"] = _json.dumps({"message_outcome_id": outcome.id})

        all_recipients = [to] + (cc or [])
        with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=10) as server:
            server.starttls()
            server.login(settings.smtp_user, password)
            server.sendmail(from_addr, all_recipients, msg.as_string())

        if outcome is not None:
            outcome.send_status = "sent"
            db.flush()

        logger.info("Email sent → %s cc=%s | %s", to, cc or [], subject)
        return True

    except Exception as exc:
        if outcome is not None:
            try:
                outcome.send_status = "failed"
                outcome.failure_reason = "smtp_send_error"
                db.flush()
            except Exception:
                pass
        logger.error("Failed to send email to %s (%s): %s", to, subject, exc)
        # Fire ops alert — but only if this isn't already an alert email (avoid loops)
        if settings.alert_email and to != settings.alert_email:
            try:
                _send_raw_alert(settings, f"[FA] SES send failure — {subject}", str(exc))
            except Exception:
                pass
        return False


def _send_raw_alert(settings, subject: str, body: str) -> None:
    """Minimal direct SMTP send for SES failure alerts — avoids calling send_email() recursively."""
    if not all([settings.smtp_host, settings.smtp_user, settings.smtp_pass, settings.alert_email]):
        return
    msg = MIMEText(body, "plain")
    msg["Subject"] = subject
    msg["From"] = settings.smtp_user
    msg["To"] = settings.alert_email
    with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=10) as srv:
        srv.starttls()
        srv.login(settings.smtp_user, settings.smtp_pass.get_secret_value())
        srv.sendmail(settings.smtp_user, [settings.alert_email], msg.as_string())


def send_alert(
    subject: str,
    body: str,
    html_body: Optional[str] = None,
    to: Optional[str] = None,
    attachments: Optional[List[Union[str, Path]]] = None,
) -> bool:
    """
    Send an ops alert email. By default targets ALERT_EMAIL; pass `to` to
    override (used by reports that go to a different recipient list).

    Also sends an SMS via email-to-SMS gateway if ALERT_SMS_NUMBER +
    ALERT_SMS_CARRIER are both configured (SMS path always uses plain text;
    attachments are intentionally not forwarded to the SMS path).

    Returns True if at least one channel succeeded.
    """
    settings = get_settings()
    sent = False

    # Email alert — to overrides default ALERT_EMAIL when provided
    recipient = to or settings.alert_email
    if recipient:
        sent = send_email(
            to=recipient,
            subject=subject,
            body_text=body,
            body_html=html_body,
            attachments=attachments,
        ) or sent

    # SMS via email-to-SMS gateway (no Twilio needed)
    if settings.alert_sms_number and settings.alert_sms_carrier:
        sms_addr = f"{settings.alert_sms_number}@{settings.alert_sms_carrier}"
        sms_body = body[:160]
        sent = send_email(to=sms_addr, subject=subject[:40], body_text=sms_body) or sent

    if not sent:
        logger.warning("Alert could not be sent (no recipient or SMTP). Subject: %s", subject)

    return sent


def send_welcome_email(subscriber, magic_link_url: Optional[str] = None, db=None) -> bool:
    """
    Send the dashboard-link welcome email for any new subscriber (free or paid).

    `subscriber` duck-typed: needs .email, .name, .tier, .vertical,
    .founding_member, .event_feed_uuid, .id.

    When `magic_link_url` is provided, the email includes a one-click,
    single-use login link instead of a password — no credential is ever
    generated or emailed. Callers build this URL via
    `src.services.subscriber_auth.issue_magic_link` + the `/auth/verify?token=`
    route.

    Non-blocking — caller must wrap in try/except if needed.

    Returns True only if the email was actually accepted by SMTP. Callers MUST
    gate stamp_welcome_email_sent() on this — stamping unconditionally marks a
    welcome email "sent" even when SMTP was down or the recipient suppressed,
    which then pages ops to chase a subscriber who never got a login link.
    """
    if not subscriber.email:
        return False

    _settings = get_settings()

    name = subscriber.name or "there"
    tier = (subscriber.tier or "free").title()
    vertical = (subscriber.vertical or "").replace("_", " ").title()
    founding = subscriber.founding_member

    feed_url = (
        f"{_settings.app_base_url}/dashboard/{subscriber.event_feed_uuid}"
        if subscriber.event_feed_uuid
        else _settings.app_base_url
    )

    subject = (
        "Founding member confirmed — your rate is locked forever"
        if founding
        else "You're in — your Forced Action feed is ready"
    )

    founding_line = (
        "\nAs a founding member your rate is locked for as long as you stay subscribed.\n"
        if founding else ""
    )
    # If a magic link is provided, it IS the login mechanism — the primary CTA
    # must point at it (the plain feed_url requires a session that doesn't
    # exist yet). Without one (should not happen for new subscribers), the CTA
    # falls back to the bare feed link.
    cta_url = magic_link_url or feed_url
    magic_link_note_text = (
        "This link expires shortly and can only be used once.\n\n"
        if magic_link_url else ""
    )
    body_text = (
        f"Hi {name},\n\n"
        f"Welcome to Forced Action.\n"
        f"{founding_line}\n"
        f"Plan: {tier} — {vertical}\n\n"
        f"Click below to access your private Event Feed — it's yours alone:\n"
        f"{cta_url}\n\n"
        f"{magic_link_note_text}"
        f"New distressed property leads matching your territory and vertical will appear "
        f"here automatically as our scrapers run each day.\n\n"
        f"Questions? Reply to this email or reach us at support@forcedactionleads.com\n\n"
        f"— Forced Action Team"
    )

    founding_badge = (
        lead_row(
            title="⭐ Founding Member",
            sub="Your rate is locked for life.",
        )
        if founding else ""
    )
    magic_link_note_html = (
        paragraph(
            "This link expires shortly and can only be used once.",
            muted=True,
        )
        if magic_link_url else ""
    )
    inner_html = (
        lead_row(title=f"{tier} &middot; {vertical}", sub="Your reserved plan and vertical")
        + founding_badge
        + paragraph("Your Event Feed is live and your territory is reserved.")
        + paragraph("Your private feed link is below — bookmark it once you're in.")
        + magic_link_note_html
        + paragraph(
            "Questions? Reply to this email or reach us at "
            f'<a href="mailto:support@forcedactionleads.com" style="color:{ACCENT};text-decoration:none;">'
            "support@forcedactionleads.com</a>.",
            muted=True,
        )
    )
    body_html = render_email_shell(
        headline=f"You're in, {name}.",
        subhead=f"{tier} &middot; {vertical}",
        inner_html=inner_html,
        cta_text="Open My Event Feed",
        cta_url=cta_url,
        footer_note="Forced Action — Hillsborough County Property Intelligence",
        preheader="Your private Event Feed is ready.",
    )

    sent = send_email(
        to=subscriber.email,
        subject=subject,
        body_text=body_text,
        body_html=body_html,
        tracking={
            "subscriber_id": subscriber.id,
            "template_id": "welcome_email",
            "channel": "mandrill",
            "context_snapshot": {
                "magic_link_included": bool(magic_link_url),
                "tier": subscriber.tier,
                "vertical": subscriber.vertical,
                "founding_member": bool(subscriber.founding_member),
            },
        },
        db=db,
    )
    if sent:
        logger.info("Welcome email sent → %s (subscriber=%s)", subscriber.email, subscriber.id)
    else:
        logger.warning("Welcome email NOT sent → %s (subscriber=%s)", subscriber.email, subscriber.id)
    return sent


def send_upgrade_confirmation_email(subscriber, db=None) -> bool:
    """
    Confirm a plan upgrade for a subscriber who already has dashboard access
    (e.g. a free-tier subscriber upgrading from their own dashboard).

    Unlike send_welcome_email, this never includes a magic link — the
    recipient is already authenticated, so a fresh one-time login link would
    be confusing/unnecessary. Just confirms the new plan and points back at
    the dashboard they were already using.

    `subscriber` duck-typed: needs .email, .name, .tier, .vertical,
    .founding_member, .event_feed_uuid, .id.

    Non-blocking — caller must wrap in try/except if needed.
    """
    if not subscriber.email:
        return False

    _settings = get_settings()

    name = subscriber.name or "there"
    tier = (subscriber.tier or "free").title()
    vertical = (subscriber.vertical or "").replace("_", " ").title()
    founding = subscriber.founding_member

    feed_url = (
        f"{_settings.app_base_url}/dashboard/{subscriber.event_feed_uuid}"
        if subscriber.event_feed_uuid
        else _settings.app_base_url
    )

    subject = f"You're on {tier} now — plan upgraded"

    founding_line = (
        "\nAs a founding member your rate is locked for as long as you stay subscribed.\n"
        if founding else ""
    )
    body_text = (
        f"Hi {name},\n\n"
        f"Your plan has been upgraded to {tier} ({vertical}).\n"
        f"{founding_line}\n"
        f"Your new territory is locked and new leads will start appearing in your feed:\n"
        f"{feed_url}\n\n"
        f"Questions? Reply to this email or reach us at support@forcedactionleads.com\n\n"
        f"— Forced Action Team"
    )

    founding_badge = (
        lead_row(
            title="⭐ Founding Member",
            sub="Your rate is locked for life.",
        )
        if founding else ""
    )
    inner_html = (
        lead_row(title=f"{tier} &middot; {vertical}", sub="Your upgraded plan and vertical")
        + founding_badge
        + paragraph("Your plan has been upgraded and your new territory is locked.")
        + paragraph("New leads will start appearing in your feed — open it below.")
        + paragraph(
            "Questions? Reply to this email or reach us at "
            f'<a href="mailto:support@forcedactionleads.com" style="color:{ACCENT};text-decoration:none;">'
            "support@forcedactionleads.com</a>.",
            muted=True,
        )
    )
    body_html = render_email_shell(
        headline=f"You're on {tier} now, {name}.",
        subhead=f"{tier} &middot; {vertical}",
        inner_html=inner_html,
        cta_text="Open My Event Feed",
        cta_url=feed_url,
        footer_note="Forced Action — Hillsborough County Property Intelligence",
        preheader="Your plan upgrade is confirmed.",
    )

    sent = send_email(
        to=subscriber.email,
        subject=subject,
        body_text=body_text,
        body_html=body_html,
        tracking={
            "subscriber_id": subscriber.id,
            "template_id": "upgrade_confirmation_email",
            "channel": "mandrill",
            "context_snapshot": {
                "tier": subscriber.tier,
                "vertical": subscriber.vertical,
                "founding_member": bool(subscriber.founding_member),
            },
        },
        db=db,
    )
    if sent:
        logger.info("Upgrade confirmation email sent → %s (subscriber=%s)", subscriber.email, subscriber.id)
    else:
        logger.warning("Upgrade confirmation email NOT sent → %s (subscriber=%s)", subscriber.email, subscriber.id)
    return sent
