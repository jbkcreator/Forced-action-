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

logger = logging.getLogger(__name__)


def send_email(
    to: str,
    subject: str,
    body_text: str,
    body_html: Optional[str] = None,
    attachments: Optional[List[Union[str, Path]]] = None,
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

    from_addr = settings.email_from or settings.smtp_user
    password = settings.smtp_pass.get_secret_value()

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

        with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=10) as server:
            server.starttls()
            server.login(settings.smtp_user, password)
            server.sendmail(from_addr, [to], msg.as_string())

        logger.info("Email sent → %s | %s", to, subject)
        return True

    except Exception as exc:
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


def send_welcome_email(subscriber) -> None:
    """
    Send the dashboard-link welcome email for any new subscriber (free or paid).

    `subscriber` duck-typed: needs .email, .name, .tier, .vertical,
    .founding_member, .event_feed_uuid, .id.
    Non-blocking — caller must wrap in try/except if needed.
    """
    if not subscriber.email:
        return

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
    body_text = (
        f"Hi {name},\n\n"
        f"Welcome to Forced Action.\n"
        f"{founding_line}\n"
        f"Plan: {tier} — {vertical}\n\n"
        f"Your private Event Feed is live. Bookmark this link — it's yours alone:\n"
        f"{feed_url}\n\n"
        f"New distressed property leads matching your territory and vertical will appear "
        f"here automatically as our scrapers run each day.\n\n"
        f"Questions? Reply to this email or reach us at support@forcedaction.io\n\n"
        f"— Forced Action Team"
    )

    founding_badge = (
        '<p style="margin:0 0 16px;padding:10px 16px;background:#451a03;'
        'border:1px solid #92400e;border-radius:8px;color:#fbbf24;font-size:14px;">'
        "⭐ Founding Member — your rate is locked for life."
        "</p>"
        if founding else ""
    )
    body_html = f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/></head>
<body style="margin:0;padding:0;background:#0f172a;font-family:Inter,Arial,sans-serif;color:#e2e8f0;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#0f172a;padding:40px 0;">
    <tr><td align="center">
      <table width="560" cellpadding="0" cellspacing="0"
             style="background:#1e293b;border:1px solid rgba(255,255,255,0.08);border-radius:16px;overflow:hidden;max-width:560px;width:100%;">
        <tr>
          <td style="padding:32px 40px 24px;border-bottom:1px solid rgba(255,255,255,0.08);">
            <p style="margin:0;font-size:22px;font-weight:800;color:#ffffff;">
              Forced <span style="color:#fbbf24;">Action</span>
            </p>
          </td>
        </tr>
        <tr>
          <td style="padding:32px 40px;">
            <h1 style="margin:0 0 8px;font-size:26px;font-weight:800;color:#ffffff;">
              You&rsquo;re in, {name}.
            </h1>
            <p style="margin:0 0 24px;color:#94a3b8;font-size:15px;">
              Your Event Feed is live and your territory is reserved.
            </p>
            {founding_badge}
            <p style="margin:0 0 24px;">
              <span style="display:inline-block;background:rgba(251,191,36,0.1);border:1px solid rgba(251,191,36,0.3);
                           color:#fbbf24;font-size:13px;font-weight:700;padding:5px 14px;border-radius:999px;">
                {tier} &middot; {vertical}
              </span>
            </p>
            <p style="margin:0 0 12px;font-size:14px;color:#94a3b8;">
              Your private feed link — bookmark it:
            </p>
            <table cellpadding="0" cellspacing="0" style="margin-bottom:28px;">
              <tr>
                <td style="background:#fbbf24;border-radius:8px;">
                  <a href="{feed_url}"
                     style="display:inline-block;padding:14px 28px;color:#0f172a;font-size:15px;
                            font-weight:700;text-decoration:none;">
                    Open My Event Feed &rarr;
                  </a>
                </td>
              </tr>
            </table>
            <p style="margin:0;font-size:13px;color:#64748b;">
              Questions? Reply to this email or reach us at
              <a href="mailto:support@forcedaction.io" style="color:#fbbf24;text-decoration:none;">
                support@forcedaction.io
              </a>
            </p>
          </td>
        </tr>
        <tr>
          <td style="padding:20px 40px;border-top:1px solid rgba(255,255,255,0.08);
                     font-size:12px;color:#475569;text-align:center;">
            Forced Action &mdash; Hillsborough County Property Intelligence<br/>
            <a href="{_settings.app_base_url}" style="color:#475569;">forcedaction.io</a>
          </td>
        </tr>
      </table>
    </td></tr>
  </table>
</body>
</html>"""

    send_email(
        to=subscriber.email,
        subject=subject,
        body_text=body_text,
        body_html=body_html,
    )
    logger.info("Welcome email sent → %s (subscriber=%s)", subscriber.email, subscriber.id)
