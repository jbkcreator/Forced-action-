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
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional

from config.settings import get_settings

logger = logging.getLogger(__name__)


def send_email(
    to: str,
    subject: str,
    body_text: str,
    body_html: Optional[str] = None,
) -> bool:
    """
    Send a transactional email via SMTP.

    Args:
        to:        Recipient address.
        subject:   Email subject line.
        body_text: Plain-text body (always sent).
        body_html: Optional HTML alternative body.

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
        if body_html:
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
        return False


def send_alert(subject: str, body: str) -> bool:
    """
    Send an ops alert email to ALERT_EMAIL.
    Also sends an SMS via email-to-SMS gateway if ALERT_SMS_NUMBER +
    ALERT_SMS_CARRIER are both configured.

    Returns True if at least one channel succeeded.
    """
    settings = get_settings()
    sent = False

    # Email alert
    if settings.alert_email:
        sent = send_email(to=settings.alert_email, subject=subject, body_text=body) or sent

    # SMS via email-to-SMS gateway (no Twilio needed)
    if settings.alert_sms_number and settings.alert_sms_carrier:
        sms_addr = f"{settings.alert_sms_number}@{settings.alert_sms_carrier}"
        # SMS body: keep to 160 chars
        sms_body = body[:160]
        sent = send_email(to=sms_addr, subject=subject[:40], body_text=sms_body) or sent

    if not sent:
        logger.warning("Alert could not be sent (no ALERT_EMAIL or SMTP). Subject: %s", subject)

    return sent
