"""
Instant owner alert — 5-minute speed-to-lead notification.

Pings the founder the moment a purchase or demo request happens, so he can
call the buyer while they're warm. Primary channel is a direct Telnyx SMS to
FOUNDER_PHONE; email (ALERT_EMAIL, via send_alert()) is the fallback whenever
the SMS attempt didn't actually reach him.

Deliberately bypasses src.services.sms_compliance's quiet-hours/DNC gate: this
is a self-notification to the operator's own verified number about his own
business event, not a TCPA-regulated consumer message, and the task requires
firing at any hour. src.tasks.revenue_pulse / cora_anomaly_check use the
compliant path instead — that's the right tradeoff for their routine digests,
not for this same-moment alert.

TELNYX_SMS_ENABLED is the master kill-switch (checked here explicitly, since
telnyx_sms.send_message() has no awareness of it) — while disabled (current
EIN/10DLC-dark state), this skips straight to email rather than attempting a
live Telnyx call against unconfigured/incomplete credentials.
"""

from __future__ import annotations

import logging

from config.settings import get_settings
from src.services.email import send_alert
from src.services.telnyx_sms import TelnyxSMSError, send_message

logger = logging.getLogger(__name__)

_SMS_CHAR_LIMIT = 320


def notify_owner(subject: str, body: str) -> None:
    """Best-effort founder alert: direct Telnyx SMS, email if SMS didn't land. Never raises."""
    settings = get_settings()
    sms_delivered = False

    if settings.founder_phone and settings.telnyx_sms_enabled:
        try:
            send_message(to=settings.founder_phone, body=body[:_SMS_CHAR_LIMIT], message_type="transactional")
            sms_delivered = True
        except Exception:
            logger.warning("Owner SMS alert failed, falling back to email", exc_info=True)

    if not sms_delivered:
        try:
            send_alert(subject=subject, body=body)
        except Exception:
            logger.warning("Owner email alert failed", exc_info=True)


if __name__ == "__main__":
    from unittest.mock import patch

    import src.services.owner_alert as _self  # patch the real module, not this __main__ copy

    # No FOUNDER_PHONE / TELNYX_SMS_ENABLED → must fall back to email, never raise.
    with patch.object(_self, "get_settings") as mock_settings, \
         patch.object(_self, "send_alert") as mock_send_alert:
        mock_settings.return_value.founder_phone = None
        mock_settings.return_value.telnyx_sms_enabled = False
        mock_send_alert.return_value = True
        _self.notify_owner("Test", "body")
        assert mock_send_alert.called, "email fallback must fire when SMS is not configured"

    # Telnyx enabled but the send raises → must still fall back to email, not raise.
    with patch.object(_self, "get_settings") as mock_settings, \
         patch.object(_self, "send_message") as mock_send_message, \
         patch.object(_self, "send_alert") as mock_send_alert:
        mock_settings.return_value.founder_phone = "+18135551234"
        mock_settings.return_value.telnyx_sms_enabled = True
        mock_send_message.side_effect = TelnyxSMSError("boom")
        mock_send_alert.return_value = True
        _self.notify_owner("Test", "body")
        assert mock_send_alert.called, "email fallback must fire when Telnyx send fails"

    # Telnyx enabled and succeeds → no email fallback needed.
    with patch.object(_self, "get_settings") as mock_settings, \
         patch.object(_self, "send_message") as mock_send_message, \
         patch.object(_self, "send_alert") as mock_send_alert:
        mock_settings.return_value.founder_phone = "+18135551234"
        mock_settings.return_value.telnyx_sms_enabled = True
        mock_send_message.return_value = {"status": "sent"}
        _self.notify_owner("Test", "body")
        assert not mock_send_alert.called, "email fallback must NOT fire when SMS succeeds"

    print("owner_alert self-check passed")
