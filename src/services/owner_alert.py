"""
Instant owner alert — 5-minute speed-to-lead notification.

Pings the founder the moment a purchase or demo request happens, so he can
call the buyer while they're warm. Primary channel is a direct Telnyx SMS to
FOUNDER_PHONE; email (ALERT_EMAIL, via send_alert()) is the fallback whenever
the SMS didn't actually reach him — including a Telnyx "queued" response,
which only means Telnyx accepted the request, not that the carrier delivered
it. Delivery confirmation comes later, either via the message-status webhook
(src/api/main.py:telnyx_inbound handles message.finalized events) or the
sweep task (src/tasks/owner_alert_sweep.py) if that callback never arrives.

Every call is keyed by `idempotency_key` (e.g. "stripe:<event_id>",
"synthflow:<call_id>") and claimed via a unique-constraint insert into
owner_alert_dispatch — the same insert-then-catch-IntegrityError dedupe
pattern already used for the Stripe webhook event log. This is what makes a
webhook retry a safe no-op instead of a duplicate founder alert.

The actual vendor I/O (Telnyx: 15s timeout, SMTP: 10s timeout) runs on a
background thread so notify_owner() returns to the caller as soon as the
idempotency claim lands — a few milliseconds, not up to 25s. That matters
because notify_owner() is called from inside a Stripe webhook handler
(before that handler's DB transaction commits) and from an async Synthflow
route (where blocking I/O would stall the event loop for other requests).

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
import threading

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from config.settings import get_settings
from src.core.database import get_db_context
from src.core.models import OwnerAlertDispatch
from src.services.email import send_alert
from src.services.telnyx_sms import TelnyxSMSError, send_message

logger = logging.getLogger(__name__)

_SMS_CHAR_LIMIT = 320


def notify_owner(subject: str, body: str, idempotency_key: str) -> None:
    """
    Best-effort founder alert: direct Telnyx SMS, email if SMS didn't land.
    Never raises, never blocks beyond the idempotency claim.

    `idempotency_key` must be stable across webhook retries for the same
    source event (e.g. "stripe:evt_123", "synthflow:call_abc") so a retried
    webhook doesn't re-fire the same alert.
    """
    row_id = _claim_alert(idempotency_key, subject, body)
    if row_id is None:
        return

    threading.Thread(
        target=_dispatch_alert, args=(row_id, subject, body), daemon=True
    ).start()


def _claim_alert(alert_key: str, subject: str, body: str) -> int | None:
    """Insert the dispatch row; return its id, or None if already claimed."""
    try:
        with get_db_context() as db:
            row = OwnerAlertDispatch(alert_key=alert_key, subject=subject, body=body)
            db.add(row)
            try:
                db.commit()
            except IntegrityError:
                db.rollback()
                logger.info("Owner alert %s already dispatched — skipping duplicate", alert_key)
                return None
            db.refresh(row)
            return row.id
    except Exception:
        logger.warning("Owner alert claim failed for %s — falling back to direct email", alert_key, exc_info=True)
        try:
            send_alert(subject=subject, body=body)
        except Exception:
            logger.warning("Owner email alert failed", exc_info=True)
        return None


def _dispatch_alert(row_id: int, subject: str, body: str) -> None:
    """Runs on a background thread: does the actual Telnyx/SMTP I/O."""
    settings = get_settings()

    if settings.founder_phone and settings.telnyx_sms_enabled:
        try:
            result = send_message(to=settings.founder_phone, body=body[:_SMS_CHAR_LIMIT], message_type="transactional")
            _update_status(row_id, status="sms_sent", telnyx_message_id=result.get("message_id"))
            return
        except Exception:
            logger.warning("Owner SMS alert failed, falling back to email", exc_info=True)
            _update_status(row_id, status="sms_failed")

    _send_email_fallback(row_id, subject, body)


def _send_email_fallback(row_id: int, subject: str, body: str) -> None:
    try:
        send_alert(subject=subject, body=body)
    except Exception:
        logger.warning("Owner email alert failed", exc_info=True)
    _update_status(row_id, status="email_sent")


_FAILED_DELIVERY_STATUSES = {"delivery_failed", "sending_failed"}


def reconcile_delivery_status(telnyx_message_id: str | None, delivery_status: str) -> None:
    """
    Called from the Telnyx message.finalized webhook (src/api/main.py:
    telnyx_inbound). Marks a pending SMS as delivered, or fires the email
    fallback if the carrier ultimately failed to deliver it.
    """
    if not telnyx_message_id:
        return

    try:
        with get_db_context() as db:
            row = db.execute(
                text("SELECT id, subject, body FROM owner_alert_dispatch "
                     "WHERE telnyx_message_id = :mid AND status = 'sms_sent'"),
                {"mid": telnyx_message_id},
            ).first()
            if row is None:
                return
            row_id, subject, body = row.id, row.subject, row.body
    except Exception:
        logger.warning("Owner alert delivery reconcile lookup failed for message_id=%s", telnyx_message_id, exc_info=True)
        return

    if delivery_status == "delivered":
        _update_status(row_id, status="sms_delivered")
    elif delivery_status in _FAILED_DELIVERY_STATUSES:
        _send_email_fallback(row_id, subject, body)
    # Any other in-flight status ("sending", "sent", ...) — leave as sms_sent,
    # the sweep task or a later callback will resolve it.


def _update_status(row_id: int, *, status: str, telnyx_message_id: str | None = None) -> None:
    try:
        with get_db_context() as db:
            db.execute(
                text("UPDATE owner_alert_dispatch SET status = :status, "
                     "telnyx_message_id = COALESCE(:telnyx_message_id, telnyx_message_id) "
                     "WHERE id = :id"),
                {"status": status, "telnyx_message_id": telnyx_message_id, "id": row_id},
            )
    except Exception:
        logger.warning("Owner alert status update failed for row=%s status=%s", row_id, status, exc_info=True)


if __name__ == "__main__":
    import time
    from unittest.mock import patch

    import src.services.owner_alert as _self  # patch the real module, not this __main__ copy

    # No FOUNDER_PHONE / TELNYX_SMS_ENABLED → must fall back to email, never raise.
    with patch.object(_self, "get_settings") as mock_settings, \
         patch.object(_self, "send_alert") as mock_send_alert, \
         patch.object(_self, "_claim_alert", return_value=1):
        mock_settings.return_value.founder_phone = None
        mock_settings.return_value.telnyx_sms_enabled = False
        mock_send_alert.return_value = True
        _self.notify_owner("Test", "body", idempotency_key="test:1")
        time.sleep(0.2)  # background thread
        assert mock_send_alert.called, "email fallback must fire when SMS is not configured"

    # Telnyx enabled but the send raises → must still fall back to email, not raise.
    with patch.object(_self, "get_settings") as mock_settings, \
         patch.object(_self, "send_message") as mock_send_message, \
         patch.object(_self, "send_alert") as mock_send_alert, \
         patch.object(_self, "_claim_alert", return_value=1), \
         patch.object(_self, "_update_status"):
        mock_settings.return_value.founder_phone = "+18135551234"
        mock_settings.return_value.telnyx_sms_enabled = True
        mock_send_message.side_effect = TelnyxSMSError("boom")
        mock_send_alert.return_value = True
        _self.notify_owner("Test", "body", idempotency_key="test:2")
        time.sleep(0.2)
        assert mock_send_alert.called, "email fallback must fire when Telnyx send fails"

    # Telnyx enabled and accepts the message (status=queued) → email fallback
    # must NOT fire yet; delivery confirmation/failure decides that later.
    with patch.object(_self, "get_settings") as mock_settings, \
         patch.object(_self, "send_message") as mock_send_message, \
         patch.object(_self, "send_alert") as mock_send_alert, \
         patch.object(_self, "_claim_alert", return_value=1), \
         patch.object(_self, "_update_status"):
        mock_settings.return_value.founder_phone = "+18135551234"
        mock_settings.return_value.telnyx_sms_enabled = True
        mock_send_message.return_value = {"status": "queued", "message_id": "msg_123"}
        _self.notify_owner("Test", "body", idempotency_key="test:3")
        time.sleep(0.2)
        assert not mock_send_alert.called, "email fallback must NOT fire on a merely-queued SMS"

    # Duplicate idempotency_key → claim is skipped, no dispatch thread spawned.
    with patch.object(_self, "_claim_alert", return_value=None) as mock_claim, \
         patch.object(_self, "_dispatch_alert") as mock_dispatch:
        _self.notify_owner("Test", "body", idempotency_key="test:4")
        assert mock_claim.called
        assert not mock_dispatch.called, "dispatch must not run when the alert was already claimed"

    print("owner_alert self-check passed")
