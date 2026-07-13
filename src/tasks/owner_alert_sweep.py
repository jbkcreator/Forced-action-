"""
Owner alert delivery sweep — safety net for notify_owner() speed-to-lead
alerts whose Telnyx message.finalized callback never arrives (Telnyx outage,
dropped webhook, etc).

Run every 5 minutes via cron:

    */5 * * * * python -m src.tasks.owner_alert_sweep

Finds owner_alert_dispatch rows stuck at status='sms_sent' for longer than
STALE_AFTER_MINUTES with no delivery confirmation, and fires the email
fallback for each — the same fallback owner_alert.reconcile_delivery_status()
would fire on an explicit delivery_failed callback.
"""

import logging
import sys

from sqlalchemy import text

from src.core.database import get_db_context
from src.services.email import send_alert

logger = logging.getLogger(__name__)

STALE_AFTER_MINUTES = 5


def sweep_undelivered_owner_alerts() -> int:
    """Email-fallback any sms_sent alert with no delivery confirmation. Returns count swept."""
    with get_db_context() as db:
        stale = db.execute(
            text(
                "SELECT id, subject, body FROM owner_alert_dispatch "
                "WHERE status = 'sms_sent' "
                "AND updated_at < now() - make_interval(mins => :stale_minutes)"
            ),
            {"stale_minutes": STALE_AFTER_MINUTES},
        ).all()

        swept = 0
        for row in stale:
            try:
                send_alert(subject=row.subject, body=row.body)
            except Exception:
                logger.warning("Owner alert sweep email failed for row=%s", row.id, exc_info=True)
            db.execute(
                text("UPDATE owner_alert_dispatch SET status = 'email_sent' WHERE id = :id"),
                {"id": row.id},
            )
            swept += 1

    if swept:
        logger.info("owner_alert_sweep: emailed %d undelivered alert(s)", swept)
    return swept


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
        stream=sys.stdout,
    )
    sweep_undelivered_owner_alerts()
