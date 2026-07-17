"""
Create owner_alert_dispatch — idempotency + delivery-tracking table for
notify_owner() (speed-to-lead founder alerts from Stripe checkout and
Synthflow demo-request webhooks).

Claims an alert_key (e.g. "stripe:<event_id>", "synthflow:<call_id>") via a
unique-constraint insert, mirroring the existing dedupe pattern in
stripe_webhooks.py (insert, catch IntegrityError, treat as an idempotent
retry). Also tracks the Telnyx message_id + delivery status so a "queued"
send (accepted, not carrier-delivered) can still trigger the email fallback
once the delivery-status webhook or the sweep task (src/tasks/
owner_alert_sweep.py) confirms it never landed.

Idempotent — CREATE TABLE IF NOT EXISTS / CREATE INDEX IF NOT EXISTS.

Usage:
    PYTHONPATH=. python migrations/apply_owner_alert_dispatch.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    """
    CREATE TABLE IF NOT EXISTS owner_alert_dispatch (
        id SERIAL PRIMARY KEY,
        alert_key VARCHAR(120) NOT NULL UNIQUE,
        subject VARCHAR(200) NOT NULL,
        body TEXT NOT NULL,
        telnyx_message_id VARCHAR(80),
        status VARCHAR(20) NOT NULL DEFAULT 'pending'
            CHECK (status IN ('pending','sms_sent','sms_delivered','sms_failed','email_sent')),
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_oad_telnyx_message_id ON owner_alert_dispatch (telnyx_message_id)",
    "CREATE INDEX IF NOT EXISTS idx_oad_status_created ON owner_alert_dispatch (status, created_at)",
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt))

    logger.info("owner_alert_dispatch table ready.")


if __name__ == "__main__":
    main()
