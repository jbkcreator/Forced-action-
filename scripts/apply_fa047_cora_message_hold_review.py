"""Auto-converted from alembic migration `fa047_cora_message_hold_review` (revision fa047_cora_message_hold_review).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa047_cora_message_hold_review.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE message_outcomes ADD COLUMN send_status VARCHAR(20) DEFAULT 'sent' NOT NULL;

ALTER TABLE message_outcomes ADD COLUMN requires_review BOOLEAN DEFAULT false NOT NULL;

ALTER TABLE message_outcomes ADD COLUMN review_reason VARCHAR(255);

ALTER TABLE message_outcomes ADD COLUMN scheduled_send_at TIMESTAMP WITH TIME ZONE;

ALTER TABLE message_outcomes ADD COLUMN approved_at TIMESTAMP WITH TIME ZONE;

ALTER TABLE message_outcomes ADD COLUMN approved_by VARCHAR(100);

ALTER TABLE message_outcomes ADD COLUMN cancelled_at TIMESTAMP WITH TIME ZONE;

ALTER TABLE message_outcomes ADD COLUMN cancelled_by VARCHAR(100);

ALTER TABLE message_outcomes ADD COLUMN cancel_reason VARCHAR(255);

ALTER TABLE message_outcomes ADD COLUMN decision_id VARCHAR(36);

ALTER TABLE message_outcomes ADD CONSTRAINT check_mo_send_status CHECK (send_status IN ('pending_review','approved','sent','cancelled','failed','expired'));

CREATE INDEX idx_mo_send_status ON message_outcomes (send_status);

CREATE INDEX idx_mo_decision_id ON message_outcomes (decision_id);

CREATE INDEX idx_mo_pending_review_queue ON message_outcomes (created_at DESC, id DESC) WHERE send_status = 'pending_review' AND requires_review = true AND message_type = 'sms' AND cancelled_at IS NULL;

ALTER TABLE sms_send_logs ADD COLUMN message_outcome_id INTEGER;

ALTER TABLE sms_send_logs ADD FOREIGN KEY(message_outcome_id) REFERENCES message_outcomes (id) ON DELETE SET NULL;

CREATE INDEX idx_sml_message_outcome_id ON sms_send_logs (message_outcome_id);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa047_cora_message_hold_review")


if __name__ == "__main__":
    main()
