"""Apply fa094_a6_cora_training_overrides migration.

Usage:
    python scripts/apply_fa094_cora_training_overrides.py

Alembic CLI is unusable (multi-head tree).  This script stamps and applies
the migration directly via SQLAlchemy, per project convention.

Single shared DB — applies once, live everywhere.
"""
from __future__ import annotations

import logging
import sys

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


DDL = """
CREATE TABLE IF NOT EXISTS cora_training_overrides (
    id              BIGSERIAL PRIMARY KEY,
    source          VARCHAR(30)  NOT NULL,
    subject_type    VARCHAR(30)  NOT NULL,
    subject_id      INTEGER      NOT NULL,
    closer_call_id  INTEGER      REFERENCES closer_calls(id) ON DELETE SET NULL,
    correction_reason VARCHAR(40) NOT NULL,
    signal_type     VARCHAR(40),
    note            TEXT,
    dampener_active BOOLEAN      NOT NULL DEFAULT TRUE,
    queue_status    VARCHAR(20)  NOT NULL DEFAULT 'pending',
    created_by      VARCHAR(120) NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT ck_cora_overrides_source
        CHECK (source IN ('closer_teach', 'feedback_ritual')),
    CONSTRAINT ck_cora_overrides_queue_status
        CHECK (queue_status IN ('pending', 'exported', 'discarded'))
);

CREATE INDEX IF NOT EXISTS idx_cora_overrides_subject_active
    ON cora_training_overrides (subject_type, subject_id, dampener_active);

CREATE INDEX IF NOT EXISTS idx_cora_overrides_queue_status
    ON cora_training_overrides (queue_status);

CREATE UNIQUE INDEX IF NOT EXISTS uq_cora_override_active
    ON cora_training_overrides (
        subject_id,
        correction_reason,
        COALESCE(signal_type, '')
    )
    WHERE dampener_active;
"""

STAMP = """
INSERT INTO alembic_version (version_num)
VALUES ('fa094_a6_cora_training_overrides')
ON CONFLICT DO NOTHING;
"""


def main() -> None:
    settings = get_settings()
    engine = create_engine(str(settings.database_url))
    with engine.begin() as conn:
        logger.info("Applying fa094: creating cora_training_overrides table…")
        conn.execute(text(DDL))
        conn.execute(text(STAMP))
        logger.info("Done — cora_training_overrides created and alembic_version stamped.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        logger.error("Migration failed: %s", exc)
        sys.exit(1)
