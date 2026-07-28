"""Auto-converted from alembic migration `fa095_feedback_ritual_shared_queue_refactor` (revision fa095_feedback_ritual_shared_queue_refactor).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa095_feedback_ritual_shared_queue_refactor.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE lifecycle_training_overrides ALTER COLUMN subject_id TYPE VARCHAR(80) USING subject_id::varchar;

ALTER TABLE lifecycle_training_overrides RENAME subject_id TO subject_ref;

ALTER TABLE lifecycle_training_overrides ALTER COLUMN correction_reason DROP NOT NULL;

ALTER TABLE lifecycle_training_overrides ADD COLUMN corrected_output TEXT;

ALTER TABLE lifecycle_training_overrides ADD COLUMN review_outcome VARCHAR(30);

ALTER TABLE lifecycle_training_overrides ADD COLUMN reviewed_by VARCHAR(120);

ALTER TABLE lifecycle_training_overrides ADD COLUMN reviewed_at TIMESTAMP WITH TIME ZONE;

ALTER TABLE lifecycle_training_overrides ADD COLUMN snapshot_payload JSONB;

ALTER TABLE lifecycle_training_overrides ADD COLUMN source_metadata JSONB;

DROP INDEX IF EXISTS uq_lifecycle_override_active;

DROP INDEX idx_lifecycle_overrides_subject_active;

CREATE INDEX idx_lifecycle_overrides_subject_active ON lifecycle_training_overrides (subject_type, subject_ref, dampener_active);

CREATE UNIQUE INDEX uq_lifecycle_override_active
        ON lifecycle_training_overrides (
            subject_ref,
            correction_reason,
            COALESCE(signal_type, '')
        )
        WHERE dampener_active
          AND subject_type = 'property'
          AND correction_reason IS NOT NULL;

CREATE UNIQUE INDEX uq_lifecycle_feedback_ritual_subject
        ON lifecycle_training_overrides (subject_type, subject_ref)
        WHERE source = 'feedback_ritual';

ALTER TABLE lifecycle_training_overrides ADD CONSTRAINT ck_lifecycle_overrides_review_outcome CHECK (review_outcome IS NULL OR review_outcome IN ('approved', 'needs_correction', 'discarded'));
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa095_feedback_ritual_shared_queue_refactor")


if __name__ == "__main__":
    main()
