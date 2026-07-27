"""Apply fa095 - feedback ritual shared queue refactor.

Companion to alembic/versions/fa095_feedback_ritual_shared_queue_refactor.py.
The Alembic CLI is unusable in this repo's multi-head tree, so this script
applies the same DDL directly.

Idempotent:
  - only renames `subject_id` when `subject_ref` is absent
  - only adds new columns/constraints/indexes when missing
  - safe to rerun

Usage:
    PYTHONPATH=. python scripts/apply_fa095_feedback_ritual_queue_refactor.py
"""
from __future__ import annotations

import logging
import sys

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    """
    DO $$
    BEGIN
        IF EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_name = 'lifecycle_training_overrides'
              AND column_name = 'subject_id'
        ) AND NOT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_name = 'lifecycle_training_overrides'
              AND column_name = 'subject_ref'
        ) THEN
            ALTER TABLE lifecycle_training_overrides RENAME COLUMN subject_id TO subject_ref;
        END IF;
    END $$;
    """,
    """
    DO $$
    BEGIN
        IF EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_name = 'lifecycle_training_overrides'
              AND column_name = 'subject_ref'
              AND data_type <> 'character varying'
        ) THEN
            ALTER TABLE lifecycle_training_overrides
            ALTER COLUMN subject_ref TYPE VARCHAR(80) USING subject_ref::varchar;
        END IF;
    END $$;
    """,
    """
    ALTER TABLE lifecycle_training_overrides
    ALTER COLUMN correction_reason DROP NOT NULL
    """,
    """
    ALTER TABLE lifecycle_training_overrides
    ADD COLUMN IF NOT EXISTS corrected_output TEXT
    """,
    """
    ALTER TABLE lifecycle_training_overrides
    ADD COLUMN IF NOT EXISTS review_outcome VARCHAR(30)
    """,
    """
    ALTER TABLE lifecycle_training_overrides
    ADD COLUMN IF NOT EXISTS reviewed_by VARCHAR(120)
    """,
    """
    ALTER TABLE lifecycle_training_overrides
    ADD COLUMN IF NOT EXISTS reviewed_at TIMESTAMPTZ
    """,
    """
    ALTER TABLE lifecycle_training_overrides
    ADD COLUMN IF NOT EXISTS snapshot_payload JSONB
    """,
    """
    ALTER TABLE lifecycle_training_overrides
    ADD COLUMN IF NOT EXISTS source_metadata JSONB
    """,
    "DROP INDEX IF EXISTS uq_lifecycle_feedback_ritual_subject",
    "DROP INDEX IF EXISTS uq_lifecycle_override_active",
    "DROP INDEX IF EXISTS idx_lifecycle_overrides_subject_active",
    """
    CREATE INDEX IF NOT EXISTS idx_lifecycle_overrides_subject_active
        ON lifecycle_training_overrides (subject_type, subject_ref, dampener_active)
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS uq_lifecycle_override_active
        ON lifecycle_training_overrides (
            subject_ref,
            correction_reason,
            COALESCE(signal_type, '')
        )
        WHERE dampener_active
          AND subject_type = 'property'
          AND correction_reason IS NOT NULL
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS uq_lifecycle_feedback_ritual_subject
        ON lifecycle_training_overrides (subject_type, subject_ref)
        WHERE source = 'feedback_ritual'
    """,
    """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conname = 'ck_lifecycle_overrides_review_outcome'
        ) THEN
            ALTER TABLE lifecycle_training_overrides
            ADD CONSTRAINT ck_lifecycle_overrides_review_outcome
            CHECK (
                review_outcome IS NULL
                OR review_outcome IN ('approved', 'needs_correction', 'discarded')
            );
        END IF;
    END $$;
    """,
]

STAMP = """
INSERT INTO alembic_version (version_num)
VALUES ('fa095_feedback_ritual_shared_queue_refactor')
ON CONFLICT DO NOTHING;
"""


def main() -> None:
    settings = get_settings()
    engine = create_engine(str(settings.database_url))
    with engine.begin() as conn:
        logger.info("Applying fa095: refactoring lifecycle_training_overrides for feedback ritual...")
        for stmt in DDL:
            conn.execute(text(stmt))
            logger.info("OK: %s", " ".join(stmt.split())[:100])
        conn.execute(text(STAMP))
        logger.info("Done - fa095 shared queue refactor applied and alembic_version stamped.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        logger.error("Migration failed: %s", exc)
        sys.exit(1)
