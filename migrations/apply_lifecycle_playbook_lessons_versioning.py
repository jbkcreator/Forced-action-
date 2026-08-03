"""Add lesson versioning/confidence/scope to lifecycle_playbook (LEARN-v2.2 Layer 4, Step 11).

Builds on CLONE-v2.2's fleet-widening (migrations/apply_lifecycle_playbook_
fleet_widen.py, agent_domain/entry_kind) rather than a parallel table — see
src.core.models.LifecyclePlaybook's docstring and
src/services/playbook_writer.py's supersede_recommendation()/
mark_contradicted(). All new columns nullable/defaulted so every existing
row and caller is unaffected.

Widens the status CHECK constraint to add 'superseded' and 'contradicted'
— DROP + re-ADD since Postgres has no ALTER CHECK.

Idempotent. Usage:
    PYTHONPATH=. python migrations/apply_lifecycle_playbook_lessons_versioning.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    "ALTER TABLE lifecycle_playbook ADD COLUMN IF NOT EXISTS confidence INTEGER",
    "ALTER TABLE lifecycle_playbook ADD COLUMN IF NOT EXISTS scope JSONB",
    "ALTER TABLE lifecycle_playbook ADD COLUMN IF NOT EXISTS version INTEGER NOT NULL DEFAULT 1",
    "ALTER TABLE lifecycle_playbook ADD COLUMN IF NOT EXISTS superseded_by_id BIGINT "
    "REFERENCES lifecycle_playbook(id) ON DELETE SET NULL",

    # Widen the status check constraint (DROP + re-ADD — Postgres has no ALTER CHECK).
    """
    DO $$
    BEGIN
        IF EXISTS (
            SELECT 1 FROM information_schema.table_constraints
            WHERE table_name = 'lifecycle_playbook' AND constraint_name = 'check_lifecycle_playbook_status'
        ) THEN
            ALTER TABLE lifecycle_playbook DROP CONSTRAINT check_lifecycle_playbook_status;
        END IF;
        ALTER TABLE lifecycle_playbook ADD CONSTRAINT check_lifecycle_playbook_status
            CHECK (status IN ('recommended','adopted','rejected','retired','superseded','contradicted'));
    END $$;
    """,
    """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.table_constraints
            WHERE table_name = 'lifecycle_playbook' AND constraint_name = 'check_lifecycle_playbook_confidence'
        ) THEN
            ALTER TABLE lifecycle_playbook ADD CONSTRAINT check_lifecycle_playbook_confidence
                CHECK (confidence IS NULL OR confidence BETWEEN 0 AND 100);
        END IF;
    END $$;
    """,
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt))

    logger.info("lifecycle_playbook_lessons_versioning migration complete.")


if __name__ == "__main__":
    main()
