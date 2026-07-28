"""Apply fa080 — closer_calls table (Closer Cockpit, Sprint S1b).

Creates the closer_calls table that backs Aircall call capture + tagging +
per-call closer feedback. Deliberately separate from agent_decisions (Lifecycle-only).
See CLOSER_COCKPIT_BACKEND_DESIGN.md.

Idempotent (IF NOT EXISTS throughout). Alembic CLI is unusable on this repo's
multi-head tree, so the DDL is applied here. Usage:

    python scripts/apply_closer_calls_ddl.py
"""
import logging
import sys

from sqlalchemy import text

from src.core.database import Database

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_DDL = [
    """
    CREATE TABLE IF NOT EXISTS closer_calls (
        id                      BIGSERIAL PRIMARY KEY,
        aircall_call_id         VARCHAR(40)  NOT NULL,
        subscriber_id           INTEGER      NOT NULL REFERENCES subscribers(id),
        escalation_id           INTEGER      REFERENCES human_close_escalations(id),
        closer_aircall_user_id  VARCHAR(40),
        closer_name             VARCHAR(120),
        direction               VARCHAR(12),
        dialed_e164             VARCHAR(20),
        duration_sec            INTEGER,
        started_at              TIMESTAMPTZ,
        ended_at                TIMESTAMPTZ,
        transcript_text         TEXT,
        transcript_fetched_at   TIMESTAMPTZ,
        sentiment               VARCHAR(12),
        topics                  JSONB,
        objections              JSONB,
        objection_resolved      VARCHAR(12),
        call_outcome            VARCHAR(30),
        follow_ups              JSONB,
        tagged_at               TIMESTAMPTZ,
        objection_type          VARCHAR(40),
        pitch_variant           VARCHAR(40),
        lead_quality_rating     INTEGER,
        feedback_by             VARCHAR(120),
        feedback_at             TIMESTAMPTZ,
        created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_closer_calls_aircall_id ON closer_calls(aircall_call_id)",
    "CREATE INDEX IF NOT EXISTS idx_closer_calls_subscriber ON closer_calls(subscriber_id)",
    "CREATE INDEX IF NOT EXISTS idx_closer_calls_closer_started ON closer_calls(closer_aircall_user_id, started_at)",
    "CREATE INDEX IF NOT EXISTS idx_closer_calls_tagged_at ON closer_calls(tagged_at)",
    """
    DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='ck_closer_calls_lead_quality') THEN
            ALTER TABLE closer_calls ADD CONSTRAINT ck_closer_calls_lead_quality
                CHECK (lead_quality_rating IS NULL OR (lead_quality_rating BETWEEN 1 AND 5));
        END IF;
    END $$
    """,
    """
    DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='ck_closer_calls_outcome') THEN
            ALTER TABLE closer_calls ADD CONSTRAINT ck_closer_calls_outcome
                CHECK (call_outcome IS NULL OR call_outcome IN
                    ('committed','callback_scheduled','undecided','declined','no_meaningful_conversation'));
        END IF;
    END $$
    """,
    """
    DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='ck_closer_calls_obj_resolved') THEN
            ALTER TABLE closer_calls ADD CONSTRAINT ck_closer_calls_obj_resolved
                CHECK (objection_resolved IS NULL OR objection_resolved IN ('resolved','unresolved','none'));
        END IF;
    END $$
    """,
    """
    DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='ck_closer_calls_sentiment') THEN
            ALTER TABLE closer_calls ADD CONSTRAINT ck_closer_calls_sentiment
                CHECK (sentiment IS NULL OR sentiment IN ('positive','neutral','negative','mixed'));
        END IF;
    END $$
    """,
]


def run() -> None:
    db = Database()
    with db.session_scope() as session:
        for stmt in _DDL:
            session.execute(text(stmt.strip()))
            logger.info("OK: %s", stmt.strip().splitlines()[0][:80])
    logger.info("fa080 closer_calls DDL applied successfully.")


if __name__ == "__main__":
    run()
    sys.exit(0)
