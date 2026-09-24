"""migrations/apply_fa_max_wp_t2_6_stage_monitoring.py

WP-T2-6 Stage Monitoring and Document Chase -- durable state schema.

fa_max_file_state tracks Backflip-side stage detail the coarse
fa_max_opportunity_stage_config FSM has no room for (see plan doc,
Assumption 6). fa_max_document_requests is one row per outstanding
document ask, independent chase timers per document since a file can have
several documents outstanding at once with different request dates.

Idempotent -- CREATE TABLE IF NOT EXISTS / safe to re-run, per ADR 0024.
"""
import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logger = logging.getLogger(__name__)


def apply(engine=None):
    if engine is None:
        engine = create_engine(get_settings().database_url)

    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fa_max_file_state (
                id                      BIGSERIAL PRIMARY KEY,
                opportunity_id          UUID NOT NULL UNIQUE
                    REFERENCES fa_max_opportunities(opportunity_id) ON DELETE CASCADE,
                person_id               UUID NOT NULL
                    REFERENCES fa_max_persons(person_id) ON DELETE CASCADE,
                backflip_stage          TEXT NOT NULL DEFAULT 'submitted',
                contact_email           TEXT,
                last_stage_change_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                last_borrower_touch_at  TIMESTAMPTZ,
                expected_next_stage     TEXT,
                stall_flagged_at        TIMESTAMPTZ,
                created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                CONSTRAINT ck_fa_max_file_state_stage CHECK (
                    backflip_stage IN (
                        'submitted', 'under_review', 'conditional_approval',
                        'docs_requested', 'cleared_to_close', 'funded', 'declined'
                    )
                )
            )
        """))

        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_fa_max_file_state_stall
                ON fa_max_file_state (last_stage_change_at)
                WHERE backflip_stage NOT IN ('funded', 'declined')
        """))

        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_fa_max_file_state_touch
                ON fa_max_file_state (last_borrower_touch_at)
                WHERE backflip_stage NOT IN ('funded', 'declined')
        """))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fa_max_document_requests (
                id                       BIGSERIAL PRIMARY KEY,
                opportunity_id           UUID NOT NULL
                    REFERENCES fa_max_opportunities(opportunity_id) ON DELETE CASCADE,
                person_id                UUID NOT NULL
                    REFERENCES fa_max_persons(person_id) ON DELETE CASCADE,
                document_name            TEXT NOT NULL,
                requested_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                received_at              TIMESTAMPTZ,
                first_chase_sent_at      TIMESTAMPTZ,
                followup_chase_sent_at   TIMESTAMPTZ,
                escalated_at             TIMESTAMPTZ,
                source                   TEXT NOT NULL,
                idempotency_key          TEXT NOT NULL,
                created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                CONSTRAINT ck_fa_max_doc_request_source CHECK (source IN ('email_parsed', 'manual')),
                CONSTRAINT uq_fa_max_doc_request_idempotency UNIQUE (idempotency_key)
            )
        """))

        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_fa_max_doc_requests_outstanding
                ON fa_max_document_requests (opportunity_id)
                WHERE received_at IS NULL
        """))

        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_fa_max_doc_requests_chase_due
                ON fa_max_document_requests (first_chase_sent_at)
                WHERE received_at IS NULL AND followup_chase_sent_at IS NULL
        """))

        logger.info("fa_max_file_state and fa_max_document_requests applied")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    apply()
    print("Done.")
