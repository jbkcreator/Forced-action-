"""
Provision outbound_drafts — Cora's real draft store, replacing the interim
append-only JSON-Lines file (src/agents/cora/store.py) now that the
Cora->Lifecycle rename this build avoided colliding with has merged and its
own DB migration has run.

One row per cold-outreach draft, always send-free. Status transitions are
plain UPDATEs (draft_id is a real primary key) — the old file store's
"append a new line per transition" pattern existed only because a flat
file can't do row-level UPDATEs; nothing in the app layer ever needed the
full transition history, only the current state per draft_id.

    PYTHONPATH=. python migrations/apply_outbound_draft.py
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS outbound_drafts (
    draft_id               VARCHAR(36)   PRIMARY KEY,
    opportunity_thread_id  VARCHAR(64)   NOT NULL,
    buyer_entity_id        INTEGER       NOT NULL,
    cell_id                VARCHAR(50)   NOT NULL,
    offer                  VARCHAR(50)   NOT NULL,
    avenue                 VARCHAR(50)   NOT NULL,
    angle                  VARCHAR(50)   NOT NULL,
    subject                TEXT          NOT NULL,
    body                   TEXT          NOT NULL,
    facts_used             JSONB         NOT NULL DEFAULT '[]',
    source_refs            JSONB         NOT NULL DEFAULT '[]',
    recommended_channel    VARCHAR(20)   NOT NULL,
    confidence_score       INTEGER       NOT NULL,
    status                 VARCHAR(30)   NOT NULL DEFAULT 'draft',
    booking_link           TEXT,
    payment_link           TEXT,
    reject_reason          VARCHAR(50),
    created_at             TIMESTAMPTZ   NOT NULL DEFAULT now(),
    schema_version         INTEGER       NOT NULL DEFAULT 1,
    published              BOOLEAN       NOT NULL DEFAULT false,
    is_followup            BOOLEAN       NOT NULL DEFAULT false,
    followup_sequence      INTEGER,
    contact_email          VARCHAR(255),
    contact_phone          VARCHAR(20),
    CONSTRAINT ck_outbound_drafts_status
        CHECK (status IN ('draft', 'rejected', 'expired', 'superseded', 'approved_pending_send'))
)
"""

CREATE_INDEX_THREAD_CELL_STATUS_SQL = (
    "CREATE INDEX IF NOT EXISTS ix_outbound_drafts_thread_cell_status "
    "ON outbound_drafts (opportunity_thread_id, cell_id, status)"
)

CREATE_INDEX_CONTACT_EMAIL_SQL = (
    "CREATE INDEX IF NOT EXISTS ix_outbound_drafts_contact_email "
    "ON outbound_drafts (contact_email)"
)


def main() -> None:
    with get_db_context() as db:
        db.execute(text(CREATE_TABLE_SQL))
        db.execute(text(CREATE_INDEX_THREAD_CELL_STATUS_SQL))
        db.execute(text(CREATE_INDEX_CONTACT_EMAIL_SQL))
        db.commit()
    print("outbound_drafts: table + indexes ready.")


if __name__ == "__main__":
    main()
