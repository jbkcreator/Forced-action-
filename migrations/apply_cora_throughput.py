"""
Provision THROUGH-v2.2's tables — the founder-facing batch-approval layer
between Cora's cold-outreach drafts (outbound_drafts) and Relay's execution
queue (relay_approval_queue).

cora_draft_batches / cora_batch_items: one Slack-posted batch of drafts and
each draft's individual decision within it. Deliberately separate from
relay_approval_queue.batch_id, which groups rows claimed together by one
execution run — an *execution* batch, not an *approval* batch.

cora_standing_orders: a founder-ratified rule letting future drafts of a
given cell_id auto-approve without a Slack tap (THROUGH-v2.2 T4). Included
in this same migration per the THROUGH-v2.2 plan, even though T4 itself is
built later — it's just a DDL definition, cheaper to provision once.

    PYTHONPATH=. python migrations/apply_cora_throughput.py
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

CREATE_DRAFT_BATCHES_SQL = """
CREATE TABLE IF NOT EXISTS cora_draft_batches (
    batch_id            VARCHAR(36)   PRIMARY KEY,
    created_at          TIMESTAMPTZ   NOT NULL DEFAULT now(),
    slack_message_ts    VARCHAR(32),
    slack_channel       VARCHAR(64),
    status              VARCHAR(20)   NOT NULL DEFAULT 'pending',
    decided_by          VARCHAR(64),
    decided_at          TIMESTAMPTZ,
    CONSTRAINT ck_cora_draft_batches_status
        CHECK (status IN ('pending', 'approved', 'rejected', 'partial', 'expired'))
)
"""

CREATE_BATCH_ITEMS_SQL = """
CREATE TABLE IF NOT EXISTS cora_batch_items (
    id                  BIGSERIAL     PRIMARY KEY,
    batch_id            VARCHAR(36)   NOT NULL REFERENCES cora_draft_batches(batch_id),
    draft_id            VARCHAR(36)   NOT NULL REFERENCES outbound_drafts(draft_id),
    decision            VARCHAR(20)   NOT NULL DEFAULT 'included',
    decided_at          TIMESTAMPTZ,
    created_at          TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT uq_cora_batch_items_batch_draft UNIQUE (batch_id, draft_id),
    CONSTRAINT ck_cora_batch_items_decision
        CHECK (decision IN ('included', 'exception_rejected'))
)
"""

CREATE_STANDING_ORDERS_SQL = """
CREATE TABLE IF NOT EXISTS cora_standing_orders (
    id                  SERIAL        PRIMARY KEY,
    cell_id             VARCHAR(50)   NOT NULL,
    rule_text           TEXT          NOT NULL,
    created_by          VARCHAR(64),
    created_at          TIMESTAMPTZ   NOT NULL DEFAULT now(),
    active              BOOLEAN       NOT NULL DEFAULT true,
    slack_message_ts    VARCHAR(32)
)
"""

ADD_STANDING_ORDERS_SLACK_TS_SQL = (
    "ALTER TABLE cora_standing_orders ADD COLUMN IF NOT EXISTS slack_message_ts VARCHAR(32)"
)

CREATE_INDEX_BATCH_ITEMS_BATCH_SQL = (
    "CREATE INDEX IF NOT EXISTS ix_cora_batch_items_batch_id ON cora_batch_items (batch_id)"
)
CREATE_INDEX_BATCH_ITEMS_DRAFT_SQL = (
    "CREATE INDEX IF NOT EXISTS ix_cora_batch_items_draft_id ON cora_batch_items (draft_id)"
)
CREATE_INDEX_STANDING_ORDERS_CELL_ACTIVE_SQL = (
    "CREATE INDEX IF NOT EXISTS ix_cora_standing_orders_cell_id_active "
    "ON cora_standing_orders (cell_id, active)"
)


def main() -> None:
    with get_db_context() as db:
        db.execute(text(CREATE_DRAFT_BATCHES_SQL))
        db.execute(text(CREATE_BATCH_ITEMS_SQL))
        db.execute(text(CREATE_STANDING_ORDERS_SQL))
        db.execute(text(ADD_STANDING_ORDERS_SLACK_TS_SQL))
        db.execute(text(CREATE_INDEX_BATCH_ITEMS_BATCH_SQL))
        db.execute(text(CREATE_INDEX_BATCH_ITEMS_DRAFT_SQL))
        db.execute(text(CREATE_INDEX_STANDING_ORDERS_CELL_ACTIVE_SQL))
        db.commit()
    print("cora_draft_batches, cora_batch_items, cora_standing_orders: tables + indexes ready.")


if __name__ == "__main__":
    main()
