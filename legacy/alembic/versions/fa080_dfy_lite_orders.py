"""dfy_lite_orders — DFY-Lite pitch generation table (S3b)

Revision ID: fa080_dfy_lite_orders
Revises: (none — standalone branch; multiple active heads exist in this project)

Idempotent: uses CREATE TABLE IF NOT EXISTS + CREATE INDEX IF NOT EXISTS.
Apply via: alembic upgrade fa080_dfy_lite_orders
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "fa080_dfy_lite_orders"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(sa.text("""
        CREATE TABLE IF NOT EXISTS dfy_lite_orders (
            id                      SERIAL PRIMARY KEY,
            subscriber_id           INTEGER NOT NULL REFERENCES subscribers(id),
            property_id             INTEGER NOT NULL REFERENCES properties(id),
            sent_lead_id            INTEGER,
            source_lead_purchase_id INTEGER,
            status                  VARCHAR(30) NOT NULL DEFAULT 'Order_Received'
                CONSTRAINT ck_dfy_lite_status CHECK (status IN (
                    'Order_Received', 'Signal_Compiled', 'Pitch_Generated',
                    'Needs_Review', 'Delivered', 'Signal_Failed', 'Pitch_Failed', 'Cancelled'
                )),
            pitch_type              VARCHAR(50) NOT NULL,
            offer_angle             VARCHAR(50),
            target_vertical         VARCHAR(50) NOT NULL,
            selected_output_formats JSONB       NOT NULL DEFAULT '[]',
            custom_instructions     TEXT,
            distress_stack_json     JSONB,
            property_snapshot_json  JSONB,
            generated_outputs_json  JSONB,
            pitch_generation_number INTEGER NOT NULL DEFAULT 1,
            pitch_generation_limit  INTEGER NOT NULL DEFAULT 3,
            generated_by            VARCHAR(30) NOT NULL DEFAULT 'claude',
            reviewed_at             TIMESTAMPTZ,
            delivered_at            TIMESTAMPTZ,
            error_reason            TEXT,
            created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """))

    op.execute(sa.text("CREATE INDEX IF NOT EXISTS idx_dfy_lite_sub_id   ON dfy_lite_orders (subscriber_id)"))
    op.execute(sa.text("CREATE INDEX IF NOT EXISTS idx_dfy_lite_prop_id  ON dfy_lite_orders (property_id)"))
    op.execute(sa.text("CREATE INDEX IF NOT EXISTS idx_dfy_lite_status   ON dfy_lite_orders (status)"))
    op.execute(sa.text("CREATE INDEX IF NOT EXISTS idx_dfy_lite_created_at ON dfy_lite_orders (created_at)"))
    op.execute(sa.text("CREATE INDEX IF NOT EXISTS idx_dfy_lite_sub_prop ON dfy_lite_orders (subscriber_id, property_id)"))
    op.execute(sa.text("CREATE INDEX IF NOT EXISTS idx_dfy_lite_sent_lead ON dfy_lite_orders (sent_lead_id)"))


def downgrade() -> None:
    op.execute(sa.text("DROP TABLE IF EXISTS dfy_lite_orders"))
