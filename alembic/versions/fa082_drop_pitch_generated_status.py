"""Drop Pitch_Generated from dfy_lite_orders status CHECK constraint.

The graph never sets this status — Needs_Review is the post-generation state.
No data migration needed (no rows carry this value).

Revision ID: fa082_drop_pitch_generated_status
Revises: fa081_quora_questions
"""
from typing import Sequence, Union

from alembic import op

revision: str = "fa082_drop_pitch_generated_status"
down_revision: Union[str, Sequence[str]] = "fa081_quora_questions"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE dfy_lite_orders DROP CONSTRAINT ck_dfy_lite_status")
    op.execute("""
        ALTER TABLE dfy_lite_orders ADD CONSTRAINT ck_dfy_lite_status CHECK (
            status IN (
                'Order_Received', 'Signal_Compiled',
                'Needs_Review', 'Delivered',
                'Signal_Failed', 'Pitch_Failed', 'Cancelled'
            )
        )
    """)


def downgrade() -> None:
    op.execute("ALTER TABLE dfy_lite_orders DROP CONSTRAINT ck_dfy_lite_status")
    op.execute("""
        ALTER TABLE dfy_lite_orders ADD CONSTRAINT ck_dfy_lite_status CHECK (
            status IN (
                'Order_Received', 'Signal_Compiled', 'Pitch_Generated',
                'Needs_Review', 'Delivered',
                'Signal_Failed', 'Pitch_Failed', 'Cancelled'
            )
        )
    """)
