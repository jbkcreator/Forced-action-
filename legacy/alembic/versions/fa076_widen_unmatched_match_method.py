"""fa076 — widen unmatched_records.check_unmatched_match_method

The loader match-method taxonomy (src/loaders/base.py) was expanded to the
granular cascade stages — normalized_address, owner_name_zip, owner_name_city —
but the CHECK constraint on unmatched_records still only permitted the original
four values. Any record whose best match attempt reached one of the new stages
failed quarantine with a CheckViolation and was silently dropped instead of
being parked in unmatched_records for later re-matching.

This expands the allowed set to cover every non-LLM method the cascade can
emit. 'llm_verified' is intentionally NOT included — LLM-promoted matches clear
the destination-table threshold and never reach quarantine, and the LLM tiebreak
is not part of the unmatched path.

'address' is retained because existing rows already carry it (legacy value
predating the normalized_address rename); dropping it would fail constraint
validation against existing data.

NOTE: the alembic CLI is unusable in this tree (divergent multi-head history —
the DB is stamped at a revision whose file lives on another branch). Apply this
change on the server with the idempotent companion script instead:

    PYTHONPATH=. python scripts/apply_fa076_ddl.py

This file is the schema-of-record; the script performs the same DDL.

Revision ID: fa076_widen_unmatched_match_method
Revises: fa069_parcel_id_match_method
Create Date: 2026-06-10
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = 'fa076_widen_unmatched_match_method'
down_revision: Union[str, Sequence[str], None] = 'fa069_parcel_id_match_method'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_ALLOWED = (
    "'address', 'normalized_address', "
    "'owner_name', 'owner_name_zip', 'owner_name_city', "
    "'legal_desc', 'parcel_id'"
)


def upgrade() -> None:
    op.execute(sa.text(
        "ALTER TABLE unmatched_records "
        "DROP CONSTRAINT IF EXISTS check_unmatched_match_method"
    ))
    op.create_check_constraint(
        "check_unmatched_match_method", "unmatched_records",
        f"match_method IN ({_ALLOWED}) OR match_method IS NULL",
    )


def downgrade() -> None:
    op.execute(sa.text(
        "ALTER TABLE unmatched_records "
        "DROP CONSTRAINT IF EXISTS check_unmatched_match_method"
    ))
    op.create_check_constraint(
        "check_unmatched_match_method", "unmatched_records",
        "match_method IN ('address','owner_name','legal_desc','parcel_id') "
        "OR match_method IS NULL",
    )
