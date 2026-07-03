"""fa069 — add parcel_id to legal_and_liens match_method constraint

Expands the check_legal_match_method CHECK constraint on legal_and_liens
to allow match_method = 'parcel_id', enabling the lien and judgment loader
to record direct folio matches extracted from the Legal field.

Revision ID: fa069_parcel_id_match_method
Revises: fa068
Create Date: 2026-06-04
"""
from typing import Sequence, Union
from alembic import op

revision: str = 'fa069_parcel_id_match_method'
down_revision: Union[str, Sequence[str], None] = 'fa068'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_constraint('check_legal_match_method', 'legal_and_liens', type_='check')
    op.create_check_constraint(
        'check_legal_match_method',
        'legal_and_liens',
        "match_method IN ('parcel_id', 'legal_desc', 'owner_name', 'llm_verified', 'address', 'manual')",
    )


def downgrade() -> None:
    op.drop_constraint('check_legal_match_method', 'legal_and_liens', type_='check')
    op.create_check_constraint(
        'check_legal_match_method',
        'legal_and_liens',
        "match_method IN ('legal_desc', 'owner_name', 'llm_verified', 'address', 'manual')",
    )
