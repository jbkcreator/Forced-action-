"""populate pinellas ori fields

Revision ID: 4b5c6d7e8f9a
Revises: 369c1a7319a8
Create Date: 2026-05-08

Sets ori_column_map, ori_book_page_col, and ori_doc_type_map on the
Pinellas `liens` source row. Without these, lien_engine cannot rename
Pinellas ORI export columns (DirectName→Grantor etc.) and won't write
any rows.
"""
from typing import Sequence, Union
import json

from alembic import op
import sqlalchemy as sa


revision: str = "4b5c6d7e8f9a"
down_revision: Union[str, Sequence[str], None] = "369c1a7319a8"
branch_labels = None
depends_on = None


PINELLAS_ORI_COLUMN_MAP = {
    "DirectName":         "Grantor",
    "IndirectName":       "Grantee",
    "InstrumentNumber":   "Instrument",
    "Comments":           "Legal",
    "DocTypeDescription": "DocType",
}

PINELLAS_ORI_DOC_TYPE_MAP = {
    "JUDGEMENT LIEN":                              "JUDGMENT LIEN",
    "JUDGEMENT":                                   "JUDGMENT",
    "LIEN":                                        "LIEN",
    "LIEN (IRS)":                                  "TAX LIEN",
    "DOMESTIC RELATIONS JUDGMENT":                 "JUDGMENT",
    "PROBATE":                                     "PROBATE",
    "PROBATE REAL PROPERTY":                       "PROBATE",
    "CERTIFIED COPY OF A COURT JUDGMENT OR ORDER": "JUDGMENT",
    "CORPORATE LIEN":                              "LIEN",
    "FINANCING STATEMENT":                         "LIEN",
    "LIS PENDENS":                                 "LIS PENDENS",
    "DEED":                                        "DEED",
}


def upgrade() -> None:
    op.execute(
        sa.text(
            """
            UPDATE county_sources
            SET ori_column_map    = CAST(:col_map AS jsonb),
                ori_book_page_col = :bp_col,
                ori_doc_type_map  = CAST(:doc_map AS jsonb)
            WHERE county_id = 'pinellas' AND signal_type = 'liens'
            """
        ).bindparams(
            col_map=json.dumps(PINELLAS_ORI_COLUMN_MAP),
            bp_col="BookPage",
            doc_map=json.dumps(PINELLAS_ORI_DOC_TYPE_MAP),
        )
    )


def downgrade() -> None:
    op.execute(
        """
        UPDATE county_sources
        SET ori_column_map    = NULL,
            ori_book_page_col = NULL,
            ori_doc_type_map  = NULL
        WHERE county_id = 'pinellas' AND signal_type = 'liens'
        """
    )
