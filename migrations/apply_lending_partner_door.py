"""Partner Door (P-1): ``lending.partners``, the shared ``lending.loan_files``, and the partner
evidence tables ``lending.partner_submissions`` / ``lending.partner_file_uploads``.

``loan_files`` holds columns any source can fill; the router (R-1) adds its routing columns and
LendingFlow leads are linked in separately. Models: ``LendingPartner``, ``LendingLoanFile``,
``LendingPartnerSubmission``, ``LendingPartnerFileUpload`` in ``src/lending/models.py``. Idempotent.

Usage:
    PYTHONPATH=. python migrations/apply_lending_partner_door.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from config.lending_partners import AGREEMENT_STATUSES, FILE_STATUSES, PARTNER_TYPES
from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _in(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{value}'" for value in values)


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> None:
    partners, files = f'"{schema}".partners', f'"{schema}".loan_files'
    submissions, uploads = f'"{schema}".partner_submissions', f'"{schema}".partner_file_uploads'
    for ddl in (
        f'CREATE SCHEMA IF NOT EXISTS "{schema}"',
        f"""CREATE TABLE IF NOT EXISTS {partners} (
            id               bigserial PRIMARY KEY,
            name             varchar(120) NOT NULL,
            company          varchar(160),
            partner_type     varchar(20)  NOT NULL CONSTRAINT ck_lending_partners_type CHECK (partner_type IN ({_in(PARTNER_TYPES)})),
            phone            varchar(20),
            email            varchar(255),
            states           varchar(200),
            agreement_status varchar(20)  NOT NULL DEFAULT 'pending'
                             CONSTRAINT ck_lending_partners_agreement CHECK (agreement_status IN ({_in(AGREEMENT_STATUSES)})),
            referral_terms   text,
            sms_consent      boolean      NOT NULL DEFAULT false,
            active           boolean      NOT NULL DEFAULT true,
            created_by       varchar(80),
            token_hash       varchar(64)  NOT NULL UNIQUE,
            token_revoked_at timestamptz,
            is_test          boolean      NOT NULL DEFAULT false,
            created_at       timestamptz  NOT NULL DEFAULT now(),
            updated_at       timestamptz  NOT NULL DEFAULT now()
        )""",
        f"""CREATE TABLE IF NOT EXISTS {files} (
            id                  bigserial PRIMARY KEY,
            source              varchar(20)  NOT NULL,
            partner_id          bigint REFERENCES {partners} (id),
            borrower_first_name varchar(80)  NOT NULL,
            borrower_last_name  varchar(80)  NOT NULL,
            borrower_phone      varchar(20),
            borrower_email      varchar(255),
            product             varchar(20)  NOT NULL,
            loan_amount         bigint       NOT NULL,
            property_address    varchar(200),
            property_city       varchar(80),
            property_state      varchar(2)   NOT NULL,
            property_zip        varchar(10),
            purchase_price      bigint,
            rehab_budget        bigint,
            arv                 bigint,
            completed_projects  integer,
            credit_band         varchar(20),
            timeline            varchar(40),
            occupancy           varchar(20) NOT NULL DEFAULT 'unknown',
            notes               text,
            status              varchar(20)  NOT NULL DEFAULT 'received'
                                CONSTRAINT ck_lending_loan_files_status CHECK (status IN ({_in(FILE_STATUSES)})),
            status_reason       varchar(200),
            status_at           timestamptz  NOT NULL DEFAULT now(),
            lender_fit          jsonb,
            prequal_pdf_path    varchar(300),
            funded_at           timestamptz,
            notice_status       varchar(20),
            notice_emailed_at   timestamptz,
            notice_texted_at    timestamptz,
            notice_failed_at    timestamptz,
            is_test             boolean      NOT NULL DEFAULT false,
            created_at          timestamptz  NOT NULL DEFAULT now(),
            updated_at          timestamptz  NOT NULL DEFAULT now()
        )""",
        f"CREATE INDEX IF NOT EXISTS idx_lending_loan_files_partner ON {files} (partner_id, created_at)",
        f"CREATE INDEX IF NOT EXISTS idx_lending_loan_files_status ON {files} (status, status_at)",
        # Chunk 1 Addendum 1.1.2; create never alters an existing table
        f"ALTER TABLE {files} ADD COLUMN IF NOT EXISTS occupancy varchar(20) NOT NULL DEFAULT 'unknown'",
        f"""CREATE TABLE IF NOT EXISTS {submissions} (
            id                   bigserial PRIMARY KEY,
            file_id              bigint       NOT NULL UNIQUE REFERENCES {files} (id),
            partner_id           bigint       NOT NULL REFERENCES {partners} (id),
            consent_checked      boolean      NOT NULL,
            consent_text         text         NOT NULL,
            consented_at         timestamptz  NOT NULL,
            ip_address           varchar(45),
            page_url             varchar(300),
            user_agent           varchar(300),
            partner_confirmed_at timestamptz,
            partner_emailed_at   timestamptz,
            partner_texted_at    timestamptz,
            josh_notified_at     timestamptz,
            prequal_sent_at      timestamptz,
            confirm_failed_at    timestamptz,
            prequal_withheld_at  timestamptz,
            borrower_notified    boolean      NOT NULL DEFAULT false,
            borrower_consent_at  timestamptz,
            created_at           timestamptz  NOT NULL DEFAULT now()
        )""",
        f"""CREATE TABLE IF NOT EXISTS {uploads} (
            id            bigserial PRIMARY KEY,
            file_id       bigint       NOT NULL REFERENCES {files} (id),
            original_name varchar(200) NOT NULL,
            stored_path   varchar(300) NOT NULL,
            content_type  varchar(80)  NOT NULL,
            size_bytes    integer      NOT NULL,
            created_at    timestamptz  NOT NULL DEFAULT now()
        )""",
        f"CREATE INDEX IF NOT EXISTS idx_lending_partner_file_uploads_file ON {uploads} (file_id)",
    ):
        conn.execute(text(ddl))


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn, schema)
    logger.info("apply_lending_partner_door complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
