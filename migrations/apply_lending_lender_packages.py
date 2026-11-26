"""R1 1.7: lending.lender_packages + quarantine columns on both upload tables.

Idempotent. Run before deploying R1 1.7 code.

Tables / columns created:
  lending.lender_packages           — one draft package per file + secondary lender
  lending.partner_file_uploads      — quarantine_status, quarantine_reason, item_code (new columns)
  lending.doc_chase_uploads         — quarantine_status, quarantine_reason (new columns)
"""
from __future__ import annotations

import os
import sys

import psycopg2

DATABASE_URL = os.environ.get("DATABASE_URL", "")


def _conn():
    return psycopg2.connect(DATABASE_URL)


DDL = """
-- quarantine columns on partner_file_uploads
ALTER TABLE lending.partner_file_uploads
    ADD COLUMN IF NOT EXISTS quarantine_status  VARCHAR(20),
    ADD COLUMN IF NOT EXISTS quarantine_reason  VARCHAR(300),
    ADD COLUMN IF NOT EXISTS item_code          VARCHAR(40);

-- quarantine columns on doc_chase_uploads
ALTER TABLE lending.doc_chase_uploads
    ADD COLUMN IF NOT EXISTS quarantine_status  VARCHAR(20),
    ADD COLUMN IF NOT EXISTS quarantine_reason  VARCHAR(300);

-- lender_packages table
CREATE TABLE IF NOT EXISTS lending.lender_packages (
    id              BIGSERIAL PRIMARY KEY,
    source_type     VARCHAR(20)  NOT NULL CHECK (source_type IN ('loan_file','lf_lead')),
    source_id       BIGINT       NOT NULL,
    lender_key      VARCHAR(60)  NOT NULL,
    status          VARCHAR(20)  NOT NULL DEFAULT 'draft'
                        CHECK (status IN ('draft','blocked','ready','submitted','dismissed')),
    box_confirmed   BOOLEAN      NOT NULL DEFAULT false,
    fit_result      JSONB,
    checklist       JSONB,
    validator_verdict JSONB,
    screen_fingerprint VARCHAR(64),
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_lending_lender_packages_source
    ON lending.lender_packages (source_type, source_id);

-- at most one open package per file + lender (open = not submitted/dismissed)
CREATE UNIQUE INDEX IF NOT EXISTS uq_lending_lender_packages_open
    ON lending.lender_packages (source_type, source_id, lender_key)
    WHERE status NOT IN ('submitted', 'dismissed');
"""


def run() -> None:
    if not DATABASE_URL:
        print("DATABASE_URL not set", file=sys.stderr)
        sys.exit(1)
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(DDL)
        conn.commit()
    print("apply_lending_lender_packages: done")


if __name__ == "__main__":
    run()
