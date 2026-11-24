"""R1 item 0.1 — loan state machine tables and columns.

Adds to ``lending.loan_files``: ``stage`` (the 8-stage lifecycle, source of truth), ``closed_reason`` / ``closed_at``,
``borrower_id`` / ``property_id`` / ``entity_id``, ``payoff_balance`` / ``outstanding_balance``, ``schema_version``;
makes ``loan_amount`` and ``property_state`` nullable (unknown stays NULL, never 0).  Creates ``borrower_profiles``,
``properties``, ``entities``, ``lender_submissions``, ``loan_stage_events`` (unique ``(source, source_event_id)``) and
``loan_payments`` (unique ``(source, payment_id)``).  ``text_consents`` gets ``loan_file_id`` + ``schema_version`` and
``partner_submissions`` gets ``schema_version`` (ConsentRecord and PartnerSubmission reuse those tables).

Existing files get a stage from their status once: received->INTAKE, held->FIT, routed->SUBMITTED,
approved->ACCEPTED, funded->FUNDED.  A declined file is closed (``closed_reason`` from its status reason when that is
a known code, else ``other``) and keeps the highest stage its status history reached.  ``status`` is left as it is.

Idempotent; run after apply_lending_partner_door.py and apply_lending_loan_file_status_events.py.
RUN BEFORE deploying the code: the stage endpoint and ``ingest_loan_file`` write the new columns.

Usage:
    PYTHONPATH=. python migrations/apply_lending_loan_state_machine.py
"""
from __future__ import annotations

import logging

from sqlalchemy import Table, create_engine, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.schema import CreateIndex

from config.settings import get_settings
from src.lending.contracts import DeclineReason, LoanStage
from src.lending.models import (
    LENDING_SCHEMA,
    LendingBorrowerProfile,
    LendingEntity,
    LendingLenderSubmission,
    LendingLoanPayment,
    LendingLoanStageEvent,
    LendingProperty,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# loan_files points at the first three, so they are created first; the rest point at loan_files.
TABLES = (LendingBorrowerProfile, LendingProperty, LendingEntity, LendingLenderSubmission, LendingLoanStageEvent,
          LendingLoanPayment)

# Columns this migration adds to tables other migrations create.  The drift tests that compare a table's CREATE
# statement with its model add these back (tests/lending/test_partner_door.py).
ADDED_COLUMNS: dict[str, frozenset[str]] = {
    "loan_files": frozenset({"stage", "closed_reason", "closed_at", "borrower_id", "property_id", "entity_id",
                             "payoff_balance", "outstanding_balance", "schema_version", "ghl_opportunity_id"}),
    "partner_submissions": frozenset({"schema_version"}),
    "text_consents": frozenset({"loan_file_id", "schema_version"}),
}

_STAGES = ", ".join(f"'{s.value}'" for s in LoanStage)
_REASONS = ", ".join(f"'{r.value}'" for r in DeclineReason)

_BACKFILL_STAGE = """
UPDATE "{s}".loan_files
   SET stage = CASE status WHEN 'held' THEN 'FIT' WHEN 'routed' THEN 'SUBMITTED'
                           WHEN 'approved' THEN 'ACCEPTED' WHEN 'funded' THEN 'FUNDED' ELSE 'INTAKE' END
 WHERE stage = 'INTAKE' AND status IN ('held', 'routed', 'approved', 'funded')
"""

# A declined file: closed, and its stage is the highest one its status history reached (INTAKE when it has none).
_BACKFILL_DECLINED = """
WITH reached AS (
    SELECT f.id,
           COALESCE(MAX(CASE e.status WHEN 'held' THEN 2 WHEN 'routed' THEN 3 WHEN 'approved' THEN 4
                                      WHEN 'funded' THEN 7 ELSE 1 END), 1) AS rank
      FROM "{s}".loan_files f
      LEFT JOIN "{s}".loan_file_status_events e ON e.loan_file_id = f.id AND e.status <> 'declined'
     WHERE f.status = 'declined' AND f.closed_at IS NULL
     GROUP BY f.id
)
UPDATE "{s}".loan_files f
   SET stage = (ARRAY[{stages}])[r.rank],
       closed_reason = CASE WHEN f.status_reason IN ({reasons}) THEN f.status_reason ELSE 'other' END,
       closed_at = f.status_at
  FROM reached r
 WHERE r.id = f.id
"""


_CONSENT_FK = """
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint c
          JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY (c.conkey)
         WHERE c.conrelid = '"{s}".text_consents'::regclass AND c.contype = 'f' AND a.attname = 'loan_file_id'
    ) THEN
        ALTER TABLE "{s}".text_consents
            ADD CONSTRAINT fk_text_consents_loan_file FOREIGN KEY (loan_file_id) REFERENCES "{s}".loan_files (id);
    END IF;
END
$$
"""


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> None:
    translated = conn.execution_options(schema_translate_map={LENDING_SCHEMA: schema})
    # loan_files must exist first: run after apply_lending_partner_door.py.
    for model in TABLES[:3]:
        model.__table__.create(translated, checkfirst=True)

    conn.execute(text(f'ALTER TABLE "{schema}".loan_files ALTER COLUMN loan_amount DROP NOT NULL'))
    conn.execute(text(f'ALTER TABLE "{schema}".loan_files ALTER COLUMN property_state DROP NOT NULL'))
    conn.execute(text(
        f'ALTER TABLE "{schema}".loan_files '
        "ADD COLUMN IF NOT EXISTS stage varchar(24) NOT NULL DEFAULT 'INTAKE', "
        "ADD COLUMN IF NOT EXISTS closed_reason varchar(40), "
        "ADD COLUMN IF NOT EXISTS closed_at timestamptz, "
        f'ADD COLUMN IF NOT EXISTS borrower_id bigint REFERENCES "{schema}".borrower_profiles(id), '
        f'ADD COLUMN IF NOT EXISTS property_id bigint REFERENCES "{schema}".properties(id), '
        f'ADD COLUMN IF NOT EXISTS entity_id bigint REFERENCES "{schema}".entities(id), '
        "ADD COLUMN IF NOT EXISTS payoff_balance bigint, "
        "ADD COLUMN IF NOT EXISTS outstanding_balance bigint, "
        "ADD COLUMN IF NOT EXISTS schema_version smallint NOT NULL DEFAULT 1, "
        "ADD COLUMN IF NOT EXISTS ghl_opportunity_id varchar(100)"))
    conn.execute(text(
        f'CREATE UNIQUE INDEX IF NOT EXISTS uq_lending_loan_files_ghl_opportunity ON "{schema}".loan_files '
        "(ghl_opportunity_id) WHERE ghl_opportunity_id IS NOT NULL"))
    # A check constraint has no IF NOT EXISTS: drop and re-add so a changed value list reaches an existing table.
    for name, expr in (
        ("ck_lending_loan_files_stage", f"stage IN ({_STAGES})"),
        ("ck_lending_loan_files_closed_reason", f"closed_reason IS NULL OR closed_reason IN ({_REASONS})"),
        ("ck_lending_loan_files_closed_pair", "(closed_reason IS NULL) = (closed_at IS NULL)"),
    ):
        conn.execute(text(f'ALTER TABLE "{schema}".loan_files DROP CONSTRAINT IF EXISTS {name}'))
        conn.execute(text(f'ALTER TABLE "{schema}".loan_files ADD CONSTRAINT {name} CHECK ({expr})'))

    for model in TABLES[3:]:
        model.__table__.create(translated, checkfirst=True)
    for model in TABLES:
        table: Table = model.__table__  # type: ignore[assignment]
        for index in sorted(table.indexes, key=lambda i: str(i.name or "")):
            translated.execute(CreateIndex(index, if_not_exists=True))
    conn.execute(text(
        f'CREATE UNIQUE INDEX IF NOT EXISTS uq_lending_loan_files_borrower_property ON "{schema}".loan_files '
        "(borrower_id, property_id) WHERE borrower_id IS NOT NULL AND property_id IS NOT NULL"))

    conn.execute(text(
        f'ALTER TABLE "{schema}".text_consents '
        "ADD COLUMN IF NOT EXISTS loan_file_id bigint, "
        "ADD COLUMN IF NOT EXISTS schema_version smallint NOT NULL DEFAULT 1"))
    # The foreign key is added here, once loan_files exists (the model carries none: an older migration builds
    # text_consents from it before loan_files may exist). Named, so a re-run finds it; any FK on the column counts.
    conn.execute(text(_CONSENT_FK.format(s=schema)))
    conn.execute(text(f'ALTER TABLE "{schema}".partner_submissions ADD COLUMN IF NOT EXISTS schema_version smallint NOT NULL DEFAULT 1'))

    conn.execute(text(_BACKFILL_STAGE.format(s=schema)))
    conn.execute(text(_BACKFILL_DECLINED.format(s=schema, stages=_STAGES, reasons=_REASONS)))


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        conn.execute(text("SET LOCAL lock_timeout = '5s'"))
        apply_to(conn, schema)
    logger.info("apply_lending_loan_state_machine complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
