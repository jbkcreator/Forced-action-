"""R1 0.1 against Postgres: the "Done when" scenario, idempotent replays, payments and the migration backfill.

Runs in a throwaway schema inside one rolled-back transaction (every ``lending.`` in a statement is rewritten to
it), so no live table is touched.  Skips unless TEST_DATABASE_URL points at a throwaway Postgres.
"""
from __future__ import annotations

import uuid

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from migrations.apply_lending_loan_state_machine import apply_to
from src.lending import state_machine as sm
from src.lending.contracts import (
    BorrowerRecord,
    DeclineReason,
    EntitySchema,
    LoanFile,
    LoanStage,
    PropertySchema,
)
from src.lending.models import (
    LENDING_SCHEMA,
    LendingBase,
    LendingBorrowerProfile,
    LendingCoraInboxItem,
    LendingEntity,
    LendingLenderSubmission,
    LendingLoanFile,
    LendingLoanFileStatusEvent,
    LendingLoanPayment,
    LendingLoanStageEvent,
    LendingPartner,
    LendingPartnerSubmission,
    LendingProperty,
    LendingTextConsent,
)

TABLES = (LendingPartner, LendingBorrowerProfile, LendingProperty, LendingEntity, LendingLoanFile,
          LendingLoanFileStatusEvent, LendingTextConsent, LendingPartnerSubmission, LendingCoraInboxItem,
          LendingLenderSubmission, LendingLoanStageEvent, LendingLoanPayment)


def _connect(schema: str):
    from config.settings import get_settings

    url = get_settings().database_url
    if not url:
        pytest.skip("requires a throwaway Postgres (TEST_DATABASE_URL)")
    engine = create_engine(str(url), connect_args={"connect_timeout": 20})
    try:
        conn = engine.connect()
    except Exception:
        engine.dispose()
        pytest.skip("Postgres is not reachable")
    tx = conn.begin()
    conn.execute(text("SET LOCAL lock_timeout = '5s'"))
    conn.execute(text("SET LOCAL statement_timeout = '30s'"))
    conn.execute(text(f'CREATE SCHEMA "{schema}"'))

    @event.listens_for(conn, "before_cursor_execute", retval=True)
    def _to_schema(_conn, _cursor, statement, parameters, _context, _many):
        return statement.replace(f"{LENDING_SCHEMA}.", f"{schema}."), parameters

    return engine, conn, tx


@pytest.fixture
def sm_db():
    schema = f"r101_{uuid.uuid4().hex[:10]}"
    engine, conn, tx = _connect(schema)
    LendingBase.metadata.create_all(conn.execution_options(schema_translate_map={LENDING_SCHEMA: schema}),
                                    tables=[m.__table__ for m in TABLES])
    session = Session(bind=conn, join_transaction_mode="create_savepoint")
    session.info["schema"] = schema
    yield session
    session.close()
    tx.rollback()
    conn.close()
    engine.dispose()


def _loan(phone="813-555-0101", address="123 Oak St", **fields):
    return LoanFile(
        source="test", product="flip", borrower=BorrowerRecord(first_name="Jane", last_name="Doe", phone=phone),
        property=PropertySchema(address=address, city="Tampa", state="FL", zip_code="33601"), **fields)


def _count(db, table, where="TRUE"):
    return db.execute(text(f"SELECT count(*) FROM lending.{table} WHERE {where}")).scalar_one()


def _stage(db, file_id):
    return db.execute(text("SELECT stage FROM lending.loan_files WHERE id = :id"), {"id": file_id}).scalar_one()


def _walk(db, file_id, to):
    """Step a file forward to ``to`` through every stage in between."""
    while LoanStage(_stage(db, file_id)) is not to:
        step = sm.next_stage(LoanStage(_stage(db, file_id)))
        sm.transition(db, file_id, step, source="test", actor="test", source_event_id=f"walk-{file_id}-{step.value}")


def test_done_when_two_properties_three_submissions(sm_db):
    first = sm.ingest_loan_file(sm_db, _loan(address="123 Oak St"), actor="test")
    second = sm.ingest_loan_file(sm_db, _loan(address="9 Pine Ave"), actor="test")
    sm.record_lender_submission(sm_db, first.loan_file_id, "central_lending")
    sm.record_lender_submission(sm_db, first.loan_file_id, "corevest")
    sm.record_lender_submission(sm_db, second.loan_file_id, "rcn")

    assert first.borrower_id == second.borrower_id and first.loan_file_id != second.loan_file_id
    assert _count(sm_db, "loan_files") == 2 and _count(sm_db, "lender_submissions") == 3
    assert _count(sm_db, "borrower_profiles") == 1 and _count(sm_db, "properties") == 2


def test_ingesting_the_same_borrower_and_property_again_changes_nothing(sm_db):
    one = sm.ingest_loan_file(sm_db, _loan(), actor="test")
    again = sm.ingest_loan_file(sm_db, _loan(address="123 OAK ST."), actor="test")
    assert again.loan_file_id == one.loan_file_id and not again.created
    assert _count(sm_db, "loan_files") == 1


def test_a_resubmit_to_the_same_lender_is_a_new_attempt(sm_db):
    file_id = sm.ingest_loan_file(sm_db, _loan(), actor="test").loan_file_id
    for _ in range(2):
        sm.record_lender_submission(sm_db, file_id, "central_lending")
    attempts = sm_db.execute(text("SELECT attempt_no FROM lending.lender_submissions ORDER BY attempt_no")).scalars().all()
    assert attempts == [1, 2]


def test_a_new_file_starts_at_intake_with_unknown_inputs_null(sm_db):
    file_id = sm.ingest_loan_file(sm_db, _loan(), actor="test").loan_file_id
    row = sm_db.execute(text("SELECT stage, status, loan_amount, rehab_budget, purchase_price, payoff_balance, "
                             "outstanding_balance FROM lending.loan_files WHERE id = :id"), {"id": file_id}).one()
    assert (row.stage, row.status) == ("INTAKE", "received")
    assert (row.loan_amount, row.rehab_budget, row.purchase_price, row.payoff_balance, row.outstanding_balance) == (None,) * 5


def test_intake_to_funded_is_blocked_and_parked(sm_db):
    file_id = sm.ingest_loan_file(sm_db, _loan(), actor="test").loan_file_id
    with pytest.raises(sm.IllegalStageTransitionError):
        sm.transition(sm_db, file_id, LoanStage.FUNDED, source="ghl", actor="ghl", source_event_id="evt-1")
    assert _stage(sm_db, file_id) == "INTAKE"
    rejected = sm_db.execute(text("SELECT outcome, reason FROM lending.loan_stage_events WHERE source_event_id = 'evt-1'")).one()
    assert rejected.outcome == "rejected" and "INTAKE->FUNDED" in rejected.reason
    assert _count(sm_db, "cora_inbox", "event_type = 'loan_stage_rejected'") == 1


def test_replaying_the_funding_event_changes_nothing(sm_db):
    file_id = sm.ingest_loan_file(sm_db, _loan(), actor="test").loan_file_id
    _walk(sm_db, file_id, LoanStage.CLOSING)
    first = sm.transition(sm_db, file_id, LoanStage.FUNDED, source="ghl", actor="ghl", source_event_id="funded-1")
    events_after_first = _count(sm_db, "loan_stage_events")
    status_events_after_first = _count(sm_db, "loan_file_status_events")
    replays = [sm.transition(sm_db, file_id, LoanStage.FUNDED, source="ghl", actor="ghl", source_event_id="funded-1")
               for _ in range(3)]
    assert first.outcome == sm.OUTCOME_APPLIED and {r.outcome for r in replays} == {sm.OUTCOME_DUPLICATE}
    assert _count(sm_db, "loan_stage_events") == events_after_first
    assert _count(sm_db, "loan_file_status_events") == status_events_after_first
    assert _stage(sm_db, file_id) == "FUNDED"


def test_a_move_to_the_current_stage_is_a_noop(sm_db):
    file_id = sm.ingest_loan_file(sm_db, _loan(), actor="test").loan_file_id
    result = sm.transition(sm_db, file_id, LoanStage.INTAKE, source="ghl", actor="ghl", source_event_id="same-1")
    assert result.outcome == sm.OUTCOME_NOOP and _stage(sm_db, file_id) == "INTAKE"


def test_the_derived_status_follows_the_stage(sm_db):
    file_id = sm.ingest_loan_file(sm_db, _loan(), actor="test").loan_file_id
    _walk(sm_db, file_id, LoanStage.ACCEPTED)
    status = sm_db.execute(text("SELECT status FROM lending.loan_files WHERE id = :id"), {"id": file_id}).scalar_one()
    assert status == "approved"
    _walk(sm_db, file_id, LoanStage.FUNDED)
    funded_at = sm_db.execute(text("SELECT funded_at FROM lending.loan_files WHERE id = :id"), {"id": file_id}).scalar_one()
    assert funded_at is not None


def test_a_closed_file_freezes_its_stage_and_refuses_moves(sm_db):
    file_id = sm.ingest_loan_file(sm_db, _loan(), actor="test").loan_file_id
    _walk(sm_db, file_id, LoanStage.SUBMITTED)
    assert sm.close_file(sm_db, file_id, DeclineReason.CREDIT_BELOW_MIN, actor="josh") is True
    assert sm.close_file(sm_db, file_id, DeclineReason.OTHER, actor="josh") is False
    row = sm_db.execute(text("SELECT stage, status, closed_reason FROM lending.loan_files WHERE id = :id"), {"id": file_id}).one()
    assert tuple(row) == ("SUBMITTED", "declined", "credit_below_min")
    with pytest.raises(sm.IllegalStageTransitionError) as caught:
        sm.transition(sm_db, file_id, LoanStage.ACCEPTED, source="api", actor="josh")
    assert caught.value.reason == "file_closed"


def test_closed_reason_and_closed_at_go_together(sm_db):
    file_id = sm.ingest_loan_file(sm_db, _loan(), actor="test").loan_file_id
    with pytest.raises(IntegrityError):
        with sm_db.begin_nested():
            sm_db.execute(text("UPDATE lending.loan_files SET closed_reason = 'other' WHERE id = :id"), {"id": file_id})


def test_an_unknown_stage_value_is_refused_by_the_table(sm_db):
    file_id = sm.ingest_loan_file(sm_db, _loan(), actor="test").loan_file_id
    with pytest.raises(IntegrityError):
        with sm_db.begin_nested():
            sm_db.execute(text("UPDATE lending.loan_files SET stage = 'DONE' WHERE id = :id"), {"id": file_id})


def test_a_duplicate_payment_stores_one_row_and_recalculates_nothing(sm_db):
    file_id = sm.ingest_loan_file(sm_db, _loan(payoff_balance=250000), actor="test").loan_file_id
    _walk(sm_db, file_id, LoanStage.FUNDED)
    first = sm.record_payment(sm_db, file_id, source="lender", payment_id="pay-1", amount=250000, actor="lender")
    again = sm.record_payment(sm_db, file_id, source="lender", payment_id="pay-1", amount=250000, actor="lender")
    assert first.recorded and first.outstanding_balance == 0 and first.paid
    assert not again.recorded and not again.paid
    assert _count(sm_db, "loan_payments") == 1 and _stage(sm_db, file_id) == "PAID"


def test_a_partial_payment_lowers_the_balance_and_keeps_the_stage(sm_db):
    file_id = sm.ingest_loan_file(sm_db, _loan(payoff_balance=250000), actor="test").loan_file_id
    _walk(sm_db, file_id, LoanStage.FUNDED)
    result = sm.record_payment(sm_db, file_id, source="lender", payment_id="pay-1", amount=100000, actor="lender")
    assert result.outstanding_balance == 150000 and not result.paid and _stage(sm_db, file_id) == "FUNDED"


def test_an_unknown_payoff_balance_stays_null_after_a_payment(sm_db):
    file_id = sm.ingest_loan_file(sm_db, _loan(), actor="test").loan_file_id
    _walk(sm_db, file_id, LoanStage.FUNDED)
    result = sm.record_payment(sm_db, file_id, source="lender", payment_id="pay-1", amount=1000, actor="lender")
    assert result.outstanding_balance is None and not result.paid


def test_josh_moving_the_ghl_deal_to_paid_records_one_payment_and_moves_the_file(sm_db):
    file_id = sm.ingest_loan_file(sm_db, _loan(), actor="test").loan_file_id
    _walk(sm_db, file_id, LoanStage.FUNDED)
    for _ in range(2):
        sm.record_payment(sm_db, file_id, source="ghl", payment_id="ghl-evt-9", amount=None, actor="ghl", mark_paid=True)
    assert _count(sm_db, "loan_payments") == 1 and _stage(sm_db, file_id) == "PAID"


def test_entities_match_on_normalized_name(sm_db):
    one = sm.ingest_loan_file(sm_db, _loan(address="1 A St").model_copy(update={"entity": EntitySchema(name="Smith Holdings, LLC")}), actor="t")
    two = sm.ingest_loan_file(sm_db, _loan(address="2 B St").model_copy(update={"entity": EntitySchema(name="SMITH HOLDINGS LLC")}), actor="t")
    assert one.entity_id == two.entity_id and _count(sm_db, "entities") == 1


# ── The migration ──────────────────────────────────────────────────────────────

_LEGACY_LOAN_FILES = """
CREATE TABLE "{s}".loan_files (
    id BIGSERIAL PRIMARY KEY, source VARCHAR(20) NOT NULL, partner_id BIGINT,
    borrower_first_name VARCHAR(80) NOT NULL, borrower_last_name VARCHAR(80) NOT NULL, product VARCHAR(20) NOT NULL,
    loan_amount BIGINT NOT NULL, property_state VARCHAR(2) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'received', status_reason VARCHAR(200),
    status_at TIMESTAMPTZ NOT NULL DEFAULT now(), created_at TIMESTAMPTZ NOT NULL DEFAULT now())
"""


@pytest.fixture
def legacy_db():
    """A loan_files table in the shape it had before this migration, with files in every status."""
    schema = f"r101m_{uuid.uuid4().hex[:9]}"
    engine, conn, tx = _connect(schema)
    translated = conn.execution_options(schema_translate_map={LENDING_SCHEMA: schema})
    conn.execute(text(_LEGACY_LOAN_FILES.format(s=schema)))
    conn.execute(text(f'CREATE TABLE "{schema}".loan_file_status_events (id BIGSERIAL PRIMARY KEY, '
                      f'loan_file_id BIGINT NOT NULL REFERENCES "{schema}".loan_files(id), status VARCHAR(20) NOT NULL, '
                      "reason VARCHAR(200), at TIMESTAMPTZ NOT NULL DEFAULT now())"))
    conn.execute(text(f'CREATE TABLE "{schema}".text_consents (id SERIAL PRIMARY KEY, phone VARCHAR(20) NOT NULL, '
                      "source VARCHAR(20) NOT NULL, captured_at TIMESTAMPTZ NOT NULL DEFAULT now(), revoked_at TIMESTAMPTZ)"))
    conn.execute(text(f'CREATE TABLE "{schema}".partner_submissions (id BIGSERIAL PRIMARY KEY, file_id BIGINT)'))
    for status in ("received", "held", "routed", "approved", "funded"):
        conn.execute(text(f'INSERT INTO "{schema}".loan_files (source, borrower_first_name, borrower_last_name, product, '
                          "loan_amount, property_state, status) VALUES ('partner', 'A', :last, 'flip', 100000, 'FL', :status)"),
                     {"last": status, "status": status})
    conn.execute(text(f'INSERT INTO "{schema}".loan_files (source, borrower_first_name, borrower_last_name, product, '
                      "loan_amount, property_state, status, status_reason) VALUES ('partner', 'A', 'declined', 'flip', "
                      "100000, 'FL', 'declined', 'credit_below_min')"))
    declined = conn.execute(text(f'SELECT id FROM "{schema}".loan_files WHERE status = \'declined\'')).scalar_one()
    conn.execute(text(f"INSERT INTO \"{schema}\".loan_file_status_events (loan_file_id, status) VALUES "
                      f"({declined}, 'received'), ({declined}, 'routed'), ({declined}, 'declined')"))
    session = Session(bind=conn, join_transaction_mode="create_savepoint")
    session.info["schema"] = schema
    session.info["conn"] = conn
    yield session
    session.close()
    tx.rollback()
    conn.close()
    engine.dispose()


def test_the_migration_backfills_stages_and_closes_declined_files(legacy_db):
    schema = legacy_db.info["schema"]
    apply_to(legacy_db.info["conn"], schema)
    rows = dict(legacy_db.execute(text(f'SELECT borrower_last_name, stage FROM "{schema}".loan_files')).all())
    assert rows == {"received": "INTAKE", "held": "FIT", "routed": "SUBMITTED", "approved": "ACCEPTED",
                    "funded": "FUNDED", "declined": "SUBMITTED"}
    closed = legacy_db.execute(text(f'SELECT closed_reason, closed_at IS NOT NULL FROM "{schema}".loan_files '
                                    "WHERE status = 'declined'")).one()
    assert tuple(closed) == ("credit_below_min", True)


def test_the_migration_is_idempotent_and_relaxes_the_not_null_columns(legacy_db):
    schema, conn = legacy_db.info["schema"], legacy_db.info["conn"]
    apply_to(conn, schema)
    before = legacy_db.execute(text(f'SELECT id, stage, closed_reason FROM "{schema}".loan_files ORDER BY id')).all()
    apply_to(conn, schema)
    assert legacy_db.execute(text(f'SELECT id, stage, closed_reason FROM "{schema}".loan_files ORDER BY id')).all() == before
    legacy_db.execute(text(f'INSERT INTO "{schema}".loan_files (source, borrower_first_name, borrower_last_name, product) '
                           "VALUES ('web', 'N', 'Null', 'flip')"))  # loan_amount and property_state omitted: now allowed
