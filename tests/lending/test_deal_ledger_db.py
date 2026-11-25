"""R1 0.8 against Postgres: the "Done when" lifecycle, append-only history, card catch-up, quotes and the backfill.

Runs in a throwaway schema inside one rolled-back transaction (every ``lending.`` in a statement is rewritten to it),
so no live table is touched.  Skips unless TEST_DATABASE_URL points at a throwaway Postgres.
"""
from __future__ import annotations

import json
import uuid

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.orm import Session

from config.lending_standing_sends import AUTO_GRADUATED_ACTOR
from migrations.apply_lending_deal_audit_ledger import apply_to as apply_ledger
from migrations.apply_lending_lender_rule_snapshots import apply_to as apply_snapshots
from src.lending import state_machine as sm
from src.lending.audit import deal_ledger
from src.lending import lender_rules_store as rules
from src.lending.contracts import BorrowerRecord, DeclineReason, LoanFile, LoanStage, PropertySchema
from src.lending.models import (
    LENDING_SCHEMA,
    LendingBase,
    LendingBorrowerProfile,
    LendingCardEdit,
    LendingCoraInboxItem,
    LendingDocChase,
    LendingEntity,
    LendingInstantQuote,
    LendingLenderRuleValue,
    LendingLenderSubmission,
    LendingLoanFile,
    LendingLoanFileStatusEvent,
    LendingLoanPayment,
    LendingLoanStageEvent,
    LendingPartner,
    LendingProperty,
)

TABLES = (LendingPartner, LendingBorrowerProfile, LendingProperty, LendingEntity, LendingLoanFile,
          LendingLoanFileStatusEvent, LendingLenderSubmission, LendingLoanStageEvent, LendingLoanPayment,
          LendingCoraInboxItem, LendingLenderRuleValue, LendingCardEdit, LendingInstantQuote, LendingDocChase)

_PENDING_ACTIONS = """
CREATE TABLE "{s}".pending_actions (
    action_id bigserial PRIMARY KEY, tool_name varchar(100) NOT NULL DEFAULT 'ghl_sms', channel varchar(64) NOT NULL DEFAULT 'ghl_sms',
    payload jsonb NOT NULL DEFAULT '{{}}', summary text NOT NULL DEFAULT '', status varchar(16) NOT NULL DEFAULT 'pending',
    requested_by varchar(32), decided_by varchar(32), decided_at timestamptz, deal_ref varchar(64), recipient_phone varchar(20),
    created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now())
"""


@pytest.fixture
def ledger_db():
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
    schema = f"r108_{uuid.uuid4().hex[:10]}"
    tx = conn.begin()
    conn.execute(text("SET LOCAL lock_timeout = '5s'"))
    conn.execute(text("SET LOCAL statement_timeout = '30s'"))
    conn.execute(text(f'CREATE SCHEMA "{schema}"'))

    @event.listens_for(conn, "before_cursor_execute", retval=True)
    def _to_schema(_conn, _cursor, statement, parameters, _context, _many):
        return statement.replace(f"{LENDING_SCHEMA}.", f"{schema}."), parameters

    LendingBase.metadata.create_all(conn.execution_options(schema_translate_map={LENDING_SCHEMA: schema}),
                                    tables=[m.__table__ for m in TABLES])
    conn.execute(text(_PENDING_ACTIONS.format(s=schema)))
    apply_snapshots(conn, schema)
    apply_ledger(conn, schema)
    session = Session(bind=conn, join_transaction_mode="create_savepoint")
    session.info.update(schema=schema, conn=conn)
    yield session
    session.close()
    tx.rollback()
    conn.close()
    engine.dispose()


def _loan(phone="813-555-0101", address="123 Oak St", **fields):
    return LoanFile(source="test", product="flip", borrower=BorrowerRecord(first_name="Jane", last_name="Doe", phone=phone),
                    property=PropertySchema(address=address, city="Tampa", state="FL", zip_code="33601"), **fields)


def _new_file(db, **fields):
    return sm.ingest_loan_file(db, _loan(**fields), actor="system:test").loan_file_id


def _types(db, file_id):
    return [e["event_type"] for e in deal_ledger.lifecycle(db, file_id)]


def _count(db, where="TRUE"):
    return db.execute(text(f"SELECT count(*) FROM lending.deal_audit_events WHERE {where}")).scalar_one()


def _card(db, *, deal_ref=None, phone=None, status="pending", decided_by=None, decided=False, secs=10):
    return db.execute(text(
        "INSERT INTO lending.pending_actions (payload, summary, status, requested_by, decided_by, decided_at, deal_ref, "
        "recipient_phone, created_at, updated_at) VALUES ('{\"template_version\": \"intro-v1\"}'::jsonb, 'Intro text', :status, "
        "'U0JOSH', :by, CASE WHEN :decided THEN now() + make_interval(secs => :secs + 5) END, :ref, :phone, "
        "now() + make_interval(secs => :secs), now() + make_interval(secs => :secs)) RETURNING action_id"),
        {"status": status, "by": decided_by, "decided": decided, "ref": deal_ref, "phone": phone, "secs": secs}).scalar_one()


def _edit(db, action_id, *, by="U0JOSH", before="Hi Jane", after="Hello Jane", secs=20):
    return db.execute(text(
        "INSERT INTO lending.card_edits (action_id, revision_no, template_version, channel, tool_name, before, after, diff, "
        "instruction, edited_by, edited_at) VALUES (:a, 1, 'intro-v1', 'ghl_sms', 'ghl_sms', CAST(:b AS jsonb), CAST(:af AS jsonb), '', "
        "'warmer', :by, now() + make_interval(secs => :secs)) RETURNING id"),
        {"a": action_id, "b": json.dumps({"message": before}), "af": json.dumps({"message": after}), "by": by, "secs": secs}).scalar_one()


# ── The "Done when" ────────────────────────────────────────────────────────────

def test_done_when_the_whole_lifecycle_reads_back_in_order(ledger_db):
    db = ledger_db
    file_id = _new_file(db)                                                           # rule v1
    rules.set_rule_value(db, "central_lending", "max_ltv", 0.9, status="confirmed", actor="josh", reason="confirmed")  # rule v2
    sm.record_lender_submission(db, file_id, "central_lending")
    sm.transition(db, file_id, LoanStage.FIT, source="slack", actor="slack:U0JOSH")
    sm.update_fields(db, file_id, {"loan_amount": 250000}, actor="josh", source="api")
    draft = _card(db, phone="+18135550101", decided=True, status="approved", decided_by="U0JOSH", secs=30)
    _edit(db, draft, secs=31)
    assert deal_ledger.sync_card_events(db) == 3

    events = deal_ledger.lifecycle(db, file_id)
    assert [(e["event_type"], e["actor"]) for e in events] == [
        ("file_created", "system:test"), ("lender_submission", "system:lender_submission"),
        ("stage_transition", "human:U0JOSH"), ("field_update", "human:josh"),
        ("cora_draft", "cora"), ("card_approved", "human:U0JOSH"), ("human_edit", "human:U0JOSH")]
    by_type = {e["event_type"]: e for e in events}
    assert by_type["file_created"]["rule_version"] == 1 and by_type["stage_transition"]["rule_version"] == 2
    assert by_type["stage_transition"]["diff"]["stage"] == {"before": "INTAKE", "after": "FIT"}
    assert by_type["field_update"]["diff"] == {"loan_amount": {"before": None, "after": 250000}}
    assert by_type["human_edit"]["diff"] == {"message": {"before": "Hi Jane", "after": "Hello Jane"}}
    versions = [e["rule_version"] for e in events]
    assert versions == sorted(versions)


def test_a_rule_rollback_shows_in_the_version_each_later_event_carries(ledger_db):
    db = ledger_db
    file_id = _new_file(db)
    rules.set_rule_value(db, "central_lending", "max_ltv", 0.9, status="confirmed", actor="josh", reason="good")
    rules.set_rule_value(db, "central_lending", "max_ltv", 0.99, status="confirmed", actor="josh", reason="typo")
    sm.transition(db, file_id, LoanStage.FIT, source="api", actor="josh")
    rules.rollback_to(db, 2, actor="josh")
    sm.transition(db, file_id, LoanStage.SUBMITTED, source="api", actor="josh")
    moves = [e["rule_version"] for e in deal_ledger.lifecycle(db, file_id) if e["event_type"] == "stage_transition"]
    assert moves == [3, 4]


# ── Append-only ────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("statement", ["UPDATE lending.deal_audit_events SET actor = 'cora'",
                                       "DELETE FROM lending.deal_audit_events", "TRUNCATE lending.deal_audit_events"])
def test_the_ledger_cannot_be_changed(ledger_db, statement):
    _new_file(ledger_db)
    with pytest.raises(DBAPIError, match="append-only"):
        with ledger_db.begin_nested():
            ledger_db.execute(text(statement))


def test_an_untagged_actor_is_refused_by_the_table(ledger_db):
    file_id = _new_file(ledger_db)
    with pytest.raises(IntegrityError):
        with ledger_db.begin_nested():
            ledger_db.execute(text("INSERT INTO lending.deal_audit_events (loan_file_id, event_type, actor) VALUES (:f, 'closed', 'josh')"),
                              {"f": file_id})


def test_a_loan_file_with_history_cannot_be_deleted(ledger_db):
    file_id = _new_file(ledger_db)
    with pytest.raises(IntegrityError):
        with ledger_db.begin_nested():
            ledger_db.execute(text("DELETE FROM lending.loan_files WHERE id = :f"), {"f": file_id})


# ── Stage moves ────────────────────────────────────────────────────────────────

def test_a_refused_jump_is_ledgered_and_the_stage_does_not_move(ledger_db):
    db = ledger_db
    file_id = _new_file(db)
    with pytest.raises(sm.IllegalStageTransitionError):
        sm.transition(db, file_id, LoanStage.FUNDED, source="ghl", actor="ghl", source_event_id="evt-1")
    rejected = [e for e in deal_ledger.lifecycle(db, file_id) if e["event_type"] == "stage_rejected"]
    assert len(rejected) == 1 and rejected[0]["actor"] == "system:ghl" and rejected[0]["detail"]["reason"].startswith("illegal_jump")
    assert "stage_transition" not in _types(db, file_id)


def test_a_replayed_event_and_a_noop_add_nothing_to_the_ledger(ledger_db):
    db = ledger_db
    file_id = _new_file(db)
    sm.transition(db, file_id, LoanStage.FIT, source="ghl", actor="ghl", source_event_id="evt-1")
    before = _count(db)
    for _ in range(3):
        sm.transition(db, file_id, LoanStage.FIT, source="ghl", actor="ghl", source_event_id="evt-1")
    sm.transition(db, file_id, LoanStage.FIT, source="ghl", actor="ghl", source_event_id="evt-2")  # already at FIT
    assert _count(db) == before


def test_closing_a_file_is_ledgered_with_the_stage_it_died_at(ledger_db):
    db = ledger_db
    file_id = _new_file(db)
    sm.transition(db, file_id, LoanStage.FIT, source="api", actor="josh")
    assert sm.close_file(db, file_id, DeclineReason.CREDIT_BELOW_MIN, actor="human:josh")
    closed = [e for e in deal_ledger.lifecycle(db, file_id) if e["event_type"] == "closed"]
    assert len(closed) == 1 and closed[0]["actor"] == "human:josh" and closed[0]["detail"]["stage"] == "FIT"
    assert closed[0]["diff"]["closed_reason"] == {"before": None, "after": "credit_below_min"}
    assert not sm.close_file(db, file_id, DeclineReason.OTHER, actor="human:josh") and _types(db, file_id).count("closed") == 1


def test_a_duplicate_payment_is_ledgered_once_and_paying_off_records_the_stage_move(ledger_db):
    db = ledger_db
    file_id = _new_file(db, payoff_balance=250000)
    for stage in (LoanStage.FIT, LoanStage.SUBMITTED, LoanStage.ACCEPTED, LoanStage.TERM_SHEET_ISSUED, LoanStage.CLOSING, LoanStage.FUNDED):
        sm.transition(db, file_id, stage, source="api", actor="josh")
    for _ in range(2):
        sm.record_payment(db, file_id, source="lender", payment_id="p1", amount=250000, actor="lender")
    types = _types(db, file_id)
    assert types.count("payment_recorded") == 1 and types[-1] == "stage_transition"
    payment = [e for e in deal_ledger.lifecycle(db, file_id) if e["event_type"] == "payment_recorded"][0]
    assert payment["diff"]["outstanding_balance"] == {"before": 250000, "after": 0}


# ── Field edits ────────────────────────────────────────────────────────────────

def test_a_field_edit_records_only_what_changed_and_a_repeat_records_nothing(ledger_db):
    db = ledger_db
    file_id = _new_file(db, loan_amount=200000)
    assert sm.update_fields(db, file_id, {"loan_amount": 200000, "arv": 320000}, actor="josh", source="api") == {
        "arv": {"before": None, "after": 320000}}
    assert sm.update_fields(db, file_id, {"loan_amount": 200000, "arv": 320000}, actor="josh", source="api") == {}
    assert _types(db, file_id).count("field_update") == 1


def test_an_unknown_amount_stays_null_and_is_never_zero(ledger_db):
    db = ledger_db
    file_id = _new_file(db, rehab_budget=45000)
    sm.update_fields(db, file_id, {"rehab_budget": ""}, actor="josh", source="api")
    assert db.execute(text("SELECT rehab_budget FROM lending.loan_files WHERE id = :f"), {"f": file_id}).scalar_one() is None


def test_learning_the_payoff_later_recalculates_the_balance_and_records_both(ledger_db):
    db = ledger_db
    file_id = _new_file(db)
    sm.transition(db, file_id, LoanStage.FIT, source="api", actor="josh")
    sm.record_payment(db, file_id, source="lender", payment_id="p1", amount=1000, actor="lender")
    diff = sm.update_fields(db, file_id, {"payoff_balance": 5000}, actor="josh", source="api")
    assert diff["outstanding_balance"] == {"before": None, "after": 4000}


# ── Cora's cards ───────────────────────────────────────────────────────────────

def test_card_events_are_caught_up_once_and_only_for_cards_that_belong_to_a_file(ledger_db):
    db = ledger_db
    file_id = _new_file(db)
    _card(db, phone="+18135550101", secs=10)                      # found by the borrower's phone
    _card(db, deal_ref=f"loan_file:{file_id}", secs=11)           # found by an explicit reference
    _card(db, phone="+19995550000", secs=12)                      # about nobody in the pipeline
    assert deal_ledger.sync_card_events(db) == 2
    assert deal_ledger.sync_card_events(db) == 0
    assert _types(db, file_id) == ["file_created", "cora_draft", "cora_draft"]


def test_a_card_is_not_guessed_onto_one_of_several_files_with_the_same_phone(ledger_db):
    db = ledger_db
    first, second = _new_file(db), _new_file(db, address="9 Elm St")
    _card(db, phone="+18135550101", secs=10)
    assert deal_ledger.sync_card_events(db) == 0
    assert "cora_draft" not in _types(db, first) + _types(db, second)


def test_a_card_is_found_through_its_document_chase(ledger_db):
    db = ledger_db
    file_id = _new_file(db)
    chase = db.execute(text("INSERT INTO lending.doc_chases (lead_ref, lender_key, accepted_at, accepted_by, loan_file_id) "
                            "VALUES ('L1', 'central_lending', now(), 'slack:U1', :f) RETURNING id"), {"f": file_id}).scalar_one()
    _card(db, deal_ref=f"doc_chase:{chase}")
    assert deal_ledger.sync_card_events(db) == 1 and "cora_draft" in _types(db, file_id)


def test_approvals_rejections_and_auto_approvals_name_who_decided(ledger_db):
    db = ledger_db
    file_id = _new_file(db)
    _card(db, phone="+18135550101", status="sent", decided_by="U0JOSH", decided=True, secs=10)
    _card(db, phone="+18135550101", status="rejected", decided_by="U0JOSH", decided=True, secs=20)
    _card(db, phone="+18135550101", status="sent", decided_by=AUTO_GRADUATED_ACTOR, decided=True, secs=30)
    deal_ledger.sync_card_events(db)
    decided = [(e["event_type"], e["actor"], e["detail"].get("auto_approved")) for e in deal_ledger.lifecycle(db, file_id)
               if e["event_type"] in ("card_approved", "card_rejected")]
    assert decided == [("card_approved", "human:U0JOSH", False), ("card_rejected", "human:U0JOSH", False),
                       ("card_approved", "system:standing_send", True)]


def test_a_card_still_pending_has_a_draft_but_no_decision(ledger_db):
    db = ledger_db
    file_id = _new_file(db)
    _card(db, phone="+18135550101")
    deal_ledger.sync_card_events(db)
    assert _types(db, file_id) == ["file_created", "cora_draft"]


# ── Quotes ─────────────────────────────────────────────────────────────────────

def _quote(db, hours_ago):
    return str(db.execute(text(
        "INSERT INTO lending.instant_quotes (status, request, result, unconfirmed, api_key_label, created_at) VALUES "
        "('indicative', '{}'::jsonb, '{\"loan_amount\": 180000}'::jsonb, false, 'cora', now() - make_interval(hours => :h)) RETURNING id"),
        {"h": hours_ago}).scalar_one())


def test_a_quote_is_ledgered_once_with_the_time_and_rule_version_it_was_made(ledger_db):
    db = ledger_db
    file_id = _new_file(db)
    old, fresh = _quote(db, 2), _quote(db, 0)
    assert deal_ledger.record_quote(db, file_id, old) is not None
    assert deal_ledger.record_quote(db, file_id, old) is None
    deal_ledger.record_quote(db, file_id, fresh)
    quotes = [e for e in deal_ledger.lifecycle(db, file_id) if e["event_type"] == "quote_generated"]
    assert [q["detail"]["quote_id"] for q in quotes] == [old, fresh]            # the older quote sorts first
    assert quotes[0]["rule_version"] is None and quotes[1]["rule_version"] == 1  # no rules existed two hours ago
    assert quotes[0]["detail"]["loan_amount"] == 180000


def test_a_quote_that_does_not_exist_is_not_ledgered(ledger_db):
    file_id = _new_file(ledger_db)
    assert deal_ledger.record_quote(ledger_db, file_id, str(uuid.uuid4())) is None


# ── The migration backfill ─────────────────────────────────────────────────────

def test_the_backfill_imports_old_files_and_status_changes_once(ledger_db):
    db = ledger_db
    db.execute(text("INSERT INTO lending.loan_files (source, borrower_first_name, borrower_last_name, product, status) "
                    "VALUES ('partner', 'Old', 'File', 'flip', 'routed')"))
    old = db.execute(text("SELECT max(id) FROM lending.loan_files")).scalar_one()
    db.execute(text("INSERT INTO lending.loan_file_status_events (loan_file_id, status, reason) VALUES (:f, 'received', NULL), (:f, 'routed', 'router')"), {"f": old})
    apply_ledger(db.info["conn"], db.info["schema"])
    apply_ledger(db.info["conn"], db.info["schema"])
    events = deal_ledger.lifecycle(db, old)
    assert [(e["event_type"], e["actor"]) for e in events] == [("file_created", "system:backfill"),
                                                                ("status_change", "system:backfill"), ("status_change", "system:backfill")]


# ── PR #475 review fixes ───────────────────────────────────────────────────────

def test_a_rerun_of_the_migration_after_live_activity_adds_nothing(ledger_db):
    """Review 5: the backfill runs only on an empty ledger, so a deployment retry never duplicates live events."""
    db = ledger_db
    file_id = _new_file(db)
    sm.transition(db, file_id, LoanStage.FIT, source="api", actor="josh")
    before = _count(db)
    apply_ledger(db.info["conn"], db.info["schema"])
    assert _count(db) == before


def test_an_automatic_approval_returned_to_pending_and_then_rejected_keeps_both_decisions(ledger_db):
    """Review 3."""
    db = ledger_db
    file_id = _new_file(db)
    action = _card(db, deal_ref=f"loan_file:{file_id}", status="approved", decided_by=AUTO_GRADUATED_ACTOR, decided=True)
    deal_ledger.sync_card_events(db)
    db.execute(text("UPDATE lending.pending_actions SET status = 'rejected', decided_by = 'U0JOSH', "
                    "decided_at = decided_at + interval '2 minutes', updated_at = now() WHERE action_id = :a"),
               {"a": action})
    deal_ledger.sync_card_events(db)
    decisions = [(e["event_type"], e["actor"]) for e in deal_ledger.lifecycle(db, file_id)
                 if e["event_type"] in ("card_approved", "card_rejected")]
    assert decisions == [("card_approved", "system:standing_send"), ("card_rejected", "human:U0JOSH")]


def test_a_partner_status_event_records_the_stage_and_closure_it_changed(ledger_db):
    """Review 4."""
    from src.lending import partners

    db = ledger_db
    file_id = _new_file(db)
    partners.record_status_change(db, file_id, "routed")
    partners.record_status_change(db, file_id, "declined", "credit_below_min")
    changes = [e["diff"] for e in deal_ledger.lifecycle(db, file_id) if e["event_type"] == "status_change"]
    assert changes[0]["stage"] == {"before": "INTAKE", "after": "SUBMITTED"}
    assert changes[1]["closed"] == {"before": False, "after": True}
    assert changes[1]["closed_reason"] == {"before": None, "after": "credit_below_min"}


def test_a_document_chase_for_a_borrower_with_two_files_puts_the_quote_on_neither(ledger_db):
    """Review 2."""
    db = ledger_db
    first, second = _new_file(db), _new_file(db, address="9 Elm St")
    assert db.execute(text("SELECT min(id) FROM lending.loan_files WHERE borrower_phone = :p HAVING count(*) = 1"),
                      {"p": "+18135550101"}).first() is None
    assert "quote_generated" not in _types(db, first) + _types(db, second)

def test_ghl_card_links_only_to_the_named_opportunity(ledger_db):
    db = ledger_db
    file_id = _new_file(db)
    db.execute(text("UPDATE lending.loan_files SET ghl_opportunity_id = 'older-opp' WHERE id = :id"),
               {"id": file_id})
    _card(db, deal_ref="ghl_opportunity:other-opp", phone="+18135550101", secs=10)
    assert deal_ledger.sync_card_events(db) == 0

    _card(db, deal_ref="ghl_opportunity:older-opp", secs=20)
    assert deal_ledger.sync_card_events(db) == 1
    assert _types(db, file_id) == ["file_created", "cora_draft"]
