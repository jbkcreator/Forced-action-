"""R1 0.1: the loan lifecycle rules, the strict contracts and the stage endpoint. No database."""
from __future__ import annotations

import itertools
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr, ValidationError

from src.api import lending_loan_stage_router as stage_router
from src.lending import state_machine as sm
from src.lending.contracts import (
    LOAN_STAGE_ORDER,
    BorrowerRecord,
    ConsentRecord,
    DocumentStipulation,
    EntitySchema,
    LoanFile,
    LoanStage,
    PropertySchema,
)

PAIRS = list(itertools.product(LOAN_STAGE_ORDER, LOAN_STAGE_ORDER))


def test_the_eight_stages_are_in_spec_order():
    assert [s.value for s in LOAN_STAGE_ORDER] == [
        "INTAKE", "FIT", "SUBMITTED", "ACCEPTED", "TERM_SHEET_ISSUED", "CLOSING", "FUNDED", "PAID"]


@pytest.mark.parametrize("current,target", PAIRS)
def test_only_the_next_stage_is_a_legal_move(current, target):
    legal = LOAN_STAGE_ORDER.index(target) == LOAN_STAGE_ORDER.index(current) + 1
    assert (sm.check_transition(current, target) is None) is legal


def test_intake_to_funded_is_refused_with_a_reason():
    assert sm.check_transition(LoanStage.INTAKE, LoanStage.FUNDED) == "illegal_jump INTAKE->FUNDED"


def test_nothing_follows_paid():
    assert sm.next_stage(LoanStage.PAID) is None


def test_a_closed_file_refuses_every_move():
    for target in LOAN_STAGE_ORDER:
        assert sm.check_transition(LoanStage.SUBMITTED, target, closed=True) == "file_closed"


def test_every_stage_derives_a_status_the_table_allows():
    allowed = {"received", "held", "routed", "declined", "approved", "funded"}
    assert set(sm.STATUS_FOR_STAGE) == set(LoanStage)
    assert set(sm.STATUS_FOR_STAGE.values()) <= allowed and sm.CLOSED_STATUS in allowed


@pytest.mark.parametrize("raw,expected", [
    (None, None), ("", None), ("   ", None), ("unknown", None), ("n/a", None), (True, None),
    (0, 0), ("0", 0), (150000, 150000), ("$1,250,000", 1250000), ("85000.00", 85000), (12.0, 12),
])
def test_unknown_amounts_are_none_and_never_zero(raw, expected):
    assert sm.amount_or_none(raw) == expected


def test_unknown_payoff_balance_stays_unknown():
    assert sm.outstanding_after(None, 5000) is None
    assert sm.outstanding_after(250000, 250000) == 0
    assert sm.outstanding_after(250000, 300000) == 0  # an overpayment never goes negative
    assert sm.outstanding_after(250000, 0) == 250000


def test_one_property_key_whatever_the_punctuation():
    a = sm.normalize_address("123 Oak St.", "Tampa", "fl", "33601-1234")
    assert a == sm.normalize_address("123  OAK ST", "tampa", "FL", "33601")
    assert a != sm.normalize_address("123 Oak St", "Tampa", "FL", "33602")


def test_one_entity_key_whatever_the_suffix_style():
    assert sm.normalize_entity_name("Smith Holdings, LLC") == sm.normalize_entity_name("SMITH HOLDINGS LLC")


def _loan(**fields):
    return LoanFile(source="test", product="flip", borrower=BorrowerRecord(first_name="Jane", last_name="Doe"),
                    property=PropertySchema(address="123 Oak St"), **fields)


def test_an_unprovided_financial_input_is_none_not_zero():
    loan = _loan()
    assert (loan.rehab_budget, loan.purchase_price, loan.payoff_balance, loan.loan_amount) == (None, None, None, None)


@pytest.mark.parametrize("field", ["rehab_budget", "purchase_price", "payoff_balance", "loan_amount", "arv"])
def test_a_blank_or_text_amount_is_rejected_not_coerced(field):
    for bad in ("", "0", "unknown"):
        with pytest.raises(ValidationError):
            _loan(**{field: bad})


def test_contracts_reject_unknown_fields():
    with pytest.raises(ValidationError):
        BorrowerRecord(first_name="Jane", last_name="Doe", nickname="JD")


def test_every_contract_carries_its_schema_version():
    contracts = [
        _loan(), BorrowerRecord(first_name="a", last_name="b"), EntitySchema(name="Smith Holdings LLC"),
        PropertySchema(address="1 Main"), DocumentStipulation(item_code="bank_statements", label="Bank statements"),
    ]
    assert all(c.schema_version == 1 for c in contracts)
    with pytest.raises(ValidationError):
        BorrowerRecord(first_name="a", last_name="b", schema_version=2)


def test_consent_record_hides_the_phone_from_its_repr():
    from datetime import datetime, timezone

    record = ConsentRecord(phone="+18135550101", source="web_form", captured_at=datetime.now(timezone.utc))
    assert "8135550101" not in repr(record)


# ── The stage endpoint ─────────────────────────────────────────────────────────

class _Db:
    def __init__(self) -> None:
        self.commits = 0
        self.rollbacks = 0

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


@pytest.fixture
def api(monkeypatch):
    from config.settings import get_settings

    monkeypatch.setattr(get_settings(), "lending_admin_token", SecretStr("secret-token"), raising=False)
    db = _Db()
    app = FastAPI()
    app.include_router(stage_router.router)
    app.dependency_overrides[stage_router.get_lending_db] = lambda: db
    client = TestClient(app, raise_server_exceptions=False)
    client.db = db  # type: ignore[attr-defined]
    return client


def _post(client, token="secret-token", **body):
    payload = {"to_stage": "FIT", "actor": "tester", **body}
    headers = {stage_router.ADMIN_TOKEN_HEADER: token} if token else {}
    return client.post("/api/lending/loan-files/7/stage", json=payload, headers=headers)


def test_no_token_or_a_wrong_token_is_401(api):
    assert _post(api, token=None).status_code == 401
    assert _post(api, token="nope").status_code == 401


def test_an_unset_admin_token_refuses_every_caller(api, monkeypatch):
    from config.settings import get_settings

    monkeypatch.setattr(get_settings(), "lending_admin_token", None, raising=False)
    assert _post(api, token="anything").status_code == 401


def test_a_legal_move_is_200_and_committed(api, monkeypatch):
    monkeypatch.setattr(stage_router, "transition", lambda db, fid, to, **kw: sm.TransitionResult(
        fid, LoanStage.INTAKE, LoanStage(to), sm.OUTCOME_APPLIED))
    response = _post(api)
    assert response.status_code == 200
    assert response.json() == {"loan_file_id": 7, "from_stage": "INTAKE", "to_stage": "FIT", "outcome": "applied"}
    assert api.db.commits == 1


def test_a_replayed_event_is_200_and_reports_duplicate(api, monkeypatch):
    monkeypatch.setattr(stage_router, "transition", lambda db, fid, to, **kw: sm.TransitionResult(
        fid, LoanStage.FUNDED, LoanStage(to), sm.OUTCOME_DUPLICATE))
    response = _post(api, to_stage="FUNDED", source_event_id="evt-1")
    assert response.status_code == 200 and response.json()["outcome"] == "duplicate"


def test_an_illegal_jump_is_400_and_the_rejected_event_is_kept(api, monkeypatch):
    def refuse(db, fid, to, **kw):
        raise sm.IllegalStageTransitionError(fid, LoanStage.INTAKE, LoanStage.FUNDED, "illegal_jump INTAKE->FUNDED")

    monkeypatch.setattr(stage_router, "transition", refuse)
    response = _post(api, to_stage="FUNDED")
    assert response.status_code == 400
    assert response.json() == {"detail": "Stage change not allowed: INTAKE to FUNDED (illegal_jump INTAKE->FUNDED)."}
    assert api.db.commits == 1 and api.db.rollbacks == 0


def test_an_unknown_file_is_404(api, monkeypatch):
    def missing(db, fid, to, **kw):
        raise sm.LoanFileNotFound("loan file 7 not found")

    monkeypatch.setattr(stage_router, "transition", missing)
    response = _post(api)
    assert response.status_code == 404 and response.json() == {"detail": "Loan file not found"}


def test_an_unexpected_error_is_500_without_the_cause(api, monkeypatch):
    def boom(db, fid, to, **kw):
        raise RuntimeError("password=hunter2 in the SQL")

    monkeypatch.setattr(stage_router, "transition", boom)
    response = _post(api)
    assert response.status_code == 500 and "hunter2" not in response.text
    assert api.db.rollbacks == 1


def test_a_stage_that_does_not_exist_is_422(api):
    assert _post(api, to_stage="DONE").status_code == 422
    assert _post(api, extra_field=1).status_code == 422
