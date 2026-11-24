"""R1 0.1: GHL "Funded" / "Paid" stages move the loan file through the state machine (no database)."""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from src.lending import ghl_stage_bridge as bridge
from src.lending.contracts import LoanStage
from src.lending.state_machine import IllegalStageTransitionError, PaymentResult, TransitionResult


class _Db:
    def __init__(self, files):
        self.files = files

    def execute(self, *_args, **_kwargs):
        return SimpleNamespace(all=lambda: [SimpleNamespace(id=i, stage=s) for i, s in self.files])


@pytest.fixture
def calls(monkeypatch):
    seen: dict[str, list] = {"transition": [], "payment": [], "card": []}

    def transition(db, file_id, target, **kw):
        seen["transition"].append((file_id, target, kw))
        return TransitionResult(file_id, LoanStage.CLOSING, target, "applied")

    def record_payment(db, file_id, **kw):
        seen["payment"].append((file_id, kw))
        return PaymentResult(file_id, True, 0, True)

    monkeypatch.setattr(bridge, "transition", transition)
    monkeypatch.setattr(bridge, "record_payment", record_payment)
    monkeypatch.setattr(bridge, "add_to_inbox_safely", lambda db, **kw: seen["card"].append(kw))
    return seen


def test_a_stage_with_no_loan_meaning_is_ignored(calls):
    assert bridge.apply_stage(_Db([(1, "CLOSING")]), opportunity_id="o1", stage_name="Showed", phone="+18135550101") is None
    assert calls == {"transition": [], "payment": [], "card": []}


def test_funded_moves_the_file_keyed_by_the_opportunity_and_stage(calls):
    out = bridge.apply_stage(_Db([(7, "CLOSING")]), opportunity_id="o1", stage_name=" Funded ", phone="+18135550101")
    assert out == "applied"
    file_id, target, kw = calls["transition"][0]
    assert (file_id, target, kw["source"], kw["source_event_id"]) == (7, LoanStage.FUNDED, "ghl_stage", "o1:funded")


def test_paid_on_a_funded_file_is_a_payment_that_marks_it_paid(calls):
    out = bridge.apply_stage(_Db([(7, "FUNDED")]), opportunity_id="o1", stage_name="Paid", phone="+18135550101")
    assert out == "applied" and not calls["transition"]
    file_id, kw = calls["payment"][0]
    assert (file_id, kw["payment_id"], kw["mark_paid"]) == (7, "o1:paid", True)


def test_paid_on_a_file_that_is_not_funded_goes_to_the_state_machine_to_be_refused(calls, monkeypatch):
    def refuse(db, file_id, target, **kw):
        raise IllegalStageTransitionError(file_id, LoanStage.CLOSING, target, "skips_stage")

    monkeypatch.setattr(bridge, "transition", refuse)
    assert bridge.apply_stage(_Db([(7, "CLOSING")]), opportunity_id="o1", stage_name="paid", phone="+1813") == "rejected"
    assert not calls["payment"]


@pytest.mark.parametrize("files", [[], [(1, "FUNDED"), (2, "FUNDED")]])
def test_no_file_or_several_files_for_the_phone_moves_nothing_and_cards_josh(calls, files):
    assert bridge.apply_stage(_Db(files), opportunity_id="o1", stage_name="Funded", phone="+1813") == "unmatched"
    assert not calls["transition"] and calls["card"][0]["event_type"] == "ghl_stage_unmatched"


def test_no_phone_is_unmatched(calls):
    assert bridge.apply_stage(_Db([(1, "CLOSING")]), opportunity_id="o1", stage_name="Funded", phone=None) == "unmatched"


def test_every_stage_the_bridge_maps_counts_as_closed_for_the_scoreboard_and_document_chase():
    from config.lending_closing import CLOSED_STAGE_KEYS

    assert set(bridge.GHL_STAGE_TO_LOAN_STAGE) <= CLOSED_STAGE_KEYS
