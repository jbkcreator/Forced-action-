"""R1 0.1: GHL "Funded" / "Paid" stages move the LINKED loan file through the state machine (no database)."""
from __future__ import annotations

from contextlib import nullcontext
from types import SimpleNamespace

import pytest

from src.lending import ghl_stage_bridge as bridge
from src.lending.contracts import LoanStage
from src.lending.state_machine import IllegalStageTransitionError, PaymentResult, TransitionResult


class _Db:
    """The loan file linked to the opportunity (None: no link), and the rowcount a link UPDATE reports."""

    def __init__(self, linked=None, link_rowcount=1):
        self.linked, self.link_rowcount, self.queries = linked, link_rowcount, []

    def execute(self, stmt, params=None):
        self.queries.append((stmt, params))
        if stmt is bridge._LINK:
            return SimpleNamespace(rowcount=self.link_rowcount)
        row = SimpleNamespace(id=self.linked[0], stage=self.linked[1]) if self.linked else None
        return SimpleNamespace(first=lambda: row)


@pytest.fixture
def calls(monkeypatch):
    seen: dict = {"transition": [], "payment": [], "card": [], "current": "funded"}

    def transition(db, file_id, target, **kw):
        seen["transition"].append((file_id, target, kw))
        return TransitionResult(file_id, LoanStage.CLOSING, target, "applied")

    def record_payment(db, file_id, **kw):
        seen["payment"].append((file_id, kw))
        return PaymentResult(file_id, True, 0, True)

    monkeypatch.setattr(bridge, "transition", transition)
    monkeypatch.setattr(bridge, "record_payment", record_payment)
    monkeypatch.setattr(bridge, "add_to_inbox_safely", lambda db, **kw: seen["card"].append(kw))
    monkeypatch.setattr(bridge, "current_stage",
                        lambda db, opp: (seen["current"], None) if seen["current"] else None)
    return seen


def test_a_stage_with_no_loan_meaning_is_ignored(calls):
    assert bridge.apply_stage(_Db((1, "CLOSING")), opportunity_id="o1", stage_name="Showed") is None
    assert not calls["transition"] and not calls["card"]


def test_funded_moves_the_linked_file_keyed_by_the_opportunity_stage_and_file_stage(calls):
    out = bridge.apply_stage(_Db((7, "CLOSING")), opportunity_id="o1", stage_name=" Funded ", phone="+18135550101")
    assert out == "applied"
    file_id, target, kw = calls["transition"][0]
    assert (file_id, target, kw["source"], kw["source_event_id"]) == (7, LoanStage.FUNDED, "ghl_stage", "o1:funded@CLOSING")


def test_paid_on_a_funded_file_is_a_payment_that_marks_it_paid(calls):
    calls["current"] = "paid"
    assert bridge.apply_stage(_Db((7, "FUNDED")), opportunity_id="o1", stage_name="Paid") == "applied"
    file_id, kw = calls["payment"][0]
    assert (file_id, kw["payment_id"], kw["mark_paid"]) == (7, "o1:paid@FUNDED", True) and not calls["transition"]


def test_paid_on_a_file_that_is_not_funded_goes_to_the_state_machine_to_be_refused(calls, monkeypatch):
    def refuse(db, file_id, target, **kw):
        raise IllegalStageTransitionError(file_id, LoanStage.CLOSING, target, "skips_stage")

    calls["current"] = "paid"
    monkeypatch.setattr(bridge, "transition", refuse)
    assert bridge.apply_stage(_Db((7, "CLOSING")), opportunity_id="o1", stage_name="paid") == "rejected"
    assert not calls["payment"]


def test_a_deal_with_no_linked_file_moves_nothing_whatever_the_phone(calls):
    """PR #473 review 1: a deal that exists only in GHL must never move another loan that shares the borrower's
    phone. Only an explicit link counts; the card says how to make one."""
    out = bridge.apply_stage(_Db(None), opportunity_id="o9", stage_name="Funded", phone="+18135550101")
    assert out == "unlinked" and not calls["transition"] and not calls["payment"]
    card = calls["card"][0]
    assert card["event_type"] == "ghl_stage_unlinked" and "--link <loan file id> o9" in card["summary"]


def test_the_phone_is_never_used_to_pick_a_file(calls):
    db = _Db((7, "CLOSING"))
    bridge.apply_stage(db, opportunity_id="o1", stage_name="Funded", phone="+18135550101")
    assert all("phone" not in (params or {}) for _, params in db.queries)
    assert db.queries[0][1] == {"opp": "o1"}


def test_a_late_copy_of_funded_after_the_deal_moved_to_lost_is_skipped(calls):
    """PR #473 review 6: the event applies only while it is still the deal's current GHL stage."""
    calls["current"] = "lost"
    assert bridge.apply_stage(_Db((7, "CLOSING")), opportunity_id="o1", stage_name="Funded") == "stale"
    assert not calls["transition"] and not calls["card"]


def test_an_opportunity_with_no_recorded_stage_is_skipped(calls):
    calls["current"] = None
    assert bridge.apply_stage(_Db((7, "CLOSING")), opportunity_id="o1", stage_name="Funded") == "stale"


def test_a_too_early_move_and_the_later_real_one_get_different_keys(calls):
    bridge.apply_stage(_Db((7, "SUBMITTED")), opportunity_id="o1", stage_name="Funded")
    bridge.apply_stage(_Db((7, "CLOSING")), opportunity_id="o1", stage_name="Funded")
    keys = [kw["source_event_id"] for _, _, kw in calls["transition"]]
    assert keys == ["o1:funded@SUBMITTED", "o1:funded@CLOSING"]


def test_a_redelivery_while_nothing_changed_reuses_the_key(calls):
    for _ in range(2):
        bridge.apply_stage(_Db((7, "CLOSING")), opportunity_id="o1", stage_name="Funded")
    assert {kw["source_event_id"] for _, _, kw in calls["transition"]} == {"o1:funded@CLOSING"}


def test_linking_applies_the_deals_current_stage_and_records_the_link(calls, monkeypatch):
    recorded = []
    monkeypatch.setattr(bridge.deal_ledger, "record", lambda db, file_id, event_type, **kw: recorded.append(
        (file_id, event_type, kw["diff"], kw["dedupe_key"])) or 1)
    assert bridge.link(_Db((7, "CLOSING")), 7, "o1", actor="human:cli:josh") == "applied"
    assert calls["transition"][0][0] == 7
    assert recorded == [(7, "field_update", {"ghl_opportunity_id": {"before": None, "after": "o1"}}, "ghl_link:7:o1")]


def test_a_link_to_a_missing_or_already_linked_file_is_refused(calls):
    with pytest.raises(ValueError):
        bridge.link(_Db((7, "CLOSING"), link_rowcount=0), 7, "o1")
    assert not calls["transition"]


def test_the_replay_entry_point_applies_a_saved_event(calls):
    out = bridge.apply_stage_payload(_Db((7, "CLOSING")), {"opportunity_id": "o1", "stage_name": "Funded",
                                                           "phone": "+1813"})
    assert out == "applied" and calls["transition"][0][2]["source_event_id"] == "o1:funded@CLOSING"


def test_every_stage_the_bridge_maps_counts_as_closed_for_the_scoreboard_and_document_chase():
    from config.lending_closing import CLOSED_STAGE_KEYS

    assert set(bridge.GHL_STAGE_TO_LOAN_STAGE) <= CLOSED_STAGE_KEYS


# ── The webhook (PR #473 review 5) ─────────────────────────────────────────────

def _router(monkeypatch, saved_id):
    from src.api import lending_ghl_router as router

    saved = []

    def boom(db, **kwargs):
        raise RuntimeError("database restarting")

    monkeypatch.setattr(router, "apply_stage", boom)
    monkeypatch.setattr(router, "report_webhook_failure",
                        lambda endpoint, body, exc: saved.append((endpoint, body)) or saved_id)
    return router, saved


def test_a_failed_loan_update_is_saved_for_retry_and_ghl_gets_its_200(monkeypatch):
    router, saved = _router(monkeypatch, saved_id=41)
    router._apply_loan_stage(SimpleNamespace(begin_nested=nullcontext), "o1", "Funded", "+18135550101")
    assert saved == [("ghl_loan_stage", {"opportunity_id": "o1", "stage_name": "Funded", "phone": "+18135550101"})]


def test_when_the_retry_cannot_be_saved_the_webhook_fails_so_ghl_sends_it_again(monkeypatch):
    router, saved = _router(monkeypatch, saved_id=None)
    with pytest.raises(RuntimeError):
        router._apply_loan_stage(SimpleNamespace(begin_nested=nullcontext), "o1", "Funded", "+18135550101")
    assert len(saved) == 1


def test_the_saved_event_is_replayed_through_the_bridge():
    from src.lending import webhook_failures

    assert webhook_failures.ENDPOINT_HANDLERS[webhook_failures.GHL_LOAN_STAGE] is bridge.apply_stage_payload


def _stage_write_fails(monkeypatch):
    from src.api import lending_ghl_router as router

    def boom(db, **kwargs):
        raise RuntimeError("database restarting")

    monkeypatch.setattr(router, "upsert_current_stage", boom)
    return router


@pytest.mark.parametrize("stage", ["Funded", " paid "])
def test_a_failed_stage_write_for_funded_or_paid_fails_the_webhook_so_ghl_sends_it_again(monkeypatch, stage):
    """PR #473 re-review: without the current-stage write the bridge would skip a valid Funded / Paid as stale."""
    router = _stage_write_fails(monkeypatch)
    with pytest.raises(RuntimeError):
        router._record_current_stage(SimpleNamespace(begin_nested=nullcontext), "o1", stage, None, repeat=False)


def test_a_failed_stage_write_for_another_stage_is_only_logged(monkeypatch):
    router = _stage_write_fails(monkeypatch)
    router._record_current_stage(SimpleNamespace(begin_nested=nullcontext), "o1", "Showed", None, repeat=False)
