"""P1.3: a Deal reply to an NDL cold email is handed to GHL (contact, pipeline card, tag swap). GHL is faked."""
from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from config.lending_compliance import GHL_TAG_COLD_LEAD, GHL_TAG_INSTANTLY_INTERESTED
from src.lending import instantly_ghl_handoff as ho
from src.lending.ghl_account import GhlAccount
from src.lending.instantly_replies import ReplyProcessor
from src.lending.web_leads import DeliveryError
from tests.lending.test_instantly_replies import DEAL, FakeModel, FakeSession, _Result, row

OBJECTION = '{"intent": "objection", "summary": "Already has a lender", "subject": "Re: Funding", "body": "Understood."}'
ACCOUNT = GhlAccount(api_key="k", location_id="loc-1")


class RecordingHandoff(ho.InstantlyGhlHandoff):
    def __init__(self, cards: list[dict[str, Any]] | None = None) -> None:
        super().__init__(ACCOUNT)
        self.calls: list[tuple[str, str, str, dict[str, Any]]] = []
        self._cards = cards or []

    def _call(self, step: str, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        self.calls.append((step, method, path, kwargs))
        if step == "contact upsert":
            return {"contact": {"id": "c-1"}, "new": True}
        if step == "opportunity search":
            return {"opportunities": self._cards}
        return {}

    def steps(self) -> list[str]:
        return [c[0] for c in self.calls]

    def body(self, step: str) -> dict[str, Any]:
        return next(c[3].get("json", {}) for c in self.calls if c[0] == step)


@pytest.fixture
def pipeline(monkeypatch):
    settings = SimpleNamespace(lending_ghl_pipeline_id="pipe-1", lending_ghl_stage_new_lead="stage-1",
                               lending_instantly_ghl_handoff_enabled=True)
    monkeypatch.setattr(ho, "get_settings", lambda: settings)
    return settings


def test_a_deal_reply_creates_contact_card_tag_swap_and_note(pipeline):
    handoff = RecordingHandoff()
    assert handoff(row()) == "c-1"
    assert handoff.steps() == ["contact upsert", "add tags", "remove cold tag", "opportunity search",
                               "opportunity create", "note"]
    upsert = handoff.body("contact upsert")
    assert upsert == {"locationId": "loc-1", "email": "mark.lee@example.com", "firstName": "Mark"}
    assert handoff.body("add tags") == {"tags": [GHL_TAG_INSTANTLY_INTERESTED]}
    assert handoff.body("remove cold tag") == {"tags": [GHL_TAG_COLD_LEAD]}
    card = handoff.body("opportunity create")
    assert card["pipelineId"] == "pipe-1" and card["pipelineStageId"] == "stage-1" and card["contactId"] == "c-1"


def test_the_upsert_never_carries_dnd_tags_source_or_phone(pipeline):
    handoff = RecordingHandoff()
    handoff(row())
    assert not {"dnd", "dndSettings", "tags", "source", "phone"} & set(handoff.body("contact upsert"))


def test_an_existing_card_is_never_moved_or_duplicated(pipeline):
    handoff = RecordingHandoff(cards=[{"id": "opp-1", "pipelineStageId": "booked"}])
    handoff(row())
    assert "opportunity create" not in handoff.steps()
    assert not any(c[1] == "PUT" for c in handoff.calls)


def test_without_a_pipeline_the_lead_is_contact_only(pipeline):
    pipeline.lending_ghl_pipeline_id = None
    handoff = RecordingHandoff()
    handoff(row())
    assert "opportunity search" not in handoff.steps() and "opportunity create" not in handoff.steps()
    assert "add tags" in handoff.steps()


def test_the_handoff_is_off_by_default(monkeypatch):
    monkeypatch.setattr(ho, "get_settings", lambda: SimpleNamespace(lending_instantly_ghl_handoff_enabled=False))
    assert ho.get_handoff() is None


def test_the_handoff_needs_the_ghl_account(monkeypatch):
    monkeypatch.setattr(ho, "get_settings", lambda: SimpleNamespace(lending_instantly_ghl_handoff_enabled=True))
    monkeypatch.setattr(ho, "lending_ghl_account", lambda: None)
    assert ho.get_handoff() is None


# ── in Cora's reply sweep ───────────────────────────────────────────────────

class Session(FakeSession):
    def __init__(self, rows, opted_out: bool = False) -> None:
        super().__init__(rows)
        self._opted_out = opted_out

    def execute(self, stmt, params=None):
        if "lending.suppression_list WHERE email" in str(stmt):  # the hand-off's opt-out read
            self.executed.append((str(stmt), params or {}))
            return _Result(scalar=self._opted_out)
        return super().execute(stmt, params)


def sweep(session, model_json, hand_off):
    proc = ReplyProcessor(session_factory=lambda: session, client=FakeModel(model_json), model="m",
                          enqueue=lambda **kw: 99, post_card=lambda action_id: True, post_triage=lambda t: True,
                          post_notice=lambda t: True, hand_off=hand_off)
    return proc.run()


def test_a_deal_reply_is_handed_over_once_and_still_answered():
    seen = []
    result = sweep(Session([row()]), DEAL, seen.append)
    assert [r["id"] for r in seen] == [7]
    assert result.drafted == 1 and result.triaged == 1


def test_an_objection_is_not_handed_over():
    seen = []
    sweep(Session([row()]), OBJECTION, seen.append)
    assert seen == []


def test_an_opted_out_address_is_not_handed_over():
    seen = []
    sweep(Session([row()], opted_out=True), DEAL, seen.append)
    assert seen == []


def test_a_failed_handoff_never_blocks_the_reply_and_is_raised_to_the_inbox():
    def boom(_):
        raise RuntimeError("GHL down for mark.lee@example.com")

    session = Session([row()])
    result = sweep(session, DEAL, boom)
    assert result.triaged == 1 and result.drafted == 1
    events = [p for p in session.params("INSERT INTO lending.cora_inbox")]
    assert {"instantly_ghl_handoff_failed", "instantly_reply"} <= {e["event_type"] for e in events}
    failed = next(e for e in events if e["event_type"] == "instantly_ghl_handoff_failed")
    assert "mark.lee@example.com" not in str(failed)


def test_no_handoff_is_the_default():
    result = sweep(Session([row()]), DEAL, None)
    assert result.triaged == 1


DEAD = '{"intent": "dead", "summary": "Not interested"}'
STOP = '{"intent": "stop", "summary": "Asks to be removed"}'


@pytest.mark.parametrize("model_json", [DEAD, STOP])
def test_dead_and_stop_replies_are_not_handed_over(model_json):
    seen = []
    sweep(Session([row()]), model_json, seen.append)
    assert seen == []


def test_the_failed_step_is_named_in_the_inbox_item_without_the_address():
    def boom(_):
        raise DeliveryError("add tags: HTTP 502")

    session = Session([row()])
    sweep(session, DEAL, boom)
    failed = next(e for e in session.params("INSERT INTO lending.cora_inbox")
                  if e["event_type"] == "instantly_ghl_handoff_failed")
    assert "add tags: HTTP 502" in str(failed) and "mark.lee@example.com" not in str(failed)


def test_a_partial_handoff_stops_at_the_failing_step(pipeline):
    class Failing(RecordingHandoff):
        def _call(self, step, method, path, **kwargs):
            if step == "remove cold tag":
                raise DeliveryError("remove cold tag: HTTP 500")
            return super()._call(step, method, path, **kwargs)

    handoff = Failing()
    with pytest.raises(DeliveryError):
        handoff(row())
    assert handoff.steps() == ["contact upsert", "add tags"]


def test_the_optout_read_covers_the_suppression_list_too():
    session = Session([row()], opted_out=True)
    sweep(session, DEAL, lambda r: pytest.fail("handed over an opted-out address"))
    sql = next(q for q, _ in session.executed if "lending.suppression_list WHERE email" in q)
    assert "email_opt_outs" in sql


# ── the recommended flow, point by point ────────────────────────────────────

def test_a_phone_number_in_the_reply_goes_on_the_contact_so_the_conversation_can_move_to_ghl(pipeline):
    handoff = RecordingHandoff()
    handoff(row(reply_text="Yes, call me at (813) 555-0142 tomorrow"))
    assert handoff.body("contact upsert")["phone"] == "+18135550142"


def test_a_number_in_the_quoted_earlier_email_is_not_taken(pipeline):
    handoff = RecordingHandoff()
    handoff(row(reply_text="Interested\n\nOn Mon, Josh wrote:\n> Call us at (656) 221-1100"))
    assert "phone" not in handoff.body("contact upsert")


def test_the_first_answer_is_still_a_draft_in_the_same_instantly_thread():
    session, drafts = Session([row()]), []
    proc = ReplyProcessor(session_factory=lambda: session, client=FakeModel(DEAL), model="m",
                          enqueue=lambda **kw: drafts.append(kw) or 99, post_card=lambda a: True,
                          post_triage=lambda t: True, post_notice=lambda t: True, hand_off=lambda r: "c-1")
    proc.run()
    assert drafts[0]["channel"] == "instantly_email" and drafts[0]["payload"]["reply_to_uuid"] == "em-1"
    assert drafts[0]["payload"]["from_inbox"] == "josh@ndl-mail.com"


def test_the_whole_recommendation_in_one_pass(pipeline):
    handoff = RecordingHandoff()
    handoff(row(reply_text="Interested, my number is 813-555-0142"))
    assert handoff.body("add tags") == {"tags": ["ndl-instantly-interested"]}
    assert handoff.body("remove cold tag") == {"tags": ["ndl-cold"]}
    assert handoff.body("opportunity create")["pipelineStageId"] == "stage-1"
    assert handoff.body("contact upsert")["email"] == "mark.lee@example.com"
