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
    """A small fake GHL: contacts by id, a search that matches the query against email and phone, an upsert that
    creates a contact, and a card list. Every call is recorded."""

    def __init__(self, contacts: list[dict[str, Any]] | None = None, cards: list[dict[str, Any]] | None = None) -> None:
        super().__init__(ACCOUNT)
        self.calls: list[tuple[str, str, str, dict[str, Any]]] = []
        self.contacts = {c["id"]: dict(c) for c in (contacts or [])}
        self._cards = cards or []

    def _call(self, step: str, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        self.calls.append((step, method, path, kwargs))
        body = kwargs.get("json", {})
        if step == "contact search":
            query = body["query"].lower()
            digits = "".join(ch for ch in query if ch.isdigit())
            return {"contacts": [c for c in self.contacts.values()
                                 if query == (c.get("email") or "").lower()
                                 or (digits and digits in "".join(ch for ch in (c.get("phone") or "") if ch.isdigit()))]}
        if step == "contact upsert":
            self.contacts["c-new"] = {"id": "c-new", "email": body["email"], "tags": []}
            return {"contact": {"id": "c-new"}, "new": True}
        if step == "contact read":
            return {"contact": self.contacts[path.rsplit("/", 1)[1]]}
        if step == "contact phone":
            self.contacts[path.rsplit("/", 1)[1]]["phone"] = body["phone"]
        if step == "remove cold tag":
            cid = path.split("/")[2]
            self.contacts[cid]["tags"] = [t for t in self.contacts[cid].get("tags", []) if t not in body["tags"]]
        if step == "add tags":
            cid = path.split("/")[2]
            self.contacts[cid]["tags"] = [*self.contacts[cid].get("tags", []), *body["tags"]]
        if step == "opportunity search":
            return {"opportunities": self._cards}
        return {}

    def steps(self) -> list[str]:
        return [c[0] for c in self.calls]

    def body(self, step: str) -> dict[str, Any]:
        return next(c[3].get("json", {}) for c in self.calls if c[0] == step)

    def writes(self) -> list[tuple[str, str]]:
        return [(c[1], c[2]) for c in self.calls if c[1] != "GET" and c[0] not in {"contact search"}]


COLD_CONTACT = {"id": "c-cold", "email": "mark.lee@example.com", "phone": "+18135550100", "tags": ["ndl-cold", "ndl-src-x"],
                "dndSettings": {"Email": {"status": "active", "message": "Cold lead: calls only"},
                                "SMS": {"status": "active"}}}
WARM_CONTACT = {"id": "c-warm", "email": "mark.lee@example.com", "tags": ["ndl-cold"]}


@pytest.fixture
def pipeline(monkeypatch):
    settings = SimpleNamespace(lending_ghl_pipeline_id="pipe-1", lending_ghl_stage_new_lead="stage-1",
                               lending_instantly_ghl_handoff_enabled=True)
    monkeypatch.setattr(ho, "get_settings", lambda: settings)
    monkeypatch.setattr(ho, "is_globally_halted", lambda: False)
    return settings


def test_a_deal_reply_from_a_new_address_creates_the_contact_card_and_note(pipeline):
    handoff = RecordingHandoff()
    assert handoff(row()) == "c-new"
    assert handoff.steps() == ["contact search", "contact upsert", "contact read", "add tags", "opportunity search",
                               "opportunity create", "note"]
    assert handoff.body("contact upsert") == {"locationId": "loc-1", "email": "mark.lee@example.com", "firstName": "Mark"}
    assert handoff.body("add tags") == {"tags": [GHL_TAG_INSTANTLY_INTERESTED]}
    card = handoff.body("opportunity create")
    assert card["pipelineId"] == "pipe-1" and card["pipelineStageId"] == "stage-1" and card["contactId"] == "c-new"


def test_the_upsert_never_carries_dnd_tags_source_or_phone_even_when_the_lead_gave_a_phone(pipeline):
    handoff = RecordingHandoff()
    handoff(row(reply_text="Interested, call me on 813-555-0142"))
    assert not {"dnd", "dndSettings", "tags", "source", "phone"} & set(handoff.body("contact upsert"))


def test_an_existing_contact_is_found_by_email_not_upserted(pipeline):
    handoff = RecordingHandoff(contacts=[WARM_CONTACT])
    assert handoff(row()) == "c-warm"
    assert "contact upsert" not in handoff.steps()


def test_an_existing_card_is_never_moved_or_duplicated(pipeline):
    handoff = RecordingHandoff(contacts=[WARM_CONTACT], cards=[{"id": "opp-1", "pipelineStageId": "booked"}])
    handoff(row())
    assert "opportunity create" not in handoff.steps()
    assert not any(c[1] == "PUT" for c in handoff.calls)


def test_without_a_pipeline_the_lead_is_contact_only(pipeline):
    pipeline.lending_ghl_pipeline_id = None
    handoff = RecordingHandoff()
    handoff(row())
    assert "opportunity search" not in handoff.steps() and "opportunity create" not in handoff.steps()
    assert "add tags" in handoff.steps()


# ── review finding 1: a cold contact keeps its Instantly route while its email do-not-disturb is on ─────────

def test_a_cold_contact_with_email_do_not_disturb_keeps_ndl_cold_and_its_do_not_disturb(pipeline):
    handoff = RecordingHandoff(contacts=[COLD_CONTACT])
    handoff(row())
    assert "remove cold tag" not in handoff.steps()
    assert "ndl-cold" in handoff.contacts["c-cold"]["tags"] and GHL_TAG_INSTANTLY_INTERESTED in handoff.contacts["c-cold"]["tags"]
    assert handoff.contacts["c-cold"]["dndSettings"]["Email"]["status"] == "active"  # never lifted here
    assert "follow-up emails stay on Instantly" in handoff.body("note")["body"]


def test_a_follow_up_agent_email_to_that_contact_still_takes_the_instantly_route_not_a_blocked_ghl_one(pipeline):
    from src.lending.agent_jobs.checks import rail_for
    from src.lending.ghl_crm import _contact_from

    handoff = RecordingHandoff(contacts=[COLD_CONTACT])
    handoff(row())
    after = _contact_from(handoff.contacts["c-cold"])
    assert rail_for(SimpleNamespace(type="send_email"), after) == "instantly"


def test_a_contact_with_open_email_loses_ndl_cold_so_later_emails_go_through_ghl(pipeline):
    handoff = RecordingHandoff(contacts=[WARM_CONTACT])
    handoff(row())
    assert handoff.body("remove cold tag") == {"tags": [GHL_TAG_COLD_LEAD]}


def test_a_contact_without_ndl_cold_has_nothing_removed(pipeline):
    handoff = RecordingHandoff(contacts=[{"id": "c-x", "email": "mark.lee@example.com", "tags": []}])
    handoff(row())
    assert "remove cold tag" not in handoff.steps()


# ── review finding 2: the contact is chosen by email; a phone can never select another contact ─────────────

OTHER = {"id": "c-office", "email": "front.desk@example.com", "phone": "+18135550142", "tags": ["ndl-cold"]}
LEAD_CONTACT = {"id": "c-lead", "email": "mark.lee@example.com", "tags": []}


def test_a_phone_owned_by_another_contact_never_redirects_the_handoff(pipeline):
    handoff = RecordingHandoff(contacts=[LEAD_CONTACT, OTHER])
    assert handoff(row(reply_text="Interested, our office line is 813-555-0142")) == "c-lead"
    assert all("/c-office" not in path for _, path in handoff.writes())  # the other contact is untouched
    assert handoff.contacts["c-office"]["tags"] == ["ndl-cold"]
    assert "phone" not in handoff.contacts["c-lead"]  # not attached to the lead either
    assert "already on another GHL contact" in handoff.body("note")["body"]
    assert handoff.body("opportunity create")["contactId"] == "c-lead"


def test_a_free_phone_is_attached_to_the_contact_found_by_email(pipeline):
    handoff = RecordingHandoff(contacts=[LEAD_CONTACT])
    handoff(row(reply_text="Interested, call 813-555-0142"))
    assert handoff.contacts["c-lead"]["phone"] == "+18135550142"
    assert handoff.body("contact phone") == {"phone": "+18135550142"}


def test_a_contact_that_already_has_a_different_phone_is_left_alone(pipeline):
    handoff = RecordingHandoff(contacts=[{**LEAD_CONTACT, "phone": "+18135550199"}])
    handoff(row(reply_text="Interested, call 813-555-0142"))
    assert "contact phone" not in handoff.steps()
    assert "different phone number" in handoff.body("note")["body"]


def test_the_same_phone_already_on_the_contact_changes_nothing(pipeline):
    handoff = RecordingHandoff(contacts=[{**LEAD_CONTACT, "phone": "+18135550142"}])
    handoff(row(reply_text="Interested, call 813-555-0142"))
    assert "contact phone" not in handoff.steps()


def test_a_number_in_the_quoted_earlier_email_is_not_taken(pipeline):
    handoff = RecordingHandoff(contacts=[LEAD_CONTACT])
    handoff(row(reply_text="Interested\n\nOn Mon, Josh wrote:\n> Call us at (656) 221-1100"))
    assert "contact phone" not in handoff.steps()


def test_two_contacts_sharing_the_email_use_the_lowest_id_every_time_and_say_so(pipeline):
    twin = {**LEAD_CONTACT, "id": "c-zzz"}
    first = RecordingHandoff(contacts=[twin, LEAD_CONTACT])
    second = RecordingHandoff(contacts=[LEAD_CONTACT, twin])
    assert first(row()) == second(row()) == "c-lead"
    assert "share this email" in first.body("note")["body"]


def test_a_failing_phone_write_does_not_stop_the_tag_card_and_note(pipeline):
    class RejectsPhone(RecordingHandoff):
        def _call(self, step, method, path, **kwargs):
            if step == "contact phone":
                raise DeliveryError("contact phone: HTTP 400")
            return super()._call(step, method, path, **kwargs)

    handoff = RejectsPhone(contacts=[LEAD_CONTACT])
    assert handoff(row(reply_text="Interested, call 813-555-0142")) == "c-lead"
    assert {"add tags", "opportunity create", "note"} <= set(handoff.steps())
    assert "could not be added" in handoff.body("note")["body"]


def test_a_contact_with_global_do_not_disturb_keeps_ndl_cold(pipeline):
    handoff = RecordingHandoff(contacts=[{**WARM_CONTACT, "dnd": True}])
    handoff(row())
    assert "remove cold tag" not in handoff.steps()


@pytest.mark.parametrize("contact", [
    {**LEAD_CONTACT, "tags": ["lending-opt-out"]},
    {**LEAD_CONTACT, "dndSettings": {"Call": {"status": "active"}, "Email": {"status": "active"}}},
])
def test_a_contact_already_opted_out_in_ghl_is_not_handed_over(pipeline, contact):
    handoff = RecordingHandoff(contacts=[contact])
    assert handoff(row()) == "c-lead"
    assert handoff.writes() == []  # no tag, no card, no note


def test_nothing_is_written_while_the_emergency_halt_is_on(pipeline, monkeypatch):
    monkeypatch.setattr(ho, "is_globally_halted", lambda: True)
    handoff = RecordingHandoff(contacts=[LEAD_CONTACT])
    with pytest.raises(DeliveryError, match="halt"):
        handoff(row())
    assert handoff.calls == []


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

    handoff = Failing(contacts=[WARM_CONTACT])
    with pytest.raises(DeliveryError):
        handoff(row())
    assert handoff.steps() == ["contact search", "contact read", "add tags"]


def test_the_optout_read_covers_the_suppression_list_too():
    session = Session([row()], opted_out=True)
    sweep(session, DEAL, lambda r: pytest.fail("handed over an opted-out address"))
    sql = next(q for q, _ in session.executed if "lending.suppression_list WHERE email" in q)
    assert "email_opt_outs" in sql


# ── the first answer ────────────────────────────────────────────────────────

def test_the_first_answer_is_still_a_draft_in_the_same_instantly_thread():
    session, drafts = Session([row()]), []
    proc = ReplyProcessor(session_factory=lambda: session, client=FakeModel(DEAL), model="m",
                          enqueue=lambda **kw: drafts.append(kw) or 99, post_card=lambda a: True,
                          post_triage=lambda t: True, post_notice=lambda t: True, hand_off=lambda r: "c-1")
    proc.run()
    assert drafts[0]["channel"] == "instantly_email" and drafts[0]["payload"]["reply_to_uuid"] == "em-1"
    assert drafts[0]["payload"]["from_inbox"] == "josh@ndl-mail.com"
