"""The package card's blocks and its click / form handlers, with fake Slack and fake actions."""
from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import date

import pytest

from config.lending_packaging import ACTION_DISMISS, ACTION_OVERRIDE, ACTION_SUBMIT, OVERRIDE_CALLBACK_ID, TAG_CALLBACK_ID
from src.lending.packaging import actions, slack_card
from src.lending.packaging.package import CompileResult
from src.lending.packaging.packet_validator import DocVerdict, Outcome, Verdict
from tests.lending.packaging.fakes import FakeSession

TODAY = date(2026, 11, 26)
FIT = {"misses": [{"lender_key": "abl", "lender_name": "Asset Based Lending (ABL)", "reasons": ["state not covered"]}],
       "possible": []}


def _result(*, confirmed=True, docs=(), status=None) -> CompileResult:
    outcomes = {d.outcome for d in docs}
    worst = Outcome.QUARANTINE if Outcome.QUARANTINE in outcomes else (
        Outcome.BLOCK if Outcome.BLOCK in outcomes else Outcome.PASS)
    status = status or ("blocked" if worst is not Outcome.PASS else ("ready" if confirmed else "draft"))
    return CompileResult(11, "loan_file", 7, "corevest", status, confirmed,
                         [{"item_code": "bank_statements", "label": "Bank statements", "received": False}],
                         Verdict(worst, tuple(docs)), FIT, None)


def _text(blocks) -> str:
    return json.dumps(blocks)


def _action_ids(blocks) -> list[str]:
    return [e["action_id"] for b in blocks if b["type"] == "actions" for e in b["elements"]]


BLOCKED = DocVerdict(3, "doc_chase_uploads", "bank_statements", Outcome.BLOCK, ("name not found: x",))
QUARANTINED = DocVerdict(4, "doc_chase_uploads", "bank_statements", Outcome.QUARANTINE, ("hidden text",))
UNTYPED = DocVerdict(5, "partner_file_uploads", None, Outcome.BLOCK, ("unclassified: x",))


def test_ready_package_offers_mark_submitted_and_lists_misses():
    blocks = slack_card.card_blocks(_result())
    assert _action_ids(blocks) == [ACTION_SUBMIT, ACTION_DISMISS]
    assert "state not covered" in _text(blocks) and "Bank statements" in _text(blocks)


def test_unconfirmed_box_is_stamped_and_mark_submitted_is_not_on_the_card():
    blocks = slack_card.card_blocks(_result(confirmed=False))
    assert "Box not confirmed" in _text(blocks) and ACTION_SUBMIT not in _action_ids(blocks)
    assert "not confirmed" in _text(blocks).split("Mark submitted is not available")[1]


def test_blocked_and_quarantined_documents_remove_mark_submitted_and_show_reasons():
    blocks = slack_card.card_blocks(_result(docs=(BLOCKED, QUARANTINED, UNTYPED)))
    ids = _action_ids(blocks)
    assert ACTION_SUBMIT not in ids
    assert f"{ACTION_OVERRIDE}:doc_chase_uploads:3" in ids  # override for a block
    assert not any(i.endswith(":4") for i in ids)  # never for a quarantine
    assert "lender_pkg_tag:5" in ids  # tag type for an untyped partner upload
    assert "name not found" in _text(blocks) and "hidden text" in _text(blocks)


def test_closed_package_card_has_no_buttons():
    assert _action_ids(slack_card.card_blocks(_result(status="submitted"))) == []


class FakeSlack:
    def __init__(self):
        self.calls = []

    def __getattr__(self, name):
        return lambda **kwargs: self.calls.append((name, kwargs)) or {"ts": "1.2"}


@contextmanager
def _session():
    yield FakeSession()


def _click(action_id, value, user="U1"):
    return {"type": "block_actions", "user": {"id": user}, "channel": {"id": "C1"}, "trigger_id": "T",
            "actions": [{"action_id": action_id, "value": value}]}


def test_only_approvers_can_click(monkeypatch):
    monkeypatch.setattr(actions, "mark_submitted", lambda *a, **k: pytest.fail("must not run"))
    slack = FakeSlack()
    slack_card.handle_action(_click(ACTION_SUBMIT, "11", user="U9"), slack, approvers=frozenset({"U1"}),
                             session_factory=_session, today=TODAY)
    assert slack.calls[0][0] == "chat_postEphemeral"


def test_mark_submitted_click_runs_the_action_as_the_clicking_human(monkeypatch):
    seen = {}
    monkeypatch.setattr(actions, "mark_submitted", lambda db, pid, *, actor, today: seen.update(pid=pid, actor=actor))
    slack_card.handle_action(_click(ACTION_SUBMIT, "11"), FakeSlack(), approvers=frozenset({"U1"}),
                             session_factory=_session, today=TODAY)
    assert seen == {"pid": 11, "actor": "human:U1"}


def test_a_refused_action_is_told_to_the_user(monkeypatch):
    def refuse(*a, **k):
        raise actions.ActionRefused("This lender's box is not confirmed. Do not submit.")

    monkeypatch.setattr(actions, "mark_submitted", refuse)
    slack = FakeSlack()
    slack_card.handle_action(_click(ACTION_SUBMIT, "11"), slack, approvers=frozenset({"U1"}),
                             session_factory=_session, today=TODAY)
    assert slack.calls[-1][0] == "chat_postEphemeral" and "not confirmed" in slack.calls[-1][1]["text"]


def test_override_click_opens_a_reason_form():
    slack = FakeSlack()
    value = json.dumps({"p": 11, "t": "doc_chase_uploads", "u": 3})
    payload = _click(f"{ACTION_OVERRIDE}:doc_chase_uploads:3", value)
    assert slack_card.is_package_action(payload) and slack_card.opens_form(payload)
    slack_card.handle_action(payload, slack, approvers=frozenset({"U1"}), session_factory=_session, today=TODAY)
    name, kwargs = slack.calls[0]
    assert name == "views_open" and kwargs["view"]["callback_id"] == OVERRIDE_CALLBACK_ID


def _form(callback_id, block, value, user="U1"):
    cell = ({"type": "static_select", "selected_option": {"value": value}} if block == "type_b"
            else {"type": "plain_text_input", "value": value})
    return {"type": "view_submission", "user": {"id": user},
            "view": {"callback_id": callback_id, "private_metadata": json.dumps({"p": 11, "t": "partner_file_uploads", "u": 5}),
                     "state": {"values": {block: {"v": cell}}}}}


def test_forms_are_checked_before_the_ack_and_applied_after():
    later = []
    run_later = lambda *args: later.append(args)  # noqa: E731
    approvers = frozenset({"U1"})
    empty = slack_card.handle_submission(_form(OVERRIDE_CALLBACK_ID, "reason_b", ""), None, run_later,
                                         approvers=approvers, session_factory=_session, today=TODAY)
    assert empty["response_action"] == "errors" and not later
    bad = slack_card.handle_submission(_form(TAG_CALLBACK_ID, "type_b", "selfie"), None, run_later,
                                       approvers=approvers, session_factory=_session, today=TODAY)
    assert bad["response_action"] == "errors"
    stranger = slack_card.handle_submission(_form(TAG_CALLBACK_ID, "type_b", "pricing_sheet", user="U9"), None,
                                            run_later, approvers=approvers, session_factory=_session, today=TODAY)
    assert stranger["response_action"] == "errors"
    ok = slack_card.handle_submission(_form(TAG_CALLBACK_ID, "type_b", "pricing_sheet"), None, run_later,
                                      approvers=approvers, session_factory=_session, today=TODAY)
    assert ok == {} and len(later) == 1


def test_tag_form_tags_the_document(monkeypatch):
    seen = {}
    monkeypatch.setattr(actions, "tag_document_type",
                        lambda db, uid, code, *, actor: seen.update(uid=uid, code=code, actor=actor))
    slack_card._apply_form(FakeSlack(), False, {"p": 11, "u": 5}, "pricing_sheet", "U1", _session, TODAY)
    assert seen == {"uid": 5, "code": "pricing_sheet", "actor": "human:U1"}


def test_post_card_remembers_the_card_and_attaches_the_pdf(tmp_path):
    db = FakeSession()
    slack = FakeSlack()
    result = _result()
    pdf = tmp_path / "p.pdf"
    pdf.write_bytes(b"%PDF")
    result = CompileResult(**{**result.__dict__, "deal_facts_path": str(pdf)})
    assert slack_card.post_card(db, slack, "C1", result) == "1.2"
    assert db.ran("SET card_channel = :c")[0] == {"c": "C1", "ts": "1.2", "id": 11}
    assert [c[0] for c in slack.calls] == ["chat_postMessage", "files_upload_v2"]
