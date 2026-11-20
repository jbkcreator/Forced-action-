"""The Milo Confirm / Cancel card (team decision Q44): the full application in Slack, approvers only, one package one
application, and nothing sent while no Milo filler is connected."""
from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path

import pytest

from config.lending_milo import ACTION_CANCEL, ACTION_CONFIRM
from packages.agent_core.redaction import slack_escape
from src.lending.milo import slack_card
from src.lending.milo.mapping import map_package
from src.lending.milo.package import load_package
from src.lending.milo.submissions import Claim

PACKAGE = Path(__file__).resolve().parents[2] / "config" / "lending_milo_test_package.json"
JOSH = "U_JOSH"


@pytest.fixture
def application():
    return map_package(load_package(PACKAGE))


def test_the_card_shows_every_field_the_flags_and_both_buttons(application):
    blocks = slack_card.card_blocks(7, application, is_test=True)
    text = "\n".join(str(block) for block in blocks)
    for value in (*application.step1.values(), *application.step2_property.values(),
                  *application.step2_borrower.values(), *application.checks.values()):
        if value:
            assert slack_escape(value) in text  # Slack markup escaped, e.g. "Fix &amp; flip"
    assert "TEST - " in blocks[0]["text"]["text"] and "Check before confirming" in text
    buttons = blocks[-1]["elements"]
    assert [(b["action_id"], b["value"]) for b in buttons] == [(ACTION_CONFIRM, "7"), (ACTION_CANCEL, "7")]
    assert "confirm" in buttons[0]  # Slack asks "Submit to Central Lending?" before the click counts


def test_a_stored_application_reads_back_unchanged(application):
    assert slack_card.application_from_json(application.to_json()) == application


# ── clicks ───────────────────────────────────────────────────────────────────

class _Result:
    def __init__(self, row):
        self._row = row

    def mappings(self):
        return self

    def first(self):
        return self._row


class _Db:
    def __init__(self, card):
        self.card, self.decided, self.commits = card, [], 0

    def execute(self, statement, params):
        if "FOR UPDATE" in str(statement):
            return _Result(self.card)
        self.decided.append(params["status"])
        return None

    def commit(self):
        self.commits += 1

    def rollback(self):
        pass


class _Slack:
    def __init__(self):
        self.ephemeral, self.updates = [], []

    def chat_postEphemeral(self, **kwargs):
        self.ephemeral.append(kwargs["text"])

    def chat_update(self, **kwargs):
        self.updates.append(kwargs)


def _click(action_id, user=JOSH):
    return {"type": "block_actions", "user": {"id": user}, "channel": {"id": "C_APPROVALS"},
            "actions": [{"action_id": action_id, "value": "7"}],
            "message": {"ts": "1.0", "text": "Milo", "blocks": [{"type": "header"}, {"type": "actions"}]}}


def _run(application, action_id, *, user=JOSH, submitter=None, status="open"):
    db = _Db({"id": 7, "package_ref": application.package_ref, "application": application.to_json(),
              "is_test": True, "status": status})

    @contextmanager
    def session():
        yield db

    slack = _Slack()
    outcome = slack_card.handle_action(_click(action_id, user), slack, approvers=frozenset({JOSH}),
                                       submitter=submitter, session_factory=session)
    return outcome, db, slack


def test_only_an_approver_may_press_either_button(application):
    outcome, db, slack = _run(application, ACTION_CONFIRM, user="U_SOMEONE")
    assert outcome == "not_allowed" and db.decided == [] and "approvers" in slack.ephemeral[0]


def test_cancel_closes_the_card_and_sends_nothing(application):
    outcome, db, slack = _run(application, ACTION_CANCEL)
    assert outcome == "cancelled" and db.decided == ["cancelled"]
    assert "Nothing was sent" in slack.updates[0]["blocks"][-1]["elements"][0]["text"]
    assert all(block["type"] != "actions" for block in slack.updates[0]["blocks"])


def test_confirm_with_no_milo_filler_sends_nothing_and_keeps_the_card_open(application):
    outcome, db, slack = _run(application, ACTION_CONFIRM, submitter=None)
    assert outcome == "no_submitter" and db.decided == [] and slack.updates == []
    assert "not connected" in slack.ephemeral[0]


def test_a_decided_card_cannot_be_pressed_again(application):
    outcome, db, _ = _run(application, ACTION_CONFIRM, submitter=lambda app: "M-1", status="cancelled")
    assert outcome == "already_decided" and db.decided == []


def test_confirm_submits_once_through_the_duplicate_guard(application, monkeypatch):
    calls = []
    monkeypatch.setattr(slack_card.submissions, "claim", lambda db, app, **kw: calls.append(("claim", kw)) or Claim(True))
    monkeypatch.setattr(slack_card.submissions, "mark_submitted", lambda db, ref, milo_ref: calls.append(("done", milo_ref)))
    sent = []
    outcome, db, slack = _run(application, ACTION_CONFIRM, submitter=lambda app: sent.append(app) or "M-1")
    assert outcome == "submitted" and sent == [application] and db.decided == ["confirmed"]
    assert calls == [("claim", {"is_test": True, "requested_by": f"slack:{JOSH}"}), ("done", "M-1")]
    assert "M-1" in slack.updates[0]["blocks"][-1]["elements"][0]["text"]


def test_a_package_already_in_milo_is_not_sent_again(application, monkeypatch):
    monkeypatch.setattr(slack_card.submissions, "claim",
                        lambda db, app, **kw: Claim(False, existing_status="submitted", milo_ref="M-1"))
    sent = []
    outcome, db, slack = _run(application, ACTION_CONFIRM, submitter=lambda app: sent.append(app))
    assert outcome == "duplicate" and sent == [] and "already submitted" in slack.ephemeral[0]


def test_a_refused_fill_is_recorded_and_the_card_stays_open(application, monkeypatch):
    monkeypatch.setattr(slack_card.submissions, "claim", lambda db, app, **kw: Claim(True))
    failed = []
    monkeypatch.setattr(slack_card.submissions, "mark_failed", lambda db, ref, error: failed.append(error))

    def refused(app):
        raise RuntimeError("Milo said no")

    outcome, db, slack = _run(application, ACTION_CONFIRM, submitter=refused)
    assert outcome == "failed" and failed == ["RuntimeError"] and db.decided == [] and slack.updates == []


def test_the_listener_routes_milo_clicks_off_the_ack(monkeypatch):
    from src.lending.routing import slack_extension

    later = []
    routing = slack_extension.RoutingSlack(web_client=object(), run_later=lambda fn, *args: later.append(fn))
    assert routing.handle("interactive", _click(ACTION_CONFIRM)) is True
    assert later == [slack_extension._milo_click]
