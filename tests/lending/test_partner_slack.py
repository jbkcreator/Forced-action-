"""P-1 ``/partners`` in Cora's Slack app: permissions, field errors, the one-time link view and the audit post."""
from __future__ import annotations

import logging
from contextlib import contextmanager

import pytest
from sqlalchemy import text
from sqlalchemy.orm import Session

from config.settings import get_settings
from src.lending import partner_slack, partners
from src.lending.routing.slack_extension import RoutingSlack
from tests.lending.test_partner_door import engine  # noqa: F401 - fixture

BASE = "https://api.nextdeallending.com"


class Client:
    def __init__(self) -> None:
        self.posted: list[str] = []
        self.opened: list[dict] = []
        self.told: list[str] = []
        self.updated: list[tuple] = []

    def chat_postMessage(self, channel, text):  # noqa: N802 - slack_sdk API name
        self.posted.append(text)

    def views_open(self, trigger_id, view):
        self.opened.append(view)
        return {"view": {"id": "V1", "hash": "h1"}}

    def views_update(self, view_id, view, hash=None):
        self.updated.append((view_id, hash, view))

    def chat_postEphemeral(self, channel, user, text):  # noqa: N802 - slack_sdk API name
        self.told.append(text)


@pytest.fixture
def slack(engine, monkeypatch):  # noqa: F811
    @contextmanager
    def session():
        s = Session(engine)
        try:
            yield s
            s.commit()
        except Exception:
            s.rollback()
            raise
        finally:
            s.close()

    client = Client()
    settings = get_settings()
    monkeypatch.setattr(partner_slack, "lending_session", session)
    monkeypatch.setattr(partner_slack, "allowed_user_ids", lambda: frozenset({"UJOSH"}))
    monkeypatch.setattr(partner_slack.notify, "_client", lambda: client)
    monkeypatch.setattr(partner_slack.notify, "_channel", lambda: "C_CORA")
    monkeypatch.setattr(settings, "lending_partner_link_base_url", BASE)
    monkeypatch.setattr(settings, "lending_partner_consent_text", "I have the borrower's permission.")
    client.engine = engine
    return client


def _text(value: str) -> dict:
    return {"v": {"type": "plain_text_input", "value": value}}


def _select(value: str) -> dict:
    return {"v": {"type": "static_select", "selected_option": {"value": value}}}


def _ticked() -> dict:
    return {"v": {"type": "checkboxes", "selected_options": [{"value": "yes"}]}}


def _submission(action: str, user: str = "UJOSH", **blocks) -> dict:
    values = {"action_b": _select(action), **blocks}
    return {"type": "view_submission", "user": {"id": user},
            "view": {"callback_id": partner_slack.CALLBACK_ID, "state": {"values": values}}}


def _add(name: str = "Maria Lopez", email: str = "maria@example.com", **extra) -> dict:
    blocks = {"name_b": _text(name), "type_b": _select("wholesaler"), "email_b": _text(email),
              "agreement_b": _select("pending"), "states_b": _text("fl, ga")}
    blocks.update(extra)
    return _submission("add", **blocks)


def _count(engine) -> int:  # noqa: F811
    with Session(engine) as s:
        return s.execute(text("SELECT COUNT(*) FROM lending.partners")).scalar()


def _link_in(response: dict) -> str:
    body = response["view"]["blocks"][0]["text"]["text"]
    return body.split("```")[1]


def test_add_shows_the_full_link_once_and_audits_without_it(slack, caplog) -> None:
    caplog.set_level(logging.DEBUG)
    response = partner_slack.handle_submission(_add())
    assert response["response_action"] == "update" and "submit" not in response["view"]
    link = _link_in(response)
    assert link.startswith(f"{BASE}/api/lending/partners/")
    token = link.rsplit("/", 1)[1]
    with Session(slack.engine) as s:
        row = s.execute(text("SELECT created_by, token_hash FROM lending.partners")).one()
        assert row.created_by == "slack:UJOSH" and row.token_hash == partners.hash_token(token)
        assert partners.partner_for_token(s, token) is not None
    assert len(slack.posted) == 1 and "*New partner added* by <@UJOSH>" in slack.posted[0]
    for detail in ("*Partner:* #", "Maria Lopez", "*Type:* wholesaler", "*Agreement:* pending", "*States:* FL, GA",
                   "email maria@example.com", "no status texts"):
        assert detail in slack.posted[0], detail
    assert token not in slack.posted[0] and token not in caplog.text


def test_a_redelivered_add_makes_one_partner_and_a_working_link(slack) -> None:
    first = _link_in(partner_slack.handle_submission(_add()))
    second = partner_slack.handle_submission(_add())
    assert _count(slack.engine) == 1
    assert "created a moment ago" in second["view"]["blocks"][0]["text"]["text"]
    with Session(slack.engine) as s:
        assert partners.partner_for_token(s, first.rsplit("/", 1)[1]) is None
        assert partners.partner_for_token(s, _link_in(second).rsplit("/", 1)[1]) is not None


@pytest.mark.parametrize("submission, block", [
    (_add(name=" "), "name_b"),
    (_add(email="nope"), "email_b"),
    (_add(states_b=_text("Florida")), "states_b"),
    (_submission("add", name_b=_text("X"), type_b=_select("lender"), email_b=_text("x@example.com")), "type_b"),
    (_submission("revoke", partner_b=_select("1")), "confirm_b"),
    (_submission("new_link", confirm_b=_ticked()), "partner_b"),
    (_submission(""), "action_b"),
])
def test_bad_fields_are_shown_under_their_field_and_change_nothing(slack, submission, block) -> None:
    response = partner_slack.handle_submission(submission)
    assert response["response_action"] == "errors" and block in response["errors"], response
    assert _count(slack.engine) == 0 and slack.posted == []


def test_only_approvers_may_use_it(slack) -> None:
    response = partner_slack.handle_submission({**_add(), "user": {"id": "UOTHER"}})
    assert response["response_action"] == "errors" and _count(slack.engine) == 0
    extension = RoutingSlack(slack)
    assert extension.handle("slash_commands", {"command": "/partners", "user_id": "UOTHER", "channel_id": "C1"})
    assert slack.opened == [] and "Only approved users" in slack.told[0]
    assert extension.handle("slash_commands", {"command": "/partners", "user_id": "UJOSH", "trigger_id": "T1"})
    assert slack.opened[0]["callback_id"] == partner_slack.CALLBACK_ID


def test_the_form_opens_before_the_partner_list_is_read(slack, monkeypatch) -> None:
    order: list[str] = []
    monkeypatch.setattr(partner_slack, "_partners", lambda: order.append("read") or [])
    real_open = slack.views_open
    monkeypatch.setattr(slack, "views_open", lambda **kw: order.append("open") or real_open(**kw))
    partner_slack.open_form(slack, {"user_id": "UJOSH", "trigger_id": "T1"})
    assert order == ["open", "read"]  # Slack's 3 s trigger is used before any database work
    assert "Loading" in slack.opened[0]["blocks"][0]["text"]["text"]
    view_id, view_hash, view = slack.updated[0]
    assert (view_id, view_hash) == ("V1", "h1") and "No partners yet" in view["blocks"][0]["text"]["text"]


def test_no_link_host_means_nothing_is_created(slack, monkeypatch) -> None:
    monkeypatch.setattr(get_settings(), "lending_partner_link_base_url", "")
    response = partner_slack.handle_submission(_add())
    assert "LENDING_PARTNER_LINK_BASE_URL" in response["errors"]["action_b"] and _count(slack.engine) == 0


def test_a_failed_commit_shows_no_link(slack, monkeypatch) -> None:
    @contextmanager
    def failing():
        s = Session(slack.engine)
        try:
            yield s
            raise RuntimeError("commit failed")
        finally:
            s.rollback()
            s.close()

    monkeypatch.setattr(partner_slack, "lending_session", failing)
    response = partner_slack.handle_submission(_add())
    assert response == {"response_action": "errors", "errors": {"action_b": "Something went wrong; nothing was changed."}}
    assert _count(slack.engine) == 0 and slack.posted == []


def test_new_link_and_revoke(slack) -> None:
    old = _link_in(partner_slack.handle_submission(_add())).rsplit("/", 1)[1]
    with Session(slack.engine) as s:
        partner_id = s.execute(text("SELECT id FROM lending.partners")).scalar()
    renewed = partner_slack.handle_submission(_submission("new_link", partner_b=_select(str(partner_id)), confirm_b=_ticked()))
    new = _link_in(renewed).rsplit("/", 1)[1]
    assert partner_slack.handle_submission(_submission("revoke", partner_b=_select(str(partner_id)), confirm_b=_ticked())) == {}
    again = partner_slack.handle_submission(_submission("revoke", partner_b=_select(str(partner_id)), confirm_b=_ticked()))
    assert "already switched off" in again["errors"]["partner_b"]
    with Session(slack.engine) as s:
        assert partners.partner_for_token(s, old) is None and partners.partner_for_token(s, new) is None
        assert partners.partner_by_id(s, partner_id) is not None
    assert [p.split(" by ")[0] for p in slack.posted] == [":handshake: *New partner added*", ":link: New link issued",
                                                          ":no_entry: Link switched off"]
    assert all(old not in p and new not in p for p in slack.posted)


def test_partner_names_cannot_ping_the_channel(slack) -> None:
    partner_slack.handle_submission(_add(name="<!channel> Bob"))
    assert "<!channel>" not in slack.posted[0] and "&lt;!channel&gt;" in slack.posted[0]


def test_the_form_follows_the_action_and_lenders_still_works(slack, monkeypatch) -> None:
    add = partner_slack.build_form_view([], "add")
    assert {b.get("block_id") for b in add["blocks"]} >= {"name_b", "type_b", "email_b", "sms_b"}
    revoke = partner_slack.build_form_view([{"id": 3, "name": "Ann", "partner_type": "broker", "agreement_status": "signed",
                                             "link_state": "live", "created_at": "2026-10-21", "is_test": False}], "revoke")
    assert {b.get("block_id") for b in revoke["blocks"]} >= {"partner_b", "confirm_b"}
    assert "*#3* Ann" in revoke["blocks"][0]["text"]["text"]

    from src.lending.routing import slack_admin
    opened: list[str] = []
    monkeypatch.setattr(slack_admin, "open_form", lambda client, payload: opened.append("lenders"))
    extension = RoutingSlack(slack)
    assert extension.handle("slash_commands", {"command": "/lenders", "user_id": "UJOSH"}) and opened == ["lenders"]
    assert extension.handle("slash_commands", {"command": "/other"}) is False
    assert extension.respond("interactive", {"type": "view_submission", "view": {"callback_id": "someone_else"}}) is None

