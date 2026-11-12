"""P1.3: Instantly replies -> triage post + drafted answer card; STOP / unsubscribe -> suppressed everywhere."""
from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr

from src.lending import instantly_client, instantly_replies, instantly_webhook
from src.lending.instantly_replies import (
    InstantlyEvent,
    ReplyProcessor,
    TriageError,
    classify_reply,
    is_stop_request,
    new_reply_text,
    parse_event,
    triage_text,
)

SECRET = "s3cret"
LEAD = "mark.lee@example.com"


def reply_body(text="Yes, I have a fix and flip in Tampa. What do you need from me?", **extra):
    body = {"event_type": "reply_received", "lead_email": LEAD, "email_account": "josh@ndl-mail.com",
            "email_id": "em-1", "reply_subject": "Re: Funding", "reply_text": text, "firstName": "Mark",
            "campaign_name": "FL investors"}
    body.update(extra)
    return body


# ── STOP detection ───────────────────────────────────────────────────────────

@pytest.mark.parametrize("reply", [
    "STOP", "stop.", "Unsubscribe", "Please remove me from your list", "take me off this list",
    "Stop emailing me", "do not contact me again", "Don't email me", "opt out", "Opt-out please",
    "Thanks\nSTOP\nSent from my iPhone",
])
def test_a_stop_request_is_recognised(reply):
    assert is_stop_request(reply) is True


@pytest.mark.parametrize("reply", [
    "I can't stop thinking about this deal, call me", "Yes, interested", "Not now, maybe next quarter",
    "Who is this?", "",
])
def test_ordinary_replies_are_not_stop_requests(reply):
    assert is_stop_request(reply) is False


def test_stop_words_in_the_quoted_email_do_not_count():
    reply = "Sounds good, send details\n\nOn Mon, Nov 9, 2026 Josh wrote:\n> Reply STOP to unsubscribe"
    assert new_reply_text(reply) == "Sounds good, send details"
    assert is_stop_request(reply) is False


# ── parsing ──────────────────────────────────────────────────────────────────

def test_a_reply_is_parsed_with_its_thread_details():
    event = parse_event(reply_body())
    assert event.event_type == "reply_received" and event.lead_email == LEAD
    assert event.dedup_key == "reply:em-1" and event.reply_to_uuid == "em-1"
    assert event.email_account == "josh@ndl-mail.com" and event.first_name == "Mark"


def test_unhandled_events_and_events_without_an_address_are_ignored():
    assert parse_event({"event_type": "email_opened", "lead_email": LEAD}) is None
    assert parse_event({"event_type": "reply_received", "reply_text": "hi"}) is None


def test_an_unsubscribe_is_keyed_by_the_address_so_a_redelivery_is_one_event():
    first = parse_event({"event_type": "lead_unsubscribed", "lead_email": LEAD.upper()})
    again = parse_event({"event": "lead_unsubscribed", "email": LEAD})
    assert first.dedup_key == again.dedup_key and first.lead_email == LEAD


def test_a_reply_without_an_email_id_gets_a_stable_key():
    body = reply_body(email_id=None, timestamp="2026-11-11T10:00:00Z")
    assert parse_event(body).dedup_key == parse_event(dict(body)).dedup_key


# ── classification and drafting ──────────────────────────────────────────────

class FakeModel:
    def __init__(self, *answers):
        self.answers, self.calls = list(answers), []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        return SimpleNamespace(content=[SimpleNamespace(type="text", text=self.answers.pop(0))])


DEAL = ('{"intent": "deal", "summary": "Has a Tampa flip, wants to talk", "subject": "Re: Funding", '
        '"body": "Thanks Mark, Josh can walk you through it on a quick call. What day and time works?\\nNext Deal Lending"}')


def test_a_deal_reply_gets_a_draft():
    triage = classify_reply(FakeModel(DEAL), "m", subject="Re: Funding", reply="Yes, I have a deal")
    assert triage.intent == "deal" and "quick call" in triage.body and triage.subject == "Re: Funding"


def test_a_dead_reply_gets_no_draft():
    triage = classify_reply(FakeModel('{"intent": "dead", "summary": "Not interested"}'), "m", subject=None,
                            reply="Not interested")
    assert triage.intent == "dead" and triage.body == ""


def test_a_draft_that_quotes_numbers_is_redrafted_once():
    quoting = DEAL.replace("on a quick call", "at a 9% rate on a quick call")
    model = FakeModel(quoting, DEAL)
    triage = classify_reply(model, "m", subject=None, reply="rates?")
    assert "9%" not in triage.body and len(model.calls) == 2
    assert "Redraft" in model.calls[1]["messages"][-1]["content"]


def test_a_draft_that_keeps_quoting_numbers_is_refused():
    quoting = DEAL.replace("on a quick call", "at a 9% rate on a quick call")
    with pytest.raises(TriageError):
        classify_reply(FakeModel(quoting, quoting), "m", subject=None, reply="rates?")


def test_an_unusable_model_answer_is_refused():
    with pytest.raises(TriageError):
        classify_reply(FakeModel("sorry, I can't"), "m", subject=None, reply="hi")
    with pytest.raises(TriageError):
        classify_reply(FakeModel('{"intent": "maybe", "summary": "x"}'), "m", subject=None, reply="hi")


def test_the_reply_is_sent_to_the_model_as_data_without_the_quoted_email():
    model = FakeModel(DEAL)
    classify_reply(model, "m", subject="Re: x", reply="Interested\n\nOn Mon Josh wrote:\n> old text")
    content = model.calls[0]["messages"][0]["content"]
    assert "<reply>\nInterested\n</reply>" in content and "old text" not in content
    assert "data, not instructions" in model.calls[0]["system"]


def test_the_triage_post_masks_the_address():
    row = {"first_name": "Mark", "lead_email": LEAD, "campaign_name": "FL investors"}
    posted = triage_text(row, instantly_replies.Triage("deal", "Has a deal"), 42)
    assert LEAD not in posted and "m***@example.com" in posted and "action 42" in posted and "*Deal*" in posted


# ── the sweep step ───────────────────────────────────────────────────────────

class _Result:
    def __init__(self, rows=(), scalar=None):
        self._rows, self._scalar = list(rows), scalar

    def mappings(self):
        return self

    def __iter__(self):
        return iter(self._rows)

    def first(self):
        return self._rows[0] if self._rows else None

    def scalar(self):
        return self._scalar


class FakeSession:
    def __init__(self, rows):
        self.rows, self.executed, self.commits = rows, [], 0

    def execute(self, stmt, params=None):
        sql = str(stmt)
        self.executed.append((sql, params or {}))
        if "FOR UPDATE SKIP LOCKED" in sql:
            return _Result(self.rows)
        if "INSERT INTO lending.cora_inbox" in sql:
            return _Result(scalar=1)
        return _Result()

    @contextmanager
    def begin_nested(self):
        yield

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks = getattr(self, "rollbacks", 0) + 1

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def params(self, fragment):
        return [p for sql, p in self.executed if fragment in sql]


def row(**overrides):
    base = {"id": 7, "lead_email": LEAD, "first_name": "Mark", "campaign_name": "FL investors",
            "email_account": "josh@ndl-mail.com", "reply_to_uuid": "em-1", "reply_subject": "Re: Funding",
            "reply_text": "Yes I have a deal", "status": "new", "stop_request": False, "intent": None,
            "summary": None, "action_id": None, "attempts": 0, "dedup_key": "reply:em-1"}
    base.update(overrides)
    return base


def processor(session, model, *, posted=True):
    calls = SimpleNamespace(enqueued=[], cards=[], triage=[], notices=[])

    def enqueue(**kwargs):
        calls.enqueued.append(kwargs)
        return 99

    proc = ReplyProcessor(session_factory=lambda: session, client=model, model="m", enqueue=enqueue,
                          post_card=lambda action_id: calls.cards.append(action_id) or True,
                          post_triage=lambda text: calls.triage.append(text) or posted,
                          post_notice=lambda text: calls.notices.append(text) or posted)
    return proc, calls


def test_a_deal_reply_is_drafted_onto_an_approval_card_and_triaged():
    session = FakeSession([row()])
    proc, calls = processor(session, FakeModel(DEAL))
    result = proc.run()
    assert result.triaged == 1 and result.drafted == 1
    draft = calls.enqueued[0]
    assert draft["channel"] == "instantly_email" and draft["tool_name"] == "draft_instantly_reply"
    assert draft["payload"]["from_inbox"] == "josh@ndl-mail.com" and draft["payload"]["reply_to_uuid"] == "em-1"
    assert draft["recipient_email"] == LEAD and draft["idempotency_key"] == "instantly_reply:7"
    assert calls.cards == [99] and "action 99" in calls.triage[0]
    done = session.params("SET status = 'done'")[0]
    assert done["intent"] == "deal" and done["action_id"] == 99 and done["posted"] is True
    inbox = session.params("INSERT INTO lending.cora_inbox")[0]
    assert inbox["event_type"] == "instantly_reply" and inbox["target_channel"] == "replies"
    assert LEAD not in inbox["payload"] and "Yes I have a deal" not in inbox["payload"]


def test_a_dead_reply_is_triaged_with_no_draft_and_no_suppression():
    session = FakeSession([row()])
    proc, calls = processor(session, FakeModel('{"intent": "dead", "summary": "Not interested"}'))
    proc.run()
    assert calls.enqueued == [] and "No answer drafted." in calls.triage[0]
    assert not session.params("opt_out_events") and not session.params("suppression_list")


def test_a_reply_that_cannot_be_answered_in_the_thread_is_triaged_without_a_draft():
    session = FakeSession([row(reply_to_uuid=None)])
    proc, calls = processor(session, FakeModel(DEAL))
    proc.run()
    assert calls.enqueued == [] and "answer it by hand" in calls.triage[0]


def test_a_model_failure_is_retried_and_then_posted_unclassified():
    session = FakeSession([row()])
    proc, calls = processor(session, FakeModel("not json"))
    assert proc.run().failed == 1 and calls.triage == []
    last = FakeSession([row(attempts=instantly_replies.MAX_ATTEMPTS - 1)])
    proc, calls = processor(last, FakeModel("not json"))
    proc.run()
    assert "*Unclassified*" in calls.triage[0]


def test_an_unposted_triage_still_reaches_coras_inbox():
    session = FakeSession([row()])
    proc, _ = processor(session, FakeModel(DEAL), posted=False)
    proc.run()
    assert session.params("SET status = 'done'")[0]["posted"] is False
    assert session.params("INSERT INTO lending.cora_inbox")[0]["channel_posted"] is False


def test_the_sweep_retries_a_stop_whose_opt_out_was_not_recorded(monkeypatch):
    recorded = []
    monkeypatch.setattr(instantly_replies, "suppress_reply", lambda db, reply_id: recorded.append(reply_id) or 5)
    session = FakeSession([row(status="suppressed", stop_request=True, intent="unsubscribe")])
    proc, calls = processor(session, FakeModel())
    assert proc.run().suppressed == 1
    assert recorded == [7] and calls.enqueued == [] and "Suppressed everywhere" in calls.notices[0]
    assert LEAD not in calls.notices[0]


def test_a_stop_that_never_records_is_alerted_for_a_person(monkeypatch):
    def broken(db, reply_id):
        raise RuntimeError("db down")

    monkeypatch.setattr(instantly_replies, "suppress_reply", broken)
    session = FakeSession([row(status="suppressed", attempts=instantly_replies.MAX_ATTEMPTS - 1)])
    proc, calls = processor(session, FakeModel())
    proc.run()
    assert "NOT suppressed: record it by hand" in calls.notices[0]


def test_suppress_reply_uses_the_email_opt_out_path(monkeypatch):
    seen = {}

    def propagate(db, **kwargs):
        seen.update(kwargs)
        return 31

    monkeypatch.setattr(instantly_replies, "propagate_opt_out", propagate)

    class Db(FakeSession):
        def execute(self, stmt, params=None):
            if "SELECT id, lead_email" in str(stmt):
                return _Result([{"id": 7, "lead_email": LEAD, "dedup_key": "reply:em-1", "suppressed_at": None}])
            return super().execute(stmt, params)

    db = Db([])
    assert instantly_replies.suppress_reply(db, 7) == 31
    assert seen["email"] == LEAD and seen["source_ref"] == "instantly:reply:em-1"
    assert seen["channel"].value == "email"
    assert db.params("SET suppressed_at = now()")[0]["event_id"] == 31


# ── the webhook ──────────────────────────────────────────────────────────────

@pytest.fixture
def client(monkeypatch):
    from config.settings import get_settings
    from src.api.deps import get_db

    monkeypatch.setattr(get_settings(), "lending_instantly_webhook_secret", SecretStr(SECRET), raising=False)
    monkeypatch.setattr(get_settings(), "lending_instantly_api_key", None, raising=False)
    app = FastAPI()
    app.include_router(instantly_webhook.router)
    db = FakeSession([])
    app.dependency_overrides[get_db] = lambda: db
    test_client = TestClient(app)
    test_client.db = db
    test_client.notices = []
    monkeypatch.setattr(instantly_webhook, "post_ops_notice", lambda text: test_client.notices.append(text) or True)
    return test_client


def post(client, body, secret=SECRET):
    headers = {"X-Instantly-Webhook-Secret": secret} if secret else {}
    return client.post("/webhooks/lending/instantly", headers=headers, json=body)


def test_the_webhook_is_closed_without_a_secret_and_refuses_a_wrong_one(client, monkeypatch):
    from config.settings import get_settings

    assert post(client, reply_body(), secret="wrong").status_code == 403
    assert post(client, reply_body(), secret=None).status_code == 403
    monkeypatch.setattr(get_settings(), "lending_instantly_webhook_secret", None, raising=False)
    assert post(client, reply_body()).status_code == 503


def test_an_event_we_do_not_handle_is_acknowledged_and_ignored(client):
    assert post(client, {"event_type": "email_opened", "lead_email": LEAD}).json() == {"status": "ignored"}
    assert client.db.executed == []


def test_a_reply_is_stored_for_coras_sweep(client, monkeypatch):
    monkeypatch.setattr(instantly_webhook, "record_event",
                        lambda db, event: instantly_replies.RecordResult("recorded", 7, False))
    assert post(client, reply_body()).json() == {"status": "recorded"}
    assert client.notices == []


def test_a_stop_reply_is_suppressed_at_once_and_announced(client, monkeypatch):
    stored = []
    monkeypatch.setattr(instantly_webhook, "record_event",
                        lambda db, event: stored.append(event) or instantly_replies.RecordResult("recorded", 7, True))
    monkeypatch.setattr(instantly_webhook, "suppress_reply", lambda db, reply_id: 31)
    assert post(client, reply_body(text="STOP")).json() == {"status": "suppressed"}
    assert stored[0].lead_email == LEAD
    assert "replied asking to stop" in client.notices[0] and "#31" in client.notices[0] and LEAD not in client.notices[0]
    assert client.db.params("INSERT INTO lending.cora_inbox")[0]["event_type"] == "instantly_opt_out"


def test_an_unsubscribe_event_is_suppressed_too(client, monkeypatch):
    monkeypatch.setattr(instantly_webhook, "record_event",
                        lambda db, event: instantly_replies.RecordResult("recorded", 8, True))
    monkeypatch.setattr(instantly_webhook, "suppress_reply", lambda db, reply_id: 32)
    assert post(client, {"event_type": "lead_unsubscribed", "lead_email": LEAD}).json() == {"status": "suppressed"}
    assert "unsubscribe link" in client.notices[0]


def test_a_failed_opt_out_is_left_for_the_sweep_not_lost(client, monkeypatch):
    def broken(db, reply_id):
        raise RuntimeError("db down")

    monkeypatch.setattr(instantly_webhook, "record_event",
                        lambda db, event: instantly_replies.RecordResult("recorded", 7, True))
    monkeypatch.setattr(instantly_webhook, "suppress_reply", broken)
    assert post(client, reply_body(text="STOP")).json() == {"status": "suppressed"}
    assert client.notices == []


def test_a_redelivered_event_is_a_duplicate(client, monkeypatch):
    monkeypatch.setattr(instantly_webhook, "record_event", lambda db, event: instantly_replies.RecordResult("duplicate"))
    assert post(client, reply_body()).json() == {"status": "duplicate"}


def test_a_storage_failure_asks_instantly_to_retry(client, monkeypatch):
    def broken(db, event):
        raise RuntimeError("db down")

    monkeypatch.setattr(instantly_webhook, "record_event", broken)
    response = post(client, reply_body())
    assert response.status_code == 503 and LEAD not in response.text


# ── the Instantly send ───────────────────────────────────────────────────────

@pytest.fixture
def not_halted(monkeypatch):
    monkeypatch.setattr("src.lending.global_halt.is_globally_halted", lambda: False)


def test_the_emergency_halt_refuses_the_send_before_calling_instantly(monkeypatch):
    called = []
    monkeypatch.setattr("src.lending.global_halt.is_globally_halted", lambda: True)
    monkeypatch.setattr(instantly_client.requests, "post", lambda *a, **k: called.append(1))
    with pytest.raises(instantly_client.InstantlyError, match="emergency halt"):
        instantly_client.send_reply(instantly_client.InstantlyAccount("k", "https://x"), eaccount="a@b.com",
                                    reply_to_uuid="em-1", subject="s", body="b")
    assert called == []


class ScriptedEngine:
    """Answers the send check's queries in order."""

    def __init__(self, *answers):
        self.answers, self.queries = list(answers), []

    @contextmanager
    def connect(self):
        yield self

    def execute(self, statement, params):
        self.queries.append((str(statement), dict(params)))
        return SimpleNamespace(scalar=lambda: self.answers.pop(0))


def _instantly_action(channel="instantly_email"):
    from packages.agent_core.pending_actions import ActionStatus, PendingAction

    return PendingAction(
        action_id=1, tool_name="draft_instantly_reply", channel=channel, payload={}, summary="",
        status=ActionStatus.SENDING, requested_by=None, source_channel=None, source_thread_ts=None, card_channel=None,
        card_ts=None, decided_by="U_JOSH", revised_by=None, revision_note=None, revisions=(), recipient_phone=None,
        recipient_email="Mark.Lee@Example.com", contact_ref=None, deal_ref=None, idempotency_key=None,
        expires_at=None, provider_ref=None, error=None)


def test_an_instantly_answer_to_an_address_opted_out_in_fa_is_blocked_at_send_time():
    from src.lending.cora_agent import make_send_check

    engine = ScriptedEngine(False, True)  # not on the lending list yet, but in FA's email_opt_outs
    assert make_send_check(engine)(_instantly_action()) == "recipient is suppressed or marked do-not-contact"
    assert "email_opt_outs" in engine.queries[1][0] and engine.queries[1][1]["e"] == LEAD


def test_an_instantly_answer_to_a_clear_address_may_send_and_ghl_email_is_unchanged():
    from src.lending.cora_agent import make_send_check

    window_open = {"email_window": lambda phone: True}  # P1.1: every email waits for 8am-8pm; not under test here
    assert make_send_check(ScriptedEngine(False, False), **window_open)(_instantly_action()) is None
    ghl = ScriptedEngine(False)
    assert make_send_check(ghl, **window_open)(_instantly_action("ghl_email")) is None
    assert len(ghl.queries) == 1  # GHL email keeps its single suppression query


def test_an_approved_answer_is_sent_inside_the_thread(monkeypatch, not_halted):
    from src.lending.cora_agent import instantly_executor

    sent = {}

    def fake_post(url, json, timeout, headers):
        sent.update(url=url, json=json, headers=headers)
        return SimpleNamespace(status_code=200, json=lambda: {"id": "out-1"})

    monkeypatch.setattr(instantly_client.requests, "post", fake_post)
    send = instantly_executor(instantly_client.InstantlyAccount("key", "https://api.instantly.ai"))
    ref = send({"to_email": LEAD, "subject": "Re: Funding", "body": "Hi Mark\n<b>call?</b>",
                "from_inbox": "josh@ndl-mail.com", "reply_to_uuid": "em-1"})
    assert ref == "out-1" and sent["url"] == "https://api.instantly.ai/api/v2/emails/reply"
    assert sent["json"]["eaccount"] == "josh@ndl-mail.com" and sent["json"]["reply_to_uuid"] == "em-1"
    assert sent["json"]["body"]["html"] == "Hi Mark<br>&lt;b&gt;call?&lt;/b&gt;"
    assert sent["headers"]["Authorization"] == "Bearer key"


def test_an_instantly_refusal_fails_the_send_without_the_address(monkeypatch, not_halted):
    monkeypatch.setattr(instantly_client.requests, "post",
                        lambda *a, **k: SimpleNamespace(status_code=422, json=lambda: {}))
    with pytest.raises(instantly_client.InstantlyError) as raised:
        instantly_client.send_reply(instantly_client.InstantlyAccount("k", "https://x"), eaccount="a@b.com",
                                    reply_to_uuid="em-1", subject="s", body="b")
    assert "422" in str(raised.value) and "@" not in str(raised.value)


def test_instantly_answers_follow_the_no_numbers_rule_and_revise_subject_and_body_only():
    from src.lending.cora_agent import EDITABLE_FIELDS
    from src.lending.cora_tools import draft_content_check

    assert EDITABLE_FIELDS["instantly_email"] == ("subject", "body")
    assert draft_content_check("instantly_email", {"subject": "Re: x", "body": "Our rate is 9%"}) is not None
    assert draft_content_check("instantly_email", {"subject": "Re: x", "body": "Happy to talk"}) is None


# ── the table (shared database; skipped without one) ─────────────────────────

def test_an_event_is_stored_once(lending_db):
    from migrations.apply_lending_instantly_replies import apply_to

    apply_to(lending_db.connection())
    event = InstantlyEvent(event_type="reply_received", lead_email=LEAD, dedup_key="reply:test-em-1",
                           reply_text="Interested", reply_to_uuid="test-em-1", email_account="josh@ndl-mail.com")
    first = instantly_replies.record_event(lending_db, event)
    assert first.status == "recorded" and first.stop_request is False
    assert instantly_replies.record_event(lending_db, event).status == "duplicate"
    stop = InstantlyEvent(event_type="reply_received", lead_email=LEAD, dedup_key="reply:test-em-2", reply_text="STOP")
    assert instantly_replies.record_event(lending_db, stop).stop_request is True
    apply_to(lending_db.connection())  # idempotent
