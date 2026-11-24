"""R1 0.4 safe send, no database: the breaker's decisions, the senders' hooks, the resume command, re-validation.

The breaker's SQL runs against a scripted fake session; the Postgres behaviour is covered by test_channel_breaker_db.py.
"""
from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any, Callable

import pytest

from packages.agent_core.pending_actions import ActionStatus, PendingAction
from packages.agent_core.relay import SendHeld, SendHold, SendRetryLater
from src.lending import cora_agent, ghl_email, ghl_sms
from src.lending.dispatch import circuit_breaker as cb
from src.lending.dispatch import revalidate
from src.lending.dispatch.resume_command import ChannelResume, first_handled, resume_channel
from src.lending.ghl_sms import GhlAccount, GhlChannelPaused, GhlSmsError, GhlSmsSender

PHONE = "+18135558601"
ACCOUNT = GhlAccount(api_key="k-secret", location_id="loc-ndl")


# ── A scripted stand-in for the lending session ────────────────────────────────

class Result:
    def __init__(self, rows=(), scalar=None):
        self._rows, self._scalar = list(rows), scalar

    def first(self):
        return self._rows[0] if self._rows else None

    def all(self):
        return self._rows

    def scalar_one_or_none(self):
        return self._scalar

    def scalars(self):
        return iter(self._rows)


class FakeDb:
    def __init__(self, handler: Callable[[Any, dict], Result]):
        self.calls: list[tuple[Any, dict]] = []
        self._handler = handler

    def execute(self, stmt, params=None):
        self.calls.append((stmt, params or {}))
        return self._handler(stmt, params or {})

    def statements(self) -> list[Any]:
        return [stmt for stmt, _ in self.calls]


def _session(monkeypatch, handler) -> FakeDb:
    db = FakeDb(handler)

    @contextmanager
    def session():
        yield db

    monkeypatch.setattr(cb, "lending_session", session)
    return db


def _row(**fields):
    return SimpleNamespace(**fields)


# ── Pure decisions ─────────────────────────────────────────────────────────────

@pytest.mark.parametrize("failures,state,trips", [
    (4, "closed", False), (5, "closed", True), (9, "closed", True), (5, "open", False),
])
def test_five_failures_in_a_row_trip_a_closed_breaker(failures, state, trips):
    assert cb.should_trip(failures, state) is trips


def test_resume_words_name_the_channels():
    assert [cb.parse_resume_word(w) for w in ("sms", "SMS", "ghl_sms", "email", "ghl-email", "instantly", "fax")] == [
        cb.CHANNEL_SMS, cb.CHANNEL_SMS, cb.CHANNEL_SMS, cb.CHANNEL_EMAIL, cb.CHANNEL_EMAIL, cb.CHANNEL_INSTANTLY, None]


def test_every_channel_maps_to_the_card_channels_that_send_through_it():
    assert set(cb.CARD_CHANNELS) == set(cb.CHANNELS)
    assert cb.CARD_CHANNELS[cb.CHANNEL_INSTANTLY] == ("instantly_email", "instantly_cold_email")


# ── admit ──────────────────────────────────────────────────────────────────────

def _admit_handler(state="closed", reason=None, slot=1, row_missing=False):
    def handler(stmt, params):
        if stmt is cb._STATE:
            return Result([] if row_missing else [_row(state=state, tripped_reason=reason)])
        if stmt is cb._TAKE_SLOT:
            return Result(scalar=slot)
        return Result()
    return handler


def test_an_open_breaker_refuses_the_send_and_takes_no_slot(monkeypatch):
    db = _session(monkeypatch, _admit_handler(state="open", reason="HTTP 503"))
    with pytest.raises(cb.ChannelPaused) as paused:
        cb.admit(cb.CHANNEL_SMS)
    assert not isinstance(paused.value, cb.RateLimited) and "HTTP 503" in str(paused.value)
    assert cb._TAKE_SLOT not in db.statements()


def test_a_closed_breaker_under_the_cap_takes_a_slot(monkeypatch):
    db = _session(monkeypatch, _admit_handler(slot=7))
    cb.admit(cb.CHANNEL_SMS)
    (_, params), = [call for call in db.calls if call[0] is cb._TAKE_SLOT]
    assert params == {"c": cb.CHANNEL_SMS, "limit": 60}


def test_a_new_minute_prunes_old_counters(monkeypatch):
    db = _session(monkeypatch, _admit_handler(slot=1))
    cb.admit(cb.CHANNEL_EMAIL)
    assert cb._PRUNE in db.statements()


def test_over_the_cap_is_rate_limited(monkeypatch):
    _session(monkeypatch, _admit_handler(slot=None))
    with pytest.raises(cb.RateLimited):
        cb.admit(cb.CHANNEL_SMS)


def test_approval_cards_leave_ten_slots_for_live_replies(monkeypatch):
    db = _session(monkeypatch, _admit_handler(slot=3))
    with cb.card_sends():
        cb.admit(cb.CHANNEL_SMS)
    cb.admit(cb.CHANNEL_SMS)  # a live reply: the whole cap
    limits = [params["limit"] for stmt, params in db.calls if stmt is cb._TAKE_SLOT]
    assert limits == [50, 60]


def test_the_cap_comes_from_settings_and_is_never_below_one(monkeypatch):
    from config.settings import get_settings

    settings = get_settings()
    monkeypatch.setattr(settings, "lending_sms_per_minute", 5, raising=False)
    db = _session(monkeypatch, _admit_handler(slot=1))
    with cb.card_sends():
        cb.admit(cb.CHANNEL_SMS)  # 5 - 10 would be negative
    assert [p["limit"] for s, p in db.calls if s is cb._TAKE_SLOT] == [1]


def test_a_missing_breaker_row_or_an_unreadable_database_fails_closed(monkeypatch):
    _session(monkeypatch, _admit_handler(row_missing=True))
    with pytest.raises(cb.ChannelPaused):
        cb.admit(cb.CHANNEL_SMS)

    def broken(stmt, params):
        raise RuntimeError("connection refused")

    _session(monkeypatch, broken)
    with pytest.raises(cb.ChannelPaused) as paused:
        cb.admit(cb.CHANNEL_SMS)
    assert "could not be read" in str(paused.value)


# ── record_failure / record_success ─────────────────────────────────────────────

def _failure_handler(state, failures, held=(7, 8)):
    def handler(stmt, params):
        if stmt is cb._LOCK:
            return Result([_row(state=state, consecutive_failures=failures)])
        if stmt is cb._HOLD_APPROVED:
            return Result(list(held))
        return Result()
    return handler


def test_the_fifth_failure_trips_the_breaker_holds_the_approved_cards_and_announces(monkeypatch):
    db = _session(monkeypatch, _failure_handler("closed", 4))
    announced = []
    monkeypatch.setattr(cb, "_announce_trip", lambda channel, error, held: announced.append((channel, error, held)))
    assert cb.record_failure(cb.CHANNEL_SMS, "HTTP 503", ambiguous=True) is True
    count = next(params for stmt, params in db.calls if stmt is cb._COUNT_FAILURE)
    assert count["trip"] is True and count["err"] == "HTTP 503"
    hold = next(params for stmt, params in db.calls if stmt is cb._HOLD_APPROVED)
    assert hold["channels"] == ["ghl_sms"] and hold["reason"] == cb.HELD_BREAKER_OPEN
    assert cb._EVENT in db.statements()
    assert announced == [(cb.CHANNEL_SMS, "HTTP 503", 2)]


def test_a_fourth_failure_only_counts(monkeypatch):
    db = _session(monkeypatch, _failure_handler("closed", 3))
    monkeypatch.setattr(cb, "_announce_trip", lambda *a: pytest.fail("announced without a trip"))
    assert cb.record_failure(cb.CHANNEL_SMS, "HTTP 500") is False
    assert next(p for s, p in db.calls if s is cb._COUNT_FAILURE)["trip"] is False
    assert cb._HOLD_APPROVED not in db.statements() and cb._EVENT not in db.statements()


def test_failures_after_the_trip_do_not_announce_again(monkeypatch):
    _session(monkeypatch, _failure_handler("open", 9))
    monkeypatch.setattr(cb, "_announce_trip", lambda *a: pytest.fail("announced twice"))
    assert cb.record_failure(cb.CHANNEL_SMS, "HTTP 500") is False


def test_recording_never_raises_even_when_the_database_is_down(monkeypatch):
    def broken(stmt, params):
        raise RuntimeError("down")

    _session(monkeypatch, broken)
    assert cb.record_failure(cb.CHANNEL_SMS, "HTTP 500") is False
    cb.record_success(cb.CHANNEL_SMS)


def test_a_success_resets_the_run_only_while_closed(monkeypatch):
    db = _session(monkeypatch, lambda stmt, params: Result())
    cb.record_success(cb.CHANNEL_EMAIL)
    assert db.statements() == [cb._COUNT_SUCCESS] and "state = 'closed'" in str(cb._COUNT_SUCCESS)


# ── The senders ────────────────────────────────────────────────────────────────

class Resp:
    def __init__(self, status, body=None):
        self.status_code, self._body = status, body

    def json(self):
        if self._body is None:
            raise ValueError("no json")
        return self._body


class Recorder:
    def __init__(self, *responses):
        self.calls, self._responses = [], list(responses)

    def __call__(self, method, url, **kwargs):
        self.calls.append((method, url))
        return self._responses.pop(0)


@pytest.fixture
def breaker(monkeypatch):
    """The breaker's three entry points, recorded; ``admit`` can be told to refuse."""
    seen = SimpleNamespace(admitted=[], failures=[], successes=[], refuse=None)

    def admit(channel):
        if seen.refuse is not None:
            raise seen.refuse(channel, "test")
        seen.admitted.append(channel)

    monkeypatch.setattr(cb, "admit", admit)
    monkeypatch.setattr(cb, "recheck", lambda channel: None)
    monkeypatch.setattr(cb, "record_failure", lambda channel, error, *, ambiguous=False:
                        seen.failures.append((channel, error, ambiguous)) or False)
    monkeypatch.setattr(cb, "record_success", lambda channel: seen.successes.append(channel))
    return seen


def _sms(*responses, internal_alert=False):
    return GhlSmsSender(ACCOUNT, "+18135550100", request=Recorder(*responses), internal_alert=internal_alert)


GOOD = (Resp(200, {"contact": {"id": "ct1"}}), Resp(201, {"messageId": "m1"}))


def test_a_good_text_asks_the_breaker_first_and_resets_the_run(breaker):
    assert _sms(*GOOD)(PHONE, "hi", None) == "m1"
    assert breaker.admitted == [cb.CHANNEL_SMS] and breaker.successes == [cb.CHANNEL_SMS] and breaker.failures == []


def test_a_paused_channel_refuses_before_any_request_is_made(breaker):
    breaker.refuse = cb.ChannelPaused
    sender = _sms(*GOOD)
    with pytest.raises(GhlChannelPaused) as err:
        sender(PHONE, "hi", None)
    assert err.value.rate_limited is False and err.value.ambiguous is False and isinstance(err.value, GhlSmsError)
    assert sender._http().calls == []


def test_a_rate_limited_channel_is_marked_so_the_card_can_wait_for_the_next_minute(breaker):
    breaker.refuse = cb.RateLimited
    with pytest.raises(GhlChannelPaused) as err:
        _sms(*GOOD)(PHONE, "hi", None)
    assert err.value.rate_limited is True


@pytest.mark.parametrize("send_response,expected", [
    (Resp(503, {}), ("HTTP 503", True)),
    (Resp(400, {}), ("HTTP 400", False)),
    (Resp(201, {}), ("no_message_id", True)),
])
def test_send_failures_count_toward_the_breaker(breaker, send_response, expected):
    with pytest.raises(GhlSmsError):
        _sms(Resp(200, {"contact": {"id": "ct1"}}), send_response)(PHONE, "hi", None)
    assert breaker.failures == [(cb.CHANNEL_SMS, *expected)] and breaker.successes == []


def test_a_timeout_on_the_send_counts_and_is_ambiguous(breaker):
    calls = []

    def request(method, url, **kwargs):
        calls.append(url)
        if len(calls) == 1:
            return Resp(200, {"contact": {"id": "ct1"}})
        raise TimeoutError("read timed out")

    with pytest.raises(GhlSmsError) as err:
        GhlSmsSender(ACCOUNT, "+18135550100", request=request)(PHONE, "hi", None)
    assert err.value.ambiguous is True and breaker.failures == [(cb.CHANNEL_SMS, "TimeoutError", True)]


def test_a_bad_contact_on_the_upsert_is_not_the_channels_health_but_a_5xx_is(breaker):
    with pytest.raises(GhlSmsError):
        _sms(Resp(422, {}))(PHONE, "hi", None)
    assert breaker.failures == []
    with pytest.raises(GhlSmsError):
        _sms(Resp(502, {}))(PHONE, "hi", None)
    assert breaker.failures == [(cb.CHANNEL_SMS, "HTTP 502", False)]


def test_alerts_to_the_teams_own_phone_skip_the_breaker_and_the_cap(breaker):
    breaker.refuse = cb.ChannelPaused
    assert _sms(*GOOD, internal_alert=True)(PHONE, "alarm", None) == "m1"
    assert breaker.admitted == [] and breaker.successes == []


def test_email_has_its_own_breaker_channel(breaker):
    request = Recorder(Resp(200, {"contact": {"id": "ct1"}}), Resp(201, {"messageId": "e1"}))
    sender = ghl_email.GhlEmailSender(ACCOUNT, "from@ndl.test", request=request)
    sender._sender_for = lambda to, phone: ("from@ndl.test", [])
    assert sender("a@b.test", "Subject", "Body") == "e1"
    assert breaker.admitted == [cb.CHANNEL_EMAIL] and breaker.successes == [cb.CHANNEL_EMAIL]


# ── The card executor wrapper ───────────────────────────────────────────────────

def _wrapped(error: Exception):
    def executor(payload):
        raise error

    return cora_agent.breaker_aware(executor)


def test_a_tripped_channel_holds_the_card():
    with pytest.raises(SendHeld) as held:
        _wrapped(GhlChannelPaused(cb.ChannelPaused(cb.CHANNEL_SMS, "tripped")))({})
    assert held.value.held_reason == cb.HELD_BREAKER_OPEN


def test_a_rate_limited_channel_retries_the_card_next_sweep():
    with pytest.raises(SendRetryLater):
        _wrapped(GhlChannelPaused(cb.RateLimited(cb.CHANNEL_SMS, "over 50 a minute")))({})


def test_an_uncertain_send_is_held_and_a_definite_failure_stays_failed():
    with pytest.raises(SendHeld) as held:
        _wrapped(GhlSmsError("GHL message send failed: HTTP 503", ambiguous=True))({})
    assert held.value.held_reason == revalidate.HOLD_UNCERTAIN_OUTCOME
    with pytest.raises(GhlSmsError):
        _wrapped(GhlSmsError("GHL message send failed: HTTP 400"))({})


def test_the_card_wrapper_marks_the_sends_inside_as_card_sends(monkeypatch):
    seen = []
    monkeypatch.setattr(cb, "admit", lambda channel: seen.append(cb._RESERVE.get()))

    def executor(payload):
        cb.admit(cb.CHANNEL_SMS)
        return "m1"

    assert cora_agent.breaker_aware(executor)({}) == "m1"
    assert seen == [cb.LIVE_REPLY_RESERVE] and cb._RESERVE.get() == 0


# ── @Cora resume ───────────────────────────────────────────────────────────────

@pytest.mark.parametrize("text_,channel", [
    ("<@UCORA> resume sms", cb.CHANNEL_SMS), ("resume email please", cb.CHANNEL_EMAIL),
    ("Cora, resume ghl sms now", cb.CHANNEL_SMS), ("resume instantly", cb.CHANNEL_INSTANTLY),
    ("resume", None), ("can you resume sms for me", None), ("emergency resume", None), ("resume fax", None),
])
def test_resume_command_parsing(text_, channel):
    assert resume_channel(text_) == channel


class Chat:
    def __init__(self):
        self.posts = []

    def post(self, **kwargs):
        self.posts.append(kwargs)


def _message(text_, user="U_APPROVER"):
    return SimpleNamespace(text=text_, user_id=user, channel="C1", reply_thread_ts="1.0")


def test_only_an_approver_can_reopen_a_channel():
    chat, resets = Chat(), []
    hook = ChannelResume(chat=chat, approver_ids={"U_APPROVER"}, reset=lambda ch, who: resets.append((ch, who)))
    assert hook(_message("resume sms", user="U_STRANGER")) is True
    assert resets == [] and "Only an approver" in chat.posts[0]["text"]


def test_an_approver_reopens_the_channel_and_is_told_what_came_back_for_approval():
    chat, resets = Chat(), []

    def reset(channel, who):
        resets.append((channel, who))
        return cb.ResetResult(channel, True, 3)

    assert ChannelResume(chat=chat, approver_ids={"U_APPROVER"}, reset=reset)(_message("@Cora resume sms")) is True
    assert resets == [(cb.CHANNEL_SMS, "slack:U_APPROVER")]
    assert "3 held card(s)" in chat.posts[0]["text"] and "approved again" in chat.posts[0]["text"]


def test_other_messages_fall_through_to_the_next_hook():
    chat = Chat()
    resume = ChannelResume(chat=chat, approver_ids={"U_APPROVER"}, reset=lambda *a: pytest.fail("reset"))
    assert resume(_message("good morning Cora")) is False
    handled = []
    assert first_handled(lambda m: False, lambda m: handled.append(m) or True)(_message("x")) is True and handled


# ── Re-validation ──────────────────────────────────────────────────────────────

def _action(**fields) -> PendingAction:
    base = dict(action_id=1, tool_name="draft_sms", channel="ghl_sms", payload={}, summary="", status=ActionStatus.APPROVED,
                requested_by=None, source_channel=None, source_thread_ts=None, card_channel=None, card_ts=None,
                decided_by="U1", revised_by=None, revision_note=None, revisions=(), recipient_phone=PHONE,
                recipient_email="sam@example.com", contact_ref="web_lead:12", deal_ref=None, idempotency_key=None,
                expires_at=None, provider_ref=None, error=None)
    return PendingAction(**{**base, **fields})


class Conn:
    def __init__(self, rows):
        self.rows, self.queries = rows, []

    def execute(self, stmt, params=None):
        self.queries.append(params)
        return Result(self.rows)


SMS, EMAIL = {"ghl_sms"}, {"ghl_email", "instantly_email", "instantly_cold_email"}


def test_an_unchanged_lead_passes():
    conn = Conn([(PHONE, "sam@example.com")])
    assert revalidate.changed_recipient(conn, _action(), sms_channels=SMS, email_channels=EMAIL) is None


def test_a_phone_edited_after_approval_holds_a_text():
    conn = Conn([("+18135559999", "sam@example.com")])
    hold = revalidate.changed_recipient(conn, _action(), sms_channels=SMS, email_channels=EMAIL)
    assert isinstance(hold, SendHold) and hold == "recipient_changed"


def test_an_email_edited_after_approval_holds_an_email_but_a_phone_edit_does_not():
    conn = Conn([("+18135559999", "sam@example.com")])
    email = _action(channel="ghl_email")
    assert revalidate.changed_recipient(conn, email, sms_channels=SMS, email_channels=EMAIL) is None
    conn = Conn([(PHONE, "new@example.com")])
    assert revalidate.changed_recipient(conn, email, sms_channels=SMS, email_channels=EMAIL) == "recipient_changed"


def test_formatting_differences_are_not_edits():
    conn = Conn([("(813) 555-8601", "  SAM@Example.com ")])
    assert revalidate.changed_recipient(conn, _action(), sms_channels=SMS, email_channels=EMAIL) is None
    assert revalidate.changed_recipient(Conn([("813-555-8601", " SAM@Example.com ")]), _action(channel="ghl_email"),
                                        sms_channels=SMS, email_channels=EMAIL) is None


def test_a_lead_that_no_longer_exists_holds_the_send():
    assert revalidate.changed_recipient(Conn([]), _action(), sms_channels=SMS, email_channels=EMAIL) == "lead_not_found"


def test_a_ghl_contact_with_no_lead_record_is_allowed_and_a_foreign_reference_is_not_compared():
    assert revalidate.changed_recipient(Conn([]), _action(contact_ref="ghl:abc"), sms_channels=SMS,
                                        email_channels=EMAIL) is None
    conn = Conn([("+18135559999", None)])
    assert revalidate.changed_recipient(conn, _action(contact_ref="external:5"), sms_channels=SMS,
                                        email_channels=EMAIL) is None and conn.queries == []


def test_crm_writes_and_actions_without_a_reference_have_no_recipient_to_compare():
    conn = Conn([("+18135559999", None)])
    assert revalidate.changed_recipient(conn, _action(channel="ghl_stage"), sms_channels=SMS, email_channels=EMAIL) is None
    assert revalidate.changed_recipient(conn, _action(contact_ref=None), sms_channels=SMS, email_channels=EMAIL) is None
    assert conn.queries == []


def test_a_ghl_contact_edited_in_our_records_holds_the_send():
    conn = Conn([("+18135559999", None), ("+18135558888", None)])
    assert revalidate.changed_recipient(conn, _action(contact_ref="ghl:abc"), sms_channels=SMS,
                                        email_channels=EMAIL) == "recipient_changed"

def test_reply_and_chase_recipient_edits_hold_the_send():
    for ref in ("instantly_reply:5", "doc_chase:5"):
        action = _action(channel="ghl_email", contact_ref=ref)
        assert revalidate.changed_recipient(Conn([(None, "new@example.com")]), action, sms_channels=SMS, email_channels=EMAIL) == "recipient_changed"


# ── PR #476 review fixes ───────────────────────────────────────────────────────

def _ghl_action(**fields):
    return _action(contact_ref="ghl:abc", **fields)


def test_a_ghl_card_is_checked_against_the_ghl_contact_itself():
    """Review 2: agent jobs draft from GHL, so the send check reads GHL, not our intake records."""
    reader = lambda contact_id: SimpleNamespace(phone="+18135559999", email="sam@example.com")
    conn = Conn([(PHONE, "sam@example.com")])  # our records still carry the old number: not consulted
    hold = revalidate.changed_recipient(conn, _ghl_action(), sms_channels=SMS, email_channels=EMAIL, ghl_reader=reader)
    assert hold == "recipient_changed" and conn.queries == []
    same = lambda contact_id: SimpleNamespace(phone=PHONE, email="sam@example.com")
    assert revalidate.changed_recipient(Conn([]), _ghl_action(), sms_channels=SMS, email_channels=EMAIL,
                                        ghl_reader=same) is None


def test_a_deleted_ghl_contact_holds_and_a_failed_read_defers():
    from packages.agent_core.relay import SendDeferred

    gone = revalidate.changed_recipient(Conn([]), _ghl_action(), sms_channels=SMS, email_channels=EMAIL,
                                        ghl_reader=lambda contact_id: None)
    assert gone == "lead_not_found" and isinstance(gone, SendHold)

    def down(contact_id):
        raise RuntimeError("GHL 503")

    later = revalidate.changed_recipient(Conn([]), _ghl_action(), sms_channels=SMS, email_channels=EMAIL,
                                         ghl_reader=down)
    assert isinstance(later, SendDeferred)


def test_the_breaker_is_checked_again_before_the_final_send_request(breaker, monkeypatch):
    """Review 3: another worker can trip the breaker while this one upserts the contact."""
    def tripped(channel):
        raise cb.ChannelPaused(channel, "tripped while this send was being prepared")

    monkeypatch.setattr(cb, "recheck", tripped)
    sender = _sms(*GOOD)
    with pytest.raises(GhlChannelPaused):
        sender(PHONE, "hi", None)
    assert [url.rsplit("/", 1)[-1] for _, url in sender._http().calls] == ["upsert"]  # the message send never went


def test_the_recheck_takes_no_rate_limit_slot(monkeypatch):
    db = _session(monkeypatch, _admit_handler(state="closed"))
    cb.recheck(cb.CHANNEL_SMS)
    assert cb._TAKE_SLOT not in db.statements()
    _session(monkeypatch, _admit_handler(state="open"))
    with pytest.raises(cb.ChannelPaused):
        cb.recheck(cb.CHANNEL_SMS)


def test_a_paused_channel_puts_a_missed_call_text_back_to_pending_not_failed(monkeypatch):
    """Review 4: the text is retried on the next poll while still inside its 60 s window."""
    from datetime import datetime, timezone

    from src.lending import text_back

    recorded = []
    monkeypatch.setattr(text_back, "_decide", lambda db, item, **kw: "send")
    monkeypatch.setattr(text_back, "_render", lambda item, number, booking_link: ("k", "body"))
    monkeypatch.setattr(text_back, "_record", lambda db, event_id, status, *a: recorded.append(status))

    def paused(*a, **k):
        raise GhlChannelPaused(cb.RateLimited(cb.CHANNEL_SMS, "over the cap"))

    item = SimpleNamespace(event_id=1, phone=PHONE, borrower_name="Sam", ended_at=datetime.now(timezone.utc))
    out = text_back._decide_and_send(None, item, sender=paused, enabled=True, number="+18135550100",
                                     now=datetime.now(timezone.utc), started=[False])
    assert out == "deferred_channel_paused" and recorded == ["pending"]


def test_an_instantly_reply_without_an_id_is_unconfirmed_and_never_resets_the_run(breaker, monkeypatch):
    """Review 5."""
    from src.lending import instantly_client

    account = instantly_client.InstantlyAccount("k", "https://api.instantly.ai")
    monkeypatch.setattr("src.lending.global_halt.is_globally_halted", lambda: False)
    monkeypatch.setattr(instantly_client.requests, "post", lambda *a, **k: Resp(200, {}))
    with pytest.raises(instantly_client.InstantlyError) as err:
        instantly_client.send_reply(account, eaccount="a@x.test", reply_to_uuid="u", subject="s", body="b")
    assert err.value.ambiguous is True and breaker.successes == []
    assert breaker.failures == [(cb.CHANNEL_INSTANTLY, "no_reply_id", True)]
