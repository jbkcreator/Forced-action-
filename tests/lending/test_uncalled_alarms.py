"""T-10 uncalled-lead alarms: pickup, call classification (CDR + GHL), once-only alarms, wording."""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from migrations.apply_lending_uncalled_alarms import apply_to

ARRIVED = datetime(2026, 10, 9, 15, 0, tzinfo=timezone.utc)
LEAD_PHONE = "+17275550100"
JOSH = "+18135550199"
OPS = "C-OPS"

# Used only when T-11's real table is absent. The shared DB already has it (with rows), so CREATE IF NOT EXISTS
# is a no-op there and lead() fills the real table's NOT NULL columns.
_T11_LEADS = """
CREATE TABLE IF NOT EXISTS lending.lendingflow_leads (
    id SERIAL PRIMARY KEY,
    lead_uuid UUID NOT NULL DEFAULT gen_random_uuid(),
    vendor_lead_id VARCHAR(100) NOT NULL UNIQUE,
    dedupe_hash VARCHAR(64) NOT NULL UNIQUE,
    raw_payload JSONB NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    first_name VARCHAR(80),
    phone VARCHAR(20) NOT NULL,
    property_state VARCHAR(20),
    suppressed BOOLEAN NOT NULL DEFAULT false,
    ghl_contact_id VARCHAR(64)
)"""


class Sms:
    def __init__(self, error: Exception | None = None):
        self.sent, self.error = [], error

    def __call__(self, to, body, first_name=None, *, deadline=None):
        if self.error:
            raise self.error
        self.sent.append((to, body))
        return "msg"


class Slack:
    def __init__(self, error: Exception | None = None):
        self.posts, self.error = [], error

    def chat_postMessage(self, channel, text):
        if self.error:
            raise self.error
        self.posts.append((channel, text))


@pytest.fixture
def db(lending_db):
    conn = lending_db.connection()
    conn.execute(text(_T11_LEADS))
    apply_to(conn)  # existing shared-DB leads become 'preexisting' and never alarm in tests
    return lending_db


def lead(db, *, arrived=ARRIVED, phone=LEAD_PHONE, suppressed=False, ghl_contact_id=None,
         first_name="Jane", state="FL") -> int:
    unique = uuid.uuid4().hex
    return db.execute(text(
        "INSERT INTO lending.lendingflow_leads (vendor_lead_id, dedupe_hash, raw_payload, received_at, first_name, "
        "phone, property_state, suppressed, ghl_contact_id) "
        "VALUES (:v, :h, '{}'::jsonb, :r, :n, :p, :st, :s, :g) RETURNING id"),
        {"v": f"t10-{unique}", "h": unique + unique, "r": arrived, "n": first_name, "p": phone, "st": state,
         "s": suppressed, "g": ghl_contact_id}).scalar()


def call(db, *, started, phone=LEAD_PHONE, direction="outbound", disposition=None, talk_seconds=60, call_id=None):
    db.execute(text(
        "INSERT INTO lending.call_dispositions (dialer_call_id, direction, phone, call_started_at, call_ended_at, "
        "disposition, talk_duration_sec, raw_event) VALUES (:id, :d, :p, :s, :s, :disp, :talk, '{}')"),
        {"id": call_id or f"t10-{uuid.uuid4().hex}", "d": direction, "p": phone, "s": started,
         "disp": disposition, "talk": talk_seconds})


def alarm(db, lead_id):
    return db.execute(text("SELECT * FROM lending.uncalled_alarms WHERE lendingflow_lead_id = :i"),
                      {"i": lead_id}).mappings().first()


def test_migration_is_idempotent(db):
    apply_to(db.connection())
    assert db.execute(text("SELECT to_regclass('lending.uncalled_alarms')")).scalar() is not None


def test_migration_marks_existing_leads_preexisting(db):
    lead_id = lead(db)
    apply_to(db.connection())  # the deploy step re-runs the migration right before the flag goes on
    row = alarm(db, lead_id)
    assert row["resolved_reason"] == "preexisting" and row["fired_120_at"] is None


from src.lending.uncalled_alarm_worker import pick_up_new_leads  # noqa: E402


def test_pickup_starts_the_stopwatch_at_received_at(db):
    lead_id = lead(db)
    pick_up_new_leads(db)
    row = alarm(db, lead_id)
    assert row["arrived_at"] == ARRIVED and row["phone"] == LEAD_PHONE and row["resolved_at"] is None


def test_pickup_twice_makes_one_alarm(db):
    lead_id = lead(db)
    pick_up_new_leads(db)
    pick_up_new_leads(db)
    assert db.execute(text("SELECT count(*) FROM lending.uncalled_alarms WHERE lendingflow_lead_id = :i"),
                      {"i": lead_id}).scalar() == 1


def test_suppressed_lead_is_never_picked_up(db):
    lead_id = lead(db, suppressed=True)
    pick_up_new_leads(db)
    assert alarm(db, lead_id) is None


def test_old_lead_is_still_picked_up(db):
    lead_id = lead(db, arrived=ARRIVED - timedelta(hours=3))
    pick_up_new_leads(db)
    assert alarm(db, lead_id)["resolved_at"] is None


from src.lending.ghl_account import GhlAccount  # noqa: E402
from src.lending.uncalled_alarm_worker import ATTEMPTED, CONNECTED, NONE, call_status, ghl_call_lookup  # noqa: E402


def _row(phone=LEAD_PHONE, ghl_contact_id=None, lead_uuid="u-1"):
    return {"phone": phone, "arrived_at": ARRIVED, "ghl_contact_id": ghl_contact_id, "lead_uuid": lead_uuid}


def test_no_calls_is_none(db):
    assert call_status(db, _row(), None) == NONE


@pytest.mark.parametrize("disposition", ["BOOKED", "CALLBACK_REQUESTED", "CONNECTED_NOT_INTERESTED", "WRONG_PERSON"])
def test_answered_disposition_is_connected(db, disposition):
    call(db, started=ARRIVED + timedelta(seconds=90), disposition=disposition)
    assert call_status(db, _row(), None) == CONNECTED


@pytest.mark.parametrize("disposition", ["NO_ANSWER", "LEFT_VOICEMAIL", "CALL_FAILED", "BAD_NUMBER"])
def test_unanswered_disposition_is_attempted(db, disposition):
    call(db, started=ARRIVED + timedelta(seconds=90), disposition=disposition, talk_seconds=20)
    assert call_status(db, _row(), None) == ATTEMPTED


@pytest.mark.parametrize("talk_seconds,expected", [(45, CONNECTED), (0, ATTEMPTED), (None, ATTEMPTED)])
def test_cdr_without_disposition_uses_talk_time(db, talk_seconds, expected):
    call(db, started=ARRIVED + timedelta(seconds=90), talk_seconds=talk_seconds)
    assert call_status(db, _row(), None) == expected


def test_one_connected_call_beats_earlier_unanswered_attempts(db):
    call(db, started=ARRIVED + timedelta(seconds=30), disposition="NO_ANSWER", talk_seconds=0)
    call(db, started=ARRIVED + timedelta(seconds=90), disposition="BOOKED")
    assert call_status(db, _row(), None) == CONNECTED


@pytest.mark.parametrize("direction,started", [("inbound", 60), ("outbound", -60)])
def test_inbound_call_or_call_before_arrival_does_not_count(db, direction, started):
    call(db, started=ARRIVED + timedelta(seconds=started), direction=direction, disposition="BOOKED")
    assert call_status(db, _row(), None) == NONE


def test_lead_without_ghl_contact_uses_cdr_only(db):
    def must_not_run(contact_id, since):
        raise AssertionError("GHL must not be queried without a contact id")

    assert call_status(db, _row(ghl_contact_id=None), must_not_run) == NONE


def test_ghl_result_is_combined_with_cdr(db):
    assert call_status(db, _row(ghl_contact_id="g"), lambda c, s: CONNECTED) == CONNECTED
    assert call_status(db, _row(ghl_contact_id="g"), lambda c, s: ATTEMPTED) == ATTEMPTED
    call(db, started=ARRIVED + timedelta(seconds=30), disposition="NO_ANSWER", talk_seconds=0)
    assert call_status(db, _row(ghl_contact_id="g"), lambda c, s: NONE) == ATTEMPTED


def test_ghl_failure_falls_back_to_cdr(db):
    def broken(contact_id, since):
        raise TimeoutError

    assert call_status(db, _row(ghl_contact_id="g"), broken) == NONE
    call(db, started=ARRIVED + timedelta(seconds=30), disposition="BOOKED")
    assert call_status(db, _row(ghl_contact_id="g"), broken) == CONNECTED


def test_one_call_closes_two_leads_with_the_same_phone(db):
    call(db, started=ARRIVED + timedelta(seconds=30), disposition="BOOKED")
    assert call_status(db, _row(lead_uuid="u-1"), None) == CONNECTED
    assert call_status(db, _row(lead_uuid="u-2"), None) == CONNECTED


class _Response:
    def __init__(self, body):
        self.body = body

    def raise_for_status(self):
        pass

    def json(self):
        return self.body


def _ghl_http(messages):
    def http(url, **kwargs):
        if url.endswith("/conversations/search"):
            assert kwargs["params"]["contactId"] == "ghl-1"
            return _Response({"conversations": [{"id": "conv-1"}]})
        assert kwargs["params"]["type"] == "TYPE_CALL"
        return _Response({"messages": {"messages": messages}})
    return http


def test_ghl_lookup_classifies_outbound_call_messages_after_arrival():
    account = GhlAccount("key", "loc")
    before = {"direction": "outbound", "dateAdded": "2026-10-09T14:59:00.000Z", "meta": {"callStatus": "completed"}}
    inbound = {"direction": "inbound", "dateAdded": "2026-10-09T15:01:00.000Z", "meta": {"callStatus": "completed"}}
    no_answer = {"direction": "outbound", "dateAdded": "2026-10-09T15:01:00.000Z", "meta": {"callStatus": "no-answer"}}
    answered = {"direction": "outbound", "dateAdded": "2026-10-09T15:02:00.000Z", "meta": {"callStatus": "completed"}}
    assert ghl_call_lookup(account, http=_ghl_http([before, inbound]))("ghl-1", ARRIVED) == NONE
    assert ghl_call_lookup(account, http=_ghl_http([no_answer]))("ghl-1", ARRIVED) == ATTEMPTED
    assert ghl_call_lookup(account, http=_ghl_http([no_answer, answered]))("ghl-1", ARRIVED) == CONNECTED


def test_ghl_lookup_naive_or_bad_dates_count_as_no_call():
    lookup = ghl_call_lookup(GhlAccount("key", "loc"), http=_ghl_http([
        {"direction": "outbound", "dateAdded": "2026-10-09T15:01:00", "meta": {"callStatus": "completed"}},
        {"direction": "outbound", "dateAdded": "not-a-date"},
        {"direction": "outbound"},
    ]))
    assert lookup("ghl-1", ARRIVED) == NONE


from src.lending.uncalled_alarm_worker import process_due  # noqa: E402


def cycle(db, seconds, *, sms=None, slack=None, ghl_status=None, sms_to: str | None = JOSH, ops=OPS):
    return process_due(db, now=ARRIVED + timedelta(seconds=seconds), sms=sms, slack=slack,
                       ghl_status=ghl_status, sms_to=sms_to, ops_channel=ops)


def test_uncalled_lead_alarms_once_at_120_then_escalates_once_at_300(db):
    lead_id = lead(db)
    sms, slack = Sms(), Slack()
    for seconds in (30, 119):
        cycle(db, seconds, sms=sms, slack=slack)
    assert sms.sent == [] and slack.posts == []

    cycle(db, 120, sms=sms, slack=slack)
    cycle(db, 200, sms=sms, slack=slack)
    assert len(sms.sent) == 1 and slack.posts == []
    to, body = sms.sent[0]
    assert to == JOSH
    assert body.startswith("NDL LEAD ALERT: New LendingFlow lead Jane (FL) came in 2 min ago and no call is logged yet.")
    assert "If you're already on the phone with them, ignore this." in body

    cycle(db, 300, sms=sms, slack=slack)
    cycle(db, 400, sms=sms, slack=slack)
    assert len(sms.sent) == 2 and sms.sent[1][1].startswith("URGENT - NDL LEAD STILL UNCALLED: ")
    assert "came in 5 min ago" in sms.sent[1][1]
    assert len(slack.posts) == 1 and slack.posts[0][0] == OPS
    assert slack.posts[0][1].startswith(":rotating_light: *URGENT: LendingFlow lead still uncalled*")
    assert "Jane" not in slack.posts[0][1] and LEAD_PHONE not in slack.posts[0][1]
    row = alarm(db, lead_id)
    assert (row["alarm_120_kind"], row["alarm_300_kind"]) == ("no_call", "no_call")
    assert (row["sms_120_status"], row["sms_300_status"], row["slack_300_status"]) == ("sent", "sent", "sent")
    assert row["resolved_reason"] == "escalated"


def test_unanswered_attempt_sends_not_reached_alarms(db):
    lead_id = lead(db)
    call(db, started=ARRIVED + timedelta(seconds=60), disposition="NO_ANSWER", talk_seconds=0)
    sms, slack = Sms(), Slack()
    cycle(db, 120, sms=sms, slack=slack)
    cycle(db, 300, sms=sms, slack=slack)
    assert sms.sent[0][1].startswith("NDL LEAD ALERT - NOT REACHED: LendingFlow lead Jane (FL) was called but not reached")
    assert sms.sent[1][1].startswith("URGENT - NDL LEAD STILL NOT REACHED: ")
    assert slack.posts[0][1].startswith(":warning: *URGENT: LendingFlow lead not reached*")
    row = alarm(db, lead_id)
    assert (row["alarm_120_kind"], row["alarm_300_kind"]) == ("not_reached", "not_reached")


def test_connected_before_120_never_alarms(db):
    lead_id = lead(db)
    call(db, started=ARRIVED + timedelta(seconds=90), disposition="BOOKED")
    sms, slack = Sms(), Slack()
    for seconds in (120, 300):
        cycle(db, seconds, sms=sms, slack=slack)
    assert sms.sent == [] and slack.posts == []
    assert alarm(db, lead_id)["resolved_reason"] == "connected"


def test_connected_between_alarms_stops_the_escalation(db):
    lead_id = lead(db)
    sms, slack = Sms(), Slack()
    cycle(db, 120, sms=sms, slack=slack)
    call(db, started=ARRIVED + timedelta(seconds=200), disposition="CALLBACK_REQUESTED")
    cycle(db, 300, sms=sms, slack=slack)
    assert len(sms.sent) == 1 and slack.posts == []
    assert alarm(db, lead_id)["resolved_reason"] == "connected"


def test_late_start_sends_only_the_escalation(db):
    lead_id = lead(db)
    sms, slack = Sms(), Slack()
    cycle(db, 360, sms=sms, slack=slack)  # worker was down through the 2-minute mark
    assert len(sms.sent) == 1 and sms.sent[0][1].startswith("URGENT") and "came in 6 min ago" in sms.sent[0][1]
    assert len(slack.posts) == 1
    assert alarm(db, lead_id)["sms_120_status"] == "skipped_late"


def test_old_lead_is_still_picked_up_and_alarms_late(db):
    lead(db, arrived=ARRIVED - timedelta(minutes=37))
    sms = Sms()
    cycle(db, 0, sms=sms, slack=Slack())
    assert len(sms.sent) == 1 and "came in 37 min ago" in sms.sent[0][1]


def test_worker_restart_still_escalates_once(db):
    lead_id = lead(db)
    cycle(db, 120, sms=Sms(), slack=Slack())
    sms, slack = Sms(), Slack()  # a fresh process: new senders, state only in Postgres
    for seconds in (150, 300, 310):
        cycle(db, seconds, sms=sms, slack=slack)
    assert len(sms.sent) == 1 and sms.sent[0][1].startswith("URGENT") and len(slack.posts) == 1
    assert alarm(db, lead_id)["resolved_reason"] == "escalated"


def test_claimed_alarm_is_never_sent_twice(db):
    lead_id = lead(db)
    pick_up_new_leads(db)
    db.execute(text("UPDATE lending.uncalled_alarms SET fired_120_at = :t WHERE lendingflow_lead_id = :i"),
               {"t": ARRIVED + timedelta(seconds=120), "i": lead_id})  # claimed, then the process died before sending
    sms = Sms()
    cycle(db, 130, sms=sms)
    assert sms.sent == [] and alarm(db, lead_id)["sms_120_status"] is None


def test_unexpected_sms_error_is_unknown_never_resent_and_does_not_block_slack(db):
    lead_id = lead(db)
    failing, slack = Sms(error=RuntimeError("boom")), Slack()
    for seconds in (120, 150, 300):
        cycle(db, seconds, sms=failing, slack=slack)
    row = alarm(db, lead_id)
    assert (row["sms_120_status"], row["sms_300_status"], row["slack_300_status"]) == ("send_unknown", "send_unknown", "sent")
    assert row["last_error"] == "RuntimeError"
    assert len(slack.posts) == 1


def test_missing_josh_number_or_channel_records_not_configured(db):
    lead_id = lead(db)
    cycle(db, 300, sms=Sms(), slack=None, sms_to=None, ops="")
    row = alarm(db, lead_id)
    assert (row["sms_300_status"], row["slack_300_status"]) == ("not_configured", "not_configured")


def test_alarm_text_without_name_or_state(db):
    lead(db, first_name=None, state=None)
    sms = Sms()
    cycle(db, 120, sms=sms)
    assert "lead (no name) came in" in sms.sent[0][1]


def test_preexisting_lead_never_alarms(db):
    lead_id = lead(db)
    apply_to(db.connection())  # marks it preexisting, as the deploy step does
    sms = Sms()
    cycle(db, 300, sms=sms, slack=Slack())
    assert sms.sent == [] and alarm(db, lead_id)["resolved_reason"] == "preexisting"


def test_messages_are_plain_ascii():
    from config.lending_alarms import SLACK_TEXT, SMS_TEXT

    for template in [*SMS_TEXT.values(), *SLACK_TEXT.values()]:
        assert template.isascii(), template


def test_logs_carry_no_phone_numbers(db, caplog):
    lead(db)
    with caplog.at_level(logging.DEBUG):
        cycle(db, 120, sms=Sms(error=RuntimeError(LEAD_PHONE)))
        cycle(db, 300, sms=Sms(), slack=Slack())
    assert LEAD_PHONE not in caplog.text and JOSH not in caplog.text and "Jane" not in caplog.text


from src.lending import uncalled_alarm_worker  # noqa: E402


def test_run_cycle_does_nothing_when_disabled(monkeypatch):
    class Settings:
        lending_uncalled_alarms_enabled = False

    def no_db():
        raise AssertionError("no DB access when disabled")

    monkeypatch.setattr(uncalled_alarm_worker, "get_settings", lambda: Settings())
    monkeypatch.setattr(uncalled_alarm_worker, "lending_session", no_db)
    assert uncalled_alarm_worker.run_cycle() == {}


# --- delivery retries: definite failures retry, unknown outcomes never resend -------------------------------------

from src.lending.ghl_sms import GhlSmsError  # noqa: E402
from slack_sdk.errors import SlackApiError  # noqa: E402

DEFINITE = GhlSmsError("GHL contact upsert failed: HTTP 500", ambiguous=False)
UNKNOWN = GhlSmsError("GHL message send failed: HTTP 502", ambiguous=True)


class FlakySms(Sms):
    """Raises ``errors`` in order, then succeeds."""

    def __init__(self, errors):
        super().__init__()
        self.errors, self.attempts = list(errors), 0

    def __call__(self, to, body, first_name=None, *, deadline=None):
        self.attempts += 1
        if self.errors:
            raise self.errors.pop(0)
        return super().__call__(to, body, first_name, deadline=deadline)


def states(db, lead_id):
    row = alarm(db, lead_id)
    return {k: row[k] for k in ("sms_120_status", "sms_300_status", "slack_300_status",
                                "sms_120_attempts", "sms_300_attempts", "slack_300_attempts", "resolved_reason")}


def test_definite_sms_failure_is_retried_after_a_short_wait_then_sent(db):
    lead_id = lead(db)
    sms = FlakySms([DEFINITE])
    cycle(db, 120, sms=sms)
    assert states(db, lead_id)["sms_120_status"] == "retry" and states(db, lead_id)["sms_120_attempts"] == 1
    cycle(db, 130, sms=sms)  # before the 15 s wait is over
    assert sms.attempts == 1
    cycle(db, 135, sms=sms)
    assert sms.attempts == 2 and len(sms.sent) == 1
    assert "came in 2 min ago" in sms.sent[0][1]
    assert states(db, lead_id)["sms_120_status"] == "sent" and states(db, lead_id)["sms_120_attempts"] == 2
    cycle(db, 200, sms=sms)
    assert sms.attempts == 2  # a sent alarm is never repeated


def test_unknown_sms_outcome_is_never_resent(db):
    lead_id = lead(db)
    sms = FlakySms([UNKNOWN])
    for seconds in (120, 135, 200):
        cycle(db, seconds, sms=sms)
    assert sms.attempts == 1 and states(db, lead_id)["sms_120_status"] == "send_unknown"


def test_retries_stop_after_three_attempts_and_log_an_error(db, caplog):
    lead_id = lead(db)
    sms = FlakySms([DEFINITE] * 10)
    with caplog.at_level(logging.ERROR):
        for seconds in (120, 135, 179, 180, 240, 280):
            cycle(db, seconds, sms=sms)
    assert sms.attempts == 3  # 120 s, +15 s, +45 s
    assert states(db, lead_id)["sms_120_status"] == "failed"
    assert any("gave up" in r.getMessage() and "sms_120" in r.getMessage() for r in caplog.records)
    assert LEAD_PHONE not in caplog.text and JOSH not in caplog.text


def test_pending_first_alarm_retry_is_dropped_when_the_escalation_fires(db):
    lead_id = lead(db)
    sms = FlakySms([DEFINITE])
    cycle(db, 120, sms=sms)        # fails, retry due at 135
    cycle(db, 300, sms=sms, slack=Slack())   # escalation: the 2-minute retry is superseded
    assert states(db, lead_id)["sms_120_status"] == "superseded"
    assert len(sms.sent) == 1 and sms.sent[0][1].startswith("URGENT")


def test_escalation_stays_open_until_its_retries_settle_then_resolves(db):
    lead_id = lead(db)
    sms, slack = FlakySms([DEFINITE]), Slack()
    cycle(db, 300, sms=sms, slack=slack)
    state = states(db, lead_id)
    assert (state["sms_300_status"], state["slack_300_status"], state["resolved_reason"]) == ("retry", "sent", None)
    cycle(db, 315, sms=sms, slack=slack)
    state = states(db, lead_id)
    assert (state["sms_300_status"], state["resolved_reason"]) == ("sent", "escalated")
    assert len(sms.sent) == 1 and len(slack.posts) == 1


def test_slack_rejection_is_retried_until_the_bot_is_in_the_channel(db):
    lead_id = lead(db)
    slack = Slack(error=slack_error("not_in_channel"))
    sms = Sms()
    cycle(db, 300, sms=sms, slack=slack)
    assert states(db, lead_id)["slack_300_status"] == "retry"
    slack.error = None  # someone invites the bot
    cycle(db, 315, sms=sms, slack=slack)
    assert states(db, lead_id)["slack_300_status"] == "sent" and len(slack.posts) == 1
    assert states(db, lead_id)["resolved_reason"] == "escalated"


def test_slack_timeout_is_unknown_and_not_reposted(db):
    lead_id = lead(db)
    slack = Slack(error=TimeoutError())
    for seconds in (300, 315, 400):
        cycle(db, seconds, sms=Sms(), slack=slack)
    assert states(db, lead_id)["slack_300_status"] == "send_unknown"


def test_retry_is_skipped_when_the_lead_has_been_reached_meanwhile(db):
    lead_id = lead(db)
    sms = FlakySms([DEFINITE])
    cycle(db, 120, sms=sms)
    call(db, started=ARRIVED + timedelta(seconds=130), disposition="BOOKED")
    cycle(db, 135, sms=sms)
    assert sms.attempts == 1 and states(db, lead_id)["sms_120_status"] == "superseded"


def test_retry_after_the_window_is_expired_not_sent(db):
    lead_id = lead(db)
    sms = FlakySms([DEFINITE])
    cycle(db, 300, sms=sms, slack=Slack())   # sms_300 fails, slack sent
    cycle(db, 300 + 20 * 60, sms=sms, slack=Slack())   # worker was down for 20 minutes
    assert sms.attempts == 1 and states(db, lead_id)["sms_300_status"] == "failed"
    assert states(db, lead_id)["resolved_reason"] == "escalated"


def test_channels_retry_independently(db):
    lead_id = lead(db)
    sms = FlakySms([DEFINITE])
    slack = Slack(error=slack_error("channel_not_found"))
    cycle(db, 300, sms=sms, slack=slack)
    slack.error = None
    cycle(db, 315, sms=sms, slack=slack)
    state = states(db, lead_id)
    assert (state["sms_300_status"], state["slack_300_status"]) == ("sent", "sent")
    assert (state["sms_300_attempts"], state["slack_300_attempts"]) == (2, 2)


def test_crash_after_a_retry_claim_becomes_unknown_and_is_not_resent(db):
    lead_id = lead(db)
    sms = FlakySms([DEFINITE])
    cycle(db, 120, sms=sms)
    db.execute(text("UPDATE lending.uncalled_alarms SET sms_120_status = 'sending', claimed_at = :t "
                    "WHERE lendingflow_lead_id = :i"), {"t": ARRIVED + timedelta(seconds=135), "i": lead_id})
    cycle(db, 140, sms=sms)   # claim is fresh: left alone
    assert states(db, lead_id)["sms_120_status"] == "sending"
    cycle(db, 135 + 130, sms=sms)   # claim is stale
    assert sms.attempts == 1
    assert states(db, lead_id)["sms_120_status"] == "send_unknown"


def test_crash_after_the_escalation_claim_is_settled_as_unknown(db):
    lead_id = lead(db)
    pick_up_new_leads(db)
    db.execute(text("UPDATE lending.uncalled_alarms SET fired_120_at = :t, fired_300_at = :t, alarm_300_kind = 'no_call' "
                    "WHERE lendingflow_lead_id = :i"), {"t": ARRIVED + timedelta(seconds=300), "i": lead_id})
    sms = Sms()
    cycle(db, 310, sms=sms, slack=Slack())   # fresh claim: nothing is resent, nothing is settled yet
    assert states(db, lead_id)["resolved_reason"] is None
    cycle(db, 300 + 130, sms=sms, slack=Slack())
    state = states(db, lead_id)
    assert (state["sms_300_status"], state["slack_300_status"], state["resolved_reason"]) == (
        "send_unknown", "send_unknown", "escalated")
    assert sms.sent == []


def test_migration_adds_the_retry_columns_to_an_existing_table(db):
    conn = db.connection()
    for column in ("sms_120_attempts", "sms_300_attempts", "slack_300_attempts", "retry_at", "claimed_at"):
        conn.execute(text(f"ALTER TABLE lending.uncalled_alarms DROP COLUMN {column}"))
    apply_to(conn)
    columns = {r[0] for r in conn.execute(text(
        "SELECT column_name FROM information_schema.columns WHERE table_schema='lending' AND table_name='uncalled_alarms'"))}
    assert {"sms_120_attempts", "sms_300_attempts", "slack_300_attempts", "retry_at", "claimed_at"} <= columns


# --- live clock: a slow batch must not judge later leads by the cycle's start time ---------------------------------


def test_a_slow_batch_escalates_later_leads_directly(db):
    state = {"t": ARRIVED + timedelta(seconds=120)}

    class SlowSms(Sms):
        """Every send takes 40 s of wall-clock time."""

        def __call__(self, to, body, first_name=None, *, deadline=None):
            result = super().__call__(to, body, first_name, deadline=deadline)
            state["t"] += timedelta(seconds=40)
            return result

    ids = [lead(db, phone=f"+1727555020{n}") for n in range(6)]
    sms, slack = SlowSms(), Slack()
    process_due(db, clock=lambda: state["t"], sms=sms, slack=slack, ghl_status=None, sms_to=JOSH, ops_channel=OPS)

    bodies = [body for _to, body in sms.sent]
    assert len(bodies) == 6
    assert sum(b.startswith("URGENT") for b in bodies) == 1  # the 6th lead, reached at 5 min 20 s
    escalated = next(b for b in bodies if b.startswith("URGENT"))
    assert "came in 5 min ago" in escalated and len(slack.posts) == 1
    first = [b for b in bodies if not b.startswith("URGENT")]
    assert all("came in" in b for b in first)
    rows = [alarm(db, i) for i in ids]
    assert sum(r["resolved_reason"] == "escalated" for r in rows) == 1
    assert sum(r["sms_120_status"] == "skipped_late" for r in rows) == 1


def test_message_age_is_taken_after_the_slow_call_lookup_not_at_cycle_start(db):
    state = {"t": ARRIVED + timedelta(seconds=120)}

    class SlowCallLookup:
        def __call__(self, contact_id, since):
            state["t"] += timedelta(seconds=70)   # the GHL history lookup is slow
            return "none"

    lead(db, ghl_contact_id="g-1")
    sms = Sms()
    process_due(db, clock=lambda: state["t"], sms=sms, slack=None, ghl_status=SlowCallLookup(), sms_to=JOSH,
                ops_channel="")
    assert len(sms.sent) == 1 and "came in 3 min ago" in sms.sent[0][1]   # 190 s, not the cycle's 120 s


# --- Slack error semantics: only a documented rejection is retried ---------------------------------------------------

from slack_sdk.web import SlackResponse  # noqa: E402


def slack_error(error, status=200):
    """The exception slack_sdk raises for ``ok=false`` (HTTP 200) or a non-200 reply, built from a real SlackResponse."""
    response = SlackResponse(client=None, http_verb="POST", api_url="https://slack.com/api/chat.postMessage",
                             req_args={}, data={"ok": False, "error": error}, headers={}, status_code=status)
    return SlackApiError("The request to the Slack API failed.", response)


@pytest.mark.parametrize("error,status", [
    ("channel_not_found", 200), ("not_in_channel", 200), ("is_archived", 200), ("invalid_auth", 200),
    ("account_inactive", 200), ("missing_scope", 200), ("restricted_action", 200), ("ratelimited", 429)])
def test_a_documented_slack_rejection_is_retried(db, error, status):
    lead_id = lead(db)
    cycle(db, 300, sms=Sms(), slack=Slack(error=slack_error(error, status)))
    assert states(db, lead_id)["slack_300_status"] == "retry"


@pytest.mark.parametrize("error,status", [
    ("internal_error", 200),        # Slack: "some aspect of the operation" may have succeeded
    ("fatal_error", 200),           # same wording
    ("internal_error", 500),        # an HTTP 500 raises SlackApiError too
    ("service_unavailable", 503),
    ("request_timeout", 200),       # the request body was cut off: not proof that nothing was posted
    ("some_future_error_code", 200)])   # default-deny: an error we do not recognise is never retried
def test_an_uncertain_slack_error_is_unknown_and_never_reposted(db, error, status):
    lead_id = lead(db)
    slack = Slack(error=slack_error(error, status))
    for seconds in (300, 315, 400):
        cycle(db, seconds, sms=Sms(), slack=slack)
    assert states(db, lead_id)["slack_300_status"] == "send_unknown"
    assert states(db, lead_id)["slack_300_attempts"] == 1 and slack.posts == []
    assert states(db, lead_id)["resolved_reason"] == "escalated"


def test_a_server_error_status_is_unknown_even_with_a_rejection_code(db):
    lead_id = lead(db)
    cycle(db, 300, sms=Sms(), slack=Slack(error=slack_error("channel_not_found", status=502)))
    assert states(db, lead_id)["slack_300_status"] == "send_unknown"
