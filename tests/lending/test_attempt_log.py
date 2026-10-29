"""Attempt log for one LendingFlow lead: every outbound try, who made it, and whether the lead ever replied."""
from __future__ import annotations

import csv
import io
from datetime import datetime, timedelta, timezone

from src.lending.attempt_log import (
    AUTOMATED,
    PERSON,
    CallRow,
    LeadRef,
    build_attempt_log,
    to_csv,
)

T0 = datetime(2026, 10, 20, 14, 0, tzinfo=timezone.utc)
LEAD = LeadRef(lead_id=7, vendor_lead_id="LF-7", phone="+18135550123", received_at=T0, ghl_contact_id="c1")


def call(minutes: int, *, direction: str = "outbound", disposition: str | None = "NO_ANSWER", talk: int = 0,
         source: str = "batchdialer", recording_status: str | None = None, recording_ref: str | None = None,
         raw: str | None = None) -> CallRow:
    return CallRow(at=T0 + timedelta(minutes=minutes), direction=direction, disposition=disposition,
                   disposition_raw=raw, talk_sec=talk, call_source=source, recording_status=recording_status, recording_ref=recording_ref)


def message(minutes: int, *, kind: str = "TYPE_SMS", direction: str = "outbound", source: str | None = "workflow",
            user_id: str | None = None, status: str = "delivered") -> dict:
    added = (T0 + timedelta(minutes=minutes)).isoformat()
    return {"id": f"m{minutes}", "messageType": kind, "direction": direction, "dateAdded": added, "source": source,
            "userId": user_id, "status": status}


def test_calls_voicemails_texts_and_emails_are_attempts_in_time_order() -> None:
    log = build_attempt_log(
        LEAD,
        [call(30, disposition="LEFT_VOICEMAIL"), call(5)],
        [message(1), message(60, kind="TYPE_EMAIL", source="app", user_id="u1")],
    )
    assert [(a.channel, a.sent_by) for a in log.attempts] == [
        ("text", AUTOMATED), ("call", PERSON), ("voicemail", PERSON), ("email", PERSON)]
    assert log.first_attempt_at == T0 + timedelta(minutes=1)
    assert log.last_attempt_at == T0 + timedelta(minutes=60)
    assert log.minutes_to_first_attempt == 1


def test_inbound_messages_and_calls_are_not_attempts_but_mark_the_reply() -> None:
    log = build_attempt_log(
        LEAD,
        [call(5), call(90, direction="inbound", disposition="CALLBACK_REQUESTED", talk=40)],
        [message(1), message(45, direction="inbound")],
    )
    assert len(log.attempts) == 2
    assert log.first_reply_at == T0 + timedelta(minutes=45)


def test_an_answered_outbound_call_counts_as_a_reply() -> None:
    log = build_attempt_log(LEAD, [call(5, disposition="CALLBACK_REQUESTED", talk=60)], [])
    assert log.first_reply_at == T0 + timedelta(minutes=5)


def test_a_lead_that_never_engaged_has_no_reply() -> None:
    log = build_attempt_log(LEAD, [call(5), call(30, disposition="LEFT_VOICEMAIL")], [message(1)])
    assert log.first_reply_at is None


def test_ghl_call_messages_are_skipped_because_calls_come_from_the_call_log() -> None:
    log = build_attempt_log(LEAD, [call(5, source="ghl")], [message(5, kind="TYPE_CALL")])
    assert [a.channel for a in log.attempts] == ["call"]


def test_messages_before_the_lead_arrived_are_left_out() -> None:
    log = build_attempt_log(LEAD, [], [message(-10), message(-5, direction="inbound"), message(2)])
    assert len(log.attempts) == 1
    assert log.first_reply_at is None


def test_bad_number_and_wrong_person_are_flagged() -> None:
    log = build_attempt_log(LEAD, [call(5, disposition="BAD_NUMBER"), call(9, disposition="WRONG_PERSON")], [])
    assert log.bad_number and log.wrong_person


def test_raw_dialer_results_are_mapped_to_our_codes() -> None:
    log = build_attempt_log(LEAD, [call(5, disposition=None, raw="Answering Machine"),
                                   call(9, disposition=None, raw="DISCONNECTED_NUMBER")], [])
    assert [a.channel for a in log.attempts] == ["voicemail", "call"]
    assert log.bad_number


def test_a_voicemail_with_talk_time_is_not_a_reply() -> None:
    log = build_attempt_log(LEAD, [call(5, disposition="LEFT_VOICEMAIL", talk=25)], [])
    assert log.first_reply_at is None


def test_an_inbound_call_counts_as_a_reply_even_when_missed() -> None:
    log = build_attempt_log(LEAD, [call(5, direction="inbound", disposition="NO_ANSWER")], [])
    assert log.attempts == [] and log.first_reply_at == T0 + timedelta(minutes=5)


def test_call_rows_carry_the_recording_state() -> None:
    log = build_attempt_log(LEAD, [call(5, recording_status="readable", recording_ref="rec-1"), call(9)], [])
    assert [a.recording for a in log.attempts] == ["readable", ""]
    assert [a.recording_url for a in log.attempts] == ["rec-1", ""]


def test_csv_has_one_row_per_attempt_and_no_full_phone() -> None:
    log = build_attempt_log(LEAD, [call(5)], [message(1)])
    body = to_csv(log)
    rows = list(csv.DictReader(io.StringIO(body)))
    assert [r["channel"] for r in rows] == ["text", "call"]
    assert rows[0]["sent_by"] == AUTOMATED
    assert "8135550123" not in body
    assert "0123" in body
    assert rows[0]["attempted_at_et"].endswith("-04:00")
    assert "recording_url" in rows[0]


class _Channels:
    def __init__(self, fail: bool = False) -> None:
        self.fail, self.uploads = fail, []

    def upload(self, channel_id, thread_ts, content, filename, title) -> None:
        if self.fail:
            raise RuntimeError("files:write missing")
        self.uploads.append((channel_id, thread_ts, filename, content))


def _toolkit(monkeypatch, channels):
    from contextlib import contextmanager

    from src.lending import cora_tools

    log = build_attempt_log(LEAD, [call(5)], [message(1)])
    monkeypatch.setattr(cora_tools, "load_lead_calls", lambda session, lead_id: None)
    monkeypatch.setattr(cora_tools, "attempt_log_for", lambda lead_calls, reader: (log, True))
    toolkit = cora_tools.CoraToolkit(engine=None, memory=None, channels=channels)

    @contextmanager
    def no_db():
        yield None

    monkeypatch.setattr(toolkit, "_read_only", no_db)
    return toolkit


def _context():
    from packages.agent_core.tools import ToolContext

    return ToolContext(user_id="U1", is_approver=True, channel="C1", thread_ts="1.0", tool_use_id="t1")


def test_cora_tool_returns_the_summary_and_attaches_the_csv_to_the_thread(monkeypatch) -> None:
    from src.lending.cora_tools import AttemptLogInput

    channels = _Channels()
    result = _toolkit(monkeypatch, channels).attempt_log(AttemptLogInput(lead_id=7), _context())
    assert result["attempts"] == 2 and result["automated"] == 1 and result["replied"] is False
    assert result["csv_attached"] is True
    assert channels.uploads[0][:3] == ("C1", "1.0", "attempt-log-lead-7.csv")
    assert [row["channel"] for row in result["attempt_rows"]] == ["text", "call"]


def test_cora_tool_still_answers_when_the_upload_fails(monkeypatch) -> None:
    from src.lending.cora_tools import AttemptLogInput

    result = _toolkit(monkeypatch, _Channels(fail=True)).attempt_log(AttemptLogInput(lead_id=7), _context())
    assert result["csv_attached"] is False and result["attempts"] == 2


def test_cora_tool_is_registered_read_only() -> None:
    from packages.agent_core.governance import SafetyLevel
    from src.lending.cora_tools import CoraToolkit

    tool = next(t for t in CoraToolkit(engine=None, memory=None).tools() if t.name == "get_attempt_log")
    assert tool.safety == SafetyLevel.READ_ONLY


def test_messages_are_read_outside_the_database_and_a_failed_read_leaves_calls_only() -> None:
    from src.lending.attempt_log import LeadCalls, attempt_log_for

    def broken(contact_id: str) -> list:
        raise RuntimeError("ghl down")

    log, read = attempt_log_for(LeadCalls(LEAD, [call(5)], None), broken)
    assert read is False and [a.channel for a in log.attempts] == ["call"]
    log, read = attempt_log_for(LeadCalls(LEAD, [call(5)], None), lambda contact_id: [message(1)])
    assert read is True and len(log.attempts) == 2


def test_bad_number_on_an_inbound_call_is_not_flagged() -> None:
    log = build_attempt_log(LEAD, [call(5, direction="inbound", disposition="BAD_NUMBER")], [])
    assert not log.bad_number


def test_a_ghl_voicemail_drop_is_an_automated_voicemail_attempt() -> None:
    log = build_attempt_log(LEAD, [], [message(3, kind="TYPE_VOICEMAIL")])
    assert [(a.channel, a.sent_by) for a in log.attempts] == [("voicemail", AUTOMATED)]
