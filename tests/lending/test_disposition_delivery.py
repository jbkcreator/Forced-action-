"""Sheet and Slack delivery for logged dispositions. Database-free."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from pydantic import SecretStr

from config.lending_dispositions import SHEET_COLUMNS
from src.lending import disposition_delivery as dd

NOW = datetime(2026, 9, 29, 16, 30, tzinfo=timezone.utc)  # 12:30 ET


def _row(**over):
    row = {
        "id": 1, "dialer_call_id": "c1", "disposition": "CONNECTED_NOT_INTERESTED", "phone": "+18135550142",
        "caller_seat": "7", "caller_name": "Sam Caller", "campaign_tag": "Builders", "queue": "builders", "dialer_contact_id": None,
        "unfunded_cause": "fit", "disposition_list_version": "2026-10-01", "booking_blocked": False, "recording_ref": "https://dialer.example/rec/c1", "talk_duration_sec": 45,
        "call_started_at": datetime(2026, 9, 29, 16, 0, tzinfo=timezone.utc), "call_ended_at": NOW,
        "disposition_at": NOW, "disposition_raw": None, "slack_ts": None,
        "sheet_synced_disposition": None, "slack_posted_disposition": None,
    }
    row.update(over)
    return row


@pytest.fixture(autouse=True)
def settings(monkeypatch):
    monkeypatch.setattr(dd, "get_settings", lambda: SimpleNamespace(
        lending_disposition_sheet_id="sheet-1", lending_disposition_sheet_tab="Dispositions",
        lending_dial_tasks_channel="C123", lending_slack_bot_token=SecretStr("xoxb-test"),
        lending_sheets_service_account_key_path="/key.json"))


# ── Sheet row ───────────────────────────────────────────────────────────────

def test_sheet_row_matches_the_column_order():
    line = dd.build_sheet_row(_row(), NOW)
    assert len(line) == len(SHEET_COLUMNS)
    assert line[0] == "c1"
    assert line[1] == "2026-09-29 12:00:00"
    assert line[4] == "builders"
    assert line[6] == "CONNECTED_NOT_INTERESTED"
    assert line[7] == "45"
    assert line[8:10] == ["N", "N"]
    assert line[10:12] == ["fit", "2026-10-01"]
    assert line[12] == "2026-09-29 12:30:00"


def test_sheet_flags_booked_and_dnc():
    assert dd.build_sheet_row(_row(disposition="BOOKED"), NOW)[8] == "Y"
    assert dd.build_sheet_row(_row(disposition="BOOKED", booking_blocked=True), NOW)[8] == "N"
    assert dd.build_sheet_row(_row(disposition="DNC_REQUEST"), NOW)[9] == "Y"


# ── Sheet sync ──────────────────────────────────────────────────────────────

def _sheets(existing):
    values = MagicMock()
    values.get.return_value.execute.return_value = {"values": existing}
    service = MagicMock()
    service.spreadsheets.return_value.values.return_value = values
    return service, values


def test_new_call_is_appended_after_the_header():
    service, values = _sheets([["Call ID"], ["other"]])
    dd.sync_sheet(_row(), service, NOW)
    values.update.assert_not_called()
    assert values.append.call_args.kwargs["body"]["values"][0][0] == "c1"


def test_changed_result_updates_the_same_row_in_place():
    service, values = _sheets([["Call ID"], ["other"], ["c1"]])
    dd.sync_sheet(_row(disposition="DNC_REQUEST"), service, NOW)
    values.append.assert_not_called()
    assert values.update.call_args.kwargs["range"] == "Dispositions!A3:M3"


def test_empty_sheet_gets_a_header_first():
    service, values = _sheets([])
    dd.sync_sheet(_row(), service, NOW)
    bodies = [c.kwargs["body"]["values"][0] for c in values.append.call_args_list]
    assert bodies[0] == list(SHEET_COLUMNS)
    assert bodies[1][0] == "c1"


# ── Slack ───────────────────────────────────────────────────────────────────

RECORD = {"borrower_name": "Pat Builder", "entity_name": "Pat Homes LLC", "property_address": "12 Oak St"}


def _text(blocks):
    return " ".join(b.get("text", {}).get("text", "") for b in blocks if "text" in b)


def test_standard_message_shows_full_phone_and_details():
    fallback, blocks = dd.build_slack_message(_row(), RECORD)
    body = _text(blocks)
    assert "Call result: CONNECTED_NOT_INTERESTED" in fallback
    assert "+18135550142" in body
    assert "Pat Builder" in body and "12 Oak St" in body and "builders" in body
    assert "https://dialer.example/rec/c1" not in body


def test_booked_gets_the_highlighted_card():
    fallback, blocks = dd.build_slack_message(_row(disposition="BOOKED"), RECORD)
    assert "Booked" in blocks[0]["text"]["text"]


def test_booked_on_a_nurture_list_is_flagged_not_celebrated():
    _, blocks = dd.build_slack_message(_row(disposition="BOOKED", booking_blocked=True), RECORD)
    assert "not counted" in blocks[0]["text"]["text"]


def test_dnc_request_gets_a_clear_marker():
    _, blocks = dd.build_slack_message(_row(disposition="DNC_REQUEST"), RECORD)
    assert "DNC request" in blocks[0]["text"]["text"]


def test_missing_load_record_shows_dashes_not_errors():
    _, blocks = dd.build_slack_message(_row(), None)
    assert "—" in _text(blocks)


def test_first_post_then_update_in_place():
    client = MagicMock()
    client.chat_postMessage.return_value = {"ts": "111.1"}
    assert dd.post_slack(_row(), None, client) == "111.1"
    client.chat_update.assert_not_called()

    assert dd.post_slack(_row(slack_ts="111.1"), None, client) == "111.1"
    assert client.chat_update.call_args.kwargs["ts"] == "111.1"


# ── deliver_disposition ─────────────────────────────────────────────────────

def _factory(row):
    db = MagicMock()
    db.execute.return_value.mappings.return_value.first.return_value = row

    @contextmanager
    def factory():
        yield db

    return factory, db


def test_sheet_failure_does_not_block_slack(monkeypatch):
    monkeypatch.setattr(dd, "lookup_load_record", lambda *a, **k: None)
    sheets, values = _sheets([])
    values.append.side_effect = RuntimeError("quota")
    slack = MagicMock()
    slack.chat_postMessage.return_value = {"ts": "1.1"}
    factory, db = _factory(_row())

    dd.deliver_disposition(1, session_factory=factory, slack_client=slack, sheets_service=sheets)

    slack.chat_postMessage.assert_called_once()
    updates = [str(c.args[0]) for c in db.execute.call_args_list if "UPDATE" in str(c.args[0])]
    assert len(updates) == 1 and "slack_posted_at" in updates[0]


def test_sync_stamps_the_delivered_disposition_not_the_rows_current_one(monkeypatch):
    monkeypatch.setattr(dd, "lookup_load_record", lambda *a, **k: None)
    sheets, _ = _sheets([])
    slack = MagicMock()
    slack.chat_postMessage.return_value = {"ts": "1.1"}
    factory, db = _factory(_row(disposition="NO_ANSWER"))

    dd.deliver_disposition(1, session_factory=factory, slack_client=slack, sheets_service=sheets)

    updates = [c for c in db.execute.call_args_list if "UPDATE" in str(c.args[0])]
    assert len(updates) == 2
    for call in updates:
        assert "= :d" in str(call.args[0]) and call.args[1]["d"] == "NO_ANSWER"


def test_delivery_is_skipped_while_another_run_holds_the_row():
    slack = MagicMock()
    sheets, values = _sheets([])
    factory, db = _factory(_row())
    db.execute.return_value.scalar.return_value = False  # advisory lock not granted

    dd.deliver_disposition(1, session_factory=factory, slack_client=slack, sheets_service=sheets)

    slack.chat_postMessage.assert_not_called()
    values.append.assert_not_called()
    assert not any("pg_advisory_unlock" in str(c.args[0]) for c in db.execute.call_args_list)


def test_lock_is_released_even_when_delivery_fails(monkeypatch):
    factory, db = _factory(_row())
    db.execute.return_value.scalar.return_value = True
    monkeypatch.setattr(dd, "_deliver_locked", MagicMock(side_effect=RuntimeError("boom")))

    dd.deliver_disposition(1, session_factory=factory)

    assert any("pg_advisory_unlock" in str(c.args[0]) for c in db.execute.call_args_list)


def test_already_delivered_disposition_is_not_resent(monkeypatch):
    slack = MagicMock()
    sheets, values = _sheets([])
    factory, _ = _factory(_row(sheet_synced_disposition="CONNECTED_NOT_INTERESTED", slack_posted_disposition="CONNECTED_NOT_INTERESTED"))
    dd.deliver_disposition(1, session_factory=factory, slack_client=slack, sheets_service=sheets)
    slack.chat_postMessage.assert_not_called()
    values.append.assert_not_called()


def test_row_that_never_had_a_result_is_skipped():
    slack = MagicMock()
    sheets, values = _sheets([])
    factory, _ = _factory(_row(disposition=None))
    dd.deliver_disposition(1, session_factory=factory, slack_client=slack, sheets_service=sheets)
    slack.chat_postMessage.assert_not_called()
    slack.chat_update.assert_not_called()
    values.append.assert_not_called()


def test_removed_result_blanks_the_sheet_row_and_updates_the_slack_message():
    slack = MagicMock()
    sheets, values = _sheets([["Call ID"], ["c1"]])
    factory, _ = _factory(_row(
        disposition=None, sheet_synced_disposition="CONNECTED_NOT_INTERESTED",
        slack_posted_disposition="CONNECTED_NOT_INTERESTED", slack_ts="5.5"))
    dd.deliver_disposition(1, session_factory=factory, slack_client=slack, sheets_service=sheets)
    assert values.update.call_args.kwargs["body"]["values"][0][6] == ""
    assert "Result removed" in slack.chat_update.call_args.kwargs["text"]


def test_removed_result_without_an_earlier_slack_post_posts_nothing():
    slack = MagicMock()
    factory, _ = _factory(_row(disposition=None, sheet_synced_disposition=None, slack_posted_disposition="CONNECTED_NOT_INTERESTED"))
    dd.deliver_disposition(1, session_factory=factory, slack_client=slack, sheets_service=MagicMock())
    slack.chat_postMessage.assert_not_called()


def test_dnc_alert_posts_to_the_channel_without_a_phone_number():
    slack = MagicMock()
    dd.alert_unpropagated_dnc("c9", "7", client=slack)
    kwargs = slack.chat_postMessage.call_args.kwargs
    assert kwargs["channel"] == "C123"
    assert "c9" in kwargs["text"] and "manually" in kwargs["text"]


def test_dnc_alert_never_raises():
    slack = MagicMock()
    slack.chat_postMessage.side_effect = RuntimeError("slack down")
    dd.alert_unpropagated_dnc("c9", None, client=slack)


def test_delivery_never_raises():
    def boom():
        raise RuntimeError("db down")

    dd.deliver_disposition(1, session_factory=boom)


def test_unknown_code_alert_names_the_code_and_never_raises():
    slack = MagicMock()
    dd.alert_unknown_code("c9", "Hot Lead", "7", client=slack)
    assert "Hot Lead" in slack.chat_postMessage.call_args.kwargs["text"]
    slack.chat_postMessage.side_effect = RuntimeError("slack down")
    dd.alert_unknown_code("c9", "Hot Lead", None, client=slack)


def test_dnc_removal_pending_alert_says_to_remove_manually_and_never_raises():
    slack = MagicMock()
    dd.alert_dnc_removal_pending("c9", "7", client=slack)
    assert "NOT yet removed" in slack.chat_postMessage.call_args.kwargs["text"]
    slack.chat_postMessage.side_effect = RuntimeError("slack down")
    dd.alert_dnc_removal_pending("c9", None, client=slack)


def test_card_never_shows_a_recording_link():
    record = {"borrower_name": "Pat Doe", "property_address": "1 Main St"}
    for status in ("readable", "pending", "forbidden", "missing"):
        _, blocks = dd.build_slack_message(_row(recording_status=status), record)
        assert "recording" not in blocks[1]["text"]["text"].lower()
        assert "https://dialer.example/rec/c1" not in str(blocks)
