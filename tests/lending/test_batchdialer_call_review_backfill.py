"""The call-review data fix against the real lending schema (rolled back; skipped without TEST_DATABASE_URL)."""
from __future__ import annotations

import json

from sqlalchemy import text

from migrations.apply_lending_batchdialer_call_review_backfill import run_steps


def _insert(conn, call_id, *, disposition=None, raw=None, talk=0, contact="559662160", transcript=None,
            posted=None, recording_enabled=1):
    conn.execute(text(
        "INSERT INTO lending.call_dispositions (dialer_call_id, call_source, direction, disposition, disposition_raw, "
        "talk_duration_sec, dialer_contact_id, transcript_status, slack_posted_disposition, sheet_synced_disposition, "
        "call_ended_at, raw_event) VALUES (:c, 'batchdialer', 'outbound', :d, :r, :t, :k, :ts, :p, :p, now(), "
        "CAST(:raw AS jsonb))"),
        {"c": call_id, "d": disposition, "r": raw, "t": talk, "k": contact, "ts": transcript, "p": posted,
         "raw": json.dumps({"recordingenabled": recording_enabled})})


def _row(conn, call_id):
    return conn.execute(text("SELECT * FROM lending.call_dispositions WHERE dialer_call_id = :c"),
                        {"c": call_id}).mappings().one()


def test_backfill_changes_each_kind_once_and_is_idempotent(lending_db):
    conn = lending_db.connection()
    _insert(conn, "rec", disposition="CALLBACK_REQUESTED", raw="Call Back", talk=48, posted="CALLBACK_REQUESTED",
            transcript="failed")
    _insert(conn, "unk0", raw="Unknown", talk=0)
    _insert(conn, "amd", disposition="LEFT_VOICEMAIL", raw="Answering Machine", posted="LEFT_VOICEMAIL")
    _insert(conn, "unk288", raw="Unknown", talk=288)
    _insert(conn, "off", disposition="NO_ANSWER", raw="No Answer", talk=30, recording_enabled=0)

    first = run_steps(conn)
    assert first["unknown_no_talk"] == 1 and first["answering_machine"] == 1
    assert first["failed_extractions"] == 1 and first["old_unknown_alerts"] == 1

    rec = _row(conn, "rec")
    assert rec["recording_status"] == "recorded" and rec["transcript_status"] == "deferred"
    assert rec["recording_view_url"] == "https://app.batchdialer.com/contacts/view/559662160"
    assert _row(conn, "off")["recording_status"] == "none"
    for call_id in ("unk0", "amd"):
        row = _row(conn, call_id)
        assert row["disposition"] == "NO_ANSWER" and row["recording_status"] == "none"
        # nothing left for the delivery retry: no Slack post, no Sheet write
        assert row["slack_posted_disposition"] == "NO_ANSWER" and row["sheet_synced_disposition"] == "NO_ANSWER"
        assert row["unfunded_cause"] is not None  # same provisional cause the live path gives NO_ANSWER
    old = _row(conn, "unk288")
    assert old["disposition_missing_alerted_at"] is not None
    # handled with no post: "No result yet" delivery and its retry skip it
    assert old["slack_posted_at"] is not None and old["sheet_synced_at"] is not None and old["slack_ts"] is None
    assert first["old_unknown_posts"] == 1

    assert set(run_steps(conn).values()) == {0}


def test_backfill_never_touches_ghl_rows(lending_db):
    conn = lending_db.connection()
    conn.execute(text(
        "INSERT INTO lending.call_dispositions (dialer_call_id, call_source, direction, disposition_raw, "
        "talk_duration_sec, dialer_contact_id, raw_event) "
        "VALUES ('ghl-m1', 'ghl', 'outbound', 'Unknown', 0, 'x', '{}'::jsonb)"))
    run_steps(conn)
    row = _row(conn, "ghl-m1")
    assert row["disposition"] is None and row["recording_view_url"] is None
