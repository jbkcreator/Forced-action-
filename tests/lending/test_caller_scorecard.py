"""R1 1.6: David's 2:45 PM scorecard: data and Slack blocks (SQLite with an attached ``lending`` schema)."""
from __future__ import annotations

import json
from datetime import date, datetime, timezone

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from config.lending_dialer_queue import NOTES_LIST_MAX, NOTES_OPEN_ACTION_ID
from src.lending.dialer import scorecard
from src.lending.dialer.scorecard import (
    Scorecard, ScoredCall, build_scorecard, needs_notes, quality_score, render_notes_thread, render_scorecard,
    top_calls,
)

DAY = date(2026, 11, 25)
AS_OF = datetime(2026, 11, 25, 19, 45, tzinfo=timezone.utc)  # 2:45 PM ET
CALL_AT = datetime(2026, 11, 25, 15, 0, tzinfo=timezone.utc)

_DDL = (
    "CREATE TABLE lending.call_dispositions (dialer_call_id TEXT PRIMARY KEY, phone TEXT, call_source TEXT,"
    " direction TEXT, caller_seat TEXT, dialer_campaign_id TEXT, talk_duration_sec INTEGER, disposition TEXT,"
    " call_ended_at TIMESTAMP, recording_status TEXT, recording_disclosure_logged BOOLEAN NOT NULL DEFAULT 0,"
    " dialer_contact_id TEXT)",
    "CREATE TABLE lending.call_extractions (dialer_call_id TEXT PRIMARY KEY, phone TEXT, fields TEXT NOT NULL,"
    " extracted_at TIMESTAMP, source TEXT NOT NULL DEFAULT 'transcript', entered_by TEXT)",
)


def call(cid="c1", *, talk=130, disposition="BOOKED", recorded=True, disclosure=True, contact="B1", ext=None,
         source=None, phone="+18135551234") -> ScoredCall:
    return ScoredCall(dialer_call_id=cid, phone_last4=phone[-4:], talk_sec=talk, disposition=disposition,
                      recorded=recorded, disclosure_logged=disclosure, dialer_contact_id=contact, extraction=ext,
                      notes_source=source)


def card(calls=(), **kw) -> Scorecard:
    base = dict(day=DAY, as_of=AS_OF, dials=0, connects=0, long_conversations=0, booked_stage=0, booked_reported=0,
                calls=tuple(calls), read_failed=False)
    return Scorecard(**{**base, **kw})


def flatten(blocks) -> str:
    return json.dumps(blocks)


def test_quality_score_counts_five_checks():
    full = {"next_action": "send docs", "decision_maker": "owner", "deal_status": "under_contract"}
    assert quality_score(call(disposition="BOOKED", talk=130, ext=full)) == 5
    assert quality_score(call(disposition=None, talk=30, ext={})) == 0
    assert quality_score(call(ext=None)) is None


def test_each_quality_check_is_worth_one_point():
    assert quality_score(call(disposition="BOOKED", talk=10, ext={})) == 1
    assert quality_score(call(disposition=None, talk=120, ext={})) == 1
    assert quality_score(call(disposition=None, talk=10, ext={"next_action": "x"})) == 1
    assert quality_score(call(disposition=None, talk=10, ext={"decision_maker": "x"})) == 1
    for field in ("objection", "kill_reason_verbatim", "deal_status"):
        assert quality_score(call(disposition=None, talk=10, ext={field: "x"})) == 1


def test_top_calls_booked_first_then_talk_time():
    c1 = call("c1", talk=600, disposition="NOT_INTERESTED")
    c2 = call("c2", talk=60, disposition="BOOKED")
    c3 = call("c3", talk=0, disposition="NO_ANSWER")
    assert [c.dialer_call_id for c in top_calls([c1, c2, c3])] == ["c2", "c1"]  # unconnected calls left out


def test_top_calls_ties_break_by_call_id_and_cap_at_n():
    same = [call(f"c{n}", talk=100, disposition="X") for n in (3, 1, 2)]
    assert [c.dialer_call_id for c in top_calls(same, 2)] == ["c1", "c2"]


def test_needs_notes_only_unrecorded_connected_without_notes():
    wanted = call("a", recorded=False, ext=None)
    others = [call("b", recorded=True, ext=None), call("c", recorded=False, ext={"next_action": "x"}),
              call("d", recorded=False, talk=0, ext=None)]
    assert needs_notes([wanted, *others]) == [wanted]


def test_zero_call_day_renders_zeros():
    out = flatten(render_scorecard(card()))
    assert "Dials" in out and "0" in out and "quality" in out.lower()


def test_post_shows_quality_average_and_how_many_calls_have_notes():
    scored = [call("a", ext={"next_action": "x"}), call("b", ext={}), call("c", ext=None)]
    out = flatten(render_scorecard(card(scored, connects=3)))
    assert "provisional" in out and "2 of 3" in out and "1 without notes" in out


def test_recorded_calls_without_a_reported_disclosure_are_counted_and_worded_as_not_reported():
    out = flatten(render_scorecard(card([call("a", disclosure=False), call("b", disclosure=True),
                                         call("c", recorded=False, disclosure=False)])))
    assert "no disclosure reported by the dialer: 1" in out


def test_post_shows_booked_by_stage_and_what_he_reported():
    out = flatten(render_scorecard(card(booked_stage=2, booked_reported=3)))
    assert "all callers" in out and "2" in out and "reported 3" in out


def test_notes_thread_has_a_button_per_call_and_caps_at_20_with_a_count_of_the_rest():
    many = [call(f"c{n:02d}", recorded=False, ext=None) for n in range(NOTES_LIST_MAX + 5)]
    blocks = render_notes_thread(card(many))
    buttons = [b["accessory"] for b in blocks if b.get("accessory")]
    assert len(buttons) == NOTES_LIST_MAX and buttons[0]["action_id"] == NOTES_OPEN_ACTION_ID
    assert "5 more" in flatten(blocks)


def test_notes_thread_is_empty_when_no_call_needs_notes():
    assert render_notes_thread(card([call(recorded=True)])) == []


def test_no_full_phone_in_blocks():
    calls = [call("a", phone="+18135559876", recorded=False)]
    out = flatten(render_scorecard(card(calls))) + flatten(render_notes_thread(card(calls)))
    assert "9876" in out and "8135559876" not in out


@pytest.fixture
def db(monkeypatch):
    eng = create_engine("sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False})

    @event.listens_for(eng, "connect")
    def _attach(dbapi_conn, _):
        dbapi_conn.execute("ATTACH DATABASE ':memory:' AS lending")

    with eng.begin() as conn:
        for statement in _DDL:
            conn.execute(text(statement))
    monkeypatch.setattr(scorecard, "stage_counts", lambda db, day: (2, 1))
    with Session(eng) as session:
        yield session
    eng.dispose()


def _insert(db, cid, *, seat="D1", talk=130, disposition="BOOKED", recording="recorded", disclosure=False,
            direction="outbound", source="batchdialer", campaign="K1", ended=CALL_AT, contact="B1"):
    db.execute(text(
        "INSERT INTO lending.call_dispositions (dialer_call_id, phone, call_source, direction, caller_seat,"
        " dialer_campaign_id, talk_duration_sec, disposition, call_ended_at, recording_status,"
        " recording_disclosure_logged, dialer_contact_id) VALUES (:c, '+18135551234', :src, :dir, :seat, :camp,"
        " :talk, :disp, :ended, :rec, :disc, :contact)"),
        {"c": cid, "src": source, "dir": direction, "seat": seat, "camp": campaign, "talk": talk, "disp": disposition,
         "ended": ended, "rec": recording, "disc": disclosure, "contact": contact})


def test_build_scorecard_reads_only_davids_seat(db):
    _insert(db, "mine-1")
    _insert(db, "mine-2", talk=0, disposition="NO_ANSWER", recording="none")
    _insert(db, "theirs", seat="D2")
    _insert(db, "inbound", direction="inbound")
    _insert(db, "yesterday", ended=datetime(2026, 11, 24, 15, 0, tzinfo=timezone.utc))
    result = build_scorecard(db, DAY, agent_id="D1", as_of=AS_OF)
    assert [c.dialer_call_id for c in result.calls] == ["mine-1"]  # connected calls only
    assert (result.dials, result.connects, result.long_conversations) == (3, 1, 1)  # dials = his campaigns' dials
    assert (result.booked_stage, result.booked_reported, result.read_failed) == (2, 1, False)


def test_build_scorecard_joins_the_extraction_and_its_source(db):
    _insert(db, "c1", recording="none")
    db.execute(text("INSERT INTO lending.call_extractions (dialer_call_id, fields, source)"
                    " VALUES ('c1', '{\"next_action\": \"send docs\"}', 'caller_notes')"))
    (only,) = build_scorecard(db, DAY, agent_id="D1", as_of=AS_OF).calls
    assert only.extraction == {"next_action": "send docs"} and only.notes_source == "caller_notes"
    assert only.phone_last4 == "1234" and not only.recorded and not only.disclosure_logged


def test_failed_read_is_flagged_not_hidden(db):
    db.execute(text("DROP TABLE lending.call_extractions"))
    result = build_scorecard(db, DAY, agent_id="D1", as_of=AS_OF)
    assert result.read_failed and result.calls == ()
    assert "could not be read" in flatten(render_scorecard(result))
