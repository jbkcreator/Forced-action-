import os
from datetime import date, datetime, timezone

import pytest
from sqlalchemy import text

from src.lending.scoreboard import UNATTRIBUTED, build_scoreboard, format_slack

pytestmark = pytest.mark.skipif(not os.environ.get("DATABASE_URL"), reason="requires a live Postgres DATABASE_URL")

DAY = date(2026, 10, 6)
NOON = datetime(2026, 10, 6, 16, 0, tzinfo=timezone.utc)  # 12:00 ET
NAMES = {"11": "Verified maturity", "12": "Builders"}


def _call(db, cid, *, seat="Dana", camp="11", tag="Maturity A", disp="NO_ANSWER", direction="outbound",
          blocked=False, ended=NOON, phone=None):
    db.execute(text(
        "INSERT INTO lending.call_dispositions (dialer_call_id, caller_name, dialer_campaign_id, campaign_tag, "
        "disposition, direction, booking_blocked, call_ended_at, phone, raw_event) "
        "VALUES (:c, :s, :camp, :t, :d, :dir, :b, :e, :ph, '{}'::jsonb)"),
        {"c": cid, "s": seat, "camp": camp, "t": tag, "d": disp, "dir": direction, "b": blocked, "e": ended, "ph": phone})


def _stage(db, opp, stage="Held", *, phone=None, booked_by=None, at=NOON):
    db.execute(text(
        "INSERT INTO lending.ghl_stage_events (ghl_opportunity_id, stage_name, stage_key, phone, booked_by, event_at, raw_event) "
        "VALUES (:o, :s, lower(:s), :p, :b, :at, '{}'::jsonb)"),
        {"o": opp, "s": stage, "p": phone, "b": booked_by, "at": at})


def test_counts_per_caller_and_per_dialer_campaign(lending_db):
    _call(lending_db, "1")
    _call(lending_db, "2", disp="CONNECTED_NOT_INTERESTED")
    _call(lending_db, "3", disp="BOOKED")
    _call(lending_db, "4", disp="GATE_FAILED_NURTURE", seat="Sam", camp="12")
    data = build_scoreboard(lending_db, DAY, NAMES)
    dana = next(r for r in data.by_caller if r.name == "Dana")
    assert (dana.dials, dana.live, dana.booked) == (3, 2, 1)
    builders = next(r for r in data.by_campaign if r.name == "Builders")
    assert (builders.dials, builders.nurture, builders.gated) == (1, 1, 1)
    assert data.total.dials == 4


def test_hook_tag_section_groups_by_campaign_tag(lending_db):
    _call(lending_db, "1", tag="Maturity A")
    _call(lending_db, "2", tag="Maturity B", disp="BOOKED")
    names = {r.name: r for r in build_scoreboard(lending_db, DAY, NAMES).by_hook}
    assert names["Maturity B"].booked == 1 and names["Maturity A"].booked == 0


def test_blocked_booking_not_counted(lending_db):
    _call(lending_db, "1", disp="BOOKED", blocked=True)
    assert build_scoreboard(lending_db, DAY, NAMES).total.booked == 0


def test_unknown_campaign_id_and_missing_id(lending_db):
    _call(lending_db, "1", camp="99")
    _call(lending_db, "2", camp=None)
    rows = {r.name: r for r in build_scoreboard(lending_db, DAY, NAMES).by_campaign}
    assert rows["Campaign 99"].dials == 1 and rows[UNATTRIBUTED].connect_rate == 0.0


def test_inbound_and_other_days_excluded(lending_db):
    _call(lending_db, "1", direction="inbound", disp="CALLBACK_REQUESTED")
    _call(lending_db, "2", ended=datetime(2026, 10, 5, 16, 0, tzinfo=timezone.utc))
    assert build_scoreboard(lending_db, DAY, NAMES).total.dials == 0


def test_slack_text_names_every_column(lending_db):
    _call(lending_db, "1", disp="BOOKED")
    out = format_slack(build_scoreboard(lending_db, DAY, NAMES), DAY)
    for word in ("Dials", "Live", "Gated", "Booked", "Showed", "Nurture", "Connect", "Book rate"):
        assert word in out
    assert "Showed 0" in out


def test_only_a_blocked_booking_gives_zero_booked_and_no_division_error(lending_db):
    _call(lending_db, "1", disp="BOOKED", blocked=True)
    _call(lending_db, "2", camp="12", disp="NO_ANSWER")
    data = build_scoreboard(lending_db, DAY, NAMES)
    assert data.total.booked == 0 and data.total.book_rate == 0.0
    empty = next(r for r in data.by_campaign if r.name == "Builders")
    assert (empty.live, empty.connect_rate, empty.book_rate) == (0, 0.0, 0.0)
    format_slack(data, DAY)


def test_showed_is_credited_to_the_caller_who_booked_even_with_no_dials_today(lending_db):
    _call(lending_db, "b1", disp="BOOKED", phone="+18135550101", ended=datetime(2026, 10, 5, 16, 0, tzinfo=timezone.utc))
    _stage(lending_db, "opp1", "Held", phone="+18135550101")
    data = build_scoreboard(lending_db, DAY, NAMES)
    dana = next(r for r in data.by_caller if r.name == "Dana")
    assert (dana.dials, dana.showed) == (0, 1)
    assert next(r for r in data.by_campaign if r.name == "Verified maturity").showed == 1
    assert data.total.showed == 1
    assert "Showed 1" in format_slack(data, DAY)


def test_only_showed_stages_on_the_day_count_and_booked_by_is_the_fallback(lending_db):
    _stage(lending_db, "opp1", "Booked", phone="+18135550102")
    _stage(lending_db, "opp2", "Held", phone="+18135550103", booked_by="Sam")
    _stage(lending_db, "opp3", "Held", phone="+18135550104", at=datetime(2026, 10, 5, 16, 0, tzinfo=timezone.utc))
    data = build_scoreboard(lending_db, DAY, NAMES)
    assert data.total.showed == 1
    assert next(r for r in data.by_caller if r.name == "Sam").showed == 1
    assert next(r for r in data.by_campaign if r.name == UNATTRIBUTED).showed == 1
