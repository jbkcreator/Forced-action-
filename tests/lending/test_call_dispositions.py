"""Call disposition recording (spec §4.4): one row per call, five results, replay-safe."""
from __future__ import annotations

import pytest
from sqlalchemy import text

from config.lending_dispositions import DISPOSITIONS
from src.lending.dispositions import record_aircall_event, resolve_disposition, tag_names

PHONE_RAW = "813-555-0142"
PHONE = "+18135550142"


def _data(call_id="c1", tags=None, **extra):
    data = {
        "id": call_id, "direction": "outbound", "raw_digits": PHONE_RAW,
        "started_at": 1790000000, "ended_at": 1790000060, "duration": 45,
        "user": {"id": 7, "name": "Sam Caller"}, "number": {"id": 99},
    }
    if tags is not None:
        data["tags"] = [{"name": t} for t in tags]
    data.update(extra)
    return data


def _rows(db):
    return db.execute(text("SELECT * FROM lending.call_dispositions ORDER BY id")).mappings().all()


# ── pure tag logic ──────────────────────────────────────────────────────────

def test_resolve_no_result_tag():
    assert resolve_disposition(None, ["VIP"]) == (None, False)


def test_resolve_single_result_tag():
    assert resolve_disposition(None, ["CONNECTED"]) == ("CONNECTED", False)


def test_resolve_second_result_tag_wins_and_is_flagged():
    assert resolve_disposition("CONNECTED", ["CONNECTED", "QUALIFIED_APPOINTMENT"]) == (
        "QUALIFIED_APPOINTMENT", True)


def test_resolve_removed_tag_falls_back_to_remaining():
    assert resolve_disposition("QUALIFIED_APPOINTMENT", ["CONNECTED"]) == ("CONNECTED", False)


def test_tag_names_accepts_dicts_strings_and_missing():
    assert tag_names([{"name": "A"}, "B"]) == ["A", "B"]
    assert tag_names(None) is None


# ── recording ───────────────────────────────────────────────────────────────

def test_call_ended_without_tag_still_records_an_attempt(lending_db):
    result = record_aircall_event(lending_db, "call.ended", _data())
    row = _rows(lending_db)[0]
    assert result.disposition is None
    assert row["disposition"] is None
    assert row["call_ended_at"] is not None
    assert row["direction"] == "outbound"
    assert row["phone"] == PHONE
    assert row["caller_seat"] == "7"
    assert row["caller_line"] == "99"
    assert row["talk_duration_sec"] == 45


def test_replayed_event_keeps_one_row(lending_db):
    for _ in range(2):
        record_aircall_event(lending_db, "call.ended", _data())
    assert len(_rows(lending_db)) == 1


def test_ended_then_tagged_completes_one_row(lending_db):
    record_aircall_event(lending_db, "call.ended", _data())
    result = record_aircall_event(lending_db, "call.tagged", _data(tags=["CONNECTED"]))
    rows = _rows(lending_db)
    assert len(rows) == 1
    assert rows[0]["disposition"] == "CONNECTED" == result.disposition
    assert rows[0]["disposition_at"] is not None
    assert rows[0]["call_ended_at"] is not None


def test_tagged_before_ended_is_not_blanked_by_the_later_event(lending_db):
    tagged = _data(tags=["BAD_NUMBER"])
    tagged.pop("ended_at")
    record_aircall_event(lending_db, "call.tagged", tagged)
    assert _rows(lending_db)[0]["call_ended_at"] is None
    record_aircall_event(lending_db, "call.ended", _data())
    row = _rows(lending_db)[0]
    assert len(_rows(lending_db)) == 1
    assert row["disposition"] == "BAD_NUMBER"
    assert row["call_ended_at"] is not None


@pytest.mark.parametrize("tag", DISPOSITIONS)
def test_each_of_the_five_results_is_recorded(lending_db, tag):
    record_aircall_event(lending_db, "call.tagged", _data(call_id=f"c-{tag}", tags=[tag]))
    assert _rows(lending_db)[0]["disposition"] == tag


def test_non_result_tag_is_ignored(lending_db):
    record_aircall_event(lending_db, "call.tagged", _data(tags=["VIP"]))
    assert _rows(lending_db)[0]["disposition"] is None


def test_second_result_tag_wins_and_sets_the_warning_flag(lending_db):
    record_aircall_event(lending_db, "call.tagged", _data(tags=["CONNECTED"]))
    record_aircall_event(lending_db, "call.tagged", _data(tags=["CONNECTED", "DNC_REQUEST"]))
    row = _rows(lending_db)[0]
    assert row["disposition"] == "DNC_REQUEST"
    assert row["multiple_dispositions"] is True


def test_untagging_the_result_clears_it(lending_db):
    record_aircall_event(lending_db, "call.tagged", _data(tags=["CONNECTED"]))
    record_aircall_event(lending_db, "call.untagged", _data(tags=[]))
    assert _rows(lending_db)[0]["disposition"] is None


def test_event_without_tags_leaves_the_disposition_alone(lending_db):
    record_aircall_event(lending_db, "call.tagged", _data(tags=["CONNECTED"]))
    record_aircall_event(lending_db, "call.ended", _data())
    assert _rows(lending_db)[0]["disposition"] == "CONNECTED"


def test_replayed_call_ended_with_empty_tags_does_not_erase_the_result(lending_db):
    record_aircall_event(lending_db, "call.tagged", _data(tags=["QUALIFIED_APPOINTMENT"]))
    record_aircall_event(lending_db, "call.ended", _data(tags=[]))
    assert _rows(lending_db)[0]["disposition"] == "QUALIFIED_APPOINTMENT"


def test_call_ended_carrying_a_result_tag_records_it(lending_db):
    record_aircall_event(lending_db, "call.ended", _data(tags=["CONNECTED"]))
    assert _rows(lending_db)[0]["disposition"] == "CONNECTED"


def test_dnc_tag_is_flagged_even_when_another_result_tag_wins(lending_db):
    result = record_aircall_event(lending_db, "call.tagged", _data(tags=["DNC_REQUEST", "CONNECTED"]))
    assert result.disposition == "CONNECTED"
    assert result.dnc_tagged is True


def test_unnormalizable_phone_still_records_the_call(lending_db):
    record_aircall_event(lending_db, "call.ended", _data(raw_digits="not a number"))
    row = _rows(lending_db)[0]
    assert row["phone"] is None
    assert row["call_ended_at"] is not None


def test_other_events_and_missing_call_id_are_ignored(lending_db):
    assert record_aircall_event(lending_db, "call.answered", _data()) is None
    assert record_aircall_event(lending_db, "call.ended", {"direction": "outbound"}) is None
    assert _rows(lending_db) == []


def test_raw_event_does_not_keep_the_webhook_token(lending_db):
    record_aircall_event(lending_db, "call.ended", _data())
    assert "token" not in _rows(lending_db)[0]["raw_event"]


def test_migration_is_idempotent():
    from sqlalchemy import create_engine

    from config.settings import get_settings
    from migrations.apply_lending_call_dispositions import apply

    url = get_settings().database_url
    if not url:
        pytest.skip("requires a live Postgres DATABASE_URL")
    engine = create_engine(str(url), connect_args={"connect_timeout": 5})
    try:
        engine.connect().close()
    except Exception:
        engine.dispose()
        pytest.skip("Postgres is not reachable")
    schema = "lending_t_disp_migration"
    try:
        apply(engine=engine, schema=schema)
        apply(engine=engine, schema=schema)
        with engine.connect() as c:
            cols = {r[0] for r in c.execute(
                text("SELECT column_name FROM information_schema.columns "
                     "WHERE table_schema = :s AND table_name = 'call_dispositions'"), {"s": schema})}
        assert {"aircall_call_id", "phone", "call_ended_at", "direction", "disposition"} <= cols
    finally:
        with engine.begin() as c:
            c.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        engine.dispose()


# ── contract with the attempt cap ───────────────────────────────────────────

def _cap_status(db, now):
    from src.lending.compliance import can_dial_now

    return can_dial_now(PHONE, db, now=now)


def test_recorded_attempts_feed_the_three_per_24h_cap(lending_db):
    from datetime import datetime, timedelta, timezone

    now = datetime(2026, 9, 29, 16, 0, tzinfo=timezone.utc)  # 12:00 ET, inside the calling window
    for i in range(3):
        ended = int((now - timedelta(hours=i + 1)).timestamp())
        record_aircall_event(lending_db, "call.ended", _data(call_id=f"cap{i}", ended_at=ended))
    assert not _cap_status(lending_db, now).allowed


def test_inbound_calls_and_calls_without_an_end_time_do_not_count(lending_db):
    from datetime import datetime, timedelta, timezone

    now = datetime(2026, 9, 29, 16, 0, tzinfo=timezone.utc)
    for i in range(2):
        ended = int((now - timedelta(hours=i + 1)).timestamp())
        record_aircall_event(lending_db, "call.ended", _data(call_id=f"out{i}", ended_at=ended))
    record_aircall_event(lending_db, "call.ended", _data(
        call_id="in", direction="inbound", ended_at=int((now - timedelta(hours=3)).timestamp())))
    tagged_only = _data(call_id="tag", tags=["CONNECTED"])
    tagged_only.pop("ended_at")
    record_aircall_event(lending_db, "call.tagged", tagged_only)
    assert _cap_status(lending_db, now).allowed
