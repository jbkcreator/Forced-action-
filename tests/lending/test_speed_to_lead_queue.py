"""P2.1 speed-to-lead call queue against Postgres: one cascade at a time, fresh leads first then the oldest, every rule
re-checked at release (gate, handled in GHL, a live GHL call read that fails closed), callers' hours, 72-hour expiry,
the daily notices, and the shared lock between the fresh-lead start and the queue worker (a real two-session race).

Skipped without a test Postgres (tests/_database_guard.py). Nothing is ever sent: GHL tags, the call reader and the
Slack post are fakes."""
from __future__ import annotations

import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from config.lending_speed_to_lead import (
    CALL_BLOCKED,
    CALL_EXPIRED,
    CALL_QUEUED,
    CALL_TAGGED,
    GHL_TAG_AI_VOICE_OK,
    GHL_TAG_SPEED_TO_LEAD,
)
from src.lending import speed_to_lead

# Far from any real row: the spacing check and the candidate list read the whole table.
NOW = datetime(2031, 3, 4, 15, 0, tzinfo=timezone.utc)


class FakeTags:
    def __init__(self, delay: float = 0.0):
        self.calls: list[tuple[str, str, str]] = []
        self.delay = delay

    def add_tag(self, contact_id, tag):
        time.sleep(self.delay)
        self.calls.append(("add", contact_id, tag))

    def remove_tag(self, contact_id, tag):
        self.calls.append(("remove", contact_id, tag))

    def started(self) -> list[str]:
        return [c[1] for c in self.calls if c[0] == "add" and c[2] == GHL_TAG_SPEED_TO_LEAD]


@pytest.fixture
def rules(monkeypatch):
    """Every rule allows a call; tests flip one."""
    state = SimpleNamespace(enabled=True, hours=True, halted=False, blocks={})
    monkeypatch.setattr(speed_to_lead, "get_settings", lambda: SimpleNamespace(
        lending_speed_to_lead_enabled=state.enabled, lending_intro_sms_enabled=False, lending_ghl_cf_intro_name=None,
        lending_sms_carrier_approved=False))
    monkeypatch.setattr(speed_to_lead, "callers_hours_open", lambda now: state.hours)
    monkeypatch.setattr(speed_to_lead, "callers_hours_start", lambda now: now - timedelta(hours=1))
    monkeypatch.setattr(speed_to_lead, "is_globally_halted", lambda: state.halted)
    monkeypatch.setattr("src.lending.ghl_call_gate.blocked_reasons",
                        lambda db, phones, **k: {p: state.blocks[p] for p in phones if p in state.blocks})
    monkeypatch.setattr("src.lending.ghl_list_scrub.list_scrub", lambda *a, **k: None)
    return state


def _phone() -> str:
    return f"+1813555{uuid.uuid4().int % 10000:04d}"


def web_lead(db, *, received: datetime, handled=None, ai=True) -> tuple[int, str, str]:
    phone = _phone()
    lead_id = db.execute(text(
        "INSERT INTO lending.web_leads (received_at, name, phone, sms_consent, call_consent, ai_call_consent, "
        "ai_call_consent_text_matches, handled_in_ghl_at) VALUES (:r, 'Test Lead', :p, false, true, :ai, :ai, :h) "
        "RETURNING id"), {"r": received, "p": phone, "ai": ai, "h": handled}).scalar()
    contact = f"c-{uuid.uuid4().hex[:10]}"
    db.execute(text("UPDATE lending.web_leads SET ghl_contact_id = :c WHERE id = :i"), {"c": contact, "i": lead_id})
    return lead_id, phone, contact


def queued(db, *, received: datetime, reason="outside_hours", **kw) -> tuple[int, str, str]:
    lead_id, phone, contact = web_lead(db, received=received, **kw)
    db.execute(text("INSERT INTO lending.speed_to_lead (source, lead_id, ghl_contact_id, received_at, call_status, "
                    "queued_at, queue_reason) VALUES ('web', :i, :c, :r, 'queued', :r, :q)"),
               {"i": lead_id, "c": contact, "r": received, "q": reason})
    return lead_id, phone, contact


def row(db, lead_id):
    return db.execute(text("SELECT * FROM lending.speed_to_lead WHERE source = 'web' AND lead_id = :i"),
                      {"i": lead_id}).mappings().one()


def release(db, *, tags=None, reader=None, notify=None, now=NOW):
    return speed_to_lead.release_queue(db, tags=tags if tags is not None else FakeTags(), scrubber=lambda p: [],
                                       read_calls=reader or (lambda contact: []), notify=notify,
                                       contact_url=lambda c: f"https://ghl/{c}", now=now)


def ghl_connected_call(db, phone, at):
    db.execute(text("INSERT INTO lending.call_dispositions (dialer_call_id, direction, phone, call_started_at, "
                    "call_ended_at, disposition, talk_duration_sec, raw_event, call_source) "
                    "VALUES (:id, 'outbound', :p, :s, :s, 'CALLBACK_REQUESTED', 90, '{}', 'ghl')"),
               {"id": f"stl-{uuid.uuid4().hex}", "p": phone, "s": at})


# ── order and pacing ──────────────────────────────────────────────────────────

def test_one_at_a_time_fresh_first_then_the_oldest(web_leads_db, rules):
    db = web_leads_db
    newer_id, _, newer_c = queued(db, received=NOW - timedelta(hours=10))
    oldest_id, _, oldest_c = queued(db, received=NOW - timedelta(hours=30))
    fresh_id, _, fresh_c = queued(db, received=NOW - timedelta(minutes=1), reason="waiting_turn")
    tags = FakeTags()

    assert release(db, tags=tags).released == 1 and tags.started() == [fresh_c]
    assert release(db, tags=tags, now=NOW + timedelta(minutes=2, seconds=59)).released == 0  # 3-minute spacing
    assert release(db, tags=tags, now=NOW + timedelta(minutes=3)).released == 1
    assert release(db, tags=tags, now=NOW + timedelta(minutes=6)).released == 1
    assert tags.started() == [fresh_c, oldest_c, newer_c]
    done = row(db, oldest_id)
    assert (done["call_status"], done["released_at"], done["call_tagged_at"]) == (
        CALL_TAGGED, NOW + timedelta(minutes=3), NOW + timedelta(minutes=3))
    assert row(db, newer_id)["call_status"] == CALL_TAGGED and row(db, fresh_id)["call_status"] == CALL_TAGGED


def test_a_queued_lead_gets_voice_ai_like_a_new_lead(web_leads_db, rules):
    lead_id, _, contact = queued(web_leads_db, received=NOW - timedelta(hours=12))
    tags = FakeTags()
    release(web_leads_db, tags=tags)
    assert tags.calls == [("add", contact, GHL_TAG_AI_VOICE_OK), ("remove", contact, GHL_TAG_SPEED_TO_LEAD),
                          ("add", contact, GHL_TAG_SPEED_TO_LEAD)]
    assert row(web_leads_db, lead_id)["voice_eligible"] is True


def test_a_crash_while_tagging_never_rings_the_lead_twice(web_leads_db, rules, monkeypatch):
    lead_id, _, _ = queued(web_leads_db, received=NOW - timedelta(hours=4))
    real_begin = speed_to_lead._begin

    def crash(*args, **kwargs):
        raise RuntimeError("worker killed mid-release")

    monkeypatch.setattr(speed_to_lead, "_begin", crash)
    release(web_leads_db)
    claimed = row(web_leads_db, lead_id)
    assert (claimed["call_status"], claimed["released_at"]) == ("releasing", NOW)  # the claim was committed first
    monkeypatch.setattr(speed_to_lead, "_begin", real_begin)
    other_id, _, other_c = queued(web_leads_db, received=NOW - timedelta(hours=2))
    tags = FakeTags()
    result = release(web_leads_db, tags=tags, now=NOW + timedelta(minutes=10))
    assert result.released == 1 and tags.started() == [other_c]  # the queue moves on; the crashed lead is not re-rung
    assert row(web_leads_db, lead_id)["call_status"] == "releasing"
    assert row(web_leads_db, other_id)["call_status"] == CALL_TAGGED
    posts: list[str] = []
    release(web_leads_db, notify=lambda m: posts.append(m) or True, now=NOW + timedelta(minutes=20, seconds=1))
    assert row(web_leads_db, lead_id)["call_status"] == "interrupted"
    assert any(f"Website lead {lead_id})" in p and "release was interrupted" in p for p in posts)


# ── nothing released unless every rule allows it ─────────────────────────────

@pytest.mark.parametrize("flip", ["hours", "halted", "no_ghl", "no_reader"])
def test_nothing_is_released_when_a_condition_is_missing(web_leads_db, rules, flip):
    lead_id, _, _ = queued(web_leads_db, received=NOW - timedelta(hours=2))
    tags = FakeTags()
    if flip == "hours":
        rules.hours = False
    elif flip == "halted":
        rules.halted = True
    if flip == "no_ghl":
        result = speed_to_lead.release_queue(web_leads_db, tags=None, scrubber=None, read_calls=lambda c: [], now=NOW)
    elif flip == "no_reader":
        result = speed_to_lead.release_queue(web_leads_db, tags=tags, scrubber=None, read_calls=None, now=NOW)
    else:
        result = release(web_leads_db, tags=tags)
    assert result.released == 0 and tags.calls == [] and row(web_leads_db, lead_id)["call_status"] == CALL_QUEUED


def test_a_lead_connected_in_ghl_while_queued_is_skipped_and_the_next_one_goes(web_leads_db, rules):
    db = web_leads_db
    called_id, phone, _ = queued(db, received=NOW - timedelta(hours=20))
    next_id, _, next_c = queued(db, received=NOW - timedelta(hours=10))
    ghl_connected_call(db, phone, NOW - timedelta(hours=15))  # Josh got through from his GHL list overnight
    tags = FakeTags()
    assert release(db, tags=tags).released == 1 and tags.started() == [next_c]
    skipped = row(db, called_id)
    assert (skipped["call_status"], skipped["call_block_reason"]) == (CALL_BLOCKED, "HANDLED_IN_GHL")


def test_a_lead_stamped_handled_in_ghl_is_never_released(web_leads_db, rules):
    lead_id, _, _ = queued(web_leads_db, received=NOW - timedelta(hours=3), handled=NOW - timedelta(hours=1))
    tags = FakeTags()
    release(web_leads_db, tags=tags)
    assert tags.calls == [] and row(web_leads_db, lead_id)["call_block_reason"] == "HANDLED_IN_GHL"


def test_a_failed_ghl_call_read_never_releases_that_lead_and_never_wedges_the_queue(web_leads_db, rules):
    stuck_id, _, stuck_c = queued(web_leads_db, received=NOW - timedelta(hours=5))
    next_id, _, next_c = queued(web_leads_db, received=NOW - timedelta(hours=3))

    def reader(contact):
        if contact == stuck_c:
            raise RuntimeError("GHL 404: contact merged")
        return []

    tags = FakeTags()
    result = release(web_leads_db, tags=tags, reader=reader)
    assert result.ghl_check_failed == 1 and tags.started() == [next_c]
    stuck = row(web_leads_db, stuck_id)
    assert (stuck["call_status"], stuck["queue_reason"]) == (CALL_QUEUED, "GHL_CHECK_FAILED")
    assert stuck["recheck_at"] == NOW + timedelta(minutes=2)


def test_a_ghl_outage_stops_the_run_after_three_failed_reads(web_leads_db, rules):
    for hours in (9, 8, 7, 6):
        queued(web_leads_db, received=NOW - timedelta(hours=hours))
    reads: list[str] = []

    def down(contact):
        reads.append(contact)
        raise RuntimeError("GHL 503")

    result = release(web_leads_db, reader=down)
    assert result.ghl_check_failed == 3 and len(reads) == 3 and result.released == 0


def test_a_connected_call_counts_even_when_storing_it_fails(web_leads_db, rules, monkeypatch):
    lead_id, _, contact = queued(web_leads_db, received=NOW - timedelta(hours=3))
    monkeypatch.setattr("src.lending.ghl_call_import._store_contact", lambda *a, **k: None)  # the store lost it
    message = {"id": "m1", "direction": "outbound", "dateAdded": (NOW - timedelta(hours=1)).isoformat(),
               "meta": {"call": {"status": "completed", "duration": 120}}}
    tags = FakeTags()
    release(web_leads_db, tags=tags, reader=lambda c: [message] if c == contact else [])
    assert tags.calls == [] and row(web_leads_db, lead_id)["call_block_reason"] == "HANDLED_IN_GHL"


def test_a_call_placed_minutes_ago_makes_the_lead_wait(web_leads_db, rules):
    db = web_leads_db
    lead_id, phone, _ = queued(db, received=NOW - timedelta(hours=3))
    db.execute(text("INSERT INTO lending.call_dispositions (dialer_call_id, direction, phone, call_started_at, "
                    "call_ended_at, disposition, talk_duration_sec, raw_event, call_source) "
                    "VALUES (:id, 'outbound', :p, :s, :s, NULL, 0, '{}', 'batchdialer')"),
               {"id": f"stl-{uuid.uuid4().hex}", "p": phone, "s": NOW - timedelta(minutes=4)})  # BatchDialer dialing
    tags = FakeTags()
    release(db, tags=tags)
    waiting = row(db, lead_id)
    assert tags.calls == [] and (waiting["call_status"], waiting["queue_reason"]) == (CALL_QUEUED, "RECENTLY_CALLED")


def test_a_ghl_tag_error_puts_the_lead_back_in_the_queue(web_leads_db, rules):
    lead_id, _, _ = queued(web_leads_db, received=NOW - timedelta(hours=3))

    class Down(FakeTags):
        def add_tag(self, contact_id, tag):
            raise RuntimeError("GHL 502")

    result = release(web_leads_db, tags=Down())
    back = row(web_leads_db, lead_id)
    assert result.released == 0 and (back["call_status"], back["queue_reason"]) == (CALL_QUEUED, "GHL_TAG_FAILED")
    # the claim time is kept: if GHL did apply the tag, its cascade still counts for the 3-minute spacing
    assert back["call_tagged_at"] == NOW and back["released_at"] is None
    assert back["recheck_at"] == NOW + timedelta(minutes=10)
    other_id, _, _ = queued(web_leads_db, received=NOW - timedelta(hours=2))
    tags = FakeTags()
    assert release(web_leads_db, tags=tags, now=NOW + timedelta(minutes=2)).released == 0  # spacing still holds
    assert release(web_leads_db, tags=tags, now=NOW + timedelta(minutes=10)).released == 1 and tags.started()


def test_not_now_leads_do_not_hide_the_callable_ones_behind_them(web_leads_db, rules, monkeypatch):
    monkeypatch.setattr(speed_to_lead, "QUEUE_SCAN_LIMIT", 2)
    db = web_leads_db
    blocked = [queued(db, received=NOW - timedelta(hours=h)) for h in (9, 8)]
    callable_id, _, callable_c = queued(db, received=NOW - timedelta(hours=7))
    rules.blocks = {phone: "OUTSIDE_CALL_WINDOW" for _, phone, _ in blocked}  # Pacific leads before 8am PT
    tags = FakeTags()
    assert release(db, tags=tags).released == 0  # this run only reached the two not-now leads ...
    assert release(db, tags=tags, now=NOW + timedelta(minutes=1)).released == 1  # ... which now wait 10 minutes
    assert tags.started() == [callable_c]


def test_gate_reasons_at_release_wait_or_end(web_leads_db, rules):
    db = web_leads_db
    waiting_id, waiting_phone, _ = queued(db, received=NOW - timedelta(hours=30))
    reached_id, reached_phone, _ = queued(db, received=NOW - timedelta(hours=20))
    ok_id, _, ok_c = queued(db, received=NOW - timedelta(hours=10))
    rules.blocks = {waiting_phone: "OUTSIDE_CALL_WINDOW", reached_phone: "REACHED_IN_BATCHDIALER"}
    tags = FakeTags()
    result = release(db, tags=tags)
    assert tags.started() == [ok_c] and (result.waiting, result.blocked, result.released) == (1, 1, 1)
    assert (row(db, waiting_id)["call_status"], row(db, waiting_id)["queue_reason"]) == (CALL_QUEUED,
                                                                                         "OUTSIDE_CALL_WINDOW")
    assert (row(db, reached_id)["call_status"], row(db, reached_id)["call_block_reason"]) == (
        CALL_BLOCKED, "REACHED_IN_BATCHDIALER")


# ── expiry and notices ────────────────────────────────────────────────────────

def test_a_lead_queued_72_hours_expires_with_a_slack_line(web_leads_db, rules):
    db = web_leads_db
    old_id, phone, contact = queued(db, received=NOW - timedelta(hours=72, seconds=1))
    kept_id, _, _ = queued(db, received=NOW - timedelta(hours=72))
    rules.hours = False  # expiry runs at any hour
    posts: list[str] = []
    assert release(db, notify=lambda m: posts.append(m) or True).expired >= 1
    assert row(db, old_id)["call_status"] == CALL_EXPIRED and row(db, old_id)["released_at"] == NOW
    assert row(db, kept_id)["call_status"] == CALL_QUEUED
    mine = [p for p in posts if f"Test Lead (Website lead {old_id})" in p]
    assert len(mine) == 1 and f"https://ghl/{contact}" in mine[0] and phone not in mine[0] and "waited 72 h" in mine[0]
    assert row(db, old_id)["alerted_at"] == NOW
    release(db, notify=lambda m: posts.append(m) or True, now=NOW + timedelta(minutes=1))
    assert len([p for p in posts if f"Website lead {old_id})" in p]) == 1  # posted once


def test_a_failed_left_queue_post_is_sent_on_the_next_run(web_leads_db, rules):
    old_id, _, _ = queued(web_leads_db, received=NOW - timedelta(hours=73))
    rules.hours = False
    release(web_leads_db, notify=lambda m: False)
    assert row(web_leads_db, old_id)["alerted_at"] is None
    posts: list[str] = []
    release(web_leads_db, notify=lambda m: posts.append(m) or True, now=NOW + timedelta(minutes=1))
    assert any(f"Website lead {old_id})" in p for p in posts)


def test_switching_speed_to_lead_off_ends_the_queue(web_leads_db, rules):
    lead_id, _, _ = queued(web_leads_db, received=NOW - timedelta(hours=3))
    rules.enabled = False
    result = release(web_leads_db)
    ended = row(web_leads_db, lead_id)
    assert result.switched_off >= 1 and (ended["call_status"], ended["call_block_reason"], ended["released_at"]) == (
        "off", "SWITCHED_OFF", NOW)  # the alarms count from here


def test_the_morning_summary_and_backlog_alert_post_once_a_day(web_leads_db, rules):
    db = web_leads_db
    for hours in (30, 20, 10):
        queued(db, received=NOW - timedelta(hours=hours))
    db.execute(text("UPDATE lending.speed_to_lead SET queued_at = :q WHERE call_status = 'queued'"),
               {"q": NOW - timedelta(hours=2)})
    rules.halted = True  # notices still post; nothing is released
    posts: list[str] = []
    release(db, notify=lambda m: posts.append(m) or True)
    release(db, notify=lambda m: posts.append(m) or True, now=NOW + timedelta(minutes=1))
    assert [p.split("*")[1] for p in posts] == ["Speed-to-lead queue:", "Speed-to-lead queue backlog:"]
    assert "waiting" in posts[0] and "60 min of calling hours" in posts[1]  # opened an hour ago (fixture)


def test_a_failed_notice_post_is_tried_again_next_minute(web_leads_db, rules):
    queued(web_leads_db, received=NOW - timedelta(hours=3))
    rules.halted = True
    attempts: list[str] = []
    release(web_leads_db, notify=lambda m: attempts.append(m) and False)
    release(web_leads_db, notify=lambda m: attempts.append(m) or True, now=NOW + timedelta(minutes=1))
    assert len([a for a in attempts if "Speed-to-lead queue:" in a]) == 2


# ── the fresh-lead start and the worker share one lock (two real sessions) ──

@pytest.fixture
def committed_db(rules):
    """Two sessions need committed rows: tables in the throwaway test database, every row this test made deleted."""
    from config.settings import get_settings
    from migrations.apply_lending_speed_to_lead import apply_to as apply_speed_to_lead
    from migrations.apply_lending_web_leads import apply_to as apply_web_leads

    url = get_settings().database_url
    if not url:
        pytest.skip("requires a test Postgres")
    engine = create_engine(str(url), connect_args={"connect_timeout": 20})
    try:
        with engine.begin() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS lending"))
            from src.lending.models import LendingCallDisposition

            LendingCallDisposition.__table__.create(conn, checkfirst=True)
            apply_web_leads(conn)
            apply_speed_to_lead(conn)
    except Exception:
        engine.dispose()
        pytest.skip("Postgres is not reachable")
    made: list[int] = []
    yield engine, made
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM lending.speed_to_lead WHERE source = 'web' AND lead_id = ANY(:ids)"),
                     {"ids": made})
        conn.execute(text("DELETE FROM lending.web_leads WHERE id = ANY(:ids)"), {"ids": made})
    engine.dispose()


@pytest.mark.parametrize("attempt", range(3))
def test_a_fresh_lead_and_the_queue_never_start_two_cascades_at_once(committed_db, attempt, caplog):
    engine, made = committed_db
    now = datetime.now(timezone.utc) + timedelta(days=3650 + attempt)  # clear of every other row's spacing
    with Session(engine) as db:
        backlog_id, _, _ = queued(db, received=now - timedelta(hours=5))
        fresh_id, _, _ = web_lead(db, received=now - timedelta(seconds=3))
        db.commit()
    made += [backlog_id, fresh_id]
    tags = FakeTags(delay=0.3)  # widen the window between the decision and the commit
    barrier = threading.Barrier(2)
    errors: list[BaseException] = []

    def fresh_start():
        try:
            with Session(engine) as db:
                barrier.wait()
                speed_to_lead.start(db, "web", fresh_id, tags=tags, scrubber=lambda p: [], now=now)
                time.sleep(0.2)
                db.commit()
        except BaseException as exc:  # surfaced below
            errors.append(exc)

    runs: list[speed_to_lead.QueueRun] = []

    def worker():
        try:
            with Session(engine) as db:
                barrier.wait()
                runs.append(speed_to_lead.release_queue(db, tags=tags, scrubber=lambda p: [],
                                                        read_calls=lambda c: [], now=now))
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=fresh_start), threading.Thread(target=worker)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
    assert not errors
    assert len(tags.started()) == 1  # exactly one cascade; the other lead is queued
    with Session(engine) as db:
        statuses = sorted(row(db, i)["call_status"] for i in (backlog_id, fresh_id))
    assert statuses == [CALL_QUEUED, CALL_TAGGED]
    assert not any("queue release crashed" in r.getMessage() for r in caplog.records)  # the worker really ran
    with Session(engine) as db:
        fresh = row(db, fresh_id)
    if fresh["call_status"] == CALL_QUEUED:  # the worker won: the fresh lead saw its cascade and waits its turn
        assert fresh["queue_reason"] == "waiting_turn" and runs[0].released == 1
    else:  # the fresh lead won: the worker saw the running cascade (or the held lock) and released nothing
        assert runs[0].released == 0


def test_the_minute_job_skips_while_the_previous_run_is_still_going(committed_db, monkeypatch):
    from src.tasks import lending_speed_to_lead_queue as job

    engine, _ = committed_db
    calls: list[int] = []
    monkeypatch.setattr(job, "lending_session", lambda: Session(engine))
    monkeypatch.setattr(job, "_release", lambda db: calls.append(1) or speed_to_lead.QueueRun())
    with engine.connect() as holder:
        holder.begin()
        holder.execute(text("SELECT pg_advisory_xact_lock(7302020812)"))  # a slow run still holds it
        job.run()
        assert calls == []
        holder.rollback()
    job.run()
    assert calls == [1]



def test_a_scrub_paid_for_at_release_is_kept_when_the_ghl_read_fails(web_leads_db, rules, monkeypatch):
    lead_id, phone, _ = queued(web_leads_db, received=NOW - timedelta(hours=3))

    def scrub(db, phones, **kw):  # stands in for the stored Tracerfy result
        db.execute(text("INSERT INTO lending.text_consents (phone, source, captured_at) VALUES (:p, 'scrub-mark', :n)"),
                   {"p": phones[0], "n": NOW})

    monkeypatch.setattr("src.lending.ghl_list_scrub.list_scrub", scrub)

    def down(contact):
        raise RuntimeError("GHL 503")

    release(web_leads_db, reader=down)
    kept = web_leads_db.execute(text("SELECT count(*) FROM lending.text_consents WHERE phone = :p AND "
                                     "source = 'scrub-mark'"), {"p": phone}).scalar()
    assert kept == 1 and row(web_leads_db, lead_id)["queue_reason"] == "GHL_CHECK_FAILED"


def test_without_the_alert_channel_each_skipped_post_is_reported_once(web_leads_db, rules):
    old_id, _, _ = queued(web_leads_db, received=NOW - timedelta(hours=73))
    queued(web_leads_db, received=NOW - timedelta(hours=3))
    rules.halted = True  # notices only
    reported: list[str] = []
    speed_to_lead.release_queue(web_leads_db, tags=FakeTags(), scrubber=None, read_calls=lambda c: [], notify=None,
                                unset=reported.append, now=NOW)
    speed_to_lead.release_queue(web_leads_db, tags=FakeTags(), scrubber=None, read_calls=lambda c: [], notify=None,
                                unset=reported.append, now=NOW + timedelta(minutes=1))
    assert sorted(reported) == ["speed-to-lead left-queue alert", "speed-to-lead queue backlog notice",
                                "speed-to-lead queue morning notice"]
    assert row(web_leads_db, old_id)["alerted_at"] == NOW  # never flushed later as a stale line


def test_a_delivery_that_fails_to_commit_never_starts_a_second_cascade(committed_db):
    engine, made = committed_db
    now = datetime.now(timezone.utc) + timedelta(days=3700)
    with Session(engine) as db:
        lead_id, _, _ = web_lead(db, received=now - timedelta(seconds=3))
        db.commit()
    made.append(lead_id)
    claims = speed_to_lead.OwnSessionClaims(lambda: Session(engine))
    tags = FakeTags()
    with Session(engine) as delivery:
        assert speed_to_lead.start(delivery, "web", lead_id, tags=tags, scrubber=lambda p: [], now=now,
                                   claims=claims).call_status == CALL_TAGGED
        delivery.rollback()  # the delivery transaction's commit failed
    with Session(engine) as retry:  # the sweep retries the follow-up
        assert speed_to_lead.start(retry, "web", lead_id, tags=tags, scrubber=lambda p: [], now=now,
                                   claims=claims) is None
        retry.commit()
    assert len(tags.started()) == 1  # rung once
    with Session(engine) as db:
        kept = row(db, lead_id)
    assert (kept["call_status"], kept["call_tagged_at"]) == ("releasing", now)  # reported as interrupted later
