"""R1 0.4 against Postgres: the breaker's state, trip, hold, reopen and per-minute counters, and the migration.

Runs in a throwaway schema inside one rolled-back transaction (every ``lending.`` in a statement is rewritten to it), so
no live table is touched. Skips unless TEST_DATABASE_URL points at a throwaway Postgres.
"""
from __future__ import annotations

import uuid
from contextlib import nullcontext

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import Session

from migrations.apply_lending_channel_breakers import apply_to
from packages.agent_core.schema import base_statements
from src.lending.dispatch import circuit_breaker as cb
from src.lending.models import LENDING_SCHEMA


@pytest.fixture
def db(monkeypatch):
    from config.settings import get_settings

    url = get_settings().database_url
    if not url:
        pytest.skip("requires a throwaway Postgres (TEST_DATABASE_URL)")
    engine = create_engine(str(url), connect_args={"connect_timeout": 20})
    try:
        conn = engine.connect()
    except Exception:
        engine.dispose()
        pytest.skip("Postgres is not reachable")
    schema = f"r104_{uuid.uuid4().hex[:10]}"
    tx = conn.begin()
    conn.execute(text("SET LOCAL lock_timeout = '5s'"))
    conn.execute(text("SET LOCAL statement_timeout = '30s'"))

    @event.listens_for(conn, "before_cursor_execute", retval=True)
    def _to_schema(_conn, _cursor, statement, parameters, _context, _many):
        return statement.replace(f"{LENDING_SCHEMA}.", f"{schema}."), parameters

    for statement in base_statements(schema):
        conn.execute(text(statement))
    apply_to(conn, schema)
    session = Session(bind=conn, join_transaction_mode="create_savepoint")
    monkeypatch.setattr(cb, "lending_session", lambda: nullcontext(session))
    monkeypatch.setattr(cb, "_announce_trip", lambda *args: None)
    monkeypatch.setattr("src.lending.ops_alerts.post_ops_alert", lambda message: True)
    session.info["schema"] = schema
    yield session
    session.close()
    tx.rollback()
    conn.close()
    engine.dispose()


def _card(db, *, channel="ghl_sms", status="approved", held_reason=None, card_ts=None):
    return db.execute(text(
        "INSERT INTO lending.pending_actions (tool_name, channel, payload, summary, status, held_reason, card_channel, "
        "card_ts, decided_by, decided_at) VALUES ('t', :channel, '{}'::jsonb, 'x', :status, :reason, "
        "CASE WHEN :ts IS NULL THEN NULL ELSE 'C1' END, :ts, 'U1', now()) RETURNING action_id"),
        {"channel": channel, "status": status, "reason": held_reason, "ts": card_ts}).scalar_one()


def _status(db, action_id):
    return db.execute(text("SELECT status, held_reason, card_ts, decided_by FROM lending.pending_actions "
                           "WHERE action_id = :id"), {"id": action_id}).one()


def _breaker(db, channel):
    return db.execute(text("SELECT state, consecutive_failures, tripped_reason, reset_by FROM lending.channel_breakers "
                           "WHERE channel = :c"), {"c": channel}).one()


def test_the_migration_seeds_every_channel_closed_and_is_idempotent(db):
    apply_to(db.connection(), db.info["schema"])
    rows = db.execute(text("SELECT channel, state, consecutive_failures FROM lending.channel_breakers ORDER BY channel")).all()
    assert [tuple(r) for r in rows] == [("ghl_email", "closed", 0), ("ghl_sms", "closed", 0), ("instantly", "closed", 0)]


def test_five_failures_in_a_row_trip_the_channel_and_hold_its_approved_cards_only(db):
    sms_one, sms_two = _card(db), _card(db)
    email, pending = _card(db, channel="ghl_email"), _card(db, status="pending")
    for _ in range(4):
        assert cb.record_failure(cb.CHANNEL_SMS, "HTTP 503") is False
    assert cb.record_failure(cb.CHANNEL_SMS, "HTTP 503", ambiguous=True) is True
    assert tuple(_breaker(db, cb.CHANNEL_SMS)) == ("open", 5, "HTTP 503", None)
    for held in (sms_one, sms_two):
        assert tuple(_status(db, held))[:2] == ("held", "breaker_open")
    assert _status(db, email).status == "approved" and _status(db, pending).status == "pending"
    event_row = db.execute(text("SELECT event, actor, detail FROM lending.channel_breaker_events")).one()
    assert (event_row.event, event_row.actor) == ("tripped", "system:breaker")
    assert sorted(event_row.detail["held_cards"]) == sorted([sms_one, sms_two])
    with pytest.raises(cb.ChannelPaused):
        cb.admit(cb.CHANNEL_SMS)
    cb.admit(cb.CHANNEL_EMAIL)  # other channels keep sending


def test_a_success_in_between_starts_the_run_over(db):
    for _ in range(4):
        cb.record_failure(cb.CHANNEL_EMAIL, "HTTP 500")
    cb.record_success(cb.CHANNEL_EMAIL)
    assert _breaker(db, cb.CHANNEL_EMAIL).consecutive_failures == 0
    for _ in range(4):
        assert cb.record_failure(cb.CHANNEL_EMAIL, "HTTP 500") is False
    assert _breaker(db, cb.CHANNEL_EMAIL).state == "closed"


def test_a_person_reopens_the_channel_and_the_held_cards_come_back_for_approval(db):
    breaker_held = _card(db, card_ts="1700.1")
    for _ in range(5):
        cb.record_failure(cb.CHANNEL_SMS, "HTTP 503")
    other_hold = _card(db, status="held", held_reason="recipient_changed", card_ts="1700.2")
    email_hold = _card(db, channel="ghl_email", status="held", held_reason="breaker_open", card_ts="1700.3")
    result = cb.reset(cb.CHANNEL_SMS, "slack:U_APPROVER")
    assert result == cb.ResetResult(cb.CHANNEL_SMS, True, 1)
    assert tuple(_breaker(db, cb.CHANNEL_SMS)) == ("closed", 0, "HTTP 503", "slack:U_APPROVER")
    pending = _status(db, breaker_held)
    assert (pending.status, pending.held_reason, pending.card_ts, pending.decided_by) == ("pending", None, None, None)
    assert _status(db, other_hold).status == "held"      # held for another reason: a person still has to look
    assert _status(db, email_hold).status == "held"      # another channel's breaker
    cb.admit(cb.CHANNEL_SMS)
    assert cb.reset(cb.CHANNEL_SMS, "cli:someone") == cb.ResetResult(cb.CHANNEL_SMS, False, 0)


def test_one_held_card_can_be_reopened_on_its_own(db):
    held = _card(db, status="held", held_reason="uncertain_outcome", card_ts="1700.1")
    assert cb.release_held(held) is True and _status(db, held).status == "pending"
    assert cb.release_held(held) is False


def test_a_bulk_batch_drips_out_within_the_per_minute_cap(db, monkeypatch):
    from config.settings import get_settings

    monkeypatch.setattr(get_settings(), "lending_sms_per_minute", 12, raising=False)
    sent = rate_limited = 0
    for _ in range(100):  # 100 texts queued inside one minute
        try:
            cb.admit(cb.CHANNEL_SMS)
            sent += 1
        except cb.RateLimited:
            rate_limited += 1
    assert (sent, rate_limited) == (12, 88)
    assert db.execute(text("SELECT sent FROM lending.channel_rate_counters")).scalar_one() == 12


def test_approval_cards_leave_room_for_a_live_reply(db, monkeypatch):
    from config.settings import get_settings

    monkeypatch.setattr(get_settings(), "lending_sms_per_minute", 12, raising=False)
    card_sends = 0
    with cb.card_sends():
        for _ in range(20):
            try:
                cb.admit(cb.CHANNEL_SMS)
                card_sends += 1
            except cb.RateLimited:
                break
    assert card_sends == 2  # 12 - 10 reserved
    for _ in range(10):
        cb.admit(cb.CHANNEL_SMS)  # the missed-call text-backs still fit
    with pytest.raises(cb.RateLimited):
        cb.admit(cb.CHANNEL_SMS)


def test_counters_are_per_channel_and_an_unmigrated_database_fails_closed(db):
    cb.admit(cb.CHANNEL_SMS)
    cb.admit(cb.CHANNEL_EMAIL)
    assert db.execute(text("SELECT count(*) FROM lending.channel_rate_counters")).scalar_one() == 2
    db.execute(text("DROP TABLE lending.channel_breakers"))
    with pytest.raises(cb.ChannelPaused):
        cb.admit(cb.CHANNEL_SMS)
