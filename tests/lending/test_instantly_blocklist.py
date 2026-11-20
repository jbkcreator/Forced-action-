"""P1.3: every opt-out with an email is queued for Instantly's block list and retried until Instantly accepts it; and an
automatically approved message is re-checked against approved wording at send time."""
from __future__ import annotations

from contextlib import contextmanager, nullcontext
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import text
from sqlalchemy.orm import Session

from config.lending_compliance import OptOutChannel
from src.lending import compliance, cora_agent, instantly_blocklist as bl, instantly_client
from src.lending.instantly_client import InstantlyAccount, InstantlyError

ACCOUNT = InstantlyAccount("key", "https://api.instantly.test")


@pytest.fixture
def queue_db(lending_db):
    from migrations.apply_lending_cora_inbox import apply_to as apply_inbox
    from migrations.apply_lending_instantly_block_queue import apply_to

    conn = lending_db.connection()
    try:
        apply_inbox(conn)
        apply_to(conn)
        apply_to(conn)  # idempotent
        lending_db.execute(text("DELETE FROM lending.instantly_block_queue"))
        lending_db.execute(text("DELETE FROM lending.cora_inbox WHERE event_type = 'instantly_block_failed'"))
    except Exception:
        lending_db.rollback()
        pytest.skip("the test database could not take the migrations")
    return lending_db


def _rows(db, status=None):
    sql = "SELECT email, status, attempts, last_error FROM lending.instantly_block_queue"
    if status:
        sql += f" WHERE status = '{status}'"
    return [tuple(r) for r in db.execute(text(sql + " ORDER BY id"))]


def _sweep(db, block, account=ACCOUNT):
    return bl.BlockListSweep(None, account=lambda: account, block=block, session_factory=lambda: nullcontext(db))


# ── queueing ────────────────────────────────────────────────────────────────

def test_every_address_is_queued_once_even_beyond_ten_and_in_any_case(queue_db):
    emails = [f"U{i}@Example.com" for i in range(25)] + ["u3@example.com"]
    assert bl.enqueue(queue_db, emails) == 25
    assert len(_rows(queue_db)) == 25 and all(r[1] == "pending" for r in _rows(queue_db))
    assert bl.enqueue(queue_db, ["u1@example.com"]) == 1  # again: no second row
    assert len(_rows(queue_db)) == 25


def test_queueing_never_breaks_the_opt_out_when_the_table_is_missing_or_the_write_fails():
    class Broken:
        @contextmanager
        def begin_nested(self):
            yield
            raise RuntimeError("relation does not exist: a@x.com")

        def execute(self, *a, **k):
            raise RuntimeError("boom")

    assert bl.enqueue(Broken(), ["a@x.com"]) == 0
    assert bl.enqueue(Broken(), []) == 0


def test_propagate_queues_the_opted_out_email_on_postgres(queue_db):
    try:
        queue_db.execute(text("SELECT 1 FROM lending.opt_out_events LIMIT 1"))
        queue_db.execute(text("SELECT 1 FROM subscribers LIMIT 1"))
    except Exception:
        queue_db.rollback()
        pytest.skip("the test database lacks the opt-out or subscriber tables")
    compliance.propagate_opt_out(queue_db, email="Queue.Me@Example.com", channel=OptOutChannel.EMAIL, actor="test",
                                 source_ref="blocklist-test:1")
    assert ("queue.me@example.com", "pending", 0, None) in _rows(queue_db)


# ── the sweep: retried until it works ──────────────────────────────────────

def test_a_large_batch_is_worked_off_over_several_passes(queue_db):
    bl.enqueue(queue_db, [f"u{i}@example.com" for i in range(25)])
    blocked = []
    sweep = _sweep(queue_db, lambda account, email: blocked.append(email) or True)
    assert [sweep().blocked for _ in range(3)] == [10, 10, 5]
    assert len(blocked) == 25 and _rows(queue_db, "pending") == []


def test_a_temporary_instantly_failure_is_retried_later_and_then_succeeds(queue_db):
    bl.enqueue(queue_db, ["a@example.com"])
    outcomes = iter([False, False, True])
    sweep = _sweep(queue_db, lambda account, email: next(outcomes))
    assert sweep().retrying == 1
    assert _rows(queue_db)[0][:3] == ("a@example.com", "pending", 1)
    assert sweep() == bl.BlockListResult()  # not due yet: the wait has not passed
    queue_db.execute(text("UPDATE lending.instantly_block_queue SET next_attempt_at = now() - interval '1 second'"))
    assert sweep().retrying == 1  # second failure, backs off again
    queue_db.execute(text("UPDATE lending.instantly_block_queue SET next_attempt_at = now() - interval '1 second'"))
    assert sweep().blocked == 1
    assert _rows(queue_db)[0][:2] == ("a@example.com", "done")


def test_the_wait_grows_with_each_failure_up_to_a_cap():
    assert [bl._wait(n) for n in (0, 1, 2, 3)] == [60, 120, 240, 480]
    assert bl._wait(30) == bl.MAX_BACKOFF_SECONDS


def test_after_the_last_attempt_the_row_is_failed_and_an_inbox_item_names_no_address(queue_db):
    bl.enqueue(queue_db, ["a@example.com"])
    queue_db.execute(text("UPDATE lending.instantly_block_queue SET attempts = :n"), {"n": bl.MAX_ATTEMPTS - 1})
    assert _sweep(queue_db, lambda account, email: False)().failed == 1
    assert _rows(queue_db)[0][1] == "failed"
    item = queue_db.execute(text("SELECT event_type, summary, payload::text FROM lending.cora_inbox "
                                 "WHERE event_type = 'instantly_block_failed'")).one()
    assert "a@example.com" not in " ".join(item)
    assert _sweep(queue_db, lambda account, email: pytest.fail("a failed row must not be retried"))() == bl.BlockListResult()


def test_an_exception_from_instantly_is_a_failed_attempt_not_a_crash(queue_db):
    bl.enqueue(queue_db, ["a@example.com", "b@example.com"])
    seen = []

    def block(account, email):
        seen.append(email)
        if email == "a@example.com":
            raise RuntimeError("boom for a@example.com")
        return True

    result = _sweep(queue_db, block)()
    assert (result.blocked, result.retrying) == (1, 1) and seen == ["a@example.com", "b@example.com"]
    assert "a@example.com" not in (_rows(queue_db)[0][3] or "")  # the error class only


def test_rows_stay_pending_while_the_instantly_account_is_not_configured(queue_db):
    bl.enqueue(queue_db, ["a@example.com"])
    assert _sweep(queue_db, lambda *a: pytest.fail("no account"), account=None)() == bl.BlockListResult()
    assert _rows(queue_db)[0][1] == "pending"


def test_a_time_budget_stops_the_pass(queue_db):
    bl.enqueue(queue_db, ["a@example.com", "b@example.com"])
    ticks = iter([0.0, 0.0, 1000.0, 1000.0, 1000.0])
    seen = []
    sweep = bl.BlockListSweep(None, account=lambda: ACCOUNT, block=lambda account, email: seen.append(email) or True,
                              clock=lambda: next(ticks), session_factory=lambda: nullcontext(queue_db))
    sweep()
    assert seen == ["a@example.com"]


def test_instantly_saying_the_address_is_already_blocked_is_a_success(monkeypatch):
    def conflict(account, path, payload):
        raise InstantlyError("/api/v2/block-lists-entries returned HTTP 409")

    monkeypatch.setattr(instantly_client, "_post", conflict)
    assert instantly_client.block_address(ACCOUNT, "a@example.com") is True
    monkeypatch.setattr(instantly_client, "_post", lambda *a: (_ for _ in ()).throw(InstantlyError("returned HTTP 500")))
    assert instantly_client.block_address(ACCOUNT, "a@example.com") is False


def test_the_sweep_step_is_wired_into_coras_minute_pass():
    calls = []
    services = cora_agent.CoraServices(session=SimpleNamespace(expire_stale_actions=lambda: 0),
                                       relay=SimpleNamespace(run=lambda: SimpleNamespace(sent=0, failed=0, blocked=0)),
                                       gate=SimpleNamespace(retry_missing_cards=lambda: 0),
                                       block_list=lambda: calls.append("ran"))
    cora_agent.run_sweep(services)
    assert calls == ["ran"]


def test_a_failing_block_list_step_never_stops_the_sends(caplog):
    relay_ran = []

    def boom():
        raise RuntimeError("db down for a@example.com")

    services = cora_agent.CoraServices(session=SimpleNamespace(expire_stale_actions=lambda: 0),
                                       relay=SimpleNamespace(run=lambda: relay_ran.append(1) or SimpleNamespace(
                                           sent=0, failed=0, blocked=0)),
                                       gate=SimpleNamespace(retry_missing_cards=lambda: 0), block_list=boom)
    cora_agent.run_sweep(services)
    assert relay_ran == [1] and "a@example.com" not in caplog.text


# ── an automatically approved message is re-checked against approved wording at send time ──────────────────

class _Engine:
    """Answers the send check's queries in order, and records the write that sends the action back."""

    def __init__(self, *answers):
        self._answers = list(answers)
        self.writes = []

    @contextmanager
    def connect(self):
        yield self

    @contextmanager
    def begin(self):
        yield SimpleNamespace(execute=lambda statement, params: self.writes.append(dict(params)) or SimpleNamespace(
            first=lambda: (1,)))

    def execute(self, statement, params):
        return SimpleNamespace(scalar=lambda: self._answers.pop(0))


def _auto_action(**payload):
    from packages.agent_core.pending_actions import ActionStatus, PendingAction
    return PendingAction(
        action_id=7, tool_name="agent_job:send_sms", channel="ghl_sms",
        payload={"template_version": "LF-D2-SMS-v1", "body": "Hi Sarah, act now", "contact_label": "Sarah K., Miami", **payload},
        summary="", status=ActionStatus.SENDING, requested_by=None, source_channel=None, source_thread_ts=None,
        card_channel=None, card_ts=None, decided_by="auto:graduated", revised_by=None, revision_note=None, revisions=(),
        recipient_phone="(727) 555-0100", recipient_email=None, contact_ref=None, deal_ref=None, idempotency_key=None,
        expires_at=None, provider_ref=None, error=None)


def test_a_message_queued_before_the_switch_is_sent_back_for_approval_when_its_wording_is_not_approved():
    seen = []
    engine = _Engine(False, True, True)  # not suppressed, text consent, still graduated
    check = cora_agent.make_send_check(engine, carrier_approved=True, text_window=lambda p: True,
                                       wording_ok=lambda *args: seen.append(args) or False)
    reason = check(_auto_action())
    assert isinstance(reason, cora_agent.SendDeferred) and "back for approval" in reason
    assert engine.writes == [{"action_id": 7, "auto_actor": "auto:graduated"}]  # returned for a person's approval
    assert seen[0][0] == "LF-D2-SMS-v1" and seen[0][1] == "ghl_sms" and seen[0][3] == ["Sarah", "Miami"]


def test_approved_wording_still_sends_automatically():
    engine = _Engine(False, True, True)
    check = cora_agent.make_send_check(engine, carrier_approved=True, text_window=lambda p: True,
                                       wording_ok=lambda *args: True)
    assert check(_auto_action()) is None and engine.writes == []


def test_a_person_approved_message_is_never_wording_checked():
    called = []
    engine = _Engine(False, True)
    check = cora_agent.make_send_check(engine, carrier_approved=True, text_window=lambda p: True,
                                       wording_ok=lambda *a: called.append(a) or False)
    from dataclasses import replace
    assert check(replace(_auto_action(), decided_by="U_JOSH")) is None and called == []


def test_without_the_switch_there_is_no_wording_check():
    engine = _Engine(False, True, True)
    check = cora_agent.make_send_check(engine, carrier_approved=True, text_window=lambda p: True)
    assert check(_auto_action()) is None and engine.writes == []


# ── team decision Q18 at send time: a cold or partner message approved automatically while Q14 and Q19 were both on
# goes back for approval if either is turned off before the relay sends it ─────────────────────────────────────────

_FLAGS = ("lending_graduation_count_rejections", "lending_graduated_approved_wording_only")


def _set_flags(monkeypatch, q14: bool, q19: bool) -> None:
    from config.settings import get_settings

    for name, value in zip(_FLAGS, (q14, q19)):
        monkeypatch.setattr(get_settings(), name, value, raising=False)


def _cold_email():
    from dataclasses import replace
    return replace(_auto_action(subject="Your loan"), channel="instantly_cold_email", tool_name="agent_job:send_email",
                   recipient_phone=None, recipient_email="sarah@example.com")


def _partner_text():
    return _auto_action(partner_contact=True)


# Send-check answers in order: not suppressed, then no email opt-out (cold email) or text consent (text), still graduated.
@pytest.mark.parametrize("make_action, answers", [(_cold_email, (False, False, True)),
                                                  (_partner_text, (False, True, True))])
def test_a_cold_or_partner_message_approved_with_both_flags_on_still_sends_while_both_stay_on(
        monkeypatch, make_action, answers):
    _set_flags(monkeypatch, True, True)
    engine = _Engine(*answers)
    check = cora_agent.make_send_check(engine, carrier_approved=True, text_window=lambda p: True,
                                       email_window=lambda p: True)
    assert check(make_action()) is None and engine.writes == []


@pytest.mark.parametrize("q14, q19", [(False, True), (True, False)])
@pytest.mark.parametrize("make_action, answers", [(_cold_email, (False, False, True)),
                                                  (_partner_text, (False, True, True))])
def test_turning_off_either_flag_sends_a_queued_cold_or_partner_message_back_for_approval(
        monkeypatch, make_action, answers, q14, q19):
    _set_flags(monkeypatch, q14, q19)
    engine = _Engine(*answers)
    check = cora_agent.make_send_check(engine, carrier_approved=True, text_window=lambda p: True,
                                       email_window=lambda p: True)
    reason = check(make_action())
    assert isinstance(reason, cora_agent.SendDeferred) and "back for approval" in reason
    assert engine.writes == [{"action_id": 7, "auto_actor": "auto:graduated"}]


def test_with_a_flag_off_an_automatic_message_that_is_not_cold_or_partner_still_sends(monkeypatch):
    _set_flags(monkeypatch, False, False)
    engine = _Engine(False, True, True)
    check = cora_agent.make_send_check(engine, carrier_approved=True, text_window=lambda p: True)
    assert check(_auto_action()) is None and engine.writes == []


def test_with_a_flag_off_a_person_approved_cold_email_still_sends(monkeypatch):
    from dataclasses import replace

    _set_flags(monkeypatch, False, False)
    engine = _Engine(False, False)
    check = cora_agent.make_send_check(engine, email_window=lambda p: True)
    assert check(replace(_cold_email(), decided_by="U_JOSH")) is None and engine.writes == []


# ── review follow-ups ───────────────────────────────────────────────────────

def test_a_failed_row_or_an_old_done_row_is_queued_again_by_a_new_opt_out_but_a_recent_one_is_not(queue_db):
    bl.enqueue(queue_db, ["failed@example.com", "olddone@example.com", "newdone@example.com", "pending@example.com"])
    queue_db.execute(text("UPDATE lending.instantly_block_queue SET status = 'failed', attempts = 12 "
                          "WHERE email = 'failed@example.com'"))
    queue_db.execute(text("UPDATE lending.instantly_block_queue SET status = 'done', done_at = now() - interval '2 days' "
                          "WHERE email = 'olddone@example.com'"))
    queue_db.execute(text("UPDATE lending.instantly_block_queue SET status = 'done', done_at = now() "
                          "WHERE email = 'newdone@example.com'"))
    bl.enqueue(queue_db, ["failed@example.com", "olddone@example.com", "newdone@example.com", "pending@example.com"])
    states = {r[0]: (r[1], r[2]) for r in _rows(queue_db)}
    assert states["failed@example.com"] == ("pending", 0) and states["olddone@example.com"] == ("pending", 0)
    assert states["newdone@example.com"][0] == "done" and states["pending@example.com"][0] == "pending"


def test_an_opt_out_never_waits_for_a_row_the_sweep_is_holding_locked():
    """Two real sessions: one holds the pending row locked (as the sweep does while it calls Instantly); the opt-out's
    enqueue for the same address must finish at once instead of waiting for it."""
    from sqlalchemy import create_engine

    from config.settings import get_settings
    from migrations.apply_lending_instantly_block_queue import apply_to

    url = get_settings().database_url
    if not url:
        pytest.skip("requires a live Postgres DATABASE_URL")
    engine = create_engine(str(url), connect_args={"connect_timeout": 10})
    try:
        with engine.begin() as conn:
            apply_to(conn)
            conn.execute(text("DELETE FROM lending.instantly_block_queue WHERE email = 'lock.test@example.com'"))
            conn.execute(text("INSERT INTO lending.instantly_block_queue (email) VALUES ('lock.test@example.com')"))
    except Exception:
        engine.dispose()
        pytest.skip("Postgres is not reachable or lacks the lending schema")
    holder = engine.connect()
    try:
        holder_tx = holder.begin()
        holder.execute(text("SELECT id FROM lending.instantly_block_queue WHERE email = 'lock.test@example.com' "
                            "FOR UPDATE"))
        with engine.connect() as other:
            other.execute(text("SET LOCAL lock_timeout = '1500ms'"))
            started = datetime.now(timezone.utc)
            session = Session(bind=other)
            assert bl.enqueue(session, ["lock.test@example.com"]) == 1  # not 0: no lock timeout swallowed
            assert datetime.now(timezone.utc) - started < timedelta(seconds=1.4)
            session.rollback()
        holder_tx.rollback()
    finally:
        holder.close()
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM lending.instantly_block_queue WHERE email = 'lock.test@example.com'"))
        engine.dispose()


def test_a_wording_mismatch_returns_a_claimed_action_to_pending_on_postgres(queue_db):
    """The relay has already claimed the action ("sending") when the send check runs: it must still go back to pending."""
    from packages.agent_core.pending_actions import ActionStatus
    from src.lending import standing_sends

    conn = queue_db.connection()
    try:
        queue_db.execute(text("SELECT 1 FROM lending.pending_actions LIMIT 1"))
    except Exception:
        queue_db.rollback()
        pytest.skip("the test database lacks lending.pending_actions")
    ids = []
    for status in ("sending", "approved", "sent"):
        ids.append(queue_db.execute(text(
            "INSERT INTO lending.pending_actions (tool_name, channel, payload, status, decided_by) "
            "VALUES ('agent_job', 'ghl_sms', '{}'::jsonb, :s, 'auto:graduated') RETURNING action_id"), {"s": status}).scalar())
    engine = SimpleNamespace(begin=lambda: _same(conn))
    assert [standing_sends.return_for_approval(engine, i) for i in ids] == [True, True, False]
    states = [queue_db.execute(text("SELECT status, decided_by, card_ts FROM lending.pending_actions WHERE action_id = :i"),
                               {"i": i}).one() for i in ids]
    assert tuple(states[0]) == (ActionStatus.PENDING.value, None, None) and tuple(states[1]) == ("pending", None, None)
    assert states[2][0] == "sent"  # an already sent action is never touched


@contextmanager
def _same(conn):
    yield conn
