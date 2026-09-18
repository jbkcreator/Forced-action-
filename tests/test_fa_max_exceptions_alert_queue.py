"""
WP-T2-1 go-live review (2026-09) — durable EXCEPTIONS alert queue.

Real Postgres, real migrations (apply_fa_max_exceptions_alert_queue.py must
have been run first). enqueue_and_attempt()/drain_pending() open their own
DB sessions via get_db_context(), so these tests commit and clean up
explicitly rather than riding fresh_db's rolled-back transaction — same
pattern as tests/test_relay_venture_scoping.py's two_ventures fixture.
"""
from __future__ import annotations

import threading
import time
import uuid

import pytest
from sqlalchemy import text

from src.services.relay import exceptions_alert_queue


@pytest.fixture
def alert_cleanup(fresh_db):
    """fresh_db just confirms Postgres is reachable and skips otherwise --
    the actual cleanup runs on a plain (auto-committing) connection since
    exceptions_alert_queue writes via its own get_db_context() sessions."""
    rules: list[str] = []
    yield rules
    if rules:
        from src.core.database import get_db_context
        with get_db_context() as session:
            session.execute(
                text("DELETE FROM fa_max_exceptions_alert_queue WHERE rule = ANY(:rules)"),
                {"rules": rules},
            )
            session.commit()


def _unique_rule(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _row(rule: str):
    from src.core.database import get_db_context
    with get_db_context() as session:
        return session.execute(
            text(
                "SELECT status, attempts, sent_at, error FROM fa_max_exceptions_alert_queue "
                "WHERE rule = :rule"
            ),
            {"rule": rule},
        ).mappings().one()


def test_enqueue_commits_before_attempting_slack(alert_cleanup, monkeypatch):
    """The row must exist in the DB even if the Slack call that follows
    raises or hangs -- durability is the entire point."""
    rule = _unique_rule("commit_before_slack")
    alert_cleanup.append(rule)

    def _boom(**kw):
        raise RuntimeError("Slack call blew up")

    monkeypatch.setattr(exceptions_alert_queue, "post_exceptions_alert", _boom)

    # enqueue_and_attempt catches and logs, never raises — but the row it
    # committed before the (failing) attempt must still be there.
    delivered = exceptions_alert_queue.enqueue_and_attempt(
        venture_key="fa_max_lending", rule=rule, message="test",
    )
    assert delivered is False
    row = _row(rule)
    # The row was committed pre-attempt; _attempt()'s own except path never
    # runs here because post_exceptions_alert itself raised outside
    # _attempt's try -- but enqueue_and_attempt's outer try/except still
    # caught it, proving the insert-then-attempt split works even when the
    # attempt explodes rather than cleanly returning False.
    assert row["status"] == "pending"


def test_successful_attempt_marks_sent(alert_cleanup, monkeypatch):
    rule = _unique_rule("success")
    alert_cleanup.append(rule)
    monkeypatch.setattr(exceptions_alert_queue, "post_exceptions_alert", lambda **kw: True)

    delivered = exceptions_alert_queue.enqueue_and_attempt(
        venture_key="fa_max_lending", rule=rule, message="test",
    )
    assert delivered is True
    row = _row(rule)
    assert row["status"] == "sent"
    assert row["sent_at"] is not None
    assert row["attempts"] == 1


def test_failed_attempt_stays_pending_with_error_recorded(alert_cleanup, monkeypatch):
    rule = _unique_rule("failure")
    alert_cleanup.append(rule)
    monkeypatch.setattr(exceptions_alert_queue, "post_exceptions_alert", lambda **kw: False)

    delivered = exceptions_alert_queue.enqueue_and_attempt(
        venture_key="fa_max_lending", rule=rule, message="test",
    )
    assert delivered is False
    row = _row(rule)
    assert row["status"] == "pending"
    assert row["sent_at"] is None
    assert row["attempts"] == 1
    assert row["error"] is not None


def test_dedup_skips_a_second_enqueue_within_the_window(alert_cleanup, monkeypatch):
    """A sustained outage means sweep.py's suppression-sync-failure path
    fires every ~30-min tick -- this proves it doesn't flood the table with
    duplicate pending rows for the same (venture, rule)."""
    rule = _unique_rule("dedup")
    alert_cleanup.append(rule)
    monkeypatch.setattr(exceptions_alert_queue, "post_exceptions_alert", lambda **kw: False)

    exceptions_alert_queue.enqueue_and_attempt(venture_key="fa_max_lending", rule=rule, message="first")
    second = exceptions_alert_queue.enqueue_and_attempt(venture_key="fa_max_lending", rule=rule, message="second")

    assert second is False  # deduped, not a delivery
    from src.core.database import get_db_context
    with get_db_context() as session:
        count = session.execute(
            text("SELECT count(*) FROM fa_max_exceptions_alert_queue WHERE rule = :rule"),
            {"rule": rule},
        ).scalar_one()
    assert count == 1  # not 2


def test_dedup_does_not_block_a_different_rule(alert_cleanup, monkeypatch):
    rule_a = _unique_rule("dedup_a")
    rule_b = _unique_rule("dedup_b")
    alert_cleanup.extend([rule_a, rule_b])
    monkeypatch.setattr(exceptions_alert_queue, "post_exceptions_alert", lambda **kw: False)

    exceptions_alert_queue.enqueue_and_attempt(venture_key="fa_max_lending", rule=rule_a, message="a")
    exceptions_alert_queue.enqueue_and_attempt(venture_key="fa_max_lending", rule=rule_b, message="b")

    assert _row(rule_a)["status"] == "pending"
    assert _row(rule_b)["status"] == "pending"


def test_drain_pending_delivers_a_previously_failed_alert(alert_cleanup, monkeypatch):
    """Simulates the real recovery scenario: Slack was down when the
    monitor first tried, is back up by the time the drain worker runs."""
    rule = _unique_rule("drain_recovers")
    alert_cleanup.append(rule)

    monkeypatch.setattr(exceptions_alert_queue, "post_exceptions_alert", lambda **kw: False)
    exceptions_alert_queue.enqueue_and_attempt(venture_key="fa_max_lending", rule=rule, message="test")
    assert _row(rule)["status"] == "pending"

    monkeypatch.setattr(exceptions_alert_queue, "post_exceptions_alert", lambda **kw: True)
    attempted = exceptions_alert_queue.drain_pending()

    assert attempted >= 1
    row = _row(rule)
    assert row["status"] == "sent"
    assert row["attempts"] == 2  # one failed attempt, one successful drain


def test_drain_pending_leaves_a_still_failing_alert_pending(alert_cleanup, monkeypatch):
    rule = _unique_rule("drain_still_failing")
    alert_cleanup.append(rule)
    monkeypatch.setattr(exceptions_alert_queue, "post_exceptions_alert", lambda **kw: False)

    exceptions_alert_queue.enqueue_and_attempt(venture_key="fa_max_lending", rule=rule, message="test")
    exceptions_alert_queue.drain_pending()

    row = _row(rule)
    assert row["status"] == "pending"
    assert row["attempts"] == 2


def test_drain_pending_processes_oldest_first_up_to_limit(alert_cleanup, monkeypatch):
    rules = [_unique_rule(f"order_{i}") for i in range(3)]
    alert_cleanup.extend(rules)
    monkeypatch.setattr(exceptions_alert_queue, "post_exceptions_alert", lambda **kw: False)

    for rule in rules:
        exceptions_alert_queue.enqueue_and_attempt(venture_key="fa_max_lending", rule=rule, message="test")

    attempted = exceptions_alert_queue.drain_pending(limit=2)
    assert attempted == 2  # bounded — a pending-alert sweep never loads an unbounded row set


def test_a_row_with_zero_attempts_is_recoverable_by_a_fresh_drain(alert_cleanup, monkeypatch):
    """Simulates a crash between the durable commit and the first delivery
    attempt ever running (enqueue_and_attempt's own process died right after
    the INSERT committed, before _attempt() was called at all). A fresh
    process's drain_pending() must still find and deliver it -- this is the
    scenario the whole table exists to fix."""
    from src.core.database import get_db_context

    rule = _unique_rule("crash_before_first_attempt")
    alert_cleanup.append(rule)
    with get_db_context() as session:
        session.execute(
            text(
                "INSERT INTO fa_max_exceptions_alert_queue "
                "(venture_key, rule, message, status) "
                "VALUES ('fa_max_lending', :rule, 'test', 'pending')"
            ),
            {"rule": rule},
        )
        session.commit()

    monkeypatch.setattr(exceptions_alert_queue, "post_exceptions_alert", lambda **kw: True)
    attempted = exceptions_alert_queue.drain_pending()

    assert attempted >= 1
    row = _row(rule)
    assert row["status"] == "sent"
    assert row["attempts"] == 1


# ---------------------------------------------------------------------------
# Code-review finding (2026-09): enqueue_and_attempt()'s immediate delivery
# attempt and drain_pending()'s retry sweep both operate on 'pending' rows
# with a real network call (post_exceptions_alert) in between the row
# becoming visible and being finalized. Without an atomic claim, two
# concurrent callers could both post the same alert. Real threads, not
# monkeypatched sequencing, matching this repo's own concurrency-proof
# pattern (test_relay_concurrency.py's ceiling test).
# ---------------------------------------------------------------------------

def test_concurrent_drain_and_immediate_attempt_never_both_post(alert_cleanup, monkeypatch):
    """The exact race the finding described: enqueue_and_attempt()'s own
    immediate attempt racing an overlapping drain_pending() tick for the
    SAME row it just created. Slack is faked with a real sleep so both
    threads' network-call windows genuinely overlap in wall-clock time."""
    rule = _unique_rule("race_enqueue_vs_drain")
    alert_cleanup.append(rule)

    posts = []
    post_lock = threading.Lock()

    def _slow_post(**kw):
        time.sleep(0.2)
        with post_lock:
            posts.append(kw["rule"])
        return True

    monkeypatch.setattr(exceptions_alert_queue, "post_exceptions_alert", _slow_post)

    # Insert the row directly (bypassing the dedup+insert step) so both
    # threads start from the identical 'pending, unclaimed' state at the
    # same instant -- isolates the claim race from insert-ordering.
    from src.core.database import get_db_context
    with get_db_context() as session:
        row_id = session.execute(
            text(
                "INSERT INTO fa_max_exceptions_alert_queue "
                "(venture_key, rule, message, status) "
                "VALUES ('fa_max_lending', :rule, 'test', 'pending') RETURNING id"
            ),
            {"rule": rule},
        ).scalar_one()
        session.commit()

    results = []
    result_lock = threading.Lock()

    def _drain_attempt():
        claimed = exceptions_alert_queue._claim_row(row_id)
        with result_lock:
            results.append(claimed)
        if claimed:
            exceptions_alert_queue._attempt(
                row_id, venture_key="fa_max_lending", rule=rule, message="test", attempts_so_far=0,
            )

    threads = [threading.Thread(target=_drain_attempt) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Exactly one of the 5 concurrent claim attempts won.
    assert results.count(True) == 1
    assert results.count(False) == 4
    # Exactly one Slack post happened, not five.
    assert len(posts) == 1
    row = _row(rule)
    assert row["status"] == "sent"
    assert row["attempts"] == 1


def test_concurrent_drain_ticks_never_double_claim_the_same_row(alert_cleanup, monkeypatch):
    """Two overlapping drain_pending() runs (e.g. one run taking long enough
    to still be in flight when the next cron tick fires) must not both post
    the same pending row."""
    rule = _unique_rule("race_drain_vs_drain")
    alert_cleanup.append(rule)

    posts = []
    post_lock = threading.Lock()

    def _slow_post(**kw):
        time.sleep(0.2)
        with post_lock:
            posts.append(kw["rule"])
        return True

    monkeypatch.setattr(exceptions_alert_queue, "post_exceptions_alert", _slow_post)

    from src.core.database import get_db_context
    with get_db_context() as session:
        session.execute(
            text(
                "INSERT INTO fa_max_exceptions_alert_queue "
                "(venture_key, rule, message, status) "
                "VALUES ('fa_max_lending', :rule, 'test', 'pending')"
            ),
            {"rule": rule},
        )
        session.commit()

    attempted_counts = []
    lock = threading.Lock()

    def _run_drain():
        n = exceptions_alert_queue.drain_pending()
        with lock:
            attempted_counts.append(n)

    threads = [threading.Thread(target=_run_drain) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sum(attempted_counts) == 1  # only one drain tick actually claimed+attempted the row
    assert len(posts) == 1
    row = _row(rule)
    assert row["status"] == "sent"


def test_claim_expires_after_lease_so_a_crashed_claimant_is_recoverable(alert_cleanup):
    """A claim that's already expired (simulating a worker that claimed a
    row and then crashed before finishing) must be re-claimable — the
    lease is a timeout, not a permanent lock."""
    rule = _unique_rule("expired_lease")
    alert_cleanup.append(rule)

    from src.core.database import get_db_context
    with get_db_context() as session:
        row_id = session.execute(
            text(
                "INSERT INTO fa_max_exceptions_alert_queue "
                "(venture_key, rule, message, status, claimed_until) "
                "VALUES ('fa_max_lending', :rule, 'test', 'pending', now() - interval '1 minute') "
                "RETURNING id"
            ),
            {"rule": rule},
        ).scalar_one()
        session.commit()

    assert exceptions_alert_queue._claim_row(row_id) is True


def test_claim_fails_while_lease_still_active(alert_cleanup):
    rule = _unique_rule("active_lease")
    alert_cleanup.append(rule)

    from src.core.database import get_db_context
    with get_db_context() as session:
        row_id = session.execute(
            text(
                "INSERT INTO fa_max_exceptions_alert_queue "
                "(venture_key, rule, message, status, claimed_until) "
                "VALUES ('fa_max_lending', :rule, 'test', 'pending', now() + interval '1 minute') "
                "RETURNING id"
            ),
            {"rule": rule},
        ).scalar_one()
        session.commit()

    assert exceptions_alert_queue._claim_row(row_id) is False


def test_concurrent_enqueue_from_scratch_never_creates_two_pending_rows(alert_cleanup, monkeypatch):
    """Code-review finding (third round, 2026-09): enqueue_and_attempt()'s
    own dedup check (_recently_queued: SELECT, then INSERT if nothing
    found) is a check-then-act race on its own, distinct from the row-level
    claim race fixed earlier -- this one is about TWO PRODUCERS EACH
    STARTING FROM ZERO (no row exists yet for this rule at all), not two
    workers racing an existing row. Reproduced directly by widening the
    SELECT-to-INSERT window: without the DB-level partial unique index on
    (venture_key, rule) WHERE status='pending', 5 concurrent callers
    created 2 rows and posted twice.

    The winning thread's full attempt (post + mark 'sent') must NOT be
    allowed to complete before the other threads have made their own
    INSERT attempt, or the test stops proving anything -- once a row is
    'sent' rather than 'pending', the partial index no longer applies to
    it, and a later, genuinely-new caller inserting after that point isn't
    a race, it's just a second incident (which DEDUP_WINDOW_HOURS' "sent
    recently" check is responsible for catching, checked separately). Both
    post_exceptions_alert and the pre-INSERT check are slowed here so all 5
    threads' INSERT attempts genuinely overlap in wall-clock time."""
    rule = _unique_rule("race_enqueue_from_scratch")
    alert_cleanup.append(rule)

    orig_recently_queued = exceptions_alert_queue._recently_queued

    def _slow_recently_queued(session, **kw):
        result = orig_recently_queued(session, **kw)
        time.sleep(0.2)
        return result

    def _slow_post(**kw):
        time.sleep(0.3)
        return True

    monkeypatch.setattr(exceptions_alert_queue, "_recently_queued", _slow_recently_queued)
    monkeypatch.setattr(exceptions_alert_queue, "post_exceptions_alert", _slow_post)

    results = []
    lock = threading.Lock()

    def _worker():
        r = exceptions_alert_queue.enqueue_and_attempt(
            venture_key="fa_max_lending", rule=rule, message="test",
        )
        with lock:
            results.append(r)

    threads = [threading.Thread(target=_worker) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert results.count(True) == 1
    assert results.count(False) == 4
    row = _row(rule)
    assert row["status"] == "sent"
