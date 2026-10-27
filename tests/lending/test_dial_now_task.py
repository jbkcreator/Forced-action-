"""Ops check (D1/D7): which launch queue the caller should be on now, and its line setting."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone

from config import lending_queues as q
from src.tasks import lending_dial_now as task


class FakeDb:
    def __init__(self, rows):
        self.rows = rows

    def execute(self, *_a, **_kw):
        rows = self.rows

        class R:
            def all(self):
                return rows
        return R()


def _patch(monkeypatch, rows, blocked=()):
    @contextmanager
    def fake_db():
        yield FakeDb(rows)
    monkeypatch.setattr(task, "get_db_context", fake_db)
    monkeypatch.setattr(task, "blocked_from_dialing", lambda _db, phones, _now: set(blocked) & set(phones))


def _rows(**pools):
    return [(pool, f"+1813555{pool[:2]}{i:02d}") for pool, n in pools.items() for i in range(n)]


def test_points_at_partners_while_any_remain(monkeypatch, caplog):
    _patch(monkeypatch, _rows(**{q.PARTNERS: 4, q.VERIFIED_MATURITY: 5}))
    caplog.set_level("INFO")
    assert task.main([]) == 0
    assert task.dial_now_summary({q.PARTNERS: 4, q.VERIFIED_MATURITY: 500}) == {
        "queue": q.PARTNERS, "max_lines": 3,
        "active_by_queue": {q.PARTNERS: 4, q.WHOLESALERS: 0, q.TRANSACTION_READY: 0, q.BUILDERS: 0, q.VERIFIED_MATURITY: 500},
    }


def test_maturity_only_when_everything_above_is_empty_and_at_five_lines():
    assert task.dial_now_summary({q.VERIFIED_MATURITY: 525})["queue"] == q.VERIFIED_MATURITY
    assert task.dial_now_summary({q.VERIFIED_MATURITY: 525})["max_lines"] == 5


def test_nothing_loaded_returns_no_queue_and_exit_code_signals_it(monkeypatch):
    _patch(monkeypatch, [])
    assert task.dial_now_summary({})["queue"] is None
    assert task.main([]) == task.EXIT_NOTHING_TO_DIAL


def test_a_fully_held_queue_is_skipped_for_the_next_one_with_dialable_work(lending_db):
    """A partner under an open dialing hold (call window, attempt cap, stale scrub) is not
    dialable, so it must not keep Partners selected over an available builder."""
    from sqlalchemy import text

    from src.lending.compliance import phone_hash

    from datetime import timedelta

    now = datetime(2026, 9, 29, 16, 0, tzinfo=timezone.utc)  # noon ET, inside the call window
    before = task.active_by_queue(lending_db, now=now)
    held_partner, builder = "+18135559401", "+18135559402"
    for pool, phone in ((q.PARTNERS, held_partner), (q.BUILDERS, builder)):
        lending_db.execute(text(
            "INSERT INTO lending.dialer_load_records (run_id, pool, source_record_ref, phone, phone_hash) "
            "VALUES ('test-dial-now', :pool, :ref, :phone, :hash)"
        ), {"pool": pool, "ref": f"test:{pool}", "phone": phone, "hash": phone_hash(phone)})
    lending_db.execute(text("INSERT INTO lending.dialer_holds (phone, reason) VALUES (:p, 'attempt_cap')"),
                       {"p": held_partner})

    lending_db.execute(text(
        "INSERT INTO dnc_phone_checks (phone, national_dnc, litigator, checked_at, source) "
        "VALUES (:p, false, false, :at, 'test')"), {"p": builder, "at": now - timedelta(days=1)})

    after = task.active_by_queue(lending_db, now=now)
    assert after.get(q.PARTNERS, 0) == before.get(q.PARTNERS, 0)
    assert after.get(q.BUILDERS, 0) == before.get(q.BUILDERS, 0) + 1
    if not before.get(q.PARTNERS) and not before.get(q.TRANSACTION_READY):
        assert task.dial_now_summary(after)["queue"] == q.BUILDERS


def test_a_rule_blocked_partner_without_a_hold_does_not_keep_partners_selected(monkeypatch):
    """A failed dialer removal leaves an attempt-capped contact active with no hold row: the
    rules, not the hold table, decide it is not dialable."""
    rows = _rows(**{q.PARTNERS: 1, q.BUILDERS: 1})
    partner = rows[0][1]
    _patch(monkeypatch, rows, blocked={partner})
    with task.get_db_context() as db:
        active = task.active_by_queue(db)
    assert active == {q.BUILDERS: 1}
    assert task.dial_now_summary(active)["queue"] == q.BUILDERS


def test_attempt_capped_partner_with_failed_removal_is_not_counted(lending_db):
    """Regression for failed removal on the real rules: three outbound attempts in 24 h, no hold."""
    from datetime import timedelta
    import uuid

    from sqlalchemy import text

    from src.lending.compliance import phone_hash

    now = datetime(2026, 9, 29, 16, 0, tzinfo=timezone.utc)  # noon ET, inside the call window
    partner = "+18135559411"
    lending_db.execute(text(
        "INSERT INTO lending.dialer_load_records (run_id, pool, source_record_ref, phone, phone_hash) "
        "VALUES ('test-dial-now', :pool, 'test:capped', :phone, :hash)"
    ), {"pool": q.PARTNERS, "phone": partner, "hash": phone_hash(partner)})
    lending_db.execute(text(
        "INSERT INTO dnc_phone_checks (phone, national_dnc, litigator, checked_at, source) "
        "VALUES (:p, false, false, :at, 'test')"), {"p": partner, "at": now - timedelta(days=1)})
    for minutes in (10, 20, 30):
        lending_db.execute(text(
            "INSERT INTO lending.call_dispositions (dialer_call_id, phone, direction, call_ended_at, raw_event) "
            "VALUES (:cid, :p, 'outbound', :t, '{}')"),
            {"cid": f"test-{uuid.uuid4().hex}", "p": partner, "t": now - timedelta(minutes=minutes)})
    before = task.active_by_queue(lending_db, now=now).get(q.PARTNERS, 0)
    lending_db.execute(text("DELETE FROM lending.call_dispositions WHERE phone = :p"), {"p": partner})
    after_reset = task.active_by_queue(lending_db, now=now).get(q.PARTNERS, 0)
    assert after_reset == before + 1  # same contact counts once its attempts are gone
