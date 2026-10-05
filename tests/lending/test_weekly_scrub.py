"""Go Live G10/G13: weekly re-scrub of loaded numbers with a credit cap; nurture flag for DNC blocks."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from src.lending.models import LendingDialerLoadRecord

pytestmark = pytest.mark.skipif(
    not os.environ.get("DATABASE_URL"), reason="requires a live Postgres DATABASE_URL"
)

NOW = datetime(2026, 10, 1, 15, 0, tzinfo=timezone.utc)
PHONES = [f"+1813555{n}" for n in (8401, 8402, 8403, 8404, 8405)]


@pytest.fixture
def db():
    engine = create_engine(os.environ["DATABASE_URL"])
    conn = engine.connect()
    tx = conn.begin()
    LendingDialerLoadRecord.__table__.create(conn, checkfirst=True)
    session = Session(bind=conn)
    yield session
    session.close()
    tx.rollback()
    conn.close()
    engine.dispose()


def _load(db, phone, active=True):
    db.execute(text(
        "INSERT INTO lending.dialer_load_records (run_id, pool, source_record_ref, phone, phone_hash, active, deactivated_at) "
        "VALUES ('t', 'builders', :ref, :p, 'h', :a, CASE WHEN :a THEN NULL ELSE now() END)"),
        {"ref": f"r-{phone}", "p": phone, "a": active})


def _scrubbed(db, phone, age_days):
    db.execute(text(
        "INSERT INTO lending.dnc_scrubs (phone, national_dnc, litigator, state_dnc, checked_at) "
        "VALUES (:p, false, false, false, :at)"), {"p": phone, "at": NOW - timedelta(days=age_days)})


class Scrubber:
    def __init__(self, dnc=()):
        self.calls, self.dnc = [], set(dnc)

    def __call__(self, phones):
        self.calls.append(list(phones))
        return [{"phone": p, "national_dnc": "Yes" if p in self.dnc else "No",
                 "litigator": "No", "state_dnc": "No", "phone_type": "mobile"} for p in phones]


class Remover:
    def __init__(self):
        self.removed = []

    def __call__(self, phone, *, reason):
        self.removed.append((phone, reason))


class FailOnceRemover:
    """Raises for a phone the first time it's called, succeeds every time after."""

    def __init__(self):
        self.calls = []
        self._failed_once = set()

    def __call__(self, phone, *, reason):
        self.calls.append(phone)
        if phone not in self._failed_once:
            self._failed_once.add(phone)
            raise RuntimeError("transient dialer error")


def test_every_loaded_number_is_rescrubbed_each_weekly_run_but_not_inactive_ones(db):
    """Finding 1: a scrub only 2 days old still turns stale for the sweep before the next
    weekly run, so it is selected too. Inactive load rows are never scrubbed."""
    from src.lending.weekly_scrub import stale_loaded_phones
    for p in PHONES[:4]:
        _load(db, p)
    _load(db, PHONES[4], active=False)         # inactive: not in the dialer
    _scrubbed(db, PHONES[0], age_days=2)
    _scrubbed(db, PHONES[1], age_days=9)
    # PHONES[2], PHONES[3]: never scrubbed
    picked = set(stale_loaded_phones(db, now=NOW)) & set(PHONES)
    assert picked == set(PHONES[:4])


class FailingScrubber:
    """Simulates a Tracerfy outage: the vendor call itself raises."""

    def __init__(self):
        self.calls = 0

    def __call__(self, phones):
        self.calls += 1
        raise RuntimeError("tracerfy down")


def test_a_failing_scrub_batch_charges_no_credits_and_leaves_numbers_stale(db):
    """Finding #6: a Tracerfy outage must not be silently absorbed as success — no
    credits counted, and the phone stays reachable as stale next run."""
    from src.lending.weekly_scrub import weekly_scrub

    result = weekly_scrub(db, scrubber=FailingScrubber(), max_credits=10, now=NOW, phones=[PHONES[0]],
                          dialer_remover=Remover())
    assert result.scrub_batch_failures == 1
    assert result.credits_used == 0
    assert result.scrubbed == 0


def test_main_exits_non_zero_when_a_scrub_batch_fails(monkeypatch):
    from src.lending import weekly_scrub as ws

    monkeypatch.setattr(ws, "stale_loaded_phones", lambda db, now=None: [PHONES[0]])
    monkeypatch.setattr(ws, "blocked_loaded_phones", lambda db: [])
    monkeypatch.setattr(ws, "tracerfy_scrub", FailingScrubber())

    class _FakeSession:
        def commit(self):
            pass

    class _Ctx:
        def __enter__(self):
            return _FakeSession()

        def __exit__(self, *exc):
            return False

    monkeypatch.setattr("src.core.database.get_db_context", lambda: _Ctx())
    assert ws.main(["--max-credits", "10"]) == ws.EXIT_SCRUB_FAILED


def test_credit_cap_aborts_before_a_batch_that_would_exceed_it(db):
    from src.lending.weekly_scrub import weekly_scrub
    scrubber = Scrubber()
    result = weekly_scrub(db, scrubber=scrubber, max_credits=3, batch_size=2, now=NOW, phones=PHONES[:5],
                          dialer_remover=Remover())
    assert scrubber.calls == [PHONES[:2]]      # 2nd batch would reach 4 > 3
    assert result.aborted and result.credits_used == 2 and result.scrubbed == 2
    # The 3 not reached are still loaded and dialable; they are reported, not hidden.
    assert result.left_unscrubbed == 3


def test_new_dnc_hits_leave_the_dialer_and_flag_nurture(db):
    from src.lending.weekly_scrub import weekly_scrub
    for p in PHONES[:2]:
        _load(db, p)
    remover = Remover()
    result = weekly_scrub(db, scrubber=Scrubber(dnc={PHONES[0]}), max_credits=10, batch_size=10, now=NOW,
                          phones=PHONES[:2], dialer_remover=remover)
    assert not result.aborted and result.blocked == 1
    assert remover.removed == [(PHONES[0], "opt_out")]     # permanent: DNC list, not a temporary hold
    active = db.execute(text("SELECT phone FROM lending.dialer_load_records WHERE active AND phone = ANY(:p)"),
                        {"p": PHONES[:2]}).scalars().all()
    assert active == [PHONES[1]]
    nurture = dict(db.execute(text("SELECT phone, nurture FROM lending.contacts WHERE phone = ANY(:p)"),
                              {"p": PHONES[:2]}).fetchall())
    assert nurture == {PHONES[0]: True, PHONES[1]: False}


def test_a_failed_dialer_removal_is_retried_on_the_next_run_at_no_credit_cost(db):
    """Finding #3: a transient dialer failure on removal must not strand the number —
    the next run has to retry it without spending another Tracerfy credit, because the
    blocking verdict is already known."""
    from src.lending.weekly_scrub import weekly_scrub

    phone = PHONES[0]
    _load(db, phone)
    scrubber = Scrubber(dnc={phone})
    remover = FailOnceRemover()

    first = weekly_scrub(db, scrubber=scrubber, max_credits=10, now=NOW, phones=[phone],
                         dialer_remover=remover)
    assert first.blocked == 1
    assert remover.calls == [phone]
    still_active = db.execute(
        text("SELECT active FROM lending.dialer_load_records WHERE phone = :p"), {"p": phone}
    ).scalar()
    assert still_active is True, "removal failed: the load row must stay active, not close"

    second = weekly_scrub(db, scrubber=scrubber, max_credits=10, now=NOW, phones=[], dialer_remover=remover)
    assert remover.calls == [phone, phone]          # retried
    assert scrubber.calls == [[phone]]              # no second scrub: no credit spent on the retry
    assert second.blocked == 1
    now_active = db.execute(
        text("SELECT active FROM lending.dialer_load_records WHERE phone = :p"), {"p": phone}
    ).scalar()
    assert now_active is False, "the retried removal succeeded: the load row must close"


def test_filter_loadable_flags_nurture_for_dnc_blocked_numbers(db):
    from src.lending.compliance import filter_loadable
    filter_loadable([{"phone": PHONES[2]}], db, now=NOW, scrubber=Scrubber(dnc={PHONES[2]}))
    assert db.execute(text("SELECT nurture FROM lending.contacts WHERE phone = :p"), {"p": PHONES[2]}).scalar() is True


def test_dry_run_reports_the_count_and_spends_nothing(monkeypatch, caplog):
    from src.lending import weekly_scrub as ws
    monkeypatch.setattr(ws, "stale_loaded_phones", lambda db, now=None: PHONES[:3])
    monkeypatch.setattr(ws, "blocked_loaded_phones", lambda db: [])
    monkeypatch.setattr(ws, "tracerfy_scrub", lambda phones: pytest.fail("dry run must not scrub"))
    caplog.set_level("INFO")
    assert ws.main(["--dry-run"]) == 0
    assert "3 number(s) would be scrubbed" in caplog.text


def test_loaded_phones_are_normalized_on_read_and_write(db):
    from src.lending.weekly_scrub import _close_load_rows, stale_loaded_phones
    _load(db, "(813) 555-8406")
    assert "+18135558406" in stale_loaded_phones(db, now=NOW)
    _load(db, "+18135558407")
    _close_load_rows(db, ["813-555-8407"])
    assert db.execute(text("SELECT active FROM lending.dialer_load_records WHERE phone = '+18135558407'")).scalar() is False


def _dnc_scrub(db, phone, age_days=1):
    db.execute(text(
        "INSERT INTO lending.dnc_scrubs (phone, national_dnc, litigator, state_dnc, checked_at) "
        "VALUES (:p, true, false, false, :at)"), {"p": phone, "at": NOW - timedelta(days=age_days)})


def _active(db, phone):
    return db.execute(text("SELECT active FROM lending.dialer_load_records WHERE phone = :p"), {"p": phone}).scalar()


class FailingRemover:
    def __call__(self, phone, *, reason):
        raise RuntimeError("dialer down")


def test_a_flagged_number_whose_removal_failed_is_retried_on_the_next_run_without_a_new_scrub(db):
    from src.lending.weekly_scrub import weekly_scrub
    _load(db, PHONES[0])
    first = weekly_scrub(db, scrubber=Scrubber(dnc={PHONES[0]}), max_credits=10, now=NOW, phones=[PHONES[0]],
                         dialer_remover=FailingRemover())
    assert first.removal_failed == 1 and _active(db, PHONES[0]) is True
    _dnc_scrub(db, PHONES[0])                        # the verdict is stored and fresh: no longer "due"
    scrubber, remover = Scrubber(), Remover()
    second = weekly_scrub(db, scrubber=scrubber, max_credits=10, now=NOW, dialer_remover=remover)
    assert scrubber.calls == [] and second.credits_used == 0     # no credit spent on the retry
    assert remover.removed == [(PHONES[0], "opt_out")] and second.removal_failed == 0
    assert _active(db, PHONES[0]) is False


def test_a_flagged_number_with_no_dialer_configured_counts_as_a_failed_removal(db, monkeypatch):
    from src.lending import compliance
    from src.lending.weekly_scrub import weekly_scrub
    monkeypatch.setattr(compliance, "_default_dialer_remover", lambda: None)
    _load(db, PHONES[1])
    result = weekly_scrub(db, scrubber=Scrubber(dnc={PHONES[1]}), max_credits=10, now=NOW, phones=[PHONES[1]])
    assert result.blocked == 1 and result.removal_failed == 1 and _active(db, PHONES[1]) is True


def test_the_run_exits_non_zero_while_a_flagged_number_is_still_in_the_dialer(monkeypatch):
    from src.lending import weekly_scrub as ws
    monkeypatch.setattr(ws, "weekly_scrub", lambda *a, **k: ws.WeeklyScrubResult(removal_failed=1))
    assert ws.main(["--max-credits", "5"]) == ws.EXIT_REMOVAL_FAILED
