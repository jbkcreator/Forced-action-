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


def test_only_loaded_numbers_with_a_stale_or_missing_scrub_are_selected(db):
    from src.lending.weekly_scrub import stale_loaded_phones
    for p in PHONES[:4]:
        _load(db, p)
    _load(db, PHONES[4], active=False)         # inactive: not in the dialer
    _scrubbed(db, PHONES[0], age_days=2)       # fresh
    _scrubbed(db, PHONES[1], age_days=9)       # stale
    # PHONES[2], PHONES[3]: never scrubbed
    picked = set(stale_loaded_phones(db, now=NOW)) & set(PHONES)
    assert picked == {PHONES[1], PHONES[2], PHONES[3]}


def test_credit_cap_aborts_before_a_batch_that_would_exceed_it(db):
    from src.lending.weekly_scrub import weekly_scrub
    scrubber = Scrubber()
    result = weekly_scrub(db, scrubber=scrubber, max_credits=3, batch_size=2, now=NOW, phones=PHONES[:5],
                          dialer_remover=Remover())
    assert scrubber.calls == [PHONES[:2]]      # 2nd batch would reach 4 > 3
    assert result.aborted and result.credits_used == 2 and result.scrubbed == 2


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


def test_filter_loadable_flags_nurture_for_dnc_blocked_numbers(db):
    from src.lending.compliance import filter_loadable
    filter_loadable([{"phone": PHONES[2]}], db, now=NOW, scrubber=Scrubber(dnc={PHONES[2]}))
    assert db.execute(text("SELECT nurture FROM lending.contacts WHERE phone = :p"), {"p": PHONES[2]}).scalar() is True
