"""A2 coverage check (spec §12): "100% of numbers scrubbed against Tracerfy DNC and
internal suppression tables." For a set of loaded numbers, list every number that
would violate A2. The acceptance load must return an empty list.

Read-only: never calls Tracerfy. Runs in a rolled-back transaction.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

pytestmark = pytest.mark.skipif(
    not os.environ.get("DATABASE_URL"), reason="requires a live Postgres DATABASE_URL"
)

NOW = datetime(2026, 9, 29, 12, 0, tzinfo=timezone.utc)
P_OK, P_STALE, P_NEVER, P_SUPP, P_FA_STOP, P_DNC, P_LENDING_OK = (
    f"+1813555{n}" for n in (6001, 6002, 6003, 6004, 6005, 6006, 6007)
)


@pytest.fixture
def db():
    engine = create_engine(os.environ["DATABASE_URL"])
    conn = engine.connect()
    tx = conn.begin()
    session = Session(bind=conn)
    yield session
    session.close()
    tx.rollback()
    conn.close()
    engine.dispose()


def _fa_scrub(db, phone, *, age_days, dnc=False, lit=False, state_dnc=False):
    db.execute(
        text("INSERT INTO dnc_phone_checks (phone, national_dnc, litigator, checked_at, source, raw_result) "
             "VALUES (:p, :d, :l, :at, 'test', CAST(:raw AS jsonb))"),
        {"p": phone, "d": dnc, "l": lit, "at": NOW - timedelta(days=age_days),
         "raw": '{"state_dnc": "%s"}' % ("Y" if state_dnc else "N")},
    )


def _gaps(db, phones):
    from src.lending.compliance import coverage_gaps
    return {g.phone: g.reason for g in coverage_gaps(db, phones, now=NOW)}


def test_clean_fresh_numbers_have_no_gaps(db):
    _fa_scrub(db, P_OK, age_days=30)
    db.execute(text("INSERT INTO lending.dnc_scrubs (phone, national_dnc, litigator, state_dnc, checked_at) "
                    "VALUES (:p, false, false, false, :at)"), {"p": P_LENDING_OK, "at": NOW - timedelta(days=1)})
    assert _gaps(db, [P_OK, P_LENDING_OK]) == {}


def test_every_kind_of_violation_is_listed_with_its_reason(db):
    _fa_scrub(db, P_STALE, age_days=32)
    _fa_scrub(db, P_SUPP, age_days=1)
    _fa_scrub(db, P_FA_STOP, age_days=1)
    _fa_scrub(db, P_DNC, age_days=1, state_dnc=True)
    db.execute(text("INSERT INTO lending.suppression_list (phone, reason, source_channel) "
                    "VALUES (:p, 'OPT_OUT', 'test')"), {"p": P_SUPP})
    db.execute(text("INSERT INTO sms_opt_outs (phone, keyword_used, source, opted_out_at) "
                    "VALUES (:p, 'STOP', 'inbound_sms', now())"), {"p": P_FA_STOP})

    assert _gaps(db, [P_STALE, P_NEVER, P_SUPP, P_FA_STOP, P_DNC]) == {
        P_STALE: "NO_FRESH_SCRUB",
        P_NEVER: "NO_FRESH_SCRUB",
        P_SUPP: "SUPPRESSED",
        P_FA_STOP: "SUPPRESSED",
        P_DNC: "STATE_DNC",
    }


def test_freshest_scrub_wins_across_fa_and_lending(db):
    _fa_scrub(db, P_STALE, age_days=60)
    db.execute(text("INSERT INTO lending.dnc_scrubs (phone, national_dnc, litigator, state_dnc, checked_at) "
                    "VALUES (:p, false, false, false, :at)"), {"p": P_STALE, "at": NOW - timedelta(days=2)})
    assert _gaps(db, [P_STALE]) == {}


def test_raw_format_input_is_normalized(db):
    _fa_scrub(db, P_OK, age_days=1)
    assert _gaps(db, ["(813) 555-6001"]) == {}
    assert _gaps(db, ["123"]) == {"123": "INVALID_PHONE"}


def test_report_summarises_counts_for_the_evidence_file(db):
    from src.lending.compliance import a2_coverage_report

    _fa_scrub(db, P_OK, age_days=1)
    report = a2_coverage_report(db, [P_OK, P_NEVER, P_OK], now=NOW)
    assert report.checked == 2          # distinct numbers
    assert report.passed is False
    assert report.gap_counts == {"NO_FRESH_SCRUB": 1}


def test_coverage_check_never_calls_tracerfy(db, monkeypatch):
    from src.lending import compliance

    def boom(*a, **k):
        raise AssertionError("coverage check must be read-only")
    monkeypatch.setattr(compliance, "tracerfy_scrub", boom)
    compliance.coverage_gaps(db, [P_NEVER], now=NOW)
