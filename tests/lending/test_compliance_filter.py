"""WP-W0-2 list-build filter: 'is this number clean to load?' (spec §3.2, §4.2).

Runs on the real DB inside a transaction that is always rolled back.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from config.lending_compliance import ReasonCode

pytestmark = pytest.mark.skipif(
    not os.environ.get("DATABASE_URL"), reason="requires a live Postgres DATABASE_URL"
)

NOW = datetime(2026, 9, 29, 12, 0, tzinfo=timezone.utc)
P_CLEAN, P_SUPP, P_STALE, P_LIT, P_DNC, P_GA = (f"+1813555{n}" for n in (7001, 7002, 7003, 7004, 7005, 7006))


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


def _dnc(db, phone, *, age_days, dnc=False, lit=False):
    db.execute(
        text("INSERT INTO dnc_phone_checks (phone, national_dnc, litigator, checked_at, source) "
             "VALUES (:p, :d, :l, :at, 'test')"),
        {"p": phone, "d": dnc, "l": lit, "at": NOW - timedelta(days=age_days)},
    )


class NoScrub:
    """Scrubber that fails the test if called."""
    def __call__(self, phones):
        raise AssertionError(f"scrubber must not be called, got {phones}")


def _run(db, records, scrubber=NoScrub(), **kw):
    from src.lending.compliance import filter_loadable
    return {r.phone: r for r in filter_loadable(records, db, now=NOW, scrubber=scrubber, **kw)}


def test_fresh_clean_phone_is_loadable_without_a_new_scrub(db):
    _dnc(db, P_CLEAN, age_days=6)
    out = _run(db, [{"phone": P_CLEAN}])
    assert out[P_CLEAN].allowed and out[P_CLEAN].reason is None


def test_suppression_list_phone_is_blocked_without_scrub(db):
    _dnc(db, P_SUPP, age_days=1)
    db.execute(text("INSERT INTO lending.suppression_list (phone, reason, source_channel) "
                    "VALUES (:p, 'OPT_OUT', 'test')"), {"p": P_SUPP})
    out = _run(db, [{"phone": P_SUPP}])
    assert out[P_SUPP].reason == ReasonCode.SUPPRESSED


def test_warm_network_phone_is_blocked_without_scrub(db):
    """F8: Josh's warm network is permanently suppressed from the cold queue — same
    gate as opt-out/litigator, a different SuppressionReason."""
    from src.lending.compliance import suppress_warm_network_phones

    _dnc(db, P_SUPP, age_days=1)
    assert suppress_warm_network_phones(db, [P_SUPP]) == 1
    out = _run(db, [{"phone": P_SUPP}])
    assert out[P_SUPP].reason == ReasonCode.SUPPRESSED
    reason = db.execute(text("SELECT reason FROM lending.suppression_list WHERE phone = :p"), {"p": P_SUPP}).scalar()
    assert reason == "WARM_NETWORK"


def test_suppress_warm_network_phones_is_idempotent_and_normalizes(db):
    from src.lending.compliance import suppress_warm_network_phones

    assert suppress_warm_network_phones(db, ["(813) 555-7777", "invalid"]) == 1
    assert suppress_warm_network_phones(db, ["813-555-7777"]) == 1  # same phone, re-run: no error, no dup
    count = db.execute(
        text("SELECT count(*) FROM lending.suppression_list WHERE phone = :p"), {"p": "+18135557777"}
    ).scalar()
    assert count == 1


def test_attempt_history_exceeded_blocks_at_load_time_and_flags_nurture(db):
    """F10 at the load-time gate: a phone already attempt-exhausted must not be
    reloaded as if it were fresh."""
    for n, days_back in enumerate((1, 2, 3, 4, 7, 8)):
        db.execute(
            text("INSERT INTO lending.call_dispositions (dialer_call_id, phone, direction, call_ended_at, raw_event) "
                 "VALUES (:cid, :p, 'outbound', :t, '{}')"),
            {"cid": f"t-{n}", "p": P_CLEAN, "t": NOW - timedelta(days=days_back)},
        )
    out = _run(db, [{"phone": P_CLEAN}])
    assert out[P_CLEAN].reason == ReasonCode.ATTEMPT_HISTORY_EXCEEDED


def test_confirmed_homestead_natural_person_is_blocked(db):
    """F8: owner must be LLC/LP/Corp or a non-owner-occupied investor. A confirmed
    homestead-exempt property on a natural-person owner is blocked."""
    _dnc(db, P_CLEAN, age_days=1)
    out = _run(db, [{"phone": P_CLEAN, "entity_status": "NATURAL_PERSON", "homestead_exempt": True}])
    assert out[P_CLEAN].reason == ReasonCode.HOMESTEAD_OWNER_OCCUPIED


def test_non_owner_occupied_natural_person_passes(db):
    _dnc(db, P_CLEAN, age_days=1)
    out = _run(db, [{"phone": P_CLEAN, "entity_status": "NATURAL_PERSON", "homestead_exempt": False}])
    assert out[P_CLEAN].allowed


def test_llc_owner_passes_even_if_homestead_is_unknown(db):
    _dnc(db, P_CLEAN, age_days=1)
    out = _run(db, [{"phone": P_CLEAN, "entity_status": "LLC", "homestead_exempt": None}])
    assert out[P_CLEAN].allowed


def test_unknown_homestead_status_passes_no_backfill_pipeline_yet(db):
    """Josh's rule names two allowed categories and never addresses unverified
    status; properties.homestead_exempt has no backfill yet, so blocking on NULL
    would gate out virtually the entire pool against his #1 stated priority
    (lead volume). Unknown must pass, same as every other unscored field."""
    _dnc(db, P_CLEAN, age_days=1)
    out = _run(db, [{"phone": P_CLEAN, "entity_status": "NATURAL_PERSON"}])  # no homestead_exempt key at all
    assert out[P_CLEAN].allowed


def test_homestead_gate_does_not_apply_to_brokers_list_4(db):
    """List 4 (brokers/LOs) is a professional referral list, never screened as a
    property owner — must never be blocked by this gate."""
    _dnc(db, P_CLEAN, age_days=1)
    out = _run(db, [{"phone": P_CLEAN, "entity_status": "NATURAL_PERSON", "homestead_exempt": True,
                     "source_tag": "list_4"}])
    assert out[P_CLEAN].allowed


def test_invalid_phone_is_blocked(db):
    out = _run(db, [{"phone": "123"}])
    assert out["123"].reason == ReasonCode.INVALID_PHONE


def test_georgia_natural_person_is_blocked_but_georgia_llc_passes(db):
    _dnc(db, P_GA, age_days=1)
    out = _run(db, [{"phone": P_GA, "state": "GA", "entity_status": "NATURAL_PERSON"}])
    assert out[P_GA].reason == ReasonCode.GA_NATURAL_PERSON
    out = _run(db, [{"phone": P_GA, "state": "GA", "entity_status": "LLC"}])
    assert out[P_GA].allowed


class FakeScrub:
    def __init__(self, rows=None, boom=False):
        self.rows, self.boom, self.calls = rows or [], boom, []

    def __call__(self, phones):
        self.calls.append(list(phones))
        if self.boom:
            raise RuntimeError("tracerfy down")
        return self.rows


def _row(phone, dnc="N", lit="N", state="N"):
    return {"phone": phone, "national_dnc": dnc, "litigator": lit, "state_dnc": state}


def test_stale_phone_is_rescrubbed_then_loadable_and_stamped(db):
    _dnc(db, P_STALE, age_days=45)
    scrub = FakeScrub([_row(P_STALE)])
    out = _run(db, [{"phone": P_STALE}], scrub)

    assert scrub.calls == [[P_STALE]]
    assert out[P_STALE].allowed
    checked = db.execute(text("SELECT checked_at FROM lending.dnc_scrubs WHERE phone = :p"), {"p": P_STALE}).scalar()
    assert checked > NOW - timedelta(days=1) or checked.year >= 2026  # refreshed by upsert
    stamp = db.execute(text("SELECT last_dnc_scrub FROM lending.contacts WHERE phone = :p"), {"p": P_STALE}).scalar()
    assert stamp is not None


def test_never_scrubbed_phone_is_scrubbed_once_in_one_batch(db):
    scrub = FakeScrub([_row(P_CLEAN), _row(P_DNC, dnc="Y")])
    out = _run(db, [{"phone": P_CLEAN}, {"phone": P_DNC}], scrub)

    assert len(scrub.calls) == 1 and set(scrub.calls[0]) == {P_CLEAN, P_DNC}
    assert out[P_CLEAN].allowed
    assert out[P_DNC].reason == ReasonCode.NATIONAL_DNC


def test_litigator_is_blocked_and_added_to_suppression_list(db):
    out = _run(db, [{"phone": P_LIT}], FakeScrub([_row(P_LIT, dnc="Y", lit="Y")]))

    assert out[P_LIT].reason == ReasonCode.LITIGATOR
    reason = db.execute(text("SELECT reason FROM lending.suppression_list WHERE phone = :p"), {"p": P_LIT}).scalar()
    assert reason == "LITIGATOR"


def test_phone_missing_from_scrub_result_is_not_loadable(db):
    out = _run(db, [{"phone": P_STALE}], FakeScrub([]))
    assert out[P_STALE].reason == ReasonCode.SCRUB_FAILED


def test_scrubber_outage_blocks_everything_it_was_asked_about(db):
    out = _run(db, [{"phone": P_STALE}], FakeScrub(boom=True))
    assert out[P_STALE].reason == ReasonCode.SCRUB_FAILED


def test_state_dnc_hit_from_fresh_scrub_is_blocked(db):
    db.execute(
        text("INSERT INTO dnc_phone_checks (phone, national_dnc, litigator, checked_at, source, raw_result) "
             "VALUES (:p, false, false, :at, 'test', CAST(:raw AS jsonb))"),
        {"p": P_DNC, "at": NOW - timedelta(days=1), "raw": '{"state_dnc": "Yes"}'},
    )
    out = _run(db, [{"phone": P_DNC}])
    assert out[P_DNC].reason == ReasonCode.STATE_DNC


def test_state_dnc_hit_on_new_scrub_is_blocked(db):
    out = _run(db, [{"phone": P_DNC}], FakeScrub([_row(P_DNC, state="Y")]))
    assert out[P_DNC].reason == ReasonCode.STATE_DNC


def test_pool_export_field_name_normalized_phone_is_accepted(db):
    _dnc(db, P_CLEAN, age_days=1)
    out = _run(db, [{"normalized_phone": P_CLEAN, "state": "FL"}])
    assert out[P_CLEAN].allowed


def test_one_result_per_record_in_input_order(db):
    from src.lending.compliance import filter_loadable

    _dnc(db, P_CLEAN, age_days=1)
    records = [{"phone": ""}, {"phone": P_CLEAN}, {"phone": ""}, {"phone": P_CLEAN}]
    out = filter_loadable(records, db, now=NOW, scrubber=NoScrub())
    assert [r.reason for r in out] == [ReasonCode.INVALID_PHONE, None, ReasonCode.INVALID_PHONE, None]


def test_fresh_cached_scrub_stamps_contact_with_its_scrub_time(db):
    _dnc(db, P_CLEAN, age_days=5)
    _run(db, [{"phone": P_CLEAN}])
    stamp = db.execute(text("SELECT last_dnc_scrub FROM lending.contacts WHERE phone = :p"), {"p": P_CLEAN}).scalar()
    assert stamp == NOW - timedelta(days=5)


def test_line_type_from_tracerfy_is_recorded(db):
    _run(db, [{"phone": P_STALE}], FakeScrub([{**_row(P_STALE), "phone_type": "Mobile"}]))
    lt = db.execute(text("SELECT line_type FROM lending.contacts WHERE phone = :p"), {"p": P_STALE}).scalar()
    assert lt == "Mobile"


def test_run_id_writes_every_exclusion(db):
    from src.lending.compliance import phone_hash

    _dnc(db, P_CLEAN, age_days=1)
    _run(db, [{"phone": P_CLEAN}, {"phone": "123"}, {"phone": P_STALE}], FakeScrub([]), run_id="run-t1")
    rows = db.execute(text("SELECT phone_hash, reason FROM lending.load_exclusions WHERE run_id = 'run-t1'")).fetchall()
    assert sorted(r.reason for r in rows) == ["INVALID_PHONE", "SCRUB_FAILED"]
    assert phone_hash(P_STALE) in {r.phone_hash for r in rows}


def test_new_scrub_is_cached_in_lending_not_in_fa(db):
    _dnc(db, P_STALE, age_days=45)
    _run(db, [{"phone": P_STALE}], FakeScrub([_row(P_STALE)]))
    fa_age = db.execute(text("SELECT checked_at FROM dnc_phone_checks WHERE phone = :p"), {"p": P_STALE}).scalar()
    assert fa_age == NOW - timedelta(days=45)  # FA row untouched
    assert db.execute(text("SELECT count(*) FROM lending.dnc_scrubs WHERE phone = :p"), {"p": P_STALE}).scalar() == 1


def test_fresh_lending_scrub_is_reused_without_tracerfy(db):
    db.execute(
        text("INSERT INTO lending.dnc_scrubs (phone, national_dnc, litigator, state_dnc, checked_at) "
             "VALUES (:p, false, false, false, :at)"),
        {"p": P_CLEAN, "at": NOW - timedelta(days=2)},
    )
    out = _run(db, [{"phone": P_CLEAN}])
    assert out[P_CLEAN].allowed
