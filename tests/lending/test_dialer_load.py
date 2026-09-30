"""Dialer load: compliance filter -> Backflip check -> dialer -> load records.

Runs on the real DB inside a transaction that is always rolled back; the load
table is created inside that transaction, so nothing persists. The dialer is
a fake that records calls; the Backflip snapshot is a controlled index.
"""
from __future__ import annotations

import json
import os
from dataclasses import replace
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from src.lending import dialer_load
from src.lending.backflip_conflict import BackflipIdentifierIndex, hash_phone
from src.lending.dialer_load import LoadRefused, run_dialer_load
from src.lending.models import LendingDialerLoadRecord
from src.lending.dialer_client import ContactUpsertResult, DialerRequestError

pytestmark = pytest.mark.skipif(
    not os.environ.get("DATABASE_URL"), reason="requires a live Postgres DATABASE_URL"
)

P1, P2, P3, P4 = (f"+1813555{n}" for n in (8101, 8102, 8103, 8104))
TAGS = {"builders": "DESK_CONSTRUCTION", "wholesalers": "DESK_CAPITAL_LOOP"}
EMPTY_INDEX = BackflipIdentifierIndex(block_reason=None)


class FakeDialer:
    def __init__(self, fail_phones=(), missing_ids=()):
        self.upserts: list[str] = []
        self.updates: list[int] = []
        self._next_id = 1000
        self._fail = set(fail_phones)
        self._missing = set(missing_ids)

    def upsert_contact(self, phone, display, email):
        if phone in self._fail:
            raise DialerRequestError(500)
        self.upserts.append(phone)
        self._next_id += 1
        return ContactUpsertResult(contact_id=self._next_id, created=True)

    def update_contact(self, contact_id, display, email):
        if contact_id in self._missing:
            raise DialerRequestError(404)
        self.updates.append(contact_id)


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


def _fresh_scrub(db, *phones):
    for phone in phones:
        db.execute(text(
            "INSERT INTO dnc_phone_checks (phone, national_dnc, litigator, checked_at, source) "
            "VALUES (:p, false, false, now() - interval '1 day', 'test')"
        ), {"p": phone})


def _record(ref, phone, pool="builders", **extra):
    return {"source_record_ref": ref, "phone": phone, "pool": pool, "borrower_name": "Test Borrower",
            "entity_name": f"{ref.upper()} LLC", "property_address": "1 Test St",
            "parcel_id": f"PARCEL-{ref}", "estimated_loan_value": "100000", **extra}


def _run(db, records, *, index=EMPTY_INDEX, dry_run=False, dialer=None, tags=TAGS, run_id="run-1"):
    dialer = dialer if dialer is not None else FakeDialer()
    with patch.object(dialer_load, "load_backflip_identifier_index", return_value=index):
        report = run_dialer_load(
            records, db, run_id=run_id, dry_run=dry_run,
            scrubber=None if dry_run else (lambda phones: []),
            dialer=None if dry_run else dialer,
            campaign_tags=tags, commit=db.flush,
        )
    return report, dialer


def _load_rows(db, run_id=None):
    sql = "SELECT phone, run_id, pool, campaign_tag, active, aircall_contact_id, deactivation_reason " \
          "FROM lending.dialer_load_records"
    params = {}
    if run_id:
        sql += " WHERE run_id = :r"
        params["r"] = run_id
    return db.execute(text(sql + " ORDER BY id"), params).fetchall()


def _exclusions(db, run_id="run-1"):
    return db.execute(text(
        "SELECT phone_hash, reason, detail FROM lending.load_exclusions WHERE run_id = :r ORDER BY id"
    ), {"r": run_id}).fetchall()


class TestDryRun:
    def test_reports_without_calling_the_dialer_or_writing(self, db):
        _fresh_scrub(db, P1)
        records = [_record("a", P1), _record("b", P2)]
        report, _ = _run(db, records, dry_run=True)
        assert report.loadable == 1
        assert report.excluded_by_reason == {"NEEDS_SCRUB": 1}
        assert report.loadable_by_pool == {"builders": 1}
        assert _load_rows(db) == []
        assert _exclusions(db) == []

    def test_unscrubbed_backflip_conflict_is_reported_as_conflict(self, db):
        index = replace(EMPTY_INDEX, parcel_ids=frozenset({"PARCELB"}))
        report, _ = _run(db, [_record("b", P2)], index=index, dry_run=True)
        assert report.excluded_by_reason == {"BACKFLIP_CONFLICT": 1}

    def test_reports_open_decisions_instead_of_failing(self, db):
        _fresh_scrub(db, P1)
        records = [_record("a", P1, pool="brokers"), _record("a2", P1, pool="brokers")]
        report, _ = _run(db, records, dry_run=True)
        assert report.unmapped_pools == ["brokers"]
        assert report.duplicate_phones == 1


class TestLiveLoad:
    def test_loads_clean_records_and_stores_them(self, db):
        _fresh_scrub(db, P1, P2)
        report, dialer = _run(db, [_record("a", P1), _record("b", P2, pool="wholesalers")])
        assert report.loaded == 2 and report.created == 2
        assert dialer.upserts == [P1, P2]
        rows = _load_rows(db)
        assert [(r.phone, r.campaign_tag, r.active) for r in rows] == [
            (P1, "DESK_CONSTRUCTION", True), (P2, "DESK_CAPITAL_LOOP", True)]
        assert all(r.aircall_contact_id for r in rows)

    def test_backflip_conflict_is_excluded_with_matched_criteria(self, db):
        _fresh_scrub(db, P1, P2)
        index = replace(EMPTY_INDEX, parcel_ids=frozenset({"PARCELA"}))
        report, dialer = _run(db, [_record("a", P1), _record("b", P2)], index=index)
        assert dialer.upserts == [P2]
        assert report.excluded_by_reason == {"BACKFLIP_CONFLICT": 1}
        (row,) = _exclusions(db)
        assert row.phone_hash == hash_phone(P1)
        assert row.reason == "BACKFLIP_CONFLICT"
        assert row.detail == {"matched_criteria": ["parcel_id"]}

    def test_block_on_one_record_blocks_every_record_with_that_phone(self, db):
        _fresh_scrub(db, P1)
        index = replace(EMPTY_INDEX, parcel_ids=frozenset({"PARCELA"}))
        report, dialer = _run(db, [_record("a", P1), _record("a2", P1)], index=index)
        assert dialer.upserts == []
        assert report.excluded_by_reason == {"BACKFLIP_CONFLICT": 2}
        details = [r.detail for r in _exclusions(db)]
        assert {"matched_criteria": ["parcel_id"]} in details
        assert {"blocked_via": "a"} in details

    def test_compliance_blocks_are_recorded_by_the_filter(self, db):
        _fresh_scrub(db, P1)
        db.execute(text("INSERT INTO lending.suppression_list (phone, reason, source_channel) "
                        "VALUES (:p, 'OPT_OUT', 'test')"), {"p": P2})
        report, dialer = _run(db, [_record("a", P1), _record("b", P2)])
        assert dialer.upserts == [P1]
        assert report.excluded_by_reason == {"SUPPRESSED": 1}
        assert [r.reason for r in _exclusions(db)] == ["SUPPRESSED"]

    def test_refuses_without_campaign_tag(self, db):
        _fresh_scrub(db, P1)
        with pytest.raises(LoadRefused, match="brokers"):
            _run(db, [_record("a", P1, pool="brokers")])
        assert _load_rows(db) == []

    def test_refuses_when_a_phone_would_load_twice(self, db):
        _fresh_scrub(db, P1)
        dialer = FakeDialer()
        with pytest.raises(LoadRefused, match="more than one record"):
            _run(db, [_record("a", P1), _record("a2", P1)], dialer=dialer)
        assert dialer.upserts == []

    def test_one_dialer_failure_does_not_stop_the_rest(self, db):
        _fresh_scrub(db, P1, P2)
        report, _ = _run(db, [_record("a", P1), _record("b", P2)], dialer=FakeDialer(fail_phones={P1}))
        assert report.loaded == 1
        assert report.failed == [{"record_ref": "a", "error": "DialerRequestError", "status": 500}]
        assert [r.phone for r in _load_rows(db)] == [P2]

    def test_live_load_needs_scrubber_and_dialer(self, db):
        with pytest.raises(ValueError):
            run_dialer_load([], db, run_id="r", dry_run=False)

    def test_report_holds_no_raw_phone(self, db):
        _fresh_scrub(db, P1)
        report, _ = _run(db, [_record("a", P1), _record("b", "bad-phone")])
        assert P1 not in json.dumps(report.as_dict())


class TestReload:
    def test_reload_updates_by_stored_contact_and_supersedes_old_row(self, db):
        _fresh_scrub(db, P1)
        _, first = _run(db, [_record("a", P1)], run_id="run-1")
        report, second = _run(db, [_record("a", P1)], run_id="run-2")
        assert second.upserts == []
        assert len(second.updates) == 1
        assert report.updated == 1 and report.created == 0
        rows = _load_rows(db)
        assert [(r.run_id, r.active, r.deactivation_reason) for r in rows] == [
            ("run-1", False, "superseded"), ("run-2", True, None)]
        assert rows[0].aircall_contact_id == rows[1].aircall_contact_id

    def test_contact_deleted_in_the_dialer_falls_back_to_upsert(self, db):
        _fresh_scrub(db, P1)
        _run(db, [_record("a", P1)], run_id="run-1")
        stored_id = _load_rows(db)[0].aircall_contact_id
        report, second = _run(db, [_record("a", P1)], run_id="run-2",
                              dialer=FakeDialer(missing_ids={stored_id}))
        assert second.upserts == [P1]
        assert report.loaded == 1

    def test_active_contacts_missing_from_the_run_are_counted_not_removed(self, db):
        _fresh_scrub(db, P1, P2)
        _run(db, [_record("a", P1), _record("b", P2)], run_id="run-1")
        report, _ = _run(db, [_record("a", P1)], run_id="run-2")
        assert report.active_not_in_run == 1
        assert [r.phone for r in _load_rows(db, "run-1") if r.active] == [P2]
