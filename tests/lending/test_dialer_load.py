"""Dialer load: compliance filter -> Backflip check -> Aircall -> load records.

Runs on the real DB inside a transaction that is always rolled back; the load
table is created inside that transaction, so nothing persists. Aircall is a
fake that records calls; the Backflip snapshot is a controlled index.
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
from src.lending.models import LendingDialerLoadRecord, LendingDialerUnconfirmedCreate
from src.lending.dialer_port import ContactFieldsNotSet, ContactUpsertResult, DialerRequestError

pytestmark = pytest.mark.skipif(
    not os.environ.get("DATABASE_URL"), reason="requires a live Postgres DATABASE_URL"
)

P1, P2, P3, P4 = (f"+1813555{n}" for n in (8101, 8102, 8103, 8104))
TAGS = {"builders": "DESK_CONSTRUCTION", "wholesalers": "DESK_CAPITAL_LOOP"}
EMPTY_INDEX = BackflipIdentifierIndex(block_reason=None)


class FakeAircall:
    def __init__(self, fail_phones=(), missing_ids=(), fields_fail_phones=(), fail_remove_phones=(),
                 undecided_remove_phones=()):
        self.upserts: list[str] = []
        self.campaigns: list = []
        self.updates: list[int] = []
        self.removed: list[tuple[str, str]] = []
        self._next_id = 1000
        self._fail = set(fail_phones)
        self._missing = set(missing_ids)
        self._fail_remove = set(fail_remove_phones)
        self._undecided_remove = set(undecided_remove_phones)
        self._fields_fail = set(fields_fail_phones)

    def upsert_contact(self, phone, fields, *, campaign=None, vendor_contact_id=None):
        self.campaigns.append(campaign)
        if phone in self._fail:
            raise DialerRequestError("POST /contacts", status=500)
        self.upserts.append(phone)
        self._next_id += 1
        if phone in self._fields_fail:
            raise ContactFieldsNotSet(self._next_id, DialerRequestError("PUT", status=500))
        return ContactUpsertResult(contact_id=self._next_id, created=True)

    def update_contact(self, contact_id, fields, *, phone=None, vendor_contact_id=None):
        if contact_id in self._missing:
            raise DialerRequestError("POST /contacts/id", status=404)
        self.updates.append(contact_id)
        self.update_vendor_ids = getattr(self, "update_vendor_ids", []) + [vendor_contact_id]
        return {"id": contact_id}

    def remove(self, phone, *, reason):
        if phone in self._undecided_remove:
            from src.lending.dialer_port import UnconfirmedCapability
            raise UnconfirmedCapability("campaign_remove not confirmed")
        if phone in self._fail_remove:
            raise DialerRequestError("POST /campaign/remove", status=500)
        self.removed.append((phone, reason))


@pytest.fixture
def db():
    engine = create_engine(os.environ["DATABASE_URL"])
    conn = engine.connect()
    tx = conn.begin()
    LendingDialerLoadRecord.__table__.create(conn, checkfirst=True)
    LendingDialerUnconfirmedCreate.__table__.create(conn, checkfirst=True)
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


# 12:00 ET: inside the calling window, so the call-time rail never interferes by accident.
NOON_ET = __import__("datetime").datetime(2026, 9, 29, 16, 0, tzinfo=__import__("datetime").timezone.utc)


def _run(db, records, *, index=EMPTY_INDEX, dry_run=False, aircall=None, tags=TAGS, run_id="run-1", now=NOON_ET,
         backflip_check=True):
    aircall = aircall if aircall is not None else FakeAircall()
    with patch.object(dialer_load, "load_backflip_identifier_index", return_value=index):
        report = run_dialer_load(
            records, db, run_id=run_id, dry_run=dry_run,
            scrubber=None if dry_run else (lambda phones: []),
            dialer=None if dry_run else aircall,
            campaign_tags=tags, commit=db.flush, now=now, backflip_check=backflip_check,
        )
    return report, aircall


def _load_rows(db, run_id=None):
    sql = "SELECT phone, run_id, pool, campaign_tag, active, dialer_contact_id, deactivation_reason " \
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
    def test_reports_without_calling_aircall_or_writing(self, db):
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


class TestBackflipCheckOff:
    def test_stale_backflip_feed_does_not_block_the_load_when_the_check_is_off(self, db):
        _fresh_scrub(db, P1, P2)
        stale = BackflipIdentifierIndex(block_reason="BACKFLIP_FEED_STALE")
        report, aircall = _run(db, [_record("a", P1), _record("b", P2)], index=stale, backflip_check=False)
        assert aircall.upserts == [P1, P2]
        assert "BACKFLIP_FEED_STALE" not in report.excluded_by_reason
        assert report.as_dict()["backflip_check"] is False


class TestLiveLoad:
    def test_loads_clean_records_and_stores_them(self, db):
        _fresh_scrub(db, P1, P2)
        report, aircall = _run(db, [_record("a", P1), _record("b", P2, pool="wholesalers")])
        assert report.loaded == 2 and report.created == 2
        assert aircall.upserts == [P1, P2]
        rows = _load_rows(db)
        assert [(r.phone, r.campaign_tag, r.active) for r in rows] == [
            (P1, "DESK_CONSTRUCTION", True), (P2, "DESK_CAPITAL_LOOP", True)]
        assert all(r.dialer_contact_id for r in rows)

    def test_backflip_conflict_is_excluded_with_matched_criteria(self, db):
        _fresh_scrub(db, P1, P2)
        index = replace(EMPTY_INDEX, parcel_ids=frozenset({"PARCELA"}))
        report, aircall = _run(db, [_record("a", P1), _record("b", P2)], index=index)
        assert aircall.upserts == [P2]
        assert report.excluded_by_reason == {"BACKFLIP_CONFLICT": 1}
        (row,) = _exclusions(db)
        assert row.phone_hash == hash_phone(P1)
        assert row.reason == "BACKFLIP_CONFLICT"
        assert row.detail == {"matched_criteria": ["parcel_id"]}

    def test_block_on_one_record_blocks_every_record_with_that_phone(self, db):
        _fresh_scrub(db, P1)
        index = replace(EMPTY_INDEX, parcel_ids=frozenset({"PARCELA"}))
        report, aircall = _run(db, [_record("a", P1), _record("a2", P1)], index=index)
        assert aircall.upserts == []
        assert report.excluded_by_reason == {"BACKFLIP_CONFLICT": 2}
        details = [r.detail for r in _exclusions(db)]
        assert {"matched_criteria": ["parcel_id"]} in details
        assert {"blocked_via": "a"} in details

    def test_compliance_blocks_are_recorded_by_the_filter(self, db):
        _fresh_scrub(db, P1)
        db.execute(text("INSERT INTO lending.suppression_list (phone, reason, source_channel) "
                        "VALUES (:p, 'OPT_OUT', 'test')"), {"p": P2})
        report, aircall = _run(db, [_record("a", P1), _record("b", P2)])
        assert aircall.upserts == [P1]
        assert report.excluded_by_reason == {"SUPPRESSED": 1}
        assert [r.reason for r in _exclusions(db)] == ["SUPPRESSED"]

    def test_refuses_without_campaign_tag(self, db):
        _fresh_scrub(db, P1)
        with pytest.raises(LoadRefused, match="brokers"):
            _run(db, [_record("a", P1, pool="brokers")])
        assert _load_rows(db) == []

    def test_refuses_when_a_phone_would_load_twice(self, db):
        _fresh_scrub(db, P1)
        aircall = FakeAircall()
        with pytest.raises(LoadRefused, match="more than one record"):
            _run(db, [_record("a", P1), _record("a2", P1)], aircall=aircall)
        assert aircall.upserts == []

    def test_a_refused_run_never_pays_for_a_scrub(self, db):
        scrub_calls = []
        records = [_record("a", P1, pool="brokers"), _record("b", P2)]  # P1, P2 have no fresh scrub
        with patch.object(dialer_load, "load_backflip_identifier_index", return_value=EMPTY_INDEX):
            with pytest.raises(LoadRefused, match="brokers"):
                run_dialer_load(records, db, run_id="run-1", dry_run=False,
                                scrubber=lambda phones: scrub_calls.append(phones) or [],
                                dialer=FakeAircall(), campaign_tags=TAGS, commit=db.flush, now=NOON_ET)
        assert scrub_calls == []

    def test_a_create_that_fails_with_a_5xx_is_recorded_as_unconfirmed(self, db):
        _fresh_scrub(db, P1, P2)
        aircall = FakeAircall(fail_phones=[P1])
        real_upsert = aircall.upsert_contact

        def upsert(phone, *args, **kwargs):
            try:
                return real_upsert(phone, *args, **kwargs)
            except DialerRequestError as exc:
                exc.maybe_created = True  # what the BatchDialer adapter sets for a 5xx on the create
                raise

        aircall.upsert_contact = upsert
        report, _ = _run(db, [_record("a", P1), _record("b", P2)], aircall=aircall)
        assert report.loaded == 1
        rows = db.execute(text(
            "SELECT phone, error_status, resolved_at FROM lending.dialer_unconfirmed_creates")).fetchall()
        assert [(r.phone, r.error_status, r.resolved_at) for r in rows] == [(P1, 500, None)]

    def test_a_create_rejected_with_a_4xx_is_not_unconfirmed(self, db):
        _fresh_scrub(db, P1)
        aircall = FakeAircall()
        aircall.upsert_contact = lambda *a, **k: (_ for _ in ()).throw(DialerRequestError("POST", status=422))
        _run(db, [_record("a", P1)], aircall=aircall)
        assert db.execute(text("SELECT count(*) FROM lending.dialer_unconfirmed_creates")).scalar_one() == 0

    def test_a_number_that_opts_out_after_the_gate_is_not_pushed(self, db):
        _fresh_scrub(db, P1, P2)
        aircall = FakeAircall()
        real_upsert = aircall.upsert_contact

        def upsert(phone, *args, **kwargs):
            if phone == P1:  # P2 opts out while P1 is being pushed
                db.execute(text("INSERT INTO lending.suppression_list (phone, reason, source_channel) VALUES (:p, 'OPT_OUT', 'sms')"),
                           {"p": P2})
            return real_upsert(phone, *args, **kwargs)

        aircall.upsert_contact = upsert
        with patch.object(dialer_load, "SUPPRESSION_RECHECK_BATCH", 1):
            report, _ = _run(db, [_record("a", P1), _record("b", P2)], aircall=aircall)
        assert aircall.upserts == [P1]
        assert report.suppressed_mid_run == 1
        assert [r.reason for r in _exclusions(db)] == ["SUPPRESSED"]

    def test_one_aircall_failure_does_not_stop_the_rest(self, db):
        _fresh_scrub(db, P1, P2)
        report, _ = _run(db, [_record("a", P1), _record("b", P2)], aircall=FakeAircall(fail_phones={P1}))
        assert report.loaded == 1
        assert report.failed == [{"record_ref": "a", "error": "DialerRequestError", "status": 500}]
        assert [r.phone for r in _load_rows(db)] == [P2]

    def test_a_crash_mid_load_still_records_the_contacts_already_pushed(self, db):
        """Finding 6: contacts pushed before an unexpected error must have load rows, or a
        later opt-out cannot find and remove them from the dialer."""
        class Crashy(FakeAircall):
            def upsert_contact(self, phone, fields, *, campaign=None, vendor_contact_id=None):
                if phone == P3:
                    raise RuntimeError("worker killed")
                return super().upsert_contact(phone, fields, campaign=campaign, vendor_contact_id=vendor_contact_id)

        _fresh_scrub(db, P1, P2, P3)
        with pytest.raises(RuntimeError):
            _run(db, [_record("a", P1), _record("b", P2), _record("c", P3)], aircall=Crashy())
        assert sorted(r.phone for r in _load_rows(db)) == [P1, P2]

    def test_live_load_needs_scrubber_and_aircall(self, db):
        with pytest.raises(ValueError):
            run_dialer_load([], db, run_id="r", dry_run=False)

    def test_report_holds_no_raw_phone(self, db):
        _fresh_scrub(db, P1)
        report, _ = _run(db, [_record("a", P1), _record("b", "bad-phone")])
        assert P1 not in json.dumps(report.as_dict())


class TestPartialLoad:
    def test_a_contact_left_without_its_fields_is_still_tracked_so_an_opt_out_can_delete_it(self, db):
        _fresh_scrub(db, P1)
        report, _ = _run(db, [_record("a", P1)], aircall=FakeAircall(fields_fail_phones={P1}))
        assert report.loaded == 0 and report.failed[0]["error"] == "ContactFieldsNotSet"
        rows = _load_rows(db)
        assert [(r.phone, r.active, r.dialer_contact_id) for r in rows] == [(P1, True, "1001")]

    def test_the_next_run_finishes_that_contact_instead_of_creating_another(self, db):
        _fresh_scrub(db, P1)
        _run(db, [_record("a", P1)], aircall=FakeAircall(fields_fail_phones={P1}), run_id="run-1")
        report, second = _run(db, [_record("a", P1)], run_id="run-2")
        assert second.upserts == [] and second.updates == ["1001"] and report.updated == 1


class TestReload:
    def test_reload_update_keeps_our_vendor_contact_id(self, db):
        # BatchDialer's PUT replaces every field: without the id our record link is wiped
        _fresh_scrub(db, P1)
        _run(db, [_record("a", P1)], run_id="run-1")
        _, second = _run(db, [_record("a", P1)], run_id="run-2")
        assert second.update_vendor_ids == ["a"]

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
        assert rows[0].dialer_contact_id == rows[1].dialer_contact_id

    def test_a_changed_pool_moves_the_dialer_campaign_not_just_the_fields(self, db):
        """Finding #10: a phone re-loaded under a different pool must leave its old
        campaign and join the new one, not just get an in-place field update that
        leaves BatchDialer dialing the old campaign forever."""
        _fresh_scrub(db, P1)
        _run(db, [_record("a", P1, pool="builders")], run_id="run-1")
        report, aircall = _run(db, [_record("a", P1, pool="wholesalers")], run_id="run-2")
        assert aircall.removed == [(P1, "pool_changed")]
        assert aircall.campaigns[-1] == TAGS["wholesalers"]   # re-added via upsert, not a bare update
        assert report.updated == 0 and report.created == 1
        rows = _load_rows(db)
        assert [(r.run_id, r.active, r.campaign_tag) for r in rows] == [
            ("run-1", False, TAGS["builders"]), ("run-2", True, TAGS["wholesalers"])]

    def test_an_unconfirmed_campaign_move_flags_the_record_not_silently_updates(self, db):
        """If the removal from the old campaign can't be confirmed, the record must be
        flagged (report.failed) and the old load row must stay exactly as it was —
        never silently overwritten with a campaign_tag the dialer never actually moved."""
        _fresh_scrub(db, P1)
        _run(db, [_record("a", P1, pool="builders")], run_id="run-1")
        report, aircall = _run(db, [_record("a", P1, pool="wholesalers")], run_id="run-2",
                               aircall=FakeAircall(undecided_remove_phones={P1}))
        assert report.loaded == 0 and len(report.failed) == 1
        assert report.failed[0]["record_ref"] == "a"
        rows = _load_rows(db)
        assert [(r.run_id, r.active, r.campaign_tag) for r in rows] == [("run-1", True, TAGS["builders"])]

    def test_contact_deleted_in_aircall_falls_back_to_upsert(self, db):
        _fresh_scrub(db, P1)
        _run(db, [_record("a", P1)], run_id="run-1")
        stored_id = _load_rows(db)[0].dialer_contact_id
        report, second = _run(db, [_record("a", P1)], run_id="run-2",
                              aircall=FakeAircall(missing_ids={stored_id}))
        assert second.upserts == [P1]
        assert report.loaded == 1

    def test_active_contacts_missing_from_the_run_are_counted_not_removed(self, db):
        _fresh_scrub(db, P1, P2)
        _run(db, [_record("a", P1), _record("b", P2)], run_id="run-1")
        report, _ = _run(db, [_record("a", P1)], run_id="run-2")
        assert report.active_not_in_run == 1
        assert [r.phone for r in _load_rows(db, "run-1") if r.active] == [P2]


def test_live_load_sends_each_contact_to_its_pool_campaign(db):
    _fresh_scrub(db, P1)
    dialer = FakeAircall()
    with patch.object(dialer_load, "load_backflip_identifier_index", return_value=EMPTY_INDEX):
        run_dialer_load([_record("c1", P1, pool="builders")], db, run_id="t-campaign", dry_run=False,
                        scrubber=lambda phones: [], dialer=dialer, campaign_tags=TAGS, commit=db.flush,
                        now=NOON_ET)
    assert dialer.campaigns == ["DESK_CONSTRUCTION"]


def test_live_task_refuses_without_a_configured_dialer(tmp_path, monkeypatch):
    from src.lending import dialer_port
    from src.tasks import lending_dialer_load as task
    monkeypatch.setattr(dialer_port, "get_dialer", lambda: None)
    pools = tmp_path / "pools.json"
    pools.write_text("[]", encoding="utf-8")
    assert task.main(["--input", str(pools), "--live"]) == 3



def _attempts(db, phone, n, ended_at):
    for i in range(n):
        db.execute(text(
            "INSERT INTO lending.call_dispositions (dialer_call_id, phone, direction, call_ended_at, raw_event) "
            "VALUES (:c, :p, 'outbound', :t, '{}')"), {"c": f"t-{phone}-{i}-{os.getpid()}", "p": phone, "t": ended_at})


class TestCallTimeRail:
    def test_a_number_at_the_attempt_cap_is_not_loaded(self, db):
        _fresh_scrub(db, P1, P2)
        _attempts(db, P1, 3, NOON_ET - __import__("datetime").timedelta(hours=1))
        report, dialer = _run(db, [_record("a", P1), _record("b", P2)])
        assert dialer.upserts == [P2]
        assert report.excluded_by_reason["ATTEMPT_CAP_REACHED"] == 1

    def test_nothing_loads_after_the_7_15pm_eastern_stop(self, db):
        _fresh_scrub(db, P1)
        late = __import__("datetime").datetime(2026, 9, 29, 23, 20, tzinfo=__import__("datetime").timezone.utc)
        report, dialer = _run(db, [_record("a", P1)], now=late)   # 19:20 ET
        assert dialer.upserts == []
        assert report.excluded_by_reason["OUTSIDE_CALL_WINDOW"] == 1


def test_live_task_refuses_while_a_load_endpoint_is_unconfirmed(tmp_path, monkeypatch):
    from src.lending import dialer_port
    from src.tasks import lending_dialer_load as task
    adapter = dialer_port.BatchDialerAdapter(http=lambda *a, **k: {},
                                             endpoints={"contact_upsert": ("POST", "/contact")})
    monkeypatch.setattr(dialer_port, "get_dialer", lambda: adapter)
    pools = tmp_path / "pools.json"
    pools.write_text("[]", encoding="utf-8")
    assert task.main(["--input", str(pools), "--live"]) == 3
