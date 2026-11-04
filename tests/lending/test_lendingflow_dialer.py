"""LendingFlow -> BatchDialer: loading, temporary vs permanent blocks, one contact per phone, and removal once a lead
is handled in GHL (including the races between the loader and the booking webhook).

Runs inside the rolled-back ``lending_db`` transaction; BatchDialer, Tracerfy and GHL are fakes.
"""
from __future__ import annotations

import json
import uuid
from contextlib import contextmanager, nullcontext
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest
from sqlalchemy import text

from src.lending import dialer_load, lendingflow_dialer as lfd
from src.lending.backflip_conflict import BackflipIdentifierIndex
from src.lending.dialer_load import LoadReport
from src.lending.dialer_port import ContactUpsertResult, DialerRequestError
from src.lending.models import LendingDialerLoadRecord, LendingDialerUnconfirmedCreate

NOON_ET = datetime(2026, 10, 27, 16, 0, tzinfo=timezone.utc)  # Tue 12:00 ET, inside every calling window
LATE_ET = datetime(2026, 10, 28, 0, 30, tzinfo=timezone.utc)  # Tue 20:30 ET, after the 19:15 stop
NEXT_MORNING = datetime(2026, 10, 28, 14, 0, tzinfo=timezone.utc)  # Wed 10:00 ET


def _phone() -> str:
    return f"+1813555{uuid.uuid4().int % 10_000:04d}"


class FakeDialer:
    def __init__(self):
        self.upserts: list[tuple[str, str]] = []
        self.removed: list[tuple[str, str]] = []
        self.cards: list[dict] = []
        self.fail_remove = False
        self.on_upsert = None
        self._next = 5000

    def upsert_contact(self, phone, fields, *, campaign=None, vendor_contact_id=None):
        if self.on_upsert:
            self.on_upsert(phone)
        self.upserts.append((phone, campaign))
        self.cards.append(dict(fields.card or {}))
        self._next += 1
        return ContactUpsertResult(contact_id=self._next, created=True)

    def update_contact(self, contact_id, fields, *, phone=None, vendor_contact_id=None):
        return {"id": contact_id}

    def remove(self, phone, *, reason):
        if self.fail_remove:
            raise DialerRequestError("DELETE /contact", status=503)
        self.removed.append((phone, reason))


class Calls:
    """GHL call reader: messages per contact id, or an error."""

    def __init__(self):
        self.messages: dict[str, list[dict]] = {}
        self.error = False

    def __call__(self, contact_id):
        if self.error:
            raise RuntimeError("GHL down")
        return self.messages.get(contact_id, [])

    def connected(self, contact_id, at, talk=90, message_id=None):
        self.messages.setdefault(contact_id, []).append({
            "id": message_id or f"m-{uuid.uuid4().hex[:8]}", "direction": "outbound", "dateAdded": at.isoformat(),
            "meta": {"callStatus": "completed", "call": {"duration": talk}}, "conversationId": "conv"})


@pytest.fixture
def db(lending_db, monkeypatch):
    from migrations.apply_lending_lendingflow import apply_to as apply_lendingflow
    from migrations.apply_lending_lendingflow_dialer import apply_to as apply_dialer

    conn = lending_db.connection()
    apply_lendingflow(conn)
    apply_dialer(conn)
    LendingDialerLoadRecord.__table__.create(conn, checkfirst=True)
    LendingDialerUnconfirmedCreate.__table__.create(conn, checkfirst=True)
    settings = lfd.get_settings()
    monkeypatch.setattr(settings, "lending_lendingflow_dialer_enabled", True)
    monkeypatch.setattr(settings, "lending_lendingflow_dialer_delay_minutes", 15)
    monkeypatch.setattr(settings, "lending_backflip_check_enabled", False)
    return lending_db


def _lead(db, phone=None, *, received=NOON_ET - timedelta(hours=1), consent="present", contact="ghl-1", scrub=True,
          **extra):
    phone = phone or _phone()
    row = db.execute(text("""
        INSERT INTO lending.lendingflow_leads (vendor_lead_id, dedupe_hash, phone, first_name, last_name,
               loan_amount, loan_amount_range, loan_type, property_state, received_at, consent_status, suppressed,
               ghl_status, ghl_contact_id, raw_payload)
        VALUES (:v, :h, :p, 'Ada', 'Lane', 500000, '$500K - $1M', 'fix_and_flip', :state, :r, :c, :s, 'synced', :g,
                CAST('{}' AS jsonb))
        RETURNING id, lead_uuid
    """), {"v": f"lf-{uuid.uuid4().hex}", "h": uuid.uuid4().hex, "p": phone, "r": received, "c": consent,
           "s": extra.get("suppressed", False), "g": contact, "state": extra.get("state", "FL")}).one()
    if scrub:
        db.execute(text("INSERT INTO dnc_phone_checks (phone, national_dnc, litigator, checked_at, source) "
                        "VALUES (:p, false, false, :at, 'test') ON CONFLICT (phone) DO NOTHING"),
                   {"p": phone, "at": NOON_ET - timedelta(days=1)})
    return row.id, phone


def _lead_row(db, lead_id):
    return db.execute(text("SELECT dialer_status, dialer_block_reason, duplicate_of, handled_in_ghl_at, "
                           "handled_in_ghl_reason FROM lending.lendingflow_leads WHERE id = :id"),
                      {"id": lead_id}).mappings().one()


def _active_rows(db, phone):
    return db.execute(text("SELECT active, deactivation_reason, source_record_ref FROM lending.dialer_load_records "
                           "WHERE phone = :p ORDER BY id"), {"p": phone}).all()


@contextmanager
def _same_session(db):
    yield db


SCRUBBED: list[list[str]] = []


def _scrubber(phones):
    SCRUBBED.append(list(phones))
    return []


def _run(db, *, dialer=None, calls=None, now=NOON_ET):
    with patch.object(dialer_load, "load_backflip_identifier_index",
                      return_value=BackflipIdentifierIndex(block_reason=None)):
        return lfd.run_dialer_step(dialer=dialer, scrubber=_scrubber, read_calls=calls, now=now,
                                   sessions=lambda: _same_session(db), single_run=lambda: nullcontext(True))


# ── loading ──────────────────────────────────────────────────────────────────

def test_a_ready_lead_is_loaded_once_into_the_lendingflow_campaign_with_its_card(db):
    lead_id, phone = _lead(db)
    dialer = FakeDialer()
    stats = _run(db, dialer=dialer, calls=Calls())
    assert stats.loaded == 1 and dialer.upserts == [(phone, "LendingFlow")]
    card = dialer.cards[0]
    assert card["campaign"] == "LendingFlow" and card["first_name"] == "Ada" and card["loan"] == "$500K - $1M"
    assert card["loan_type"] == "fix and flip" and card["state"] == "FL" and card["requested"] == "2026-10-27"
    assert card["prior_contact"] == "No prior contact"
    assert _lead_row(db, lead_id)["dialer_status"] == "loaded"
    assert [r.active for r in _active_rows(db, phone)] == [True]
    _run(db, dialer=dialer, calls=Calls())
    assert len(dialer.upserts) == 1


def test_a_lead_inside_josh_first_call_window_is_not_loaded_yet(db):
    lead_id, _ = _lead(db, received=NOON_ET - timedelta(minutes=5))
    dialer = FakeDialer()
    _run(db, dialer=dialer, calls=Calls())
    assert dialer.upserts == [] and _lead_row(db, lead_id)["dialer_status"] == "pending"


def test_with_no_delay_a_new_lead_loads_on_the_next_sweep(db, monkeypatch):
    monkeypatch.setattr(lfd.get_settings(), "lending_lendingflow_dialer_delay_minutes", 0)
    lead_id, _ = _lead(db, received=NOON_ET - timedelta(minutes=1))
    dialer = FakeDialer()
    _run(db, dialer=dialer, calls=Calls())
    assert len(dialer.upserts) == 1 and _lead_row(db, lead_id)["dialer_status"] == "loaded"


def test_the_default_delay_is_zero():
    from config.settings import AppSettings
    assert AppSettings.model_fields["lending_lendingflow_dialer_delay_minutes"].default == 0


def test_a_lead_that_arrives_after_hours_stays_pending_and_loads_next_morning(db):
    lead_id, _ = _lead(db, received=LATE_ET - timedelta(minutes=30))
    dialer = FakeDialer()
    _run(db, dialer=dialer, calls=Calls(), now=LATE_ET)
    row = _lead_row(db, lead_id)
    assert dialer.upserts == [] and (row["dialer_status"], row["dialer_block_reason"]) == ("pending", "OUTSIDE_CALL_WINDOW")
    _run(db, dialer=dialer, calls=Calls(), now=NEXT_MORNING)
    assert _lead_row(db, lead_id)["dialer_status"] == "loaded" and len(dialer.upserts) == 1


@pytest.mark.parametrize("kwargs, reason", [({"suppressed": True}, "SUPPRESSED"),
                                            ({"state": "GA"}, "GA_NATURAL_PERSON")])
def test_permanent_reasons_block_the_lead_for_good(db, kwargs, reason):
    lead_id, _ = _lead(db, **kwargs)
    dialer = FakeDialer()
    _run(db, dialer=dialer, calls=Calls())
    _run(db, dialer=dialer, calls=Calls(), now=NEXT_MORNING)
    row = _lead_row(db, lead_id)
    assert dialer.upserts == [] and (row["dialer_status"], row["dialer_block_reason"]) == ("blocked", reason)


def test_no_consent_certificate_and_no_dnc_scrub_are_needed(db):
    """Client decision: LendingFlow leads load without a certificate, and Tracerfy is never called for them."""
    SCRUBBED.clear()
    lead_id, phone = _lead(db, consent="missing", scrub=False)
    dialer = FakeDialer()
    _run(db, dialer=dialer, calls=Calls())
    assert dialer.upserts == [(phone, "LendingFlow")] and SCRUBBED == []
    assert _lead_row(db, lead_id)["dialer_status"] == "loaded"


def test_a_stored_dnc_hit_does_not_block_a_lendingflow_lead_but_an_opt_out_does(db):
    lead_id, phone = _lead(db, scrub=False)
    db.execute(text("INSERT INTO dnc_phone_checks (phone, national_dnc, litigator, checked_at, source) "
                    "VALUES (:p, true, true, :at, 'test') ON CONFLICT (phone) DO UPDATE SET national_dnc = true"),
               {"p": phone, "at": NOON_ET - timedelta(days=1)})
    opted_out, other = _lead(db, scrub=False)
    db.execute(text("INSERT INTO lending.suppression_list (phone, reason, source_channel) VALUES (:p, 'OPT_OUT', 'sms') "
                    "ON CONFLICT (phone) DO NOTHING"), {"p": other})
    dialer = FakeDialer()
    _run(db, dialer=dialer, calls=Calls())
    assert dialer.upserts == [(phone, "LendingFlow")]
    assert (_lead_row(db, opted_out)["dialer_status"], _lead_row(db, opted_out)["dialer_block_reason"]) ==         ("blocked", "SUPPRESSED")


def test_a_loaded_lendingflow_number_is_never_held_or_rescrubbed_for_a_missing_scrub(db):
    from src.lending import compliance, weekly_scrub

    _, phone = _lead(db, scrub=False)
    _run(db, dialer=FakeDialer(), calls=Calls())
    assert compliance.dial_blocks(db, [phone], now=NOON_ET) == {}
    assert phone not in compliance.blocked_from_dialing(db, [phone], NOON_ET)
    assert phone not in weekly_scrub.stale_loaded_phones(db, now=NOON_ET)


def test_two_leads_on_one_phone_become_one_dialer_contact(db):
    first, phone = _lead(db, received=NOON_ET - timedelta(hours=2))
    second, _ = _lead(db, phone, received=NOON_ET - timedelta(hours=1))
    dialer = FakeDialer()
    _run(db, dialer=dialer, calls=Calls())
    assert dialer.upserts == [(phone, "LendingFlow")]
    assert _lead_row(db, first)["dialer_status"] == "loaded"
    assert (_lead_row(db, second)["dialer_status"], _lead_row(db, second)["duplicate_of"]) == ("duplicate", first)


def test_a_phone_already_in_another_campaign_is_not_moved(db):
    lead_id, phone = _lead(db)
    db.execute(text("INSERT INTO lending.dialer_load_records (run_id, pool, source_record_ref, phone, phone_hash, "
                    "campaign_tag, dialer_contact_id) VALUES ('w', 'wholesalers', 'wholesaler:1', :p, 'h', "
                    "'Wholesalers', '77')"), {"p": phone})
    dialer = FakeDialer()
    _run(db, dialer=dialer, calls=Calls())
    assert dialer.upserts == [] and _lead_row(db, lead_id)["dialer_block_reason"] == "IN_OTHER_CAMPAIGN"


def test_the_emergency_halt_loads_nothing_and_keeps_the_lead_pending(db, monkeypatch):
    lead_id, _ = _lead(db)
    monkeypatch.setattr(lfd, "is_globally_halted", lambda: True)
    dialer = FakeDialer()
    _run(db, dialer=dialer, calls=Calls())
    assert dialer.upserts == [] and _lead_row(db, lead_id)["dialer_status"] == "pending"


def test_loading_switched_off_loads_nothing(db, monkeypatch):
    lead_id, _ = _lead(db)
    monkeypatch.setattr(lfd.get_settings(), "lending_lendingflow_dialer_enabled", False)
    dialer = FakeDialer()
    _run(db, dialer=dialer, calls=Calls())
    assert dialer.upserts == [] and _lead_row(db, lead_id)["dialer_status"] == "pending"


def test_a_create_with_an_unknown_outcome_is_never_pushed_again(db):
    lead_id, _ = _lead(db)
    dialer = FakeDialer()

    def boom(phone):
        raise DialerRequestError("POST /contacts", status=502, maybe_created=True)

    dialer.on_upsert = boom
    _run(db, dialer=dialer, calls=Calls())
    assert _lead_row(db, lead_id)["dialer_status"] == "unconfirmed"
    dialer.on_upsert = None
    _run(db, dialer=dialer, calls=Calls(), now=NEXT_MORNING)
    assert dialer.upserts == [] and _lead_row(db, lead_id)["dialer_status"] == "unconfirmed"


def test_a_push_error_is_retried_later_then_left_failed(db, monkeypatch):
    monkeypatch.setattr(lfd, "DIALER_MAX_FAILURES", 2)
    lead_id, _ = _lead(db)
    dialer = FakeDialer()
    dialer.on_upsert = lambda phone: (_ for _ in ()).throw(DialerRequestError("POST", status=422))
    _run(db, dialer=dialer, calls=Calls())
    assert _lead_row(db, lead_id)["dialer_status"] == "pending"
    _run(db, dialer=dialer, calls=Calls(), now=NOON_ET + timedelta(minutes=20))
    assert _lead_row(db, lead_id)["dialer_status"] == "failed"


# ── handled in GHL ───────────────────────────────────────────────────────────

def test_a_connected_ghl_call_before_the_load_keeps_the_lead_out_of_the_dialer(db):
    lead_id, _ = _lead(db, contact="ghl-a")
    calls = Calls()
    calls.connected("ghl-a", NOON_ET - timedelta(minutes=30))
    dialer = FakeDialer()
    _run(db, dialer=dialer, calls=calls)
    row = _lead_row(db, lead_id)
    assert dialer.upserts == [] and (row["dialer_status"], row["dialer_block_reason"]) == ("blocked", "HANDLED_IN_GHL")
    assert row["handled_in_ghl_reason"] == "ghl_call_connected"


def test_a_short_ghl_call_is_not_handled(db):
    lead_id, _ = _lead(db, contact="ghl-b")
    calls = Calls()
    calls.connected("ghl-b", NOON_ET - timedelta(minutes=30), talk=20)
    _run(db, dialer=FakeDialer(), calls=calls)
    assert _lead_row(db, lead_id)["dialer_status"] == "loaded"


def test_a_failed_ghl_read_never_loads_the_lead(db):
    lead_id, _ = _lead(db)
    calls = Calls()
    calls.error = True
    dialer = FakeDialer()
    _run(db, dialer=dialer, calls=calls)
    row = _lead_row(db, lead_id)
    assert dialer.upserts == [] and (row["dialer_status"], row["dialer_block_reason"]) == ("pending", "GHL_CHECK_FAILED")


def test_no_ghl_reader_means_no_loading(db):
    lead_id, _ = _lead(db)
    dialer = FakeDialer()
    _run(db, dialer=dialer, calls=None)
    assert dialer.upserts == [] and _lead_row(db, lead_id)["dialer_status"] == "pending"


def test_connected_after_the_load_removes_the_contact_and_the_dialer_sweep_never_restores_it(db):
    lead_id, phone = _lead(db, contact="ghl-c")
    dialer, calls = FakeDialer(), Calls()
    _run(db, dialer=dialer, calls=calls)
    calls.connected("ghl-c", NOON_ET + timedelta(minutes=10))
    stats = _run(db, dialer=dialer, calls=calls, now=NOON_ET + timedelta(minutes=20))
    assert stats.removed == 1 and dialer.removed == [(phone, "handled_in_ghl")]
    assert _lead_row(db, lead_id)["dialer_status"] == "removed"
    assert [(r.active, r.deactivation_reason) for r in _active_rows(db, phone)] == [(False, "handled_in_ghl")]
    assert db.execute(text("SELECT count(*) FROM lending.dialer_holds WHERE phone = :p"), {"p": phone}).scalar() == 0
    _run(db, dialer=dialer, calls=calls, now=NEXT_MORNING)
    assert len(dialer.upserts) == 1 and len(dialer.removed) == 1


def test_a_booking_for_a_loaded_lead_removes_it(db):
    lead_id, phone = _lead(db)
    dialer = FakeDialer()
    _run(db, dialer=dialer, calls=Calls())
    lfd.handle_handled_in_ghl(phone, reason=lfd.HANDLED_BOOKED, sessions=lambda: _same_session(db),
                              dialer_factory=lambda: dialer)
    row = _lead_row(db, lead_id)
    assert (row["dialer_status"], row["handled_in_ghl_reason"]) == ("removed", "booked")
    assert dialer.removed == [(phone, "handled_in_ghl")]


def test_a_booking_before_the_load_blocks_the_lead_without_touching_the_dialer(db):
    lead_id, phone = _lead(db)
    dialer = FakeDialer()
    lfd.handle_handled_in_ghl(phone, reason=lfd.HANDLED_BOOKED, sessions=lambda: _same_session(db),
                              dialer_factory=lambda: dialer)
    _run(db, dialer=dialer, calls=Calls())
    row = _lead_row(db, lead_id)
    assert (row["dialer_status"], row["dialer_block_reason"]) == ("blocked", "HANDLED_IN_GHL")
    assert dialer.upserts == [] and dialer.removed == []


def test_a_booking_that_lands_while_the_lead_is_being_pushed_is_removed_right_after_the_push(db):
    lead_id, phone = _lead(db)
    dialer = FakeDialer()

    def booked_meanwhile(pushed_phone):  # the webhook commits its stamp before the load row exists
        lfd.mark_handled_in_ghl(db, pushed_phone, reason=lfd.HANDLED_BOOKED)
        db.commit()

    dialer.on_upsert = booked_meanwhile
    _run(db, dialer=dialer, calls=Calls())
    assert dialer.upserts == [(phone, "LendingFlow")] and dialer.removed == [(phone, "handled_in_ghl")]
    assert _lead_row(db, lead_id)["dialer_status"] == "removed"
    assert [r.active for r in _active_rows(db, phone)] == [False]


def test_a_failed_removal_stays_pending_and_the_next_sweep_finishes_it(db):
    lead_id, phone = _lead(db)
    dialer = FakeDialer()
    _run(db, dialer=dialer, calls=Calls())
    dialer.fail_remove = True
    lfd.handle_handled_in_ghl(phone, reason=lfd.HANDLED_BOOKED, sessions=lambda: _same_session(db),
                              dialer_factory=lambda: dialer)
    assert _lead_row(db, lead_id)["dialer_status"] == "remove_pending"
    assert [r.active for r in _active_rows(db, phone)] == [True]
    dialer.fail_remove = False
    stats = _run(db, dialer=dialer, calls=Calls(), now=NOON_ET + timedelta(minutes=5))
    assert stats.removed == 1 and _lead_row(db, lead_id)["dialer_status"] == "removed"


def test_removal_still_runs_with_loading_switched_off(db, monkeypatch):
    lead_id, phone = _lead(db)
    dialer = FakeDialer()
    _run(db, dialer=dialer, calls=Calls())
    lfd.mark_handled_in_ghl(db, phone, reason=lfd.HANDLED_BOOKED)
    db.commit()
    monkeypatch.setattr(lfd.get_settings(), "lending_lendingflow_dialer_enabled", False)
    _run(db, dialer=dialer, calls=Calls(), now=NOON_ET + timedelta(minutes=5))
    assert _lead_row(db, lead_id)["dialer_status"] == "removed"


def test_a_crashed_run_leaves_no_lead_stuck_loading(db):
    lead_id, phone = _lead(db)
    db.execute(text("UPDATE lending.lendingflow_leads SET dialer_status = 'loading' WHERE id = :id"), {"id": lead_id})
    dialer = FakeDialer()
    _run(db, dialer=dialer, calls=Calls())
    assert _lead_row(db, lead_id)["dialer_status"] == "loaded" and len(dialer.upserts) == 1


def test_preexisting_leads_are_never_loaded(db):
    lead_id, _ = _lead(db)
    db.execute(text("UPDATE lending.lendingflow_leads SET dialer_status = 'preexisting' WHERE id = :id"), {"id": lead_id})
    dialer = FakeDialer()
    _run(db, dialer=dialer, calls=Calls())
    assert dialer.upserts == []


# ── pure parts ───────────────────────────────────────────────────────────────

@pytest.mark.parametrize("reason, status", [("OUTSIDE_CALL_WINDOW", "pending"), ("ATTEMPT_CAP_REACHED", "pending"),
                                            ("NO_FRESH_SCRUB", "pending"), ("NATIONAL_DNC", "blocked"),
                                            ("ATTEMPT_HISTORY_EXCEEDED", "blocked"), ("SOMETHING_NEW", "pending")])
def test_loader_reasons_are_classified(reason, status):
    assert lfd._classify(reason) == status


def test_a_halt_mid_run_is_retry_later():
    report = LoadReport(run_id="r", dry_run=False, halted_mid_run=1)
    assert lfd._outcome(None, report, "lendingflow:x") == ("pending", "HALTED")


def test_the_lendingflow_hook_and_campaign_are_configured():
    from config.lending_dialer import CAMPAIGN_HOOKS, POOL_CAMPAIGN_TAGS
    assert POOL_CAMPAIGN_TAGS["lendingflow"] == "LendingFlow" and CAMPAIGN_HOOKS["LendingFlow"]


def test_the_batchdialer_adapter_deletes_a_contact_handled_in_ghl():
    from src.lending.dialer_port import BatchDialerAdapter

    sent = []
    adapter = BatchDialerAdapter(http=lambda method, path, json=None, **k: sent.append((method, path)) or {},
                                 contact_ids=lambda phone: ["11", "12"])
    adapter.remove("+18135550100", reason="handled_in_ghl")
    assert sent == [("DELETE", "/contact/11"), ("DELETE", "/contact/12")]


def test_the_migration_marks_existing_leads_preexisting_and_reruns_cleanly(lending_db):
    from migrations.apply_lending_lendingflow import apply_to as apply_lendingflow
    from migrations.apply_lending_lendingflow_dialer import apply_to as apply_dialer

    conn = lending_db.connection()
    apply_lendingflow(conn)
    conn.execute(text("ALTER TABLE lending.lendingflow_leads DROP COLUMN IF EXISTS dialer_status CASCADE"))
    lead = conn.execute(text("INSERT INTO lending.lendingflow_leads (vendor_lead_id, dedupe_hash, phone, raw_payload) "
                             "VALUES ('old', 'old-hash', '+18135550199', CAST(:p AS jsonb)) RETURNING id"),
                        {"p": json.dumps({})}).scalar()
    apply_dialer(conn)
    apply_dialer(conn)
    new = conn.execute(text("INSERT INTO lending.lendingflow_leads (vendor_lead_id, dedupe_hash, phone, raw_payload) "
                            "VALUES ('new', 'new-hash', '+18135550198', CAST('{}' AS jsonb)) RETURNING id")).scalar()
    statuses = dict(conn.execute(text("SELECT id, dialer_status FROM lending.lendingflow_leads WHERE id IN (:a, :b)"),
                                 {"a": lead, "b": new}).all())
    assert statuses == {lead: "preexisting", new: "pending"}
