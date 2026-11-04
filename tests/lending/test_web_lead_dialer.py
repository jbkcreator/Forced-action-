"""Website leads into the BatchDialer "Website" campaign: the same dialer step as LendingFlow (source WEBSITE), but
only leads who ticked the call-consent box, and with the Tracerfy DNC scrub.

Runs inside the rolled-back ``lending_db`` transaction; BatchDialer, Tracerfy and GHL are fakes.
"""
from __future__ import annotations

import uuid
from contextlib import contextmanager, nullcontext
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest
from sqlalchemy import text

from src.lending import dialer_load, lendingflow_dialer as lfd
from src.lending.backflip_conflict import BackflipIdentifierIndex
from src.lending.dialer_port import ContactUpsertResult
from src.lending.models import LendingDialerLoadRecord, LendingDialerUnconfirmedCreate

NOON_ET = datetime(2026, 10, 27, 16, 0, tzinfo=timezone.utc)


class FakeDialer:
    def __init__(self):
        self.upserts: list[tuple[str, str]] = []
        self.cards: list[dict] = []
        self.removed: list[tuple[str, str]] = []
        self._next = 7000

    def upsert_contact(self, phone, fields, *, campaign=None, vendor_contact_id=None):
        self.upserts.append((phone, campaign))
        self.cards.append(dict(fields.card or {}))
        self._next += 1
        return ContactUpsertResult(contact_id=self._next, created=True)

    def update_contact(self, contact_id, fields, *, phone=None, vendor_contact_id=None):
        return {"id": contact_id}

    def remove(self, phone, *, reason):
        self.removed.append((phone, reason))


class Scrubber:
    def __init__(self, dnc=()):
        self.calls: list[list[str]] = []
        self.dnc = set(dnc)

    def __call__(self, phones):
        self.calls.append(list(phones))
        return [{"phone": p, "national_dnc": "Y" if p in self.dnc else "N", "litigator": "N", "state_dnc": "N"}
                for p in phones]


@pytest.fixture
def db(web_leads_db, monkeypatch):
    from migrations.apply_lending_lendingflow import apply_to as apply_lendingflow

    conn = web_leads_db.connection()
    apply_lendingflow(conn)  # also adds the dialer columns to web_leads
    LendingDialerLoadRecord.__table__.create(conn, checkfirst=True)
    LendingDialerUnconfirmedCreate.__table__.create(conn, checkfirst=True)
    settings = lfd.get_settings()
    monkeypatch.setattr(settings, "lending_web_lead_dialer_enabled", True)
    monkeypatch.setattr(settings, "lending_lendingflow_dialer_delay_minutes", 0)
    monkeypatch.setattr(settings, "lending_backflip_check_enabled", False)
    return web_leads_db


def _web_lead(db, *, call_consent=True, suppressed=False, received=NOON_ET - timedelta(minutes=10)):
    phone = f"+1813555{uuid.uuid4().int % 10_000:04d}"
    lead_id = db.execute(text("""
        INSERT INTO lending.web_leads (received_at, name, phone, email, deal_type, loan_amount, property_state,
               sms_consent, call_consent, consent_text, suppressed, ghl_status, ghl_contact_id)
        VALUES (:r, 'Maya Cole', :p, 'maya@example.com', 'Fix & flip', 350000, 'FL', false, :cc, 'consent text', :s,
                'synced', 'ghl-w1')
        RETURNING id"""), {"r": received, "p": phone, "cc": call_consent, "s": suppressed}).scalar()
    return lead_id, phone


def _row(db, lead_id):
    return db.execute(text("SELECT dialer_status, dialer_block_reason FROM lending.web_leads WHERE id = :id"),
                      {"id": lead_id}).one()


@contextmanager
def _same(db):
    yield db


def _run(db, *, dialer, scrubber, calls=lambda contact_id: []):
    with patch.object(dialer_load, "load_backflip_identifier_index",
                      return_value=BackflipIdentifierIndex(block_reason=None)):
        return lfd.run_dialer_step(dialer=dialer, scrubber=scrubber, read_calls=calls, now=NOON_ET,
                                   sessions=lambda: _same(db), single_run=lambda: nullcontext(True),
                                   source=lfd.WEBSITE)


def test_a_website_lead_who_ticked_call_consent_is_scrubbed_and_loaded_into_the_website_campaign(db):
    lead_id, phone = _web_lead(db)
    dialer, scrubber = FakeDialer(), Scrubber()
    stats = _run(db, dialer=dialer, scrubber=scrubber)
    assert stats.loaded == 1 and dialer.upserts == [(phone, "Website")]
    assert scrubber.calls == [[phone]]  # the Tracerfy DNC scrub runs for website leads
    card = dialer.cards[0]
    assert (card["campaign"], card["first_name"], card["loan"], card["loan_type"], card["state"]) == \
        ("Website", "Maya", "$350,000", "Fix & flip", "FL")
    assert _row(db, lead_id) == ("loaded", None)
    ref = db.execute(text("SELECT source_record_ref FROM lending.dialer_load_records WHERE phone = :p"),
                     {"p": phone}).scalar()
    assert ref == f"web:{lead_id}"


def test_no_call_consent_tick_means_never_loaded(db):
    lead_id, _ = _web_lead(db, call_consent=False)
    dialer = FakeDialer()
    _run(db, dialer=dialer, scrubber=Scrubber())
    assert dialer.upserts == [] and _row(db, lead_id) == ("blocked", "NO_CALL_CONSENT")


def test_a_dnc_number_is_blocked(db):
    lead_id, phone = _web_lead(db)
    dialer = FakeDialer()
    _run(db, dialer=dialer, scrubber=Scrubber(dnc=[phone]))
    assert dialer.upserts == [] and _row(db, lead_id) == ("blocked", "NATIONAL_DNC")


def test_a_suppressed_website_lead_is_blocked(db):
    lead_id, _ = _web_lead(db, suppressed=True)
    _run(db, dialer=FakeDialer(), scrubber=Scrubber())
    assert _row(db, lead_id) == ("blocked", "SUPPRESSED")


def test_loading_off_loads_nothing(db, monkeypatch):
    lead_id, _ = _web_lead(db)
    monkeypatch.setattr(lfd.get_settings(), "lending_web_lead_dialer_enabled", False)
    dialer = FakeDialer()
    _run(db, dialer=dialer, scrubber=Scrubber())
    assert dialer.upserts == [] and _row(db, lead_id)[0] == "pending"


def test_a_booking_removes_a_loaded_website_lead(db):
    lead_id, phone = _web_lead(db)
    dialer = FakeDialer()
    _run(db, dialer=dialer, scrubber=Scrubber())
    lfd.handle_handled_in_ghl(phone, reason=lfd.HANDLED_BOOKED, sessions=lambda: _same(db),
                              dialer_factory=lambda: dialer)
    assert dialer.removed == [(phone, "handled_in_ghl")] and _row(db, lead_id)[0] == "removed"


def test_a_website_phone_is_not_ghl_owned_any_more(db):
    """Website leads now have their own dialer flow, so the GHL-ownership rule leaves them to it."""
    from src.lending.ghl_calling import ghl_calling_phones
    _, phone = _web_lead(db)
    assert ghl_calling_phones(db, [phone], now=NOON_ET) == set()


def test_the_migration_marks_existing_website_leads_preexisting(web_leads_db):
    from migrations.apply_lending_lendingflow_dialer import apply_to as apply_dialer

    conn = web_leads_db.connection()
    conn.execute(text("ALTER TABLE lending.web_leads DROP COLUMN IF EXISTS dialer_status CASCADE"))
    old = conn.execute(text("INSERT INTO lending.web_leads (name, phone, consent_text, ghl_status) "
                            "VALUES ('Old Lead', '+18135550111', 'x', 'synced') RETURNING id")).scalar()
    apply_dialer(conn)
    apply_dialer(conn)
    new = conn.execute(text("INSERT INTO lending.web_leads (name, phone, consent_text, ghl_status) "
                            "VALUES ('New Lead', '+18135550112', 'x', 'synced') RETURNING id")).scalar()
    statuses = dict(conn.execute(text("SELECT id, dialer_status FROM lending.web_leads WHERE id IN (:a, :b)"),
                                 {"a": old, "b": new}).all())
    assert statuses == {old: "preexisting", new: "pending"}
