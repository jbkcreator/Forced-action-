"""WP-W0-8 global stop-propagation (spec §3.2): opt-out on any channel blocks all within 60 s."""
from __future__ import annotations

import os

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from config.lending_compliance import STOP_PROPAGATION_SLA_SECONDS, ReasonCode

pytestmark = pytest.mark.skipif(
    not os.environ.get("DATABASE_URL"), reason="requires a live Postgres DATABASE_URL"
)

PHONE = "+18135559001"
EMAIL = "stop.test@example.com"


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


class FakeDialer:
    def __init__(self, boom=False):
        self.removed, self.boom = [], boom

    def __call__(self, phone):
        if self.boom:
            raise RuntimeError("aircall 500")
        self.removed.append(phone)


def _scalar(db, sql, **p):
    return db.execute(text(sql), p).scalar()


def _event(db, phone):
    from src.services.lending_compliance import phone_hash
    return db.execute(
        text("SELECT channel, source_ref, actor, status, received_at, suppression_at, sms_at, "
             "email_at, dialer_removed_at FROM lending.opt_out_events WHERE phone_hash = :h"),
        {"h": phone_hash(phone)},
    ).fetchone()


def test_verbal_decline_blocks_every_store_within_sla(db):
    from src.services.lending_compliance import filter_loadable, propagate_opt_out

    dialer = FakeDialer()
    propagate_opt_out(db, phone=PHONE, source_ref="call_42", actor="seat_a",
                      dialer_remover=dialer)

    assert _scalar(db, "SELECT reason FROM lending.suppression_list WHERE phone = :p", p=PHONE) == "OPT_OUT"
    assert _scalar(db, "SELECT 1 FROM sms_opt_outs WHERE phone = :p", p=PHONE) == 1
    assert _scalar(db, "SELECT do_not_contact FROM lending.contacts WHERE phone = :p", p=PHONE) is True
    assert dialer.removed == [PHONE]

    ev = _event(db, PHONE)
    assert (ev.channel, ev.source_ref, ev.actor, ev.status) == ("dialer", "call_42", "seat_a", "complete")
    last = max(ev.suppression_at, ev.sms_at, ev.dialer_removed_at)
    assert (last - ev.received_at).total_seconds() < STOP_PROPAGATION_SLA_SECONDS

    class NoScrub:
        def __call__(self, phones):
            raise AssertionError("suppressed phone must not reach Tracerfy")
    out = filter_loadable([{"phone": PHONE}], db, scrubber=NoScrub())
    assert out[0].reason == ReasonCode.SUPPRESSED


def test_dialer_outage_never_blocks_the_suppression_writes(db):
    from src.services.lending_compliance import propagate_opt_out

    propagate_opt_out(db, phone=PHONE, source_ref="call_43", dialer_remover=FakeDialer(boom=True))

    assert _scalar(db, "SELECT 1 FROM lending.suppression_list WHERE phone = :p", p=PHONE) == 1
    assert _scalar(db, "SELECT 1 FROM sms_opt_outs WHERE phone = :p", p=PHONE) == 1
    ev = _event(db, PHONE)
    assert ev.status == "dialer_pending" and ev.dialer_removed_at is None


def test_email_unsubscribe_reaches_lending_suppression(db):
    from src.services.email_suppression import suppress_contact

    suppress_contact(db, email=EMAIL, source="unsubscribe_link")

    assert _scalar(db, "SELECT reason FROM lending.suppression_list WHERE email = :e", e=EMAIL) == "OPT_OUT"
    assert _scalar(
        db, "SELECT channel FROM lending.opt_out_events ORDER BY id DESC LIMIT 1"
    ) == "email"


def test_inbound_sms_stop_reaches_lending_suppression_and_dialer(db, monkeypatch):
    from src.services import lending_compliance, sms_compliance

    dialer = FakeDialer()
    monkeypatch.setattr(lending_compliance, "_default_dialer_remover", lambda: dialer)

    sms_compliance.handle_inbound(PHONE, "STOP", db)

    assert _scalar(db, "SELECT 1 FROM lending.suppression_list WHERE phone = :p", p=PHONE) == 1
    assert _event(db, PHONE).channel == "sms"
    assert dialer.removed == [PHONE]


def test_bounce_is_not_recorded_as_a_lending_opt_out(db):
    from src.services.email_suppression import suppress_contact

    suppress_contact(db, email=EMAIL, source="mandrill_hard_bounce")

    assert _scalar(db, "SELECT count(*) FROM lending.suppression_list WHERE email = :e", e=EMAIL) == 0
    assert _scalar(db, "SELECT count(*) FROM email_opt_outs WHERE email = :e", e=EMAIL) == 1  # FA still blocks


def test_repeat_fa_opt_out_creates_one_event(db, monkeypatch):
    from src.services import lending_compliance, sms_compliance
    from src.services.lending_compliance import phone_hash

    monkeypatch.setattr(lending_compliance, "_default_dialer_remover", lambda: FakeDialer())
    sms_compliance.handle_inbound(PHONE, "STOP", db)
    sms_compliance.handle_inbound(PHONE, "STOP", db)

    assert _scalar(db, "SELECT count(*) FROM lending.opt_out_events WHERE phone_hash = :h", h=phone_hash(PHONE)) == 1


def test_reconcile_recovers_an_opt_out_whose_mirror_failed(db, monkeypatch):
    from src.services import lending_compliance
    from src.services.email_suppression import suppress_contact

    def broken(*a, **k):
        raise RuntimeError("lending schema unavailable")
    monkeypatch.setattr(lending_compliance, "mirror_fa_opt_out", broken)
    suppress_contact(db, email=EMAIL, source="unsubscribe_link")
    assert _scalar(db, "SELECT count(*) FROM lending.suppression_list WHERE email = :e", e=EMAIL) == 0

    monkeypatch.undo()
    lending_compliance.reconcile_suppression(db)
    assert _scalar(db, "SELECT reason FROM lending.suppression_list WHERE email = :e", e=EMAIL) == "OPT_OUT"
