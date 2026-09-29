"""WP-W0-8 global stop-propagation (spec §3.2): opt-out on any channel blocks all within 60 s.

Option B design: FA code is untouched. Dialer DNC_REQUEST enters via
propagate_opt_out; FA-channel opt-outs (SMS STOP, email UNSUBSCRIBE) are picked up
by poll_fa_opt_outs, which runs every OPT_OUT_POLL_SECONDS.
"""
from __future__ import annotations

import os

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from config.lending_compliance import OPT_OUT_POLL_SECONDS, STOP_PROPAGATION_SLA_SECONDS, ReasonCode

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
    """Records removals of the test phone only: the shared DB holds real FA
    opt-outs that a poll in the rolled-back test transaction also sees."""

    def __init__(self, boom=False):
        self.removed, self.reasons, self.boom = [], [], boom

    def __call__(self, phone, *, reason):
        if self.boom:
            raise RuntimeError("aircall 500")
        if phone == PHONE:
            self.removed.append(phone)
            self.reasons.append(reason)


def _scalar(db, sql, **p):
    return db.execute(text(sql), p).scalar()


def _events(db, phone):
    from src.lending.compliance import phone_hash
    return db.execute(
        text("SELECT channel, source_ref, actor, status, received_at, suppression_at, sms_at, "
             "email_at, dialer_removed_at FROM lending.opt_out_events WHERE phone_hash = :h ORDER BY id"),
        {"h": phone_hash(phone)},
    ).fetchall()


def test_poll_interval_leaves_room_inside_the_sla():
    assert OPT_OUT_POLL_SECONDS * 2 < STOP_PROPAGATION_SLA_SECONDS


def test_fa_code_has_no_lending_hook():
    import inspect
    from src.services import email_suppression
    assert "lending" not in inspect.getsource(email_suppression)


# ── Dialer DNC_REQUEST ────────────────────────────────────────────────────────


def test_verbal_decline_blocks_every_store_within_sla(db):
    from src.lending.compliance import filter_loadable, propagate_opt_out

    dialer = FakeDialer()
    propagate_opt_out(db, phone=PHONE, source_ref="call_42", actor="seat_a", dialer_remover=dialer)

    assert _scalar(db, "SELECT reason FROM lending.suppression_list WHERE phone = :p", p=PHONE) == "OPT_OUT"
    assert _scalar(db, "SELECT 1 FROM sms_opt_outs WHERE phone = :p", p=PHONE) == 1
    assert _scalar(db, "SELECT do_not_contact FROM lending.contacts WHERE phone = :p", p=PHONE) is True
    assert dialer.removed == [PHONE]

    [ev] = _events(db, PHONE)
    assert (ev.channel, ev.source_ref, ev.actor, ev.status) == ("dialer", "call_42", "seat_a", "complete")
    assert (ev.dialer_removed_at - ev.received_at).total_seconds() < STOP_PROPAGATION_SLA_SECONDS

    class NoScrub:
        def __call__(self, phones):
            raise AssertionError("suppressed phone must not reach Tracerfy")
    assert filter_loadable([{"phone": PHONE}], db, scrubber=NoScrub())[0].reason == ReasonCode.SUPPRESSED


def test_dialer_outage_never_blocks_the_suppression_writes(db):
    from src.lending.compliance import propagate_opt_out

    propagate_opt_out(db, phone=PHONE, source_ref="call_43", dialer_remover=FakeDialer(boom=True))

    assert _scalar(db, "SELECT 1 FROM lending.suppression_list WHERE phone = :p", p=PHONE) == 1
    assert _scalar(db, "SELECT 1 FROM sms_opt_outs WHERE phone = :p", p=PHONE) == 1
    [ev] = _events(db, PHONE)
    assert ev.status == "dialer_pending" and ev.dialer_removed_at is None


def test_redelivered_dnc_request_does_nothing_extra(db):
    from src.lending.compliance import propagate_opt_out

    dialer = FakeDialer()
    first = propagate_opt_out(db, phone=PHONE, source_ref="call_77", actor="seat_a", dialer_remover=dialer)
    again = propagate_opt_out(db, phone=PHONE, source_ref="call_77", actor="seat_a", dialer_remover=dialer)

    assert again == first
    assert dialer.removed == [PHONE]
    assert len(_events(db, PHONE)) == 1


# ── FA channels via the poller ───────────────────────────────────────────────


def test_inbound_sms_stop_reaches_lending_on_next_poll(db):
    from src.lending.compliance import poll_fa_opt_outs
    from src.services import sms_compliance

    sms_compliance.handle_inbound(PHONE, "STOP", db)
    assert _scalar(db, "SELECT count(*) FROM lending.suppression_list WHERE phone = :p", p=PHONE) == 0

    dialer = FakeDialer()
    poll_fa_opt_outs(db, dialer_remover=dialer)

    assert _scalar(db, "SELECT reason FROM lending.suppression_list WHERE phone = :p", p=PHONE) == "OPT_OUT"
    assert _scalar(db, "SELECT do_not_contact FROM lending.contacts WHERE phone = :p", p=PHONE) is True
    assert dialer.removed == [PHONE]
    [ev] = _events(db, PHONE)
    assert (ev.channel, ev.status) == ("sms", "complete")


def test_email_unsubscribe_reaches_lending_on_next_poll(db):
    from src.lending.compliance import poll_fa_opt_outs
    from src.services.email_suppression import suppress_contact

    suppress_contact(db, email=EMAIL, source="unsubscribe_link")
    poll_fa_opt_outs(db, dialer_remover=FakeDialer())

    assert _scalar(db, "SELECT reason FROM lending.suppression_list WHERE email = :e", e=EMAIL) == "OPT_OUT"
    assert _scalar(
        db, "SELECT channel FROM lending.opt_out_events WHERE source_ref = 'unsubscribe_link' ORDER BY id DESC LIMIT 1"
    ) == "email"


def test_poll_is_idempotent(db):
    from src.lending.compliance import poll_fa_opt_outs
    from src.services import sms_compliance

    sms_compliance.handle_inbound(PHONE, "STOP", db)
    dialer = FakeDialer()
    first = poll_fa_opt_outs(db, dialer_remover=dialer)
    second = poll_fa_opt_outs(db, dialer_remover=dialer)

    assert first.new_opt_outs >= 1 and second.new_opt_outs == 0
    assert dialer.removed == [PHONE]
    assert len(_events(db, PHONE)) == 1


def test_bounce_is_not_recorded_as_a_lending_opt_out(db):
    from src.lending.compliance import poll_fa_opt_outs
    from src.services.email_suppression import suppress_contact

    suppress_contact(db, email=EMAIL, source="mandrill_hard_bounce")
    poll_fa_opt_outs(db, dialer_remover=FakeDialer())

    assert _scalar(db, "SELECT count(*) FROM lending.suppression_list WHERE email = :e", e=EMAIL) == 0
    assert _scalar(db, "SELECT count(*) FROM email_opt_outs WHERE email = :e", e=EMAIL) == 1  # FA still blocks


def test_tracerfy_national_dnc_rows_are_not_opt_outs(db):
    from src.lending.compliance import poll_fa_opt_outs

    db.execute(text("INSERT INTO sms_opt_outs (phone, keyword_used, source, opted_out_at) "
                    "VALUES (:p, 'DNC', 'tracerfy_dnc_refresh', now())"), {"p": PHONE})
    poll_fa_opt_outs(db, dialer_remover=FakeDialer())
    assert _scalar(db, "SELECT count(*) FROM lending.suppression_list WHERE phone = :p", p=PHONE) == 0


def test_poll_retries_a_pending_dialer_removal(db):
    from src.lending.compliance import poll_fa_opt_outs, propagate_opt_out

    propagate_opt_out(db, phone=PHONE, source_ref="call_44", dialer_remover=FakeDialer(boom=True))
    dialer = FakeDialer()
    result = poll_fa_opt_outs(db, dialer_remover=dialer)

    assert result.dialer_retried >= 1
    assert dialer.removed == [PHONE]
    [ev] = _events(db, PHONE)
    assert ev.status == "complete" and ev.dialer_removed_at is not None


def test_poll_receive_time_is_the_fa_opt_out_time(db):
    """The 60 s evidence must include the poll lag, so received_at = FA's opted_out_at."""
    from src.lending.compliance import poll_fa_opt_outs

    db.execute(text("INSERT INTO sms_opt_outs (phone, keyword_used, source, opted_out_at) "
                    "VALUES (:p, 'STOP', 'inbound_sms', now() - interval '20 seconds')"), {"p": PHONE})
    poll_fa_opt_outs(db, dialer_remover=FakeDialer())
    [ev] = _events(db, PHONE)
    assert (ev.suppression_at - ev.received_at).total_seconds() >= 19


# ── Review v2 fixes ───────────────────────────────────────────────────────────


def test_unnormalized_fa_phone_is_processed_once(db):
    from src.lending.compliance import poll_fa_opt_outs

    db.execute(text("INSERT INTO sms_opt_outs (phone, keyword_used, source, opted_out_at) "
                    "VALUES ('(813) 555-9001', 'STOP', 'inbound_sms', now())"))
    dialer = FakeDialer()
    poll_fa_opt_outs(db, dialer_remover=dialer)
    poll_fa_opt_outs(db, dialer_remover=dialer)

    assert len(_events(db, PHONE)) == 1
    assert dialer.removed == [PHONE]
    assert _scalar(db, "SELECT 1 FROM lending.suppression_list WHERE phone = :p", p=PHONE) == 1


def test_padded_fa_email_is_processed_once(db):
    from src.lending.compliance import poll_fa_opt_outs

    db.execute(text("INSERT INTO email_opt_outs (email, source, opted_out_at) "
                    "VALUES ('  Stop.Test@Example.com ', 'unsubscribe_link', now())"))
    first = poll_fa_opt_outs(db, dialer_remover=FakeDialer())
    second = poll_fa_opt_outs(db, dialer_remover=FakeDialer())

    assert first.new_opt_outs >= 1 and second.new_opt_outs == 0
    assert _scalar(db, "SELECT 1 FROM lending.suppression_list WHERE email = :e", e=EMAIL) == 1


def test_stop_from_an_already_suppressed_phone_still_logs_and_pulls(db):
    from src.lending.compliance import poll_fa_opt_outs
    from src.services import sms_compliance

    db.execute(text("INSERT INTO lending.suppression_list (phone, reason, source_channel) "
                    "VALUES (:p, 'LITIGATOR', 'tracerfy_scrub')"), {"p": PHONE})
    sms_compliance.handle_inbound(PHONE, "STOP", db)
    dialer = FakeDialer()
    poll_fa_opt_outs(db, dialer_remover=dialer)

    assert [e.channel for e in _events(db, PHONE)] == ["sms"]
    assert dialer.removed == [PHONE]
    assert _scalar(db, "SELECT reason FROM lending.suppression_list WHERE phone = :p", p=PHONE) == "LITIGATOR"


def test_second_poller_skips_while_the_first_holds_the_lock(db):
    from src.lending.compliance import POLL_LOCK_KEY, poll_fa_opt_outs

    other = create_engine(os.environ["DATABASE_URL"]).connect()
    try:
        other.execute(text("SELECT pg_advisory_lock(:k)"), {"k": POLL_LOCK_KEY})
        result = poll_fa_opt_outs(db, dialer_remover=FakeDialer())
        assert result.skipped_locked is True
    finally:
        other.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": POLL_LOCK_KEY})
        other.close()


def test_pending_dialer_removal_is_reported_as_not_within_sla(db, caplog):
    from src.lending.compliance import propagate_opt_out

    with caplog.at_level("INFO"):
        propagate_opt_out(db, phone=PHONE, source_ref="call_90", dialer_remover=FakeDialer(boom=True))
    msgs = [r.getMessage() for r in caplog.records if "opt-out event=" in r.getMessage()]
    assert msgs and all("dialer_pending" in m and "sla_met=no" in m for m in msgs)


def test_errors_never_log_the_phone(db, caplog):
    from src.lending.compliance import propagate_opt_out

    class LeakyDialer:
        def __call__(self, phone, *, reason):
            raise RuntimeError(f"aircall rejected {phone}")

    with caplog.at_level("INFO"):
        propagate_opt_out(db, phone=PHONE, source_ref="call_91", dialer_remover=LeakyDialer())
    assert PHONE not in caplog.text and PHONE[2:] not in caplog.text


def test_opt_out_removals_pass_reason_opt_out(db):
    from src.lending.compliance import propagate_opt_out

    dialer = FakeDialer()
    propagate_opt_out(db, phone=PHONE, source_ref="call_95", dialer_remover=dialer)
    assert dialer.reasons == ["opt_out"]


class DialerRemovalUndecided(Exception):
    """Same class name Dev 3's client raises until O31 is decided."""


def test_undecided_removal_warns_once_not_every_poll(db, caplog):
    from src.lending.compliance import poll_fa_opt_outs, propagate_opt_out

    class Undecided:
        def __call__(self, phone, *, reason):
            raise DialerRemovalUndecided()

    from src.lending.compliance import phone_hash

    def mine(records):  # the shared DB holds real FA opt-outs that get their own first attempt
        tag = phone_hash(PHONE)[:12]
        return [r for r in records if "DialerRemovalUndecided" in r.getMessage() and tag in r.getMessage()]

    with caplog.at_level("DEBUG"):
        propagate_opt_out(db, phone=PHONE, source_ref="call_96", dialer_remover=Undecided())
        first = mine(caplog.records)
        caplog.clear()
        poll_fa_opt_outs(db, dialer_remover=Undecided())
        poll_fa_opt_outs(db, dialer_remover=Undecided())
        retries = mine(caplog.records)

    assert [r.levelname for r in first] == ["WARNING"]
    assert retries and all(r.levelname == "DEBUG" for r in retries)
    assert [e.status for e in _events(db, PHONE)] == ["dialer_pending"]
