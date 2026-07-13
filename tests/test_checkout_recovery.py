"""
Task 7 — abandoned-checkout recovery service.

Core guarantees under test:
  - starting recovery creates an active row and holds the contact out of the
    non-buyer nurture drip (no double-contact),
  - the hold is released back to nurture when recovery fails,
  - a paid conversion closes recovery,
  - capture + suppression are idempotent on replay.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import text

from src.core.database import get_db_context
from src.core.models import CheckoutRecovery, NonBuyerNurtureSequence, Subscriber


def _pg_url():
    try:
        from config.settings import get_settings
        return str(get_settings().database_url) if get_settings().database_url else None
    except Exception:
        return None


pytestmark = pytest.mark.skipif(_pg_url() is None, reason="requires real Postgres")


def _email() -> str:
    return f"recov-{uuid.uuid4().hex[:10]}@example.com"


def test_next_action_cadence():
    """Cadence is a pure function of touch count + timestamps — no DB."""
    from datetime import datetime, timedelta, timezone
    from src.services import checkout_recovery as cr

    t0 = datetime(2026, 7, 9, 12, 0, tzinfo=timezone.utc)

    # touch 1: nothing until FIRST_TOUCH_DELAY (1h) elapses, then 'send'
    assert cr.next_action(0, t0, None, t0 + timedelta(minutes=30)) is None
    assert cr.next_action(0, t0, None, t0 + timedelta(hours=1, minutes=1)) == "send"

    # touch 2: 24h after the first touch
    first = t0 + timedelta(hours=1)
    assert cr.next_action(1, t0, first, first + timedelta(hours=12)) is None
    assert cr.next_action(1, t0, first, first + timedelta(hours=25)) == "send"

    # after MAX_TOUCHES: 'fail' once the final grace window elapses
    second = first + timedelta(hours=24)
    assert cr.next_action(2, t0, second, second + timedelta(hours=12)) is None
    assert cr.next_action(2, t0, second, second + timedelta(hours=25)) == "fail"


def _cleanup(email: str):
    with get_db_context() as db:
        db.query(CheckoutRecovery).filter(CheckoutRecovery.email == email).delete(synchronize_session=False)
        db.query(NonBuyerNurtureSequence).filter(NonBuyerNurtureSequence.email == email).delete(synchronize_session=False)
        db.commit()


def test_build_resume_url():
    from src.services import checkout_recovery as cr
    url = cr.build_resume_url("https://app.example.com/", {"county_id": "hillsborough", "vertical": "roofing", "zip_codes": ["33601"]})
    assert url.startswith("https://app.example.com/?")
    assert "county_id=hillsborough" in url
    assert "vertical=roofing" in url
    assert url.endswith("#pricing")
    # no context → bare funnel link, still valid
    assert cr.build_resume_url("https://app.example.com", None) == "https://app.example.com/#pricing"


def _insert_active(db, email, *, touches_sent=0, started_at=None, last_touch_at=None, phone=None):
    from datetime import datetime, timezone
    db.add(CheckoutRecovery(
        email=email, source="session_expired", status="active",
        touches_sent=touches_sent,
        started_at=started_at or datetime.now(timezone.utc),
        last_touch_at=last_touch_at, phone=phone,
        resume_context={"county_id": "hillsborough", "vertical": "roofing"},
    ))


def test_sweep_is_read_only_when_flag_off(monkeypatch):
    from datetime import datetime, timedelta, timezone
    from config.settings import get_settings
    from src.tasks import checkout_recovery_sweep

    monkeypatch.setattr(get_settings(), "checkout_recovery_enabled", False, raising=False)
    email = _email()
    try:
        with get_db_context() as db:
            _insert_active(db, email, started_at=datetime.now(timezone.utc) - timedelta(hours=3))
            db.commit()

        checkout_recovery_sweep.run_sweep()

        with get_db_context() as db:
            rec = db.query(CheckoutRecovery).filter_by(email=email).first()
        assert rec.touches_sent == 0        # nothing advanced
        assert rec.status == "active"
    finally:
        _cleanup(email)


def test_sweep_sends_and_advances_when_flag_on(monkeypatch):
    from datetime import datetime, timedelta, timezone
    from config.settings import get_settings
    from src.tasks import checkout_recovery_sweep
    import src.services.email as email_mod

    monkeypatch.setattr(get_settings(), "checkout_recovery_enabled", True, raising=False)
    sent = {}
    monkeypatch.setattr(email_mod, "send_email", lambda **kw: sent.update(kw) or True)

    email = _email()
    try:
        with get_db_context() as db:
            _insert_active(db, email, started_at=datetime.now(timezone.utc) - timedelta(hours=3))
            db.commit()

        checkout_recovery_sweep.run_sweep()

        with get_db_context() as db:
            rec = db.query(CheckoutRecovery).filter_by(email=email).first()
        assert rec.touches_sent == 1
        assert rec.first_touch_at is not None
        assert sent.get("to") == email
    finally:
        _cleanup(email)


def test_sweep_fails_exhausted_row_when_flag_on(monkeypatch):
    from datetime import datetime, timedelta, timezone
    from config.settings import get_settings
    from src.tasks import checkout_recovery_sweep

    monkeypatch.setattr(get_settings(), "checkout_recovery_enabled", True, raising=False)
    email = _email()
    try:
        with get_db_context() as db:
            _insert_active(
                db, email, touches_sent=2,
                started_at=datetime.now(timezone.utc) - timedelta(days=3),
                last_touch_at=datetime.now(timezone.utc) - timedelta(hours=25),
            )
            db.commit()

        checkout_recovery_sweep.run_sweep()

        with get_db_context() as db:
            rec = db.query(CheckoutRecovery).filter_by(email=email).first()
        assert rec.status == "failed"
        assert rec.closed_at is not None
        # (nurture-release on fail is covered by test_mark_failed_releases_the_email_back_to_nurture)
    finally:
        _cleanup(email)


def test_sweep_does_not_advance_when_no_channel_delivers(monkeypatch):
    """Issue 2: a total send failure must NOT advance the touch count — the row
    stays due so the next sweep retries instead of burning the attempt."""
    from datetime import datetime, timedelta, timezone
    from config.settings import get_settings
    from src.tasks import checkout_recovery_sweep
    import src.services.email as email_mod

    monkeypatch.setattr(get_settings(), "checkout_recovery_enabled", True, raising=False)

    def _boom(**kw):
        raise RuntimeError("email provider down")
    monkeypatch.setattr(email_mod, "send_email", _boom)

    email = _email()
    try:
        with get_db_context() as db:
            _insert_active(db, email, started_at=datetime.now(timezone.utc) - timedelta(hours=3))  # no phone → email-only
            db.commit()

        result = checkout_recovery_sweep.run_sweep()

        with get_db_context() as db:
            rec = db.query(CheckoutRecovery).filter_by(email=email).first()
        assert rec.touches_sent == 0        # not advanced
        assert rec.status == "active"       # still due
        assert result["undelivered"] == 1
        assert result["sent"] == 0
    finally:
        _cleanup(email)


def test_sweep_skips_a_row_locked_by_another_worker(monkeypatch):
    """Issue 3: FOR UPDATE SKIP LOCKED — a row already claimed by a concurrent
    transaction is skipped, so overlapping cron runs never double-send."""
    from datetime import datetime, timedelta, timezone
    from config.settings import get_settings
    from src.core.database import get_db_session
    from src.tasks import checkout_recovery_sweep
    import src.services.email as email_mod

    monkeypatch.setattr(get_settings(), "checkout_recovery_enabled", True, raising=False)
    calls = []
    monkeypatch.setattr(email_mod, "send_email", lambda **kw: calls.append(kw.get("to")) or True)

    email = _email()
    holder = get_db_session()
    try:
        with get_db_context() as db:
            _insert_active(db, email, started_at=datetime.now(timezone.utc) - timedelta(hours=3))
            db.commit()

        # Simulate a concurrent worker holding the row's lock.
        holder.execute(
            text("SELECT id FROM checkout_recovery WHERE email=:e FOR UPDATE"), {"e": email}
        ).first()

        result = checkout_recovery_sweep.run_sweep()

        assert email not in calls          # skipped, not sent
        with get_db_context() as db:
            rec = db.query(CheckoutRecovery).filter_by(email=email).first()
        assert rec.touches_sent == 0       # untouched by this worker
    finally:
        holder.rollback()
        holder.close()
        _cleanup(email)


def test_start_recovery_creates_active_row_and_suppresses_nurture():
    from src.services import checkout_recovery

    email = _email()
    try:
        with get_db_context() as db:
            checkout_recovery.start_recovery(
                db, email=email, source="session_expired",
                resume_context={"tier": "starter", "vertical": "roofing", "county_id": "hillsborough", "zip_codes": ["33601"]},
            )
            db.commit()

        with get_db_context() as db:
            rec = db.query(CheckoutRecovery).filter_by(email=email).first()
            nurture = db.query(NonBuyerNurtureSequence).filter_by(email=email).first()

        assert rec is not None
        assert rec.status == "active"
        assert rec.source == "session_expired"
        assert rec.touches_sent == 0
        # Held out of nurture — non-'eligible' status excludes it from find_candidates.
        assert nurture is not None
        assert nurture.status == "in_recovery"
    finally:
        _cleanup(email)


def test_start_recovery_is_idempotent_on_replay():
    from src.services import checkout_recovery

    email = _email()
    try:
        with get_db_context() as db:
            checkout_recovery.start_recovery(db, email=email, source="session_expired")
            db.commit()
        with get_db_context() as db:
            # bump a touch, then replay start — must not reset or duplicate
            db.query(CheckoutRecovery).filter_by(email=email).update({"touches_sent": 1})
            db.commit()
        with get_db_context() as db:
            checkout_recovery.start_recovery(db, email=email, source="session_expired")
            db.commit()

        with get_db_context() as db:
            rows = db.query(CheckoutRecovery).filter_by(email=email).all()
        assert len(rows) == 1
        assert rows[0].touches_sent == 1  # not reset
    finally:
        _cleanup(email)


def test_recovering_email_is_excluded_from_nurture_candidates():
    """The no-double-contact guarantee: an email in active recovery must not
    surface in the non-buyer nurture drip, even when a free subscriber row for
    that email would otherwise make it a nurture candidate."""
    from src.services import checkout_recovery, non_buyer_nurture

    email = _email()
    sub_id = None
    try:
        with get_db_context() as db:
            sub = Subscriber(
                stripe_customer_id=f"cus_recov_{uuid.uuid4().hex[:8]}",
                tier="free", vertical="roofing", county_id="hillsborough",
                event_feed_uuid=f"recov-{uuid.uuid4().hex[:8]}",
                email=email,
            )
            db.add(sub)
            db.flush()
            sub_id = sub.id
            checkout_recovery.start_recovery(db, email=email, source="pre_payment", subscriber_id=sub_id)
            db.commit()

        with get_db_context() as db:
            candidates = non_buyer_nurture.find_candidates(db, limit=5000)
        assert email not in {c["email"] for c in candidates}
    finally:
        _cleanup(email)
        if sub_id is not None:
            with get_db_context() as db:
                db.query(Subscriber).filter_by(id=sub_id).delete(synchronize_session=False)
                db.commit()


def test_mark_failed_releases_the_email_back_to_nurture():
    from src.services import checkout_recovery

    email = _email()
    try:
        with get_db_context() as db:
            checkout_recovery.start_recovery(db, email=email, source="session_expired")
            db.commit()
        with get_db_context() as db:
            checkout_recovery.mark_failed(db, email)
            db.commit()

        with get_db_context() as db:
            rec = db.query(CheckoutRecovery).filter_by(email=email).first()
            nurture = db.query(NonBuyerNurtureSequence).filter_by(email=email).first()
        assert rec.status == "failed"
        assert rec.closed_at is not None
        # released — nurture can now pick it up
        assert nurture.status == "eligible"
    finally:
        _cleanup(email)


def test_mark_recovered_closes_the_sequence():
    from src.services import checkout_recovery

    email = _email()
    try:
        with get_db_context() as db:
            checkout_recovery.start_recovery(db, email=email, source="session_expired")
            db.commit()
        with get_db_context() as db:
            checkout_recovery.mark_recovered(db, email)
            db.commit()

        with get_db_context() as db:
            rec = db.query(CheckoutRecovery).filter_by(email=email).first()
        assert rec.status == "recovered"
        assert rec.closed_at is not None
    finally:
        _cleanup(email)


def test_sweep_no_longer_captures_bare_free_signups(monkeypatch):
    """Issue 1: recovery is captured at real checkout-start (/api/checkout),
    never inferred from a free signup. The sweep must create no recovery row
    for a free subscriber that never started checkout."""
    from config.settings import get_settings
    from src.tasks import checkout_recovery_sweep

    monkeypatch.setattr(get_settings(), "checkout_recovery_enabled", True, raising=False)
    email = _email()
    sub_id = None
    try:
        with get_db_context() as db:
            sub = Subscriber(
                stripe_customer_id=f"cus_free_{uuid.uuid4().hex[:8]}",
                tier="free", vertical="roofing", county_id="hillsborough",
                event_feed_uuid=f"free-{uuid.uuid4().hex[:8]}",
                email=email,
            )
            db.add(sub)
            db.flush()
            sub_id = sub.id
            db.commit()

        checkout_recovery_sweep.run_sweep()

        with get_db_context() as db:
            rec = db.query(CheckoutRecovery).filter_by(email=email).first()
        assert rec is None  # bare free signup is never captured
    finally:
        _cleanup(email)
        if sub_id is not None:
            with get_db_context() as db:
                db.query(Subscriber).filter_by(id=sub_id).delete(synchronize_session=False)
                db.commit()


# ── Review fixes: phone-normalize, nurture double-contact gate, lead_pack ──────

def test_start_recovery_normalizes_phone_before_storing():
    """Standards fix: every phone write must go through phone_utils.normalize."""
    from src.services import checkout_recovery

    email = _email()
    try:
        with get_db_context() as db:
            checkout_recovery.start_recovery(
                db, email=email, source="session_expired", phone="(813) 555-0142",
            )
            db.commit()
        with get_db_context() as db:
            rec = db.query(CheckoutRecovery).filter_by(email=email).first()
        assert rec.phone == "+18135550142"
    finally:
        _cleanup(email)


def test_start_recovery_skipped_when_nurture_enrolled():
    """Spec fix: don't double-contact someone the nurture drip is already sending to."""
    from src.services import checkout_recovery
    from src.core.models import NonBuyerNurtureSequence
    from datetime import datetime, timezone

    email = _email()
    try:
        with get_db_context() as db:
            db.add(NonBuyerNurtureSequence(
                email=email, source="free_signup",
                captured_at=datetime.now(timezone.utc), status="enrolled",
            ))
            db.commit()
        with get_db_context() as db:
            row = checkout_recovery.start_recovery(db, email=email, source="session_expired")
            db.commit()
            assert row is None
        with get_db_context() as db:
            assert db.query(CheckoutRecovery).filter_by(email=email).first() is None
    finally:
        _cleanup(email)


def test_start_recovery_skipped_when_nurture_opted_out():
    """Compliance: never start recovery for a contact who unsubscribed/bounced."""
    from src.services import checkout_recovery
    from src.core.models import NonBuyerNurtureSequence
    from datetime import datetime, timezone

    email = _email()
    try:
        with get_db_context() as db:
            db.add(NonBuyerNurtureSequence(
                email=email, source="free_signup",
                captured_at=datetime.now(timezone.utc), status="unsubscribed",
            ))
            db.commit()
        with get_db_context() as db:
            row = checkout_recovery.start_recovery(db, email=email, source="pre_payment")
            db.commit()
            assert row is None
        with get_db_context() as db:
            assert db.query(CheckoutRecovery).filter_by(email=email).first() is None
    finally:
        _cleanup(email)


def test_lead_pack_recovery_does_not_touch_nurture():
    """lead_pack abandoners are existing paying subscribers, not nurture
    candidates — recovery must not create/flip a nurture row for them."""
    from src.services import checkout_recovery

    email = _email()
    try:
        with get_db_context() as db:
            row = checkout_recovery.start_recovery(
                db, email=email, source="lead_pack",
                resume_context={"kind": "lead_pack", "feed_uuid": "abc", "lead_pack_zip": "33601"},
            )
            db.commit()
            assert row is not None
            assert row.source == "lead_pack"
        with get_db_context() as db:
            assert db.query(CheckoutRecovery).filter_by(email=email).first() is not None
            assert db.query(NonBuyerNurtureSequence).filter_by(email=email).first() is None
    finally:
        _cleanup(email)


def test_build_resume_url_lead_pack_targets_dashboard():
    from src.services import checkout_recovery as cr
    url = cr.build_resume_url("https://app.example.com/", {
        "kind": "lead_pack", "feed_uuid": "feed-123", "lead_pack_zip": "33601",
    })
    assert url == "https://app.example.com/dashboard/feed-123?lead_pack_zip=33601"


# ── PR #127 fixes: honour real send returns + unsubscribe + opt-out close ──────

def test_send_email_false_leaves_touch_due(monkeypatch):
    """Issue 1: send_email returning False (non-exceptional decline, e.g. SMTP
    off) must NOT advance the touch — the row stays due, counted undelivered."""
    from datetime import datetime, timedelta, timezone
    from config.settings import get_settings
    from src.tasks import checkout_recovery_sweep
    import src.services.email as email_mod

    monkeypatch.setattr(get_settings(), "checkout_recovery_enabled", True, raising=False)
    monkeypatch.setattr(email_mod, "send_email", lambda **kw: False)  # declined, no exception

    email = _email()
    try:
        with get_db_context() as db:
            _insert_active(db, email, started_at=datetime.now(timezone.utc) - timedelta(hours=3))  # email-only
            db.commit()

        result = checkout_recovery_sweep.run_sweep()

        with get_db_context() as db:
            rec = db.query(CheckoutRecovery).filter_by(email=email).first()
        assert rec.touches_sent == 0
        assert rec.status == "active"
        assert result["undelivered"] == 1
        assert result["sent"] == 0
    finally:
        _cleanup(email)


def test_both_channels_false_leaves_touch_due(monkeypatch):
    """Issue 1: email False + SMS False (e.g. marketing SMS with no
    subscriber_id) → nothing delivered → row stays due, not completed."""
    from datetime import datetime, timedelta, timezone
    from config.settings import get_settings
    from src.tasks import checkout_recovery_sweep
    import src.services.email as email_mod
    import src.services.sms_compliance as sms_mod

    monkeypatch.setattr(get_settings(), "checkout_recovery_enabled", True, raising=False)
    monkeypatch.setattr(email_mod, "send_email", lambda **kw: False)
    monkeypatch.setattr(sms_mod, "send_sms", lambda *a, **kw: False)  # compliance decline

    email = _email()
    try:
        with get_db_context() as db:
            _insert_active(db, email, phone="+18135550123",
                           started_at=datetime.now(timezone.utc) - timedelta(hours=3))
            db.commit()

        result = checkout_recovery_sweep.run_sweep()

        with get_db_context() as db:
            rec = db.query(CheckoutRecovery).filter_by(email=email).first()
        assert rec.touches_sent == 0
        assert rec.status == "active"
        assert result["undelivered"] == 1
    finally:
        _cleanup(email)


def test_recovery_email_carries_unsubscribe(monkeypatch):
    """Issue 2: promotional recovery email must include a signed unsubscribe URL
    in both the List-Unsubscribe header and the body."""
    from datetime import datetime, timedelta, timezone
    from config.settings import get_settings
    from src.tasks import checkout_recovery_sweep
    import src.services.email as email_mod

    monkeypatch.setattr(get_settings(), "checkout_recovery_enabled", True, raising=False)
    captured = {}
    monkeypatch.setattr(email_mod, "send_email", lambda **kw: captured.update(kw) or True)

    email = _email()
    try:
        with get_db_context() as db:
            _insert_active(db, email, started_at=datetime.now(timezone.utc) - timedelta(hours=3))
            db.commit()

        checkout_recovery_sweep.run_sweep()

        assert "/api/email/unsubscribe?token=" in captured.get("list_unsubscribe_url", "")
        assert "Unsubscribe:" in captured.get("body_text", "")
    finally:
        _cleanup(email)


def test_opted_out_email_closes_row_instead_of_retrying(monkeypatch):
    """Issue 2: once the email is opted out, don't retry forever — close the
    recovery row so no further touches are attempted."""
    from datetime import datetime, timedelta, timezone
    from config.settings import get_settings
    from src.core.models import EmailOptOut
    from src.tasks import checkout_recovery_sweep
    import src.services.email as email_mod

    monkeypatch.setattr(get_settings(), "checkout_recovery_enabled", True, raising=False)
    monkeypatch.setattr(email_mod, "send_email", lambda **kw: False)  # suppressed → declined

    email = _email()
    try:
        with get_db_context() as db:
            _insert_active(db, email, started_at=datetime.now(timezone.utc) - timedelta(hours=3))
            db.add(EmailOptOut(email=email, source="test"))
            db.commit()

        result = checkout_recovery_sweep.run_sweep()

        with get_db_context() as db:
            rec = db.query(CheckoutRecovery).filter_by(email=email).first()
        assert rec.status == "failed"          # closed, not left due
        assert result["failed"] >= 1
    finally:
        _cleanup(email)
        with get_db_context() as db:
            db.query(EmailOptOut).filter_by(email=email).delete(synchronize_session=False)
            db.commit()
