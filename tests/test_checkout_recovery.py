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


def _mute_capture(monkeypatch):
    """run_sweep's capture step scans the live Subscriber table; the cadence
    tests below aren't about capture, so stub it out to keep them from creating
    recovery rows for real dev-DB subscribers (single shared DB)."""
    from src.services import checkout_recovery
    monkeypatch.setattr(checkout_recovery, "find_pre_payment_candidates", lambda db, now=None, limit=200: [])


def test_sweep_is_read_only_when_flag_off(monkeypatch):
    from datetime import datetime, timedelta, timezone
    from config.settings import get_settings
    from src.tasks import checkout_recovery_sweep

    _mute_capture(monkeypatch)
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

    _mute_capture(monkeypatch)
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

    _mute_capture(monkeypatch)
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


def test_sweep_wires_capture_into_start_recovery(monkeypatch):
    """Wiring test only — find_pre_payment_candidates is mocked so this can't
    scan/mutate real Subscriber rows in the shared dev DB (unlike the active-
    row cadence tests above, this step reads the live Subscriber table, not
    a test-only one)."""
    from src.tasks import checkout_recovery_sweep
    from src.services import checkout_recovery as cr

    email = _email()
    monkeypatch.setattr(
        cr, "find_pre_payment_candidates",
        lambda db, now=None, limit=200: [{"subscriber_id": None, "email": email, "phone": None, "created_at": None}],
    )
    try:
        checkout_recovery_sweep.run_sweep(dry_run=True)

        with get_db_context() as db:
            rec = db.query(CheckoutRecovery).filter_by(email=email).first()
        assert rec is not None
        assert rec.source == "pre_payment"
        assert rec.status == "active"
    finally:
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


def _mk_free_subscriber(db, email, *, created_at, phone=None):
    sub = Subscriber(
        stripe_customer_id=f"cus_prepay_{uuid.uuid4().hex[:8]}",
        tier="free", vertical="roofing", county_id="hillsborough",
        event_feed_uuid=f"prepay-{uuid.uuid4().hex[:8]}",
        email=email, phone=phone, created_at=created_at,
    )
    db.add(sub)
    db.flush()
    return sub


def test_find_pre_payment_candidates_picks_up_aged_free_signup():
    """The gap this path exists for: openCheckout creates the free row, but the
    createCheckout call (or the Stripe form itself) never fires — no Stripe
    session ever exists, so checkout.session.expired never fires either."""
    from datetime import datetime, timedelta, timezone
    from src.services import checkout_recovery

    email = _email()
    sub_id = None
    try:
        with get_db_context() as db:
            sub = _mk_free_subscriber(
                db, email,
                created_at=datetime.now(timezone.utc) - timedelta(hours=2),
                phone="+18135550100",
            )
            sub_id = sub.id
            db.commit()

        with get_db_context() as db:
            candidates = checkout_recovery.find_pre_payment_candidates(db)
        matched = next((c for c in candidates if c["email"] == email), None)
        assert matched is not None
        assert matched["subscriber_id"] == sub_id
        assert matched["phone"] == "+18135550100"
    finally:
        _cleanup(email)
        if sub_id is not None:
            with get_db_context() as db:
                db.query(Subscriber).filter_by(id=sub_id).delete(synchronize_session=False)
                db.commit()


def test_find_pre_payment_candidates_excludes_too_fresh_signup():
    """Someone genuinely mid-checkout right now must not be captured yet —
    only PRE_PAYMENT_MIN_AGE (30min) and older."""
    from datetime import datetime, timedelta, timezone
    from src.services import checkout_recovery

    email = _email()
    sub_id = None
    try:
        with get_db_context() as db:
            sub = _mk_free_subscriber(
                db, email, created_at=datetime.now(timezone.utc) - timedelta(minutes=5),
            )
            sub_id = sub.id
            db.commit()

        with get_db_context() as db:
            candidates = checkout_recovery.find_pre_payment_candidates(db)
        assert email not in {c["email"] for c in candidates}
    finally:
        _cleanup(email)
        if sub_id is not None:
            with get_db_context() as db:
                db.query(Subscriber).filter_by(id=sub_id).delete(synchronize_session=False)
                db.commit()


def test_find_pre_payment_candidates_excludes_already_captured_email():
    """A session_expired capture that already created a checkout_recovery row
    for this email must not be double-started by the pre_payment sweep."""
    from datetime import datetime, timedelta, timezone
    from src.services import checkout_recovery

    email = _email()
    sub_id = None
    try:
        with get_db_context() as db:
            sub = _mk_free_subscriber(
                db, email, created_at=datetime.now(timezone.utc) - timedelta(hours=2),
            )
            sub_id = sub.id
            checkout_recovery.start_recovery(db, email=email, source="session_expired")
            db.commit()

        with get_db_context() as db:
            candidates = checkout_recovery.find_pre_payment_candidates(db)
        assert email not in {c["email"] for c in candidates}
    finally:
        _cleanup(email)
        if sub_id is not None:
            with get_db_context() as db:
                db.query(Subscriber).filter_by(id=sub_id).delete(synchronize_session=False)
                db.commit()


def test_find_pre_payment_candidates_excludes_converted_subscriber():
    """A subscriber who upgraded off 'free' is a real conversion, not an
    abandonment — must never be pulled in."""
    from datetime import datetime, timedelta, timezone
    from src.services import checkout_recovery

    email = _email()
    sub_id = None
    try:
        with get_db_context() as db:
            sub = Subscriber(
                stripe_customer_id=f"cus_prepay_{uuid.uuid4().hex[:8]}",
                tier="starter", vertical="roofing", county_id="hillsborough",
                event_feed_uuid=f"prepay-{uuid.uuid4().hex[:8]}",
                email=email, created_at=datetime.now(timezone.utc) - timedelta(hours=2),
            )
            db.add(sub)
            db.flush()
            sub_id = sub.id
            db.commit()

        with get_db_context() as db:
            candidates = checkout_recovery.find_pre_payment_candidates(db)
        assert email not in {c["email"] for c in candidates}
    finally:
        _cleanup(email)
        if sub_id is not None:
            with get_db_context() as db:
                db.query(Subscriber).filter_by(id=sub_id).delete(synchronize_session=False)
                db.commit()
