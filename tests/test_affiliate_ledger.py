"""Phase 7 — affiliate payout ledger read surface."""
from datetime import date, datetime, timezone

import pytest
from sqlalchemy.orm import sessionmaker

from src.services.signup_engine import create_free_account
from src.services.affiliate_engine import (
    mint_affiliate,
    confirm_referral,
    record_subscription_invoice,
    mark_invoice_reversed,
    run_monthly_payout,
    get_affiliate_ledger,
)

PERIOD = date(2026, 5, 1)
PAID_AT = datetime(2026, 5, 10, 12, 0, tzinfo=timezone.utc)


def _accrued_affiliate(db, phone, invoice_id="in_led"):
    aff = mint_affiliate(db, name="Aff")
    sub = create_free_account(phone, "landing_page", db, affiliate_ref=aff.ref_code)
    confirm_referral(db, sub.id)
    record_subscription_invoice(
        db, stripe_invoice_id=invoice_id, subscriber_id=sub.id,
        amount_collected_cents=19700, period_month=PERIOD, paid_at=PAID_AT,
        is_subscription=True,
    )
    run_monthly_payout(db, PERIOD)
    return aff, sub


def test_ledger_balance_and_lines(fresh_db):
    db = fresh_db
    aff, _ = _accrued_affiliate(db, "+18135552001")
    led = get_affiliate_ledger(db, aff.id)
    assert led["balance_cents"] == 3940
    assert len(led["lines"]) == 1
    assert led["lines"][0]["line_type"] == "accrual"


def test_ledger_nets_clawback(fresh_db):
    db = fresh_db
    aff, _ = _accrued_affiliate(db, "+18135552002", invoice_id="in_led_claw")
    mark_invoice_reversed(db, "in_led_claw", "refund")
    run_monthly_payout(db, PERIOD)
    led = get_affiliate_ledger(db, aff.id)
    assert led["balance_cents"] == 0          # accrual + clawback net to zero
    assert len(led["lines"]) == 2


def test_ledger_empty_for_unknown_affiliate(fresh_db):
    db = fresh_db
    led = get_affiliate_ledger(db, 999_999_999)
    assert led["balance_cents"] == 0
    assert led["lines"] == []


@pytest.fixture
def nonpersist_session(pg_engine):
    if pg_engine is None:
        pytest.skip("DATABASE_URL not configured")
    conn = pg_engine.connect()
    trans = conn.begin()
    sess = sessionmaker(bind=conn)()
    sess.commit = sess.flush
    yield sess
    sess.close()
    trans.rollback()
    conn.close()


def _client(session, admin=True):
    from fastapi.testclient import TestClient
    from src.api.main import app
    from src.api.deps import get_db
    from src.api.admin_router import get_current_admin

    app.dependency_overrides[get_db] = lambda: session
    if admin:
        app.dependency_overrides[get_current_admin] = lambda: {"sub": "admin"}
    else:
        app.dependency_overrides.pop(get_current_admin, None)
    return TestClient(app), app


def test_ledger_endpoint(nonpersist_session):
    aff, _ = _accrued_affiliate(nonpersist_session, "+18135552003", invoice_id="in_led_ep")
    client, app = _client(nonpersist_session, admin=True)
    try:
        resp = client.get(f"/api/admin/affiliates/{aff.id}/ledger")
        assert resp.status_code == 200
        assert resp.json()["balance_cents"] == 3940
    finally:
        app.dependency_overrides.clear()


def test_ledger_endpoint_requires_admin(nonpersist_session):
    client, app = _client(nonpersist_session, admin=False)
    try:
        resp = client.get("/api/admin/affiliates/1/ledger")
        assert resp.status_code in (401, 403)
    finally:
        app.dependency_overrides.clear()
