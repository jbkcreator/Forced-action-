"""
Tests for POST /api/admin/manual-invoice/provision — provisioning a
subscriber from a manually-sent Stripe Invoice paid outside Checkout.

Note: the endpoint under test calls db.commit() on success, which escapes
the `fresh_db` fixture's begin_nested()/rollback isolation (a plain
Session.commit() releases past a SAVEPOINT). So every test that ends up with
a truly-committed subscriber row registers its id via `created_subscriber_ids`
for explicit cleanup — belt-and-suspenders on top of the fixture's own
rollback.

Cleanup is scoped to exactly the ids a test registers, never to a LIKE
pattern over stripe_customer_id/county_id: a "cus_test_%"-shaped id is not
proof a row is disposable — a subscriber can pick up real dependents (SMS
sends, consent records, segments) from other subsystems between test runs,
and a broad pattern-match would eventually sweep one of those up.
"""

from __future__ import annotations

import uuid
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from src.core.database import get_db_context
from src.core.models import Subscriber
from src.api.main import app, get_db


def _rand_county_id() -> str:
    return f"testco_{uuid.uuid4().hex[:8]}"


def _rand_customer_id() -> str:
    return f"cus_test_{uuid.uuid4().hex[:14]}"


def _rand_invoice_id() -> str:
    return f"in_test_{uuid.uuid4().hex[:14]}"


@pytest.fixture
def created_subscriber_ids():
    """
    Tests append the id of any subscriber row they know was truly committed
    (survives fresh_db's rollback). Teardown deletes exactly those ids —
    never a name-pattern scan — clearing every FK-referencing table
    discovered from the catalog so a stray future dependent can't reintroduce
    the same blocked-DELETE failure this fixture replaced.
    """
    ids: list[int] = []
    yield ids
    if not ids:
        return
    with get_db_context() as s:
        fk_tables = s.execute(text("""
            SELECT tc.table_name, kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
              ON tc.constraint_name = ccu.constraint_name AND tc.table_schema = ccu.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND ccu.table_name = 'subscribers' AND ccu.column_name = 'id'
        """)).fetchall()

        for table_name, column_name in fk_tables:
            s.execute(
                text(f'DELETE FROM "{table_name}" WHERE "{column_name}" = ANY(:ids)'),
                {"ids": ids},
            )
        s.execute(text("DELETE FROM subscribers WHERE id = ANY(:ids)"), {"ids": ids})
        s.commit()


@pytest.fixture
def client_with_db(fresh_db, monkeypatch):
    monkeypatch.setattr("src.core.redis_client.redis_available", lambda: False)
    app.dependency_overrides[get_db] = lambda: fresh_db
    try:
        yield TestClient(app), fresh_db
    finally:
        app.dependency_overrides.pop(get_db, None)


@pytest.fixture
def admin_token(monkeypatch):
    from config.settings import settings
    from pydantic import SecretStr
    monkeypatch.setattr(settings, "admin_jwt_secret", SecretStr("test-jwt-secret"))
    monkeypatch.setattr(settings, "admin_password", SecretStr("test-admin-pass"))
    monkeypatch.setattr(settings, "stripe_secret_key", SecretStr("sk_live_fake_for_test"))
    monkeypatch.setattr(settings, "stripe_test_mode", False)

    from src.api.admin_router import create_access_token
    return create_access_token({"sub": "admin", "scope": "admin"})


@pytest.fixture
def auth_headers(admin_token):
    return {"Authorization": f"Bearer {admin_token}"}


def _paid_invoice(customer_id: str, subscription_id: str | None = None):
    return {
        "id": "in_test", "status": "paid", "amount_remaining": 0,
        "customer": customer_id, "subscription": subscription_id,
    }


def _customer(email: str, name: str = "Test Customer", phone: str | None = None):
    return {"id": "cus_test", "email": email, "name": name, "phone": phone}


def _make_existing_subscriber(db, *, stripe_customer_id: str, county_id: str) -> int:
    """ORM-constructed so all Python-side column defaults apply (created_at, founding_member, etc.)."""
    sub = Subscriber(
        stripe_customer_id=stripe_customer_id,
        tier="starter",
        vertical="roofing",
        county_id=county_id,
        status="active",
        event_feed_uuid=str(uuid.uuid4()),
    )
    db.add(sub)
    db.flush()
    return sub.id


def test_provision_creates_subscriber_and_locks_zip(client_with_db, auth_headers, created_subscriber_ids):
    client, db = client_with_db
    county_id = _rand_county_id()
    customer_id = _rand_customer_id()
    invoice_id = _rand_invoice_id()

    with (
        patch("stripe.Invoice.retrieve", return_value=_paid_invoice(customer_id, "sub_test_123")),
        patch("stripe.Customer.retrieve", return_value=_customer("newcustomer@example.com")),
    ):
        resp = client.post(
            "/api/admin/manual-invoice/provision",
            json={
                "stripe_invoice_id": invoice_id,
                "tier": "starter",
                "vertical": "roofing",
                "county_id": county_id,
                "zip_codes": ["33601"],
            },
            headers=auth_headers,
        )

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["already_provisioned"] is False
    assert body["zip_codes_locked"] == ["33601"]
    subscriber_id = body["subscriber_id"]
    created_subscriber_ids.append(subscriber_id)  # endpoint commits — survives fresh_db's rollback

    row = db.execute(
        text("SELECT tier, vertical, county_id, status, stripe_customer_id, stripe_subscription_id, "
             "email, signup_source FROM subscribers WHERE id = :id"),
        {"id": subscriber_id},
    ).mappings().first()
    assert row["tier"] == "starter"
    assert row["vertical"] == "roofing"
    assert row["county_id"] == county_id
    assert row["status"] == "active"
    assert row["stripe_customer_id"] == customer_id
    assert row["stripe_subscription_id"] == "sub_test_123"
    assert row["email"] == "newcustomer@example.com"
    assert row["signup_source"] == "admin"

    zip_row = db.execute(
        text("SELECT status, subscriber_id FROM zip_territories WHERE zip_code = '33601' "
             "AND vertical = 'roofing' AND county_id = :c"),
        {"c": county_id},
    ).mappings().first()
    assert zip_row["status"] == "locked"
    assert zip_row["subscriber_id"] == subscriber_id


def test_rejects_unpaid_invoice(client_with_db, auth_headers):
    client, db = client_with_db
    unpaid = {"id": "in_test", "status": "open", "amount_remaining": 29900, "customer": "cus_x", "subscription": None}

    with patch("stripe.Invoice.retrieve", return_value=unpaid):
        resp = client.post(
            "/api/admin/manual-invoice/provision",
            json={
                "stripe_invoice_id": "in_unpaid",
                "tier": "starter",
                "vertical": "roofing",
                "county_id": _rand_county_id(),
                "zip_codes": ["33602"],
            },
            headers=auth_headers,
        )

    assert resp.status_code == 402, resp.text


def test_idempotent_on_existing_stripe_customer_id(client_with_db, auth_headers):
    client, db = client_with_db
    county_id = _rand_county_id()
    customer_id = _rand_customer_id()
    existing_id = _make_existing_subscriber(db, stripe_customer_id=customer_id, county_id=county_id)

    with patch("stripe.Invoice.retrieve", return_value=_paid_invoice(customer_id)):
        resp = client.post(
            "/api/admin/manual-invoice/provision",
            json={
                "stripe_invoice_id": "in_dup",
                "tier": "starter",
                "vertical": "roofing",
                "county_id": county_id,
                "zip_codes": ["33603"],
            },
            headers=auth_headers,
        )

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["already_provisioned"] is True
    assert body["subscriber_id"] == existing_id

    count = db.execute(
        text("SELECT count(*) FROM subscribers WHERE stripe_customer_id = :cid"), {"cid": customer_id}
    ).scalar_one()
    assert count == 1

    zip_row = db.execute(
        text("SELECT status FROM zip_territories WHERE zip_code = '33603' AND vertical = 'roofing' AND county_id = :c"),
        {"c": county_id},
    ).mappings().first()
    assert zip_row is None  # idempotent short-circuit never attempted the ZIP claim


def test_rolls_back_entire_subscriber_when_zip_already_locked(client_with_db, auth_headers, created_subscriber_ids):
    client, db = client_with_db
    county_id = _rand_county_id()
    customer_id = _rand_customer_id()

    # Set up the "other subscriber already holds this ZIP" state on a SEPARATE,
    # truly-committed connection — in production this would be a pre-existing
    # committed row from an earlier request, not part of this request's own
    # transaction, so it must survive this request's db.rollback() untouched.
    with get_db_context() as setup_db:
        other_subscriber_id = _make_existing_subscriber(setup_db, stripe_customer_id=_rand_customer_id(), county_id=county_id)
        created_subscriber_ids.append(other_subscriber_id)
        setup_db.execute(text(
            "INSERT INTO zip_territories (zip_code, vertical, county_id, subscriber_id, status, locked_at, updated_at) "
            "VALUES ('33604', 'roofing', :county, :sub, 'locked', now(), now())"
        ), {"county": county_id, "sub": other_subscriber_id})
        setup_db.commit()

    with (
        patch("stripe.Invoice.retrieve", return_value=_paid_invoice(customer_id)),
        patch("stripe.Customer.retrieve", return_value=_customer("blocked@example.com")),
    ):
        resp = client.post(
            "/api/admin/manual-invoice/provision",
            json={
                "stripe_invoice_id": "in_conflict",
                "tier": "starter",
                "vertical": "roofing",
                "county_id": county_id,
                "zip_codes": ["33604"],
            },
            headers=auth_headers,
        )

    assert resp.status_code == 409, resp.text

    count = db.execute(
        text("SELECT count(*) FROM subscribers WHERE stripe_customer_id = :cid"), {"cid": customer_id}
    ).scalar_one()
    assert count == 0  # the new subscriber must NOT survive a failed ZIP claim

    zip_row = db.execute(
        text("SELECT subscriber_id FROM zip_territories WHERE zip_code = '33604' AND vertical='roofing' AND county_id = :c"),
        {"c": county_id},
    ).mappings().first()
    assert zip_row["subscriber_id"] == other_subscriber_id  # untouched


@pytest.mark.parametrize("field,value", [("tier", "bogus_tier"), ("vertical", "bogus_vertical")])
def test_rejects_invalid_tier_or_vertical(client_with_db, auth_headers, field, value):
    client, db = client_with_db
    payload = {
        "stripe_invoice_id": "in_bad",
        "tier": "starter",
        "vertical": "roofing",
        "county_id": _rand_county_id(),
        "zip_codes": ["33605"],
    }
    payload[field] = value
    resp = client.post("/api/admin/manual-invoice/provision", json=payload, headers=auth_headers)
    assert resp.status_code == 400, resp.text


def test_rejects_malformed_zip(client_with_db, auth_headers):
    client, db = client_with_db
    resp = client.post(
        "/api/admin/manual-invoice/provision",
        json={
            "stripe_invoice_id": "in_bad_zip",
            "tier": "starter",
            "vertical": "roofing",
            "county_id": _rand_county_id(),
            "zip_codes": ["not-a-zip"],
        },
        headers=auth_headers,
    )
    assert resp.status_code == 400, resp.text
