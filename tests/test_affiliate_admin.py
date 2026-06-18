"""Phase 1 — admin-mint Affiliate + opaque ref_code."""
import pytest
from sqlalchemy.orm import sessionmaker

from src.core.models import Affiliate
from src.services.affiliate_engine import mint_affiliate


@pytest.fixture
def nonpersist_session(pg_engine):
    """A real-PG session whose commit() is neutered to flush, wrapped in a
    transaction rolled back at teardown — so endpoint tests that commit never
    persist to the shared DB."""
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


def test_mint_generates_opaque_ref_code(fresh_db):
    db = fresh_db
    aff = mint_affiliate(db, name="Big Influencer")
    assert aff.id is not None
    # opaque: not a guessable/enumerable integer, and long enough to resist guessing
    assert not aff.ref_code.isdigit()
    assert len(aff.ref_code) >= 16


def test_mint_ref_codes_are_unique(fresh_db):
    db = fresh_db
    a = mint_affiliate(db, name="A")
    b = mint_affiliate(db, name="B")
    assert a.ref_code != b.ref_code


def test_mint_defaults_to_20_percent(fresh_db):
    db = fresh_db
    aff = mint_affiliate(db, name="Default Rate")
    db.refresh(aff)
    from decimal import Decimal
    assert aff.commission_rate == Decimal("0.20")


def test_endpoint_mints_affiliate(nonpersist_session):
    client, app = _client(nonpersist_session, admin=True)
    try:
        resp = client.post("/api/admin/affiliates", json={"name": "Endpoint Influencer"})
        assert resp.status_code == 201
        body = resp.json()
        assert body["ref_code"] and not body["ref_code"].isdigit()
        assert body["commission_rate"] == 0.20
    finally:
        app.dependency_overrides.clear()


def test_endpoint_requires_admin(nonpersist_session):
    client, app = _client(nonpersist_session, admin=False)
    try:
        resp = client.post("/api/admin/affiliates", json={"name": "No Auth"})
        assert resp.status_code in (401, 403)
    finally:
        app.dependency_overrides.clear()


@pytest.mark.parametrize("body", [
    {"name": ""},                              # empty name
    {"name": "   "},                           # whitespace name
    {},                                        # missing name
    {"name": "X", "commission_rate": 2},       # rate > 1
    {"name": "X", "commission_rate": 0},       # rate not > 0
    {"name": "X", "contact_email": "not-an-email"},  # bad email
])
def test_endpoint_rejects_invalid_body(nonpersist_session, body):
    client, app = _client(nonpersist_session, admin=True)
    try:
        resp = client.post("/api/admin/affiliates", json=body)
        assert resp.status_code == 422
    finally:
        app.dependency_overrides.clear()
