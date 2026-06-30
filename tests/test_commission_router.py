"""Commission router — fee-gate, response shape, and RBAC tests (WS-C, gaps #3/#5/#6).

The serializer tests are pure (no DB/HTTP). RBAC is exercised end-to-end via
TestClient + minted JWTs against the shared Postgres (fresh_db).
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import text

from src.api.commission_router import _serialize_entry


# ---------------------------------------------------------------------------
# Pure serializer — fee gate (#3) + flat shape the UI binds to (#6)
# ---------------------------------------------------------------------------

def _row(**over):
    base = dict(
        entry_id=uuid.uuid4(),
        lane_id=uuid.uuid4(),
        broker_id=uuid.uuid4(),
        broker_name="Jane Broker",
        gross_amount_cents=500000,
        net_lines=[
            {"party": "platform", "amount_cents": 250000},
            {"party": "broker", "amount_cents": 250000},
        ],
        split_config_id="platform_50_broker_50",
        status="posted",
        trigger_transition_id=uuid.uuid4(),
        posted_at=datetime(2026, 6, 1, tzinfo=timezone.utc),
        prospect_id=uuid.uuid4(),
        prospect_address="789 Pine Rd, Brandon FL",
        prospect_county="hillsborough",
        fee_config_flag=True,
    )
    base.update(over)
    return SimpleNamespace(**base)


def test_serialize_flat_shape_visible_when_flag_on():
    d = _serialize_entry(_row(fee_config_flag=True))
    # flat fields the UI reads (c.prospect_address etc.)
    assert d["prospect_address"] == "789 Pine Rd, Brandon FL"
    assert d["prospect_county"] == "hillsborough"
    assert "prospect_id" in d
    assert d["fee_config_flag"] is True
    assert d["broker_name"] == "Jane Broker"
    # flag ON → money visible
    assert d["gross_amount_cents"] == 500000
    assert d["net_lines"][1]["amount_cents"] == 250000


def test_serialize_gated_when_flag_off():
    d = _serialize_entry(_row(fee_config_flag=False))
    # flag OFF (pre-RESPA) → both money fields hidden, row still present
    assert d["gross_amount_cents"] is None
    assert d["net_lines"] is None
    assert d["status"] == "posted"
    assert d["entry_id"] is not None
    assert d["fee_config_flag"] is False


# ---------------------------------------------------------------------------
# RBAC — broker sees only own; admin sees all (#5.2)
# ---------------------------------------------------------------------------

def _mint_broker_token(broker_id: str) -> str:
    from jose import jwt
    from config.settings import get_settings
    secret = get_settings().admin_jwt_secret.get_secret_value()
    return jwt.encode({"sub": broker_id, "type": "broker_access"}, secret, algorithm="HS256")


def _seed_commissionable_lane(session, broker_email: str):
    """Build prospect→lane→broker→closed_won transition→commission entry. Returns (broker_id, entry_id)."""
    prop_id = session.execute(
        text("INSERT INTO properties (parcel_id, county_id, created_at, updated_at) "
             "VALUES (:pc,'hillsborough',NOW(),NOW()) RETURNING id"),
        {"pc": f"P-{uuid.uuid4().hex[:10]}"},
    ).scalar()
    prospect_id = session.execute(
        text("INSERT INTO prospects (prospect_id, property_id, contactability_state) "
             "VALUES (gen_random_uuid(), :p, 'contactable') RETURNING prospect_id"),
        {"p": prop_id},
    ).scalar()
    broker_id = session.execute(
        text("INSERT INTO brokers (email, name) VALUES (:e,'B') RETURNING broker_id"),
        {"e": broker_email},
    ).scalar()
    lane_id = session.execute(
        text("INSERT INTO lanes (prospect_id, lane_type, current_stage) "
             "VALUES (:p,'distressed-payoff','entered') RETURNING lane_id"),
        {"p": str(prospect_id)},
    ).scalar()
    tid = session.execute(
        text("INSERT INTO broker_transitions (lane_id, prospect_id, broker_id, from_state, to_state, reason_code, actor) "
             "VALUES (:l,:p,:b,'committed','closed_won','funded',:actor) RETURNING transition_id"),
        {"l": str(lane_id), "p": str(prospect_id), "b": str(broker_id), "actor": str(broker_id)},
    ).scalar()
    from src.services.commission_ledger import post_commission
    import unittest.mock as m
    with m.patch("src.services.commission_ledger.emit_event"):
        entry_id = post_commission(session, str(tid), 500000, "platform_50_broker_50")
    return str(broker_id), entry_id


def test_dispute_is_append_only(fresh_db):
    """Dispute changes only status — gross/net_lines on the original row are untouched."""
    import unittest.mock as m
    from src.services.commission_ledger import dispute_entry

    b1, entry_id = _seed_commissionable_lane(fresh_db, f"{uuid.uuid4().hex[:8]}@c.test")
    before = fresh_db.execute(
        text("SELECT gross_amount_cents, net_lines, status FROM commission_ledger "
             "WHERE entry_id = CAST(:e AS uuid)"),
        {"e": entry_id},
    ).fetchone()

    with m.patch("src.services.commission_ledger.emit_event"):
        dispute_entry(fresh_db, entry_id, actor="admin")

    after = fresh_db.execute(
        text("SELECT gross_amount_cents, net_lines, status FROM commission_ledger "
             "WHERE entry_id = CAST(:e AS uuid)"),
        {"e": entry_id},
    ).fetchone()

    assert after.status == "disputed"            # only status changed
    assert after.gross_amount_cents == before.gross_amount_cents
    assert after.net_lines == before.net_lines


def test_rbac_broker_sees_only_own(fresh_db):
    from fastapi.testclient import TestClient
    from src.api.main import app
    from src.api.deps import get_db

    b1, e1 = _seed_commissionable_lane(fresh_db, f"{uuid.uuid4().hex[:8]}@a.test")
    b2, e2 = _seed_commissionable_lane(fresh_db, f"{uuid.uuid4().hex[:8]}@b.test")

    app.dependency_overrides[get_db] = lambda: (yield fresh_db)
    try:
        client = TestClient(app, raise_server_exceptions=False)
        tok = _mint_broker_token(b1)
        resp = client.get("/api/commissions", headers={"Authorization": f"Bearer {tok}"})
        assert resp.status_code == 200
        ids = {c["entry_id"] for c in resp.json()["commissions"]}
        assert e1 in ids
        assert e2 not in ids  # broker 1 must not see broker 2's entry
    finally:
        app.dependency_overrides.pop(get_db, None)
