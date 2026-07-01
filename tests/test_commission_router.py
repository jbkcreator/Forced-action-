"""Commission router + pool tests (WS-C gaps #3/#5/#6 and review fixes).

Aligned to dev's property_id lane model and nested `property` response shape.
- serializer fee-gate (money hidden while fee_config_flag OFF)
- RBAC (broker sees only own entries)
- dispute is append-only (amounts untouched)
- /api/lanes/pool excludes guess leads (issue #1)
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import text

from src.api.commission_router import _serialize_entry


# ---------------------------------------------------------------------------
# Pure serializer — fee gate (#3) + shape (#6)
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
        property_id=101,
        property_address="789 Pine Rd, Brandon FL",
        property_county="hillsborough",
        fee_config_flag=True,
    )
    base.update(over)
    return SimpleNamespace(**base)


def test_serialize_visible_when_flag_on():
    d = _serialize_entry(_row(fee_config_flag=True))
    assert d["property"]["address"] == "789 Pine Rd, Brandon FL"
    assert d["property"]["county"] == "hillsborough"
    assert d["property"]["property_id"] == "101"
    assert d["fee_config_flag"] is True
    assert d["gross_amount_cents"] == 500000
    assert d["net_lines"][1]["amount_cents"] == 250000


def test_serialize_gated_when_flag_off():
    d = _serialize_entry(_row(fee_config_flag=False))
    assert d["gross_amount_cents"] is None
    assert d["net_lines"] is None
    assert d["status"] == "posted"
    assert d["entry_id"] is not None
    assert d["fee_config_flag"] is False


# ---------------------------------------------------------------------------
# Seed helpers (dev property_id model)
# ---------------------------------------------------------------------------

def _property(session, *, with_contact=True) -> int:
    pid = session.execute(
        text("INSERT INTO properties (parcel_id, county_id, created_at, updated_at) "
             "VALUES (:pc,'hillsborough',NOW(),NOW()) RETURNING id"),
        {"pc": f"P-{uuid.uuid4().hex[:10]}"},
    ).scalar()
    if with_contact:
        session.execute(
            text("INSERT INTO owners (property_id, owner_name, phone_1) VALUES (:p,'Owner','8135551234')"),
            {"p": pid},
        )
    return pid


def _score(session, property_id: int, *, guess: bool):
    session.execute(
        text("""INSERT INTO distress_scores
                (property_id, final_cds_score, lead_tier, distress_types, urgency_level,
                 vertical_scores, lead_confidence, is_guess_lead, score_date)
                VALUES (:p, 80, 'Platinum', '{"lien": 1}'::jsonb, 'High',
                        '{}'::jsonb, 0.9, :g, NOW())"""),
        {"p": property_id, "g": guess},
    )


def _broker(session, email: str) -> str:
    return str(session.execute(
        text("INSERT INTO brokers (email, name) VALUES (:e,'B') RETURNING broker_id"),
        {"e": email},
    ).scalar())


def _lane(session, property_id: int, *, broker_id: str | None = None) -> str:
    return str(session.execute(
        text("""INSERT INTO lanes (property_id, lane_type, current_stage, assigned_broker_id)
                VALUES (:p,'distressed-payoff','entered', :b) RETURNING lane_id"""),
        {"p": property_id, "b": broker_id},
    ).scalar())


def _commission_for(session, broker_email: str):
    """Assigned lane → closed_won transition → posted commission. Returns (broker_id, entry_id)."""
    prop = _property(session)
    broker_id = _broker(session, broker_email)
    lane_id = _lane(session, prop, broker_id=broker_id)
    tid = session.execute(
        text("""INSERT INTO broker_transitions (lane_id, broker_id, from_state, to_state, reason_code, actor)
                VALUES (:l,:b,'committed','closed_won','funded',:b) RETURNING transition_id"""),
        {"l": lane_id, "b": broker_id},
    ).scalar()
    from src.services.commission_ledger import post_commission
    entry_id = post_commission(session, str(tid), 500000, "platform_50_broker_50")
    return broker_id, entry_id


# ---------------------------------------------------------------------------
# RBAC (#5) + append-only
# ---------------------------------------------------------------------------

_BROKER_SECRET = "test-broker-jwt-secret"


def _mint_broker_token(broker_id: str) -> str:
    from jose import jwt
    return jwt.encode({"sub": broker_id, "type": "broker_access"}, _BROKER_SECRET, algorithm="HS256")


def test_rbac_broker_sees_only_own(fresh_db, monkeypatch):
    from fastapi.testclient import TestClient
    from pydantic import SecretStr
    from config.settings import get_settings
    from src.api.main import app
    from src.api.deps import get_db

    monkeypatch.setattr(get_settings(), "broker_jwt_secret", SecretStr(_BROKER_SECRET))

    b1, e1 = _commission_for(fresh_db, f"{uuid.uuid4().hex[:8]}@a.test")
    b2, e2 = _commission_for(fresh_db, f"{uuid.uuid4().hex[:8]}@b.test")

    app.dependency_overrides[get_db] = lambda: (yield fresh_db)
    try:
        client = TestClient(app, raise_server_exceptions=False)
        resp = client.get("/api/commissions", headers={"Authorization": f"Bearer {_mint_broker_token(b1)}"})
        assert resp.status_code == 200
        ids = {c["entry_id"] for c in resp.json()["commissions"]}
        assert e1 in ids and e2 not in ids
    finally:
        app.dependency_overrides.pop(get_db, None)


def test_dispute_is_append_only(fresh_db):
    import unittest.mock as m
    from src.services.commission_ledger import dispute_entry

    _b, entry_id = _commission_for(fresh_db, f"{uuid.uuid4().hex[:8]}@c.test")
    before = fresh_db.execute(
        text("SELECT gross_amount_cents, net_lines FROM commission_ledger WHERE entry_id = CAST(:e AS uuid)"),
        {"e": entry_id},
    ).fetchone()
    dispute_entry(fresh_db, entry_id, actor="admin")
    after = fresh_db.execute(
        text("SELECT gross_amount_cents, net_lines, status FROM commission_ledger WHERE entry_id = CAST(:e AS uuid)"),
        {"e": entry_id},
    ).fetchone()
    assert after.status == "disputed"
    assert after.gross_amount_cents == before.gross_amount_cents
    assert after.net_lines == before.net_lines


# ---------------------------------------------------------------------------
# Pool excludes guess leads (issue #1) — live API route
# ---------------------------------------------------------------------------

def test_pool_excludes_guess_leads(fresh_db):
    from fastapi.testclient import TestClient
    from src.api.main import app
    from src.api.deps import get_db
    from src.services.broker_auth import get_current_broker

    good_prop = _property(fresh_db); _score(fresh_db, good_prop, guess=False)
    guess_prop = _property(fresh_db); _score(fresh_db, guess_prop, guess=True)
    good_lane = _lane(fresh_db, good_prop)      # unclaimed
    guess_lane = _lane(fresh_db, guess_prop)    # unclaimed

    app.dependency_overrides[get_db] = lambda: (yield fresh_db)
    app.dependency_overrides[get_current_broker] = lambda: {"broker_id": str(uuid.uuid4())}
    try:
        client = TestClient(app, raise_server_exceptions=False)
        resp = client.get("/api/lanes/pool", params={"limit": 200})
        assert resp.status_code == 200
        lane_ids = {l["lane_id"] for l in resp.json()["lanes"]}
        assert good_lane in lane_ids
        assert guess_lane not in lane_ids   # guess lead must be filtered from the pool
    finally:
        app.dependency_overrides.pop(get_db, None)
        app.dependency_overrides.pop(get_current_broker, None)
