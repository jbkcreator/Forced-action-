"""T-B13-01 — one-tap outcome contract on the delivered-lead card.

The card posts outcome_state (closed/dead/pending). A 'dead' tap requires a
reason; the reason's fault class (lead_fault / buyer_neutral) is persisted so the
dispatcher (T-B13-02) can route only lead-fault into the CDS retune. pending is
non-terminal — it records without firing win/loss learning-loop effects. The
legacy deal_size_bucket path still works.
"""
import uuid
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from src.core.models import CoraSuppression, DealOutcome, Property, Subscriber


@pytest.fixture(scope="module")
def client():
    from src.api.main import app
    return TestClient(app)


def _mk_sub_and_prop(fresh_db):
    uid = uuid.uuid4().hex[:8]
    sub = Subscriber(
        stripe_customer_id=f"cus_b13_{uid}",
        tier="starter", vertical="roofing", county_id="hillsborough",
        event_feed_uuid=f"b13-{uid}", email=f"b13_{uid}@example.com",
        name=f"B13 {uid}", status="active",
    )
    fresh_db.add(sub)
    fresh_db.flush()
    prop = Property(
        parcel_id=f"P-B13-{uid}", address=f"13 Loop #{uid}",
        city="Tampa", state="FL", zip="33601", county_id="hillsborough",
    )
    fresh_db.add(prop)
    fresh_db.flush()
    fresh_db.commit()
    return sub, prop


def _cleanup(fresh_db, sub, prop):
    from sqlalchemy import text
    fresh_db.execute(DealOutcome.__table__.delete().where(DealOutcome.subscriber_id == sub.id))
    fresh_db.execute(CoraSuppression.__table__.delete().where(CoraSuppression.subscriber_id == sub.id))
    fresh_db.execute(text("DELETE FROM referral_prompt_funnel WHERE subscriber_id = :sid"), {"sid": sub.id})
    fresh_db.delete(sub)
    fresh_db.delete(prop)
    fresh_db.commit()


def _post(client, sub, prop, **fields):
    body = {"feed_uuid": sub.event_feed_uuid, "property_id": prop.id, **fields}
    with patch("src.services.win_graphic.generate", return_value=None):
        return client.post("/api/deal-capture", json=body)


def test_closed_writes_won_stage(client, fresh_db):
    sub, prop = _mk_sub_and_prop(fresh_db)
    resp = _post(client, sub, prop, outcome_state="closed", deal_amount=8000)
    assert resp.status_code == 201
    row = fresh_db.get(DealOutcome, resp.json()["deal_id"])
    assert row.outcome_state == "closed"
    assert row.pipeline_stage == "closed_won"
    assert row.dead_reason is None and row.reason_fault_class is None
    _cleanup(fresh_db, sub, prop)


def test_dead_lead_fault_reason_persists_fault_class(client, fresh_db):
    sub, prop = _mk_sub_and_prop(fresh_db)
    resp = _post(client, sub, prop, outcome_state="dead", dead_reason="wrong_owner")
    assert resp.status_code == 201
    row = fresh_db.get(DealOutcome, resp.json()["deal_id"])
    assert row.outcome_state == "dead"
    assert row.pipeline_stage == "closed_lost"
    assert row.dead_reason == "wrong_owner"
    assert row.reason_fault_class == "lead_fault"
    _cleanup(fresh_db, sub, prop)


def test_dead_buyer_neutral_reason_is_score_protected_class(client, fresh_db):
    sub, prop = _mk_sub_and_prop(fresh_db)
    resp = _post(client, sub, prop, outcome_state="dead", dead_reason="too_busy")
    assert resp.status_code == 201
    row = fresh_db.get(DealOutcome, resp.json()["deal_id"])
    assert row.reason_fault_class == "buyer_neutral"
    _cleanup(fresh_db, sub, prop)


def test_dead_requires_reason(client, fresh_db):
    sub, prop = _mk_sub_and_prop(fresh_db)
    resp = _post(client, sub, prop, outcome_state="dead")
    assert resp.status_code == 422
    _cleanup(fresh_db, sub, prop)


def test_dead_rejects_unknown_reason(client, fresh_db):
    sub, prop = _mk_sub_and_prop(fresh_db)
    resp = _post(client, sub, prop, outcome_state="dead", dead_reason="mood")
    assert resp.status_code == 422
    _cleanup(fresh_db, sub, prop)


def test_pending_is_non_terminal_no_autopsy(client, fresh_db):
    sub, prop = _mk_sub_and_prop(fresh_db)
    with patch("src.services.loss_autopsy.run_loss_autopsy") as loss, \
         patch("src.services.snapshot_service.capture_snapshot") as snap:
        resp = _post(client, sub, prop, outcome_state="pending")
    assert resp.status_code == 201
    row = fresh_db.get(DealOutcome, resp.json()["deal_id"])
    assert row.outcome_state == "pending"
    assert row.pipeline_stage == "negotiation"
    loss.assert_not_called()
    snap.assert_not_called()
    _cleanup(fresh_db, sub, prop)


def test_legacy_bucket_path_still_works(client, fresh_db):
    sub, prop = _mk_sub_and_prop(fresh_db)
    resp = _post(client, sub, prop, deal_size_bucket="5_10k", deal_amount=6000)
    assert resp.status_code == 201
    row = fresh_db.get(DealOutcome, resp.json()["deal_id"])
    assert row.pipeline_stage == "closed_won"
    assert row.outcome_state is None
    _cleanup(fresh_db, sub, prop)


def test_missing_both_outcome_and_bucket_rejected(client, fresh_db):
    sub, prop = _mk_sub_and_prop(fresh_db)
    resp = _post(client, sub, prop, deal_amount=6000)
    assert resp.status_code == 422
    _cleanup(fresh_db, sub, prop)
