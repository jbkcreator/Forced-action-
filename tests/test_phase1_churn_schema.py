"""Phase 1 schema tests — churn_predictions table + user_segments columns.

These tests require:
  1. DATABASE_URL env var pointing at a real Postgres instance.
  2. scripts/apply_fa051_ddl.py to have been run against that database.

All tests use the fresh_db fixture (savepoint rollback — no state leaks).
"""

import pytest
from datetime import datetime, timezone
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from src.core.models import ChurnPrediction


# ── helpers ───────────────────────────────────────────────────────────────

def _insert_subscriber(db, suffix: str) -> int:
    """Insert a minimal subscriber and return its id."""
    return db.execute(text("""
        INSERT INTO subscribers
            (stripe_customer_id, tier, vertical, county_id,
             founding_member, status, has_saved_card, auto_mode_enabled,
             created_at, updated_at)
        VALUES
            (:cus, 'starter', 'roofing', 'hillsborough',
             false, 'active', false, false,
             now(), now())
        RETURNING id
    """), {"cus": f"cus_test_churn_{suffix}"}).scalar_one()


# ── tests ─────────────────────────────────────────────────────────────────

def test_churn_predictions_table_exists(fresh_db):
    """Table, expected columns, and composite index are present after apply script."""
    # Table is queryable
    fresh_db.execute(text("SELECT 1 FROM churn_predictions LIMIT 0"))

    cols = {
        r[0]
        for r in fresh_db.execute(text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'churn_predictions'
        """)).fetchall()
    }
    for expected in (
        "id", "subscriber_id", "predicted_at", "churn_risk_score",
        "churn_risk_band", "predicted_inactivity_at", "features",
        "in_holdout", "save_offer_sent_at", "realized_inactive_at",
        "was_correct", "created_at",
    ):
        assert expected in cols, f"Missing column: {expected}"

    idx_rows = fresh_db.execute(text("""
        SELECT indexname FROM pg_indexes
        WHERE tablename = 'churn_predictions'
          AND indexname = 'ix_churn_predictions_sub_predicted'
    """)).fetchall()
    assert len(idx_rows) == 1, "Composite index ix_churn_predictions_sub_predicted missing"


def test_user_segments_churn_columns_nullable(fresh_db):
    """All 5 new churn columns on user_segments are nullable (back-compat)."""
    rows = fresh_db.execute(text("""
        SELECT column_name, is_nullable
        FROM information_schema.columns
        WHERE table_name = 'user_segments'
          AND column_name IN (
              'churn_risk_score', 'churn_risk_band',
              'predicted_inactivity_at', 'churn_risk_reason',
              'churn_risk_updated_at'
          )
    """)).fetchall()

    assert len(rows) == 5, f"Expected 5 churn columns, got {len(rows)}"
    for col_name, is_nullable in rows:
        assert is_nullable == "YES", f"Column {col_name} must be nullable for back-compat"


def test_churn_band_check_constraint(fresh_db):
    """Inserting a bogus churn_risk_band raises an IntegrityError."""
    sub_id = _insert_subscriber(fresh_db, "band_check")
    fresh_db.flush()

    with pytest.raises(IntegrityError):
        fresh_db.execute(text("""
            INSERT INTO churn_predictions
                (subscriber_id, predicted_at, churn_risk_score, churn_risk_band, in_holdout)
            VALUES
                (:sub_id, now(), 55, 'bogus', false)
        """), {"sub_id": sub_id})
        fresh_db.flush()


def test_churn_prediction_cascade_delete(fresh_db):
    """Deleting a subscriber cascades and removes its churn_predictions rows."""
    sub_id = _insert_subscriber(fresh_db, "cascade")
    fresh_db.flush()

    fresh_db.execute(text("""
        INSERT INTO churn_predictions
            (subscriber_id, predicted_at, churn_risk_score, in_holdout)
        VALUES
            (:sub_id, now(), 70, false)
    """), {"sub_id": sub_id})
    fresh_db.flush()

    fresh_db.execute(text("DELETE FROM subscribers WHERE id = :id"), {"id": sub_id})
    fresh_db.flush()

    count = fresh_db.execute(
        text("SELECT COUNT(*) FROM churn_predictions WHERE subscriber_id = :id"),
        {"id": sub_id},
    ).scalar_one()
    assert count == 0


def test_orm_roundtrip(fresh_db):
    """Write and read a ChurnPrediction row including JSONB features."""
    sub_id = _insert_subscriber(fresh_db, "orm_rt")
    fresh_db.flush()

    features = {
        "inactivity_score": 0.82,
        "usage_slope": -0.45,
        "payment_stress": 0.1,
        "engagement_dampener": 0.2,
    }
    now = datetime.now(timezone.utc)
    pred = ChurnPrediction(
        subscriber_id=sub_id,
        predicted_at=now,
        churn_risk_score=74,
        churn_risk_band="high",
        in_holdout=False,
        features=features,
        predicted_inactivity_at=now,
    )
    fresh_db.add(pred)
    fresh_db.flush()

    loaded = fresh_db.get(ChurnPrediction, pred.id)
    assert loaded is not None
    assert loaded.churn_risk_score == 74
    assert loaded.churn_risk_band == "high"
    assert loaded.in_holdout is False
    assert loaded.features["inactivity_score"] == pytest.approx(0.82)
    assert loaded.features["usage_slope"] == pytest.approx(-0.45)
    assert loaded.was_correct is None
    assert loaded.save_offer_sent_at is None
