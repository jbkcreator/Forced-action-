"""WP-T2-8 Stage D — builder loan-size feature tests."""
from datetime import date
from decimal import Decimal
from unittest.mock import Mock

from sqlalchemy import text

from src.services.builder_patterns import BuilderHit
from src.services.builder_sizing import size_builder_hit


def _hit(**kw) -> BuilderHit:
    base = dict(
        pattern="repeat_builder",
        buyer_entity_id=1,
        principal_name="TEST LLC",
    )
    base.update(kw)
    return BuilderHit(**base)


# ── job_value fallback (no property → no DB touch) ────────────────────────────

def test_job_value_fallback_applies_85pct_ltc():
    hit = _hit(property_id=None, total_job_value=Decimal("400000"))
    result = size_builder_hit(Mock(), hit)
    assert result.sizing_source == "job_value"
    assert result.estimated_loan == Decimal("340000")   # 400k * 0.85
    assert result.confidence == "low"


def test_unknown_when_no_property_and_no_job_value():
    hit = _hit(property_id=None, total_job_value=None)
    result = size_builder_hit(Mock(), hit)
    assert result.sizing_source == "none"
    assert result.estimated_loan is None
    assert result.confidence == "unknown"


# ── quote_ready path (property-backed value → 85% LTC) ────────────────────────

def test_quote_ready_path_uses_property_value(fresh_db):
    db = fresh_db
    prop = db.execute(
        text("INSERT INTO properties (parcel_id, needs_rescore, created_at, updated_at) "
             "VALUES ('SIZE-PARCEL', false, now(), now()) RETURNING id")
    ).fetchone().id
    db.execute(
        text("INSERT INTO financials (property_id, arv) VALUES (:pid, 500000)"),
        {"pid": prop},
    )
    db.flush()

    hit = _hit(property_id=prop, total_job_value=Decimal("100000"))
    result = size_builder_hit(db, hit)

    assert result.sizing_source == "quote_ready"
    # project cost = arv 500k + rehab/job_value 100k = 600k; loan = 85% LTC = 510k
    assert result.estimated_loan == Decimal("510000")


def test_falls_back_to_job_value_when_property_has_no_value(fresh_db):
    db = fresh_db
    prop = db.execute(
        text("INSERT INTO properties (parcel_id, needs_rescore, created_at, updated_at) "
             "VALUES ('NOVAL-PARCEL', false, now(), now()) RETURNING id")
    ).fetchone().id
    # no financials row → _arv_for_property returns None
    db.flush()

    hit = _hit(property_id=prop, total_job_value=Decimal("200000"))
    result = size_builder_hit(db, hit)

    assert result.sizing_source == "job_value"
    assert result.estimated_loan == Decimal("170000")   # 200k * 0.85
