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


_CONSTRUCTION_LTV = Decimal("0.90")


def _mk_property_with_financials(db, parcel, *, arv=None, assessed=None, last_sale=None):
    prop = db.execute(
        text("INSERT INTO properties (parcel_id, needs_rescore, created_at, updated_at) "
             "VALUES (:p, false, now(), now()) RETURNING id"),
        {"p": parcel},
    ).fetchone().id
    db.execute(
        text("INSERT INTO financials (property_id, arv, assessed_value_mkt, last_sale_price) "
             "VALUES (:pid, :arv, :assessed, :sale)"),
        {"pid": prop, "arv": arv, "assessed": assessed, "sale": last_sale},
    )
    db.flush()
    return prop


# ── quote_ready path (cost basis + rehab → LTC, capped by LTV × ARV) ──────────

def test_quote_ready_path_uses_cost_basis_and_rehab(fresh_db):
    db = fresh_db
    # cost basis = assessed 400k; rehab = job_value 100k → project cost 500k
    # ltc_cap = 0.85 × 500k = 425k; ltv_cap = 0.90 × 500k ARV = 450k → min = 425k
    prop = _mk_property_with_financials(db, "SIZE-OK", arv=500000, assessed=400000)
    result = size_builder_hit(db, _hit(property_id=prop, total_job_value=Decimal("100000")))

    assert result.sizing_source == "quote_ready"
    assert result.estimated_loan == Decimal("425000")
    assert result.estimated_loan <= _CONSTRUCTION_LTV * Decimal("500000")   # never exceeds LTV cap


def test_quote_ready_loan_never_exceeds_ltv_cap(fresh_db):
    db = fresh_db
    # cost basis 500k + rehab 300k = 800k → ltc_cap 0.85×800k = 680k
    # but ltv_cap 0.90 × 500k ARV = 450k → loan MUST be capped at 450k
    prop = _mk_property_with_financials(db, "SIZE-CAP", arv=500000, assessed=500000)
    result = size_builder_hit(db, _hit(property_id=prop, total_job_value=Decimal("300000")))

    assert result.sizing_source == "quote_ready"
    assert result.estimated_loan == Decimal("450000")               # LTV-capped, not 680k
    assert result.estimated_loan <= _CONSTRUCTION_LTV * Decimal("500000")


def test_no_cost_basis_falls_back_to_job_value_not_arv(fresh_db):
    db = fresh_db
    # only ARV present, no assessed / sale → ARV must NOT be reused as cost basis
    # (that bypassed the LTV cap). Fall back to job_value sizing.
    prop = _mk_property_with_financials(db, "SIZE-ARVONLY", arv=500000)
    result = size_builder_hit(db, _hit(property_id=prop, total_job_value=Decimal("200000")))

    assert result.sizing_source == "job_value"
    assert result.estimated_loan == Decimal("170000")   # 200k * 0.85


def test_falls_back_to_job_value_when_property_has_no_value(fresh_db):
    db = fresh_db
    prop = db.execute(
        text("INSERT INTO properties (parcel_id, needs_rescore, created_at, updated_at) "
             "VALUES ('NOVAL-PARCEL', false, now(), now()) RETURNING id")
    ).fetchone().id
    # no financials row → no cost basis
    db.flush()

    hit = _hit(property_id=prop, total_job_value=Decimal("200000"))
    result = size_builder_hit(db, hit)

    assert result.sizing_source == "job_value"
    assert result.estimated_loan == Decimal("170000")   # 200k * 0.85
