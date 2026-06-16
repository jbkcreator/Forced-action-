"""Tests for the financing-intent scoring engine (Sprint S1).

Each test uses fresh_db (real Postgres, SAVEPOINT-based rollback).
All tests use a unique county_id so the keyset page scan only touches
test-seeded properties and never touches prod data.
"""
from __future__ import annotations

import uuid

import pytest
from sqlalchemy import text as sa_text

from src.services.financing_intent_engine import score_properties_for_financing


# ── Helpers ────────────────────────────────────────────────────────────────────


def _uid() -> str:
    return uuid.uuid4().hex[:10]


def _county() -> str:
    return f"test_{_uid()}"


def _make_property(db, county_id: str) -> int:
    row = db.execute(sa_text("""
        INSERT INTO properties (parcel_id, zip, county_id, created_at, updated_at)
        VALUES (:pid, '33601', :county, NOW(), NOW())
        RETURNING id
    """), {"pid": _uid(), "county": county_id}).first()
    db.flush()
    return row.id


def _seed_deed(
    db,
    property_id: int,
    *,
    days_ago: int,
    confidence: float = 0.9,
    deed_type: str = "Warranty Deed",
) -> int:
    row = db.execute(sa_text("""
        INSERT INTO deeds
            (property_id, instrument_number, record_date, deed_type, match_confidence, county_id)
        VALUES
            (:pid, :inum, CURRENT_DATE - :days, :dtype, :conf, 'hillsborough')
        RETURNING id
    """), {
        "pid": property_id, "inum": _uid(),
        "days": days_ago, "dtype": deed_type, "conf": confidence,
    }).first()
    db.flush()
    return row.id


def _seed_permit(
    db,
    property_id: int,
    *,
    permit_type: str,
    status: str = "active",
    enforcement: bool = False,
) -> int:
    row = db.execute(sa_text("""
        INSERT INTO building_permits
            (property_id, permit_number, permit_type, status, is_enforcement_permit, county_id)
        VALUES
            (:pid, :pnum, :ptype, :status, :enforce, 'hillsborough')
        RETURNING id
    """), {
        "pid": property_id, "pnum": _uid(),
        "ptype": permit_type, "status": status, "enforce": enforcement,
    }).first()
    db.flush()
    return row.id


def _seed_foreclosure(
    db,
    property_id: int,
    *,
    days_ago: int = 30,
    auction_date=None,
    case_status: str = "Pending",
) -> int:
    row = db.execute(sa_text("""
        INSERT INTO foreclosures
            (property_id, case_number, lis_pendens_date, auction_date, case_status, county_id)
        VALUES
            (:pid, :cn, CURRENT_DATE - :days, :ad, :cs, 'hillsborough')
        RETURNING id
    """), {
        "pid": property_id, "cn": _uid(),
        "days": days_ago, "ad": auction_date, "cs": case_status,
    }).first()
    db.flush()
    return row.id


def _seed_divorce(
    db,
    property_id: int,
    *,
    confidence: float = 0.9,
    case_status: str = "Active",
    days_ago: int = 30,
) -> int:
    row = db.execute(sa_text("""
        INSERT INTO legal_proceedings
            (property_id, record_type, case_number, filing_date, case_status, match_confidence, county_id)
        VALUES
            (:pid, 'Divorce', :cn, CURRENT_DATE - :days, :cs, :conf, 'hillsborough')
        RETURNING id
    """), {
        "pid": property_id, "cn": _uid(),
        "days": days_ago, "cs": case_status, "conf": confidence,
    }).first()
    db.flush()
    return row.id


def _seed_financials(
    db,
    property_id: int,
    *,
    equity_pct=None,
    est_mortgage_bal=None,
) -> int:
    row = db.execute(sa_text("""
        INSERT INTO financials (property_id, equity_pct, est_mortgage_bal, county_id)
        VALUES (:pid, :eq, :mtg, 'hillsborough')
        RETURNING id
    """), {"pid": property_id, "eq": equity_pct, "mtg": est_mortgage_bal}).first()
    db.flush()
    return row.id


def _seed_owner(db, property_id: int, *, ownership_years: int) -> int:
    row = db.execute(sa_text("""
        INSERT INTO owners (property_id, owner_name, ownership_years, county_id)
        VALUES (:pid, 'Test Owner', :yrs, 'hillsborough')
        RETURNING id
    """), {"pid": property_id, "yrs": ownership_years}).first()
    db.flush()
    return row.id


def _get_score(db, property_id: int):
    return db.execute(sa_text("""
        SELECT * FROM financing_intent_scores
        WHERE property_id = :pid ORDER BY id DESC LIMIT 1
    """), {"pid": property_id}).first()


def _run(db, county_id: str, *, dry_run: bool = False, rescore_all: bool = True):
    return score_properties_for_financing(
        db, county_id=county_id, dry_run=dry_run, rescore_all=rescore_all,
    )


# ── Signal 1: Fresh Deed ───────────────────────────────────────────────────────


def test_fresh_deed_0_30_scores_25(fresh_db):
    cty = _county()
    pid = _make_property(fresh_db, cty)
    _seed_deed(fresh_db, pid, days_ago=10)
    _run(fresh_db, cty)
    row = _get_score(fresh_db, pid)
    assert row is not None
    assert row.signal_scores["fresh_deed_0_30_days"] == 25
    assert row.signal_flags["fresh_deed_0_30_days"] is True


def test_fresh_deed_31_60_scores_15(fresh_db):
    cty = _county()
    pid = _make_property(fresh_db, cty)
    _seed_deed(fresh_db, pid, days_ago=45)
    _run(fresh_db, cty)
    row = _get_score(fresh_db, pid)
    assert row is not None
    assert row.signal_scores["fresh_deed_31_60_days"] == 15
    assert "fresh_deed_0_30_days" not in row.signal_flags


def test_deed_older_60_excluded(fresh_db):
    """SQL pre-filter drops it; no score row produced."""
    cty = _county()
    pid = _make_property(fresh_db, cty)
    _seed_deed(fresh_db, pid, days_ago=90)
    _run(fresh_db, cty)
    assert _get_score(fresh_db, pid) is None


def test_deed_low_confidence_excluded(fresh_db):
    """SQL pre-filter drops it (match_confidence < 0.75)."""
    cty = _county()
    pid = _make_property(fresh_db, cty)
    _seed_deed(fresh_db, pid, days_ago=10, confidence=0.5)
    _run(fresh_db, cty)
    assert _get_score(fresh_db, pid) is None


def test_deed_tax_deed_excluded(fresh_db):
    """Fetched by SQL but excluded in Python scoring."""
    cty = _county()
    pid = _make_property(fresh_db, cty)
    _seed_deed(fresh_db, pid, days_ago=10, deed_type="Tax Deed")
    _run(fresh_db, cty)
    assert _get_score(fresh_db, pid) is None


# ── Signal 2: Active Permit ────────────────────────────────────────────────────


def test_structural_permit_scores_30(fresh_db):
    cty = _county()
    pid = _make_property(fresh_db, cty)
    _seed_permit(fresh_db, pid, permit_type="structural repair", status="active")
    _run(fresh_db, cty)
    row = _get_score(fresh_db, pid)
    assert row is not None
    assert row.signal_scores["active_structural_permit"] == 30


def test_roofing_permit_scores_20(fresh_db):
    cty = _county()
    pid = _make_property(fresh_db, cty)
    _seed_permit(fresh_db, pid, permit_type="roof replacement", status="open")
    _run(fresh_db, cty)
    row = _get_score(fresh_db, pid)
    assert row is not None
    assert row.signal_scores["active_roofing_permit"] == 20


def test_structural_beats_roofing(fresh_db):
    """When both are present, only the higher-weight structural flag is set."""
    cty = _county()
    pid = _make_property(fresh_db, cty)
    _seed_permit(fresh_db, pid, permit_type="roof replacement", status="open")
    _seed_permit(fresh_db, pid, permit_type="structural repair", status="active")
    _run(fresh_db, cty)
    row = _get_score(fresh_db, pid)
    assert row is not None
    assert "active_structural_permit" in row.signal_flags
    assert "active_roofing_permit" not in row.signal_flags


def test_permit_complete_excluded(fresh_db):
    cty = _county()
    pid = _make_property(fresh_db, cty)
    _seed_permit(fresh_db, pid, permit_type="structural repair", status="complete")
    _run(fresh_db, cty)
    assert _get_score(fresh_db, pid) is None


def test_enforcement_permit_excluded(fresh_db):
    """SQL pre-filter drops enforcement permits."""
    cty = _county()
    pid = _make_property(fresh_db, cty)
    _seed_permit(fresh_db, pid, permit_type="structural repair", status="active", enforcement=True)
    _run(fresh_db, cty)
    assert _get_score(fresh_db, pid) is None


# ── Signal 3: Early Lis Pendens ────────────────────────────────────────────────


def test_early_lp_scores_25(fresh_db):
    cty = _county()
    pid = _make_property(fresh_db, cty)
    _seed_foreclosure(fresh_db, pid, days_ago=30, auction_date=None, case_status="Pending")
    _run(fresh_db, cty)
    row = _get_score(fresh_db, pid)
    assert row is not None
    assert row.signal_scores["early_lis_pendens"] == 25


def test_lp_with_auction_excluded(fresh_db):
    from datetime import date, timedelta
    cty = _county()
    pid = _make_property(fresh_db, cty)
    _seed_foreclosure(fresh_db, pid, days_ago=30, auction_date=date.today() + timedelta(days=30))
    _run(fresh_db, cty)
    row = _get_score(fresh_db, pid)
    assert row is None or "early_lis_pendens" not in row.signal_flags
    if row:
        assert row.excluded_reasons.get("early_lis_pendens") == "auction_date_set"


def test_lp_late_stage_excluded(fresh_db):
    cty = _county()
    pid = _make_property(fresh_db, cty)
    _seed_foreclosure(fresh_db, pid, days_ago=30, case_status="Sold")
    _run(fresh_db, cty)
    row = _get_score(fresh_db, pid)
    assert row is None or "early_lis_pendens" not in row.signal_flags
    if row:
        assert "early_lis_pendens" in row.excluded_reasons


def test_lp_beyond_90_days_excluded(fresh_db):
    """SQL pre-filter drops LP beyond lookback window."""
    cty = _county()
    pid = _make_property(fresh_db, cty)
    _seed_foreclosure(fresh_db, pid, days_ago=120)
    _run(fresh_db, cty)
    assert _get_score(fresh_db, pid) is None


# ── Signal 4: Divorce ──────────────────────────────────────────────────────────


def test_divorce_scores_20(fresh_db):
    cty = _county()
    pid = _make_property(fresh_db, cty)
    _seed_divorce(fresh_db, pid, confidence=0.9, case_status="Active")
    _run(fresh_db, cty)
    row = _get_score(fresh_db, pid)
    assert row is not None
    assert row.signal_scores["divorce_match"] == 20


def test_divorce_low_conf_excluded(fresh_db):
    cty = _county()
    pid = _make_property(fresh_db, cty)
    _seed_divorce(fresh_db, pid, confidence=0.5)
    _run(fresh_db, cty)
    assert _get_score(fresh_db, pid) is None


def test_divorce_dismissed_excluded(fresh_db):
    cty = _county()
    pid = _make_property(fresh_db, cty)
    _seed_divorce(fresh_db, pid, confidence=0.9, case_status="Dismissed")
    _run(fresh_db, cty)
    row = _get_score(fresh_db, pid)
    assert row is None or "divorce_match" not in row.signal_flags
    if row:
        assert "divorce_match" in row.excluded_reasons


# ── Signal 5: Equity Proxy ─────────────────────────────────────────────────────


def test_equity_strong_scores_25(fresh_db):
    cty = _county()
    pid = _make_property(fresh_db, cty)
    _seed_financials(fresh_db, pid, equity_pct=60)
    _run(fresh_db, cty)
    row = _get_score(fresh_db, pid)
    assert row is not None
    assert row.signal_scores["equity_proxy_strong"] == 25


def test_equity_medium_scores_15(fresh_db):
    cty = _county()
    pid = _make_property(fresh_db, cty)
    _seed_financials(fresh_db, pid, equity_pct=35)
    _run(fresh_db, cty)
    row = _get_score(fresh_db, pid)
    assert row is not None
    assert row.signal_scores["equity_proxy_medium"] == 15


def test_equity_tenure_fallback_scores_15(fresh_db):
    """Long-tenure owner with no mortgage qualifies for equity_proxy_medium."""
    cty = _county()
    pid = _make_property(fresh_db, cty)
    _seed_financials(fresh_db, pid, equity_pct=None, est_mortgage_bal=None)
    _seed_owner(fresh_db, pid, ownership_years=12)
    _run(fresh_db, cty)
    row = _get_score(fresh_db, pid)
    assert row is not None
    assert row.signal_scores["equity_proxy_medium"] == 15


# ── Score assembly ─────────────────────────────────────────────────────────────


def test_score_cap_100(fresh_db):
    """All 5 signals fire → raw=125, capped at 100."""
    cty = _county()
    pid = _make_property(fresh_db, cty)
    _seed_deed(fresh_db, pid, days_ago=10)                                # 25
    _seed_permit(fresh_db, pid, permit_type="structural repair")          # 30
    _seed_foreclosure(fresh_db, pid, days_ago=30)                         # 25
    _seed_divorce(fresh_db, pid)                                          # 20
    _seed_financials(fresh_db, pid, equity_pct=60)                        # 25
    _run(fresh_db, cty)
    row = _get_score(fresh_db, pid)
    assert row is not None
    assert float(row.financing_intent_score) == 100.0


def test_tier_high(fresh_db):
    """deed(25) + structural(30) + LP(25) = 80 → high."""
    cty = _county()
    pid = _make_property(fresh_db, cty)
    _seed_deed(fresh_db, pid, days_ago=10)
    _seed_permit(fresh_db, pid, permit_type="structural repair")
    _seed_foreclosure(fresh_db, pid, days_ago=30)
    _run(fresh_db, cty)
    row = _get_score(fresh_db, pid)
    assert row is not None
    assert row.intent_tier == "high"


def test_tier_medium(fresh_db):
    """deed31-60(15) + roofing(20) + divorce(20) = 55 → medium."""
    cty = _county()
    pid = _make_property(fresh_db, cty)
    _seed_deed(fresh_db, pid, days_ago=45)
    _seed_permit(fresh_db, pid, permit_type="roof replacement")
    _seed_divorce(fresh_db, pid)
    _run(fresh_db, cty)
    row = _get_score(fresh_db, pid)
    assert row is not None
    assert row.intent_tier == "medium"


def test_tier_low(fresh_db):
    """equity_medium(15) only → low."""
    cty = _county()
    pid = _make_property(fresh_db, cty)
    _seed_financials(fresh_db, pid, equity_pct=35)
    _run(fresh_db, cty)
    row = _get_score(fresh_db, pid)
    assert row is not None
    assert row.intent_tier == "low"


def test_recommended_product_structural(fresh_db):
    cty = _county()
    pid = _make_property(fresh_db, cty)
    _seed_permit(fresh_db, pid, permit_type="structural renovation")
    _run(fresh_db, cty)
    row = _get_score(fresh_db, pid)
    assert row is not None
    assert row.recommended_product == "renovation_capital"


def test_recommended_product_divorce(fresh_db):
    cty = _county()
    pid = _make_property(fresh_db, cty)
    _seed_divorce(fresh_db, pid)
    _run(fresh_db, cty)
    row = _get_score(fresh_db, pid)
    assert row is not None
    assert row.recommended_product == "buyout_refi"


# ── Persist behaviour ──────────────────────────────────────────────────────────


def test_dry_run_no_rows(fresh_db):
    cty = _county()
    pid = _make_property(fresh_db, cty)
    _seed_deed(fresh_db, pid, days_ago=10)
    result = _run(fresh_db, cty, dry_run=True)
    assert result["new"] == 0
    assert result["updated"] == 0
    assert _get_score(fresh_db, pid) is None


def test_upsert_same_day_no_dup(fresh_db):
    """Two scoring runs same day → still exactly 1 row."""
    cty = _county()
    pid = _make_property(fresh_db, cty)
    _seed_deed(fresh_db, pid, days_ago=10)
    _run(fresh_db, cty, rescore_all=True)
    # Re-establish savepoint so the second commit doesn't hit the outer txn
    fresh_db.begin_nested()
    _run(fresh_db, cty, rescore_all=True)
    count = db_execute_scalar(fresh_db, """
        SELECT COUNT(*) FROM financing_intent_scores WHERE property_id = :pid
    """, {"pid": pid})
    assert count == 1


def test_rescore_all_overwrites(fresh_db):
    """Second run with rescore_all=True updates the existing row."""
    cty = _county()
    pid = _make_property(fresh_db, cty)
    _seed_deed(fresh_db, pid, days_ago=10)
    _run(fresh_db, cty, rescore_all=True)
    fresh_db.begin_nested()
    result2 = _run(fresh_db, cty, rescore_all=True)
    assert result2["updated"] >= 1


def test_no_signal_no_row(fresh_db):
    """Property with zero qualifying signal rows produces no FIS row."""
    cty = _county()
    pid = _make_property(fresh_db, cty)
    _run(fresh_db, cty)
    assert _get_score(fresh_db, pid) is None


# ── Batch query count ──────────────────────────────────────────────────────────


def test_query_count_batch_not_per_property(fresh_db):
    """
    3 properties → signal queries must NOT scale to 3×5=15.
    The engine issues 5 signal queries for the entire batch, regardless of size.
    """
    cty = _county()
    for _ in range(3):
        pid = _make_property(fresh_db, cty)
        _seed_deed(fresh_db, pid, days_ago=10)

    call_log: list = []
    _orig = fresh_db.execute

    def _counting(*args, **kwargs):
        call_log.append(1)
        return _orig(*args, **kwargs)

    fresh_db.execute = _counting
    try:
        _run(fresh_db, cty, rescore_all=True)
    finally:
        fresh_db.execute = _orig

    # 1 properties page + 5 signals + 1 today_rows + 1 INSERT ≤ 10
    assert len(call_log) <= 12
    # Definitely not per-property (3 × 5 = 15)
    assert len(call_log) < 3 * 5


# ── Utility ────────────────────────────────────────────────────────────────────


def db_execute_scalar(db, sql: str, params: dict):
    return db.execute(sa_text(sql), params).scalar()
