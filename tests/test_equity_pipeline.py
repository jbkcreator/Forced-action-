"""Sprint 4.4 — mortgage rollup + equity compute.

Proves the previously-missing link: deed mortgage_amount now flows into
financials.est_mortgage_bal, and the equity formula subtracts it. Runs against
real Postgres (fresh_db, rolled back).
"""
from datetime import date, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

from sqlalchemy import text

from src.core.models import Deed, Financial, Property
from src.services.cds_engine import MultiVerticalScorer
from src.services.equity_compute import compute_equity_profiles
from src.services.mortgage_aggregator import aggregate_mortgage_balances


def _prop_with_deeds(*deeds):
    """Minimal duck-typed property carrying only the lists _collect_signals reads."""
    return SimpleNamespace(
        parcel_id="EQ-SIG", owner=None, deeds=list(deeds),
        code_violations=[], legal_and_liens=[], legal_proceedings=[],
        tax_delinquencies=[], foreclosures=[], building_permits=[], incidents=[],
    )


def test_mortgage_record_is_not_a_deed_transfer_signal():
    # A recently-recorded mortgage (mortgage_amount set, no sale_price) must NOT
    # count as an ownership transfer — otherwise the dead-lead gate would wrongly
    # kill a refinancing homeowner, the ideal distressed lead.
    scorer = MultiVerticalScorer(session=MagicMock())
    recent = date.today() - timedelta(days=5)
    mortgage = SimpleNamespace(record_date=recent, sale_price=None, mortgage_amount=250000)
    sale = SimpleNamespace(record_date=recent, sale_price=300000, mortgage_amount=None)

    assert not any(s["type"] == "deed_transfers"
                   for s in scorer._collect_signals(_prop_with_deeds(mortgage)))
    assert any(s["type"] == "deed_transfers"
               for s in scorer._collect_signals(_prop_with_deeds(sale)))


def _prop(db, parcel):
    p = Property(parcel_id=parcel, zip="00461", county_id="hillsborough", address=f"{parcel} St")
    db.add(p)
    db.flush()
    return p


def _fin(db, pid, **kw):
    db.add(Financial(property_id=pid, county_id="hillsborough", **kw))


def test_mortgage_rollup_uses_latest_deed(fresh_db):
    p = _prop(fresh_db, "EQ-LATEST")
    # Older + newer mortgage; the newer one is the current debt estimate.
    fresh_db.add(Deed(property_id=p.id, instrument_number="EQ-L-1",
                      record_date=date(2019, 3, 1), mortgage_amount=150000))
    fresh_db.add(Deed(property_id=p.id, instrument_number="EQ-L-2",
                      record_date=date(2023, 8, 1), mortgage_amount=210000))
    _fin(fresh_db, p.id)
    fresh_db.flush()

    aggregate_mortgage_balances(session=fresh_db)

    bal = fresh_db.execute(
        text("SELECT est_mortgage_bal FROM financials WHERE property_id=:p"), {"p": p.id}
    ).scalar()
    assert float(bal) == 210000.0  # latest, not the sum (360000)


def test_equity_compute_subtracts_mortgage_and_liens(fresh_db):
    p = _prop(fresh_db, "EQ-FORMULA")
    _fin(fresh_db, p.id, assessed_value_mkt=300000, est_mortgage_bal=200000, total_lien_amount=10000)
    fresh_db.flush()

    compute_equity_profiles(session=fresh_db)

    r = fresh_db.execute(
        text("SELECT est_equity, equity_pct, total_debt FROM financials WHERE property_id=:p"),
        {"p": p.id},
    ).one()
    assert float(r.total_debt) == 210000.0
    assert float(r.est_equity) == 90000.0     # 300k - 200k - 10k
    assert float(r.equity_pct) == 30.0


def test_end_to_end_mortgage_flows_into_equity(fresh_db):
    # The headline fix: with a mortgage deed present, equity reflects it.
    p = _prop(fresh_db, "EQ-E2E")
    fresh_db.add(Deed(property_id=p.id, instrument_number="EQ-E2E-1",
                      record_date=date(2022, 1, 1), mortgage_amount=200000))
    _fin(fresh_db, p.id, assessed_value_mkt=300000, total_lien_amount=10000)
    fresh_db.flush()

    aggregate_mortgage_balances(session=fresh_db)
    compute_equity_profiles(session=fresh_db)

    r = fresh_db.execute(
        text("SELECT est_mortgage_bal, est_equity FROM financials WHERE property_id=:p"),
        {"p": p.id},
    ).one()
    assert float(r.est_mortgage_bal) == 200000.0
    assert float(r.est_equity) == 90000.0     # was 290000 before the rollup existed (mortgage ignored)


def test_no_mortgage_deed_leaves_balance_unset(fresh_db):
    p = _prop(fresh_db, "EQ-NOMTG")
    _fin(fresh_db, p.id, assessed_value_mkt=250000, total_lien_amount=5000)
    fresh_db.flush()

    aggregate_mortgage_balances(session=fresh_db)
    compute_equity_profiles(session=fresh_db)

    r = fresh_db.execute(
        text("SELECT est_mortgage_bal, est_equity FROM financials WHERE property_id=:p"),
        {"p": p.id},
    ).one()
    # No deed mortgage to roll up; equity_compute coalesces the missing balance
    # to 0 and writes it back, so equity reflects liens only.
    assert float(r.est_mortgage_bal or 0) == 0.0
    assert float(r.est_equity) == 245000.0     # 250k - 0 - 5k
