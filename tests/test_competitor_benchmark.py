"""Task 4.8 — Competitor benchmark flag engine (pure, no DB/network)."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
from sqlalchemy import text as sa_text

from src.services.competitor_benchmark import (
    CompetitorRow,
    OurTerms,
    classify_target,
    compute_benchmark_report,
    run_sweep,
)
from src.scrappers.competitor_rates.cambridge import parse_cambridge
from src.scrappers.competitor_rates.dscr_loan_source import parse_dscr_loan_source
from src.scrappers.competitor_rates.dscr_capital_partners import parse_dscr_capital_partners
from src.scrappers.competitor_rates.hardmoneyhome import (
    parse_hardmoneyhome,
    parse_hmh_profile,
    profile_slugs,
)
from src.scrappers.competitor_rates.equity_trac import parse_equity_trac

_FIX = Path(__file__).parent / "fixtures" / "competitor_rates"
_FIXTURE = _FIX / "cambridge_dscr.html"


def _row(rate_low=7.875, max_ltv=80.0, product="dscr", region=None):
    return CompetitorRow(
        lender_name="Test Lender",
        product=product,
        region=region,
        rate_low=rate_low,
        max_ltv=max_ltv,
    )


_OURS = {"dscr": OurTerms(product="dscr", rate=6.875, max_ltv=80.0)}


def test_rate_exactly_100bps_above_is_soft_target():
    # competitor rate = ours + 1.00% (=100bps), LTV equal → soft
    flag = classify_target(_row(rate_low=7.875, max_ltv=80.0), _OURS["dscr"])
    assert flag.status == "soft"
    assert flag.rate_delta_bps == 100.0


def test_worse_on_both_rate_and_ltv_is_strong_target():
    flag = classify_target(_row(rate_low=8.5, max_ltv=70.0), _OURS["dscr"])
    assert flag.status == "strong"


def test_at_or_below_our_terms_is_not_a_target():
    flag = classify_target(_row(rate_low=6.5, max_ltv=85.0), _OURS["dscr"])
    assert flag.status == "none"


def test_no_rate_card_for_product_stays_dark():
    # flag must stay "none" (not crash) when we have no terms to compare against
    flag = classify_target(_row(rate_low=9.0, max_ltv=60.0), None)
    assert flag.status == "none"
    assert flag.rate_delta_bps is None
    assert flag.ltv_delta_pts is None


def test_partial_scraped_row_classifies_on_present_field():
    # rate scraped, LTV missing → classify on rate only, no crash
    flag = classify_target(_row(rate_low=8.0, max_ltv=None), _OURS["dscr"])
    assert flag.status == "soft"
    assert flag.ltv_delta_pts is None


def test_term_advantage_alone_is_soft_target():
    # competitor term 12mo vs 24 baseline = 12mo shorter → term advantage, rate/ltv equal
    r = CompetitorRow("T", "dscr", None, rate_low=6.875, max_ltv=80.0, term_months=12)
    flag = classify_target(r, _OURS["dscr"])
    assert flag.term_delta_months == 12
    assert flag.status == "soft"


def test_two_axes_is_strong_even_without_third():
    # rate +100bps AND term short, LTV equal → 2 axes → strong
    r = CompetitorRow("T", "dscr", None, rate_low=7.875, max_ltv=80.0, term_months=12)
    flag = classify_target(r, _OURS["dscr"])
    assert flag.status == "strong"


def test_stale_rows_sorted_below_fresh():
    from datetime import datetime, timedelta
    as_of = date(2026, 6, 26)
    fresh = CompetitorRow("Fresh", "dscr", None, rate_low=8.5, max_ltv=70.0,
                          captured_at=datetime(2026, 6, 25))
    stale = CompetitorRow("Stale", "dscr", None, rate_low=8.5, max_ltv=70.0,
                          captured_at=datetime(2026, 1, 1))
    report = compute_benchmark_report([stale, fresh], _OURS, as_of=as_of)
    assert [t.row.lender_name for t in report.targets] == ["Fresh", "Stale"]
    assert report.targets[0].stale is False
    assert report.targets[1].stale is True


def test_adapter_registry_is_wired():
    from config.competitor_benchmark import ADAPTERS

    assert {a["name"] for a in ADAPTERS} == {
        "cambridge", "dscr_loan_source", "dscr_capital_partners", "hardmoneyhome",
        "equity_trac",
    }
    for a in ADAPTERS:
        assert callable(a["fetch"])
        assert a["source_url"].startswith("https://")
        assert a["confidence"] in ("high", "low")


def test_report_filters_to_targets_strong_first():
    rows = [
        _row(rate_low=6.5, max_ltv=85.0),    # none (cheaper + more leverage)
        _row(rate_low=7.875, max_ltv=80.0),  # soft (rate +100bps)
        _row(rate_low=8.5, max_ltv=70.0),    # strong (both)
    ]
    report = compute_benchmark_report(rows, _OURS)
    assert [t.status for t in report.targets] == ["strong", "soft"]
    assert report.scanned == 3


def test_report_skips_products_with_no_rate_card():
    rows = [_row(rate_low=15.0, max_ltv=50.0, product="private")]  # no 'private' card
    report = compute_benchmark_report(rows, _OURS)
    assert report.targets == []
    assert report.scanned == 1


def test_parse_cambridge_extracts_rate_and_ltv():
    row = parse_cambridge(_FIXTURE.read_text(encoding="utf-8"))
    assert row.lender_name == "Cambridge Home Loan"
    assert row.product == "dscr"
    assert row.max_ltv == 85.0
    assert row.rate_low == 5.75   # lowest advertised "as low as"


def test_cambridge_row_flagged_against_our_card_end_to_end():
    # real fixture → parser → engine. Cambridge LTV 85 > our 80 → not a target.
    row = parse_cambridge(_FIXTURE.read_text(encoding="utf-8"))
    flag = classify_target(row, _OURS["dscr"])
    assert flag.status == "none"
    assert flag.ltv_delta_pts == -5.0


def test_parse_dscr_loan_source_derives_ltv_from_down_payment():
    # page states "minimum 20% down" → 80% LTV; no advertised rate
    row = parse_dscr_loan_source((_FIX / "dscr_loan_source_tampa.html").read_text(encoding="utf-8"))
    assert row.lender_name == "DSCR Loan Source"
    assert row.product == "dscr"
    assert row.max_ltv == 80.0
    assert row.rate_low is None
    assert row.min_fico == 620
    assert row.min_dscr == 0.65
    assert row.prepay == "5yr"


def test_parse_dscr_capital_partners_extracts_rate_and_ltv():
    # real rate-matrix sheet: "from 6.50%", max LTV 80
    row = parse_dscr_capital_partners((_FIX / "dscr_capital_partners.html").read_text(encoding="utf-8"))
    assert row.lender_name == "DSCR Capital Partners"
    assert row.product == "dscr"
    assert row.rate_low == 6.5
    assert row.max_ltv == 85.0   # best-tier "up to 85% LTV"
    assert row.min_fico == 620
    assert row.prepay == "5yr"


def test_parse_hmh_profile_full_fields():
    row = parse_hmh_profile(
        (_FIX / "hmh_profile_temple_view_capital.html").read_text(encoding="utf-8")
    )
    assert row.lender_name == "Temple View Capital"
    assert row.product == "private"
    assert row.region == "tampa"
    assert row.rate_low == 5.875
    assert row.rate_high == 12.5
    assert row.max_ltv == 80.0
    assert row.points == 0.75
    assert row.min_fico == 660
    assert row.hq_location == "Bethesda, MD"


def test_parse_equity_trac_uses_bridge_not_rental_teaser():
    # page has 4.95% 30yr rental + 8.25/8.75% bridge — private benchmark uses bridge
    row = parse_equity_trac((_FIX / "equity_trac.html").read_text(encoding="utf-8"))
    assert row.lender_name == "Equity Trac"
    assert row.product == "private"
    assert row.region == "tampa"
    assert row.rate_low == 8.25          # bridge floor, NOT 4.95 rental
    assert row.max_ltv == 80.0
    assert row.min_fico == 650
    assert row.term_months == 12


def test_profile_slugs_from_listing():
    slugs = profile_slugs(
        (_FIX / "hardmoneyhome_tampa.html").read_text(encoding="utf-8")
    )
    assert len(slugs) == 50
    assert "temple-view-capital" in slugs


def test_parse_hardmoneyhome_private_market_avgs():
    # "average around 12.6%" rate, "72% is the average loan-to-value", private
    row = parse_hardmoneyhome((_FIX / "hardmoneyhome_tampa.html").read_text(encoding="utf-8"))
    assert row.product == "private"
    assert row.region == "tampa"
    assert row.rate_low == 12.6
    assert row.max_ltv == 72.0


# ── integration (real Postgres; needs fa103 applied) ────────────────────────

def _fake_adapter(row):
    return [{"name": "fake", "fetch": lambda: row,
             "source_url": "https://example.test", "confidence": "high"}]


def test_run_sweep_persists_rows_and_flags_targets(fresh_db, monkeypatch):
    import config.competitor_benchmark as cfg
    if not fresh_db.execute(sa_text(
        "SELECT to_regclass('competitor_rate_sheets')"
    )).scalar():
        pytest.skip("fa103 not applied")

    row = CompetitorRow("Fake Lender", "dscr", "florida", rate_low=8.5, max_ltv=70.0)
    monkeypatch.setattr(cfg, "ADAPTERS", _fake_adapter(row))

    result = run_sweep(fresh_db, dry_run=False)

    assert result["scanned"] == 1
    assert result["targets"][0]["status"] == "strong"   # +rate AND -ltv vs 6.875/80 card
    persisted = fresh_db.execute(sa_text(
        "SELECT count(*) FROM competitor_rate_sheets WHERE lender_name = 'Fake Lender'"
    )).scalar()
    assert persisted == 1


def test_run_sweep_dry_run_writes_nothing(fresh_db, monkeypatch):
    import config.competitor_benchmark as cfg
    if not fresh_db.execute(sa_text(
        "SELECT to_regclass('competitor_rate_sheets')"
    )).scalar():
        pytest.skip("fa103 not applied")

    row = CompetitorRow("DryRun Lender", "dscr", "florida", rate_low=8.5, max_ltv=70.0)
    monkeypatch.setattr(cfg, "ADAPTERS", _fake_adapter(row))

    result = run_sweep(fresh_db, dry_run=True)

    assert result["scanned"] == 1
    persisted = fresh_db.execute(sa_text(
        "SELECT count(*) FROM competitor_rate_sheets WHERE lender_name = 'DryRun Lender'"
    )).scalar()
    assert persisted == 0
