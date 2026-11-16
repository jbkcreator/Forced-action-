"""Cora P3.1 daily FA export: which records a day holds, their signals, the CSV and the GHL contacts."""
from __future__ import annotations

import csv
import io
from datetime import date, timedelta

import pytest

from config.property_radar_campaigns import (
    ASSUMED_TERM_DAYS,
    INVESTOR_PORTFOLIO_MIN_PROPERTIES,
    MATURITY_TRIGGER_DAYS,
    build_campaign_criteria,
)
from src.lending import fa_export as fx

DAY = date(2026, 11, 18)


def _record(**over):
    base = {"radar_id": "P1", "state": "FL", "county_name": "ORANGE", "county_fips": "12095",
            "owner_name": "ACME HOLDINGS LLC", "ownership_type": "Corporate", "campaign": "maturity_target_lender",
            "loan_recorded_date": "2026-05-02", "est_maturity_date": None, "property_id": None}
    return {**base, **over}


def _row(**over):
    row = fx.build_row(_record(), day=DAY, portfolio_size=0, phone="+18135550100", email=None)
    return {**row, **over}


def test_maturity_uses_known_date_else_assumes_twelve_months():
    assert fx.maturity(_record(est_maturity_date="2027-03-01")) == (date(2027, 3, 1), False)
    assert fx.maturity(_record()) == (date(2026, 5, 2) + timedelta(days=ASSUMED_TERM_DAYS), True)
    assert fx.maturity(_record(loan_recorded_date=None)) == (None, False)


def test_maturing_loan_is_exported_once_on_the_day_it_crosses_the_trigger():
    matures = DAY + timedelta(days=MATURITY_TRIGGER_DAYS)
    record = _record(est_maturity_date=matures.isoformat())
    staged_early = DAY - timedelta(days=40)
    assert fx.due_on(record, DAY, staged_early)
    assert not fx.due_on(record, DAY - timedelta(days=1), staged_early)
    assert not fx.due_on(record, DAY + timedelta(days=1), staged_early)


def test_loan_already_inside_the_trigger_is_exported_the_day_it_is_staged():
    record = _record(est_maturity_date=(DAY + timedelta(days=30)).isoformat())
    assert fx.due_on(record, DAY, DAY)
    assert not fx.due_on(record, DAY + timedelta(days=1), DAY)


def test_maturity_record_with_no_dates_is_never_exported():
    assert not fx.due_on(_record(loan_recorded_date=None), DAY, DAY)


def test_other_signals_are_exported_the_day_they_are_staged():
    record = _record(campaign="cash_purchase")
    assert fx.due_on(record, DAY, DAY)
    assert not fx.due_on(record, DAY, DAY - timedelta(days=1))


@pytest.mark.parametrize("campaign,size,expected", [
    ("maturity_target_lender", 0, ["maturing_loan"]),
    ("private_maturity", INVESTOR_PORTFOLIO_MIN_PROPERTIES, ["maturing_loan", "investor_portfolio"]),
    ("cash_purchase", 0, ["cash_purchase"]),
    ("investor_portfolio", 0, ["investor_portfolio"]),
    ("auction_winner", 0, []),
])
def test_signals(campaign, size, expected):
    assert fx.signals(_record(campaign=campaign), size) == expected


def test_record_with_no_signal_has_no_row():
    assert fx.build_row(_record(campaign="auction_winner"), day=DAY, portfolio_size=0, phone=None, email=None) is None


def test_csv_has_one_row_per_record_with_joined_signals():
    rows = [fx.build_row(_record(), day=DAY, portfolio_size=4, phone="+18135550100", email="a@b.co"),
            fx.build_row(_record(radar_id="P2", campaign="cash_purchase", loan_recorded_date=None), day=DAY,
                         portfolio_size=0, phone=None, email=None)]
    parsed = list(csv.DictReader(io.StringIO(fx.to_csv(rows).decode("utf-8"))))
    assert [r["signals"] for r in parsed] == ["maturing_loan;investor_portfolio", "cash_purchase"]
    assert parsed[0]["maturity_estimated"] == "yes" and parsed[0]["portfolio_properties"] == "4"
    assert parsed[1]["maturity_date"] == "" and parsed[1]["phone"] == ""
    assert tuple(parsed[0].keys()) == fx.COLUMNS


def test_ghl_tags_carry_ndl_state_source_and_each_signal():
    tags = fx.ghl_tags(_row(signals="maturing_loan;investor_portfolio"))
    assert tags == ["NDL", "ndl-cold", "ndl-src-property-radar", "ndl-state-fl", "ndl-signal-maturing-loan",
                    "ndl-signal-investor-portfolio"]


def test_ghl_push_skips_no_phone_suppressed_duplicates_and_georgia_people():
    pushed = []
    rows = [
        _row(),
        _row(radar_id="P2"),
        _row(radar_id="P3", phone=""),
        _row(radar_id="P4", phone="+18135550111"),
        _row(radar_id="P5", phone="+14045550122", state="GA", owner_name="RALPH SMITH", ownership_type="Individual"),
        _row(radar_id="P6", phone="+14045550133", state="GA"),
    ]
    outcome = fx.push_to_ghl(rows, lambda contact, tags: pushed.append((contact, tags)), {"+18135550111"})
    assert dict(outcome) == {"pushed": 2, "duplicate_phone": 1, "no_phone": 1, "suppressed": 1,
                             "ga_natural_person": 1}
    assert [c["phone"] for c, _ in pushed] == ["+18135550100", "+14045550133"]
    assert pushed[0][0]["entity_name"] == "ACME HOLDINGS LLC"


def test_ghl_push_counts_a_refused_contact_and_continues():
    def push(contact, tags):
        if contact["phone"].endswith("100"):
            raise RuntimeError("contact upsert: HTTP 500")

    outcome = fx.push_to_ghl([_row(), _row(radar_id="P2", phone="+18135550199")], push, set())
    assert dict(outcome) == {"failed": 1, "pushed": 1}


@pytest.mark.parametrize("state", ["FL", "GA"])
def test_new_signal_campaigns_build_criteria(state):
    cash = {c["name"]: c["value"] for c in build_campaign_criteria(state, "cash_purchase")}
    assert cash["LastTransferDownPaymentPercent"] == [[100, None]]
    assert cash["isSameMailingOrExempt"] == ["0"] and cash["State"] == [state]
    portfolio = {c["name"]: c["value"] for c in build_campaign_criteria(state, "investor_portfolio")}
    assert portfolio["OwnershipType"] == ["Corporate"] and "LastTransferRecDate" in portfolio


def test_run_uploads_one_file_and_reports_counts(monkeypatch):
    from src.tasks import lending_fa_export as task

    rows = [_row(), _row(radar_id="P2", signals="cash_purchase", phone="")]
    monkeypatch.setattr(fx, "collect", lambda session, day, backfill=False: rows)
    uploads = []
    summary = task.run(None, day=DAY, folder_id="F", upload=lambda *a: uploads.append(a), push=None)
    assert [(u[0], u[1]) for u in uploads] == [("F", "fa_export_2026-11-18.csv")]
    assert summary["rows"] == 2 and summary["with_phone"] == 1 and summary["by_state"] == {"FL": 2}
    assert summary["maturing_loan"] == 1 and summary["cash_purchase"] == 1 and "ghl" not in summary

    task.run(None, day=DAY, folder_id="F", upload=lambda *a: uploads.append(a), push=None, backfill=True)
    assert uploads[-1][1] == "fa_export_backfill_2026-11-18.csv"


def test_calling_list_records_carry_their_signals():
    assert fx.signals(_record(campaign="verified_maturity"), 0) == ["maturing_loan"]
    assert fx.signals(_record(campaign="cash_buyer"), 3) == ["cash_purchase", "investor_portfolio"]
    assert fx.signals(_record(campaign="wholesaler"), 0) == []


def test_list_maturity_without_a_term_uses_the_term_that_fits_the_pull_window():
    pulled = date(2026, 11, 3)
    two_year = _record(campaign="verified_maturity", loan_recorded_date=date(2025, 3, 10), first_seen=pulled)
    assert fx.maturity(two_year) == (date(2027, 3, 10), True)
    known = _record(campaign="verified_maturity", est_maturity_date=date(2027, 2, 1), first_seen=pulled)
    assert fx.maturity(known) == (date(2027, 2, 1), False)


def test_backfill_holds_back_only_daily_loans_still_ahead_of_their_trigger():
    far = _record(est_maturity_date=(DAY + timedelta(days=MATURITY_TRIGGER_DAYS + 1)).isoformat())
    near = _record(est_maturity_date=(DAY + timedelta(days=MATURITY_TRIGGER_DAYS)).isoformat())
    assert not fx.in_backfill(far, DAY)
    assert fx.in_backfill(near, DAY)
    assert fx.in_backfill(_record(campaign="cash_buyer", loan_recorded_date=None), DAY)
    assert fx.in_backfill(_record(campaign="verified_maturity", est_maturity_date="2027-06-30"), DAY)


def test_property_in_both_stores_is_one_row_with_joined_signals_and_filled_blanks():
    daily = fx.build_row(_record(property_id=42), day=DAY, portfolio_size=0, phone=None, email=None)
    listed = fx.build_row(_record(campaign="cash_buyer", source="pr_list:cash_buyer"), day=DAY, portfolio_size=0,
                          phone="+18135550100", email=None)
    other = fx.build_row(_record(radar_id="P9", campaign="cash_buyer", source="pr_list:cash_buyer"), day=DAY,
                         portfolio_size=0, phone=None, email=None)
    merged = fx.merge_rows([daily, listed, other])
    assert [r["radar_id"] for r in merged] == ["P1", "P9"]
    assert merged[0]["signals"] == "maturing_loan;cash_purchase"
    assert merged[0]["source"] == "pr_daily;pr_list:cash_buyer"
    assert merged[0]["phone"] == "+18135550100" and merged[0]["fa_property_id"] == 42
