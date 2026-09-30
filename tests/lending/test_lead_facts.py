"""Provenance labels assigned to facts loaded from the database rows."""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock

from src.lending.lead_facts import _signals_from_row, load_lead_facts, parse_maturity
from src.lending.lead_scoring import Provenance

ROW = {
    "property_id": 7, "property_address": "1 Test St", "entity_status": "ACTIVE",
    "sunbiz_status": "matched", "sunbiz_doc_number": "L12000000001", "equity_pct": Decimal("42.5"),
    "lender_name": "Test Lender", "loan_amount": 350000, "est_maturity_date": "2026-12-01",
    "property_count": 3, "recent_permit_count": 1, "latest_permit": "Residential, 2026-08-14",
}


def test_maturity_formats():
    assert parse_maturity("2026-12-01") == date(2026, 12, 1)
    assert parse_maturity("12/01/2026") == date(2026, 12, 1)
    assert parse_maturity("2026-12") == date(2026, 12, 1)
    assert parse_maturity("soon") is None
    assert parse_maturity("") is None
    assert parse_maturity(None) is None


def test_row_provenance():
    signals = _signals_from_row(ROW)
    assert signals.maturity_date.provenance is Provenance.ESTIMATED
    assert signals.equity_pct.provenance is Provenance.ESTIMATED
    assert signals.loan_amount.provenance is Provenance.KNOWN
    assert signals.entity_status.value == "ACTIVE" and signals.entity_status.is_known
    assert signals.entity_property_count.value == 3
    assert signals.decision_maker_confirmed.provenance is Provenance.MISSING


def test_unmatched_sunbiz_owner_has_no_entity_status():
    signals = _signals_from_row({**ROW, "sunbiz_status": "pending"})
    assert signals.entity_status.provenance is Provenance.MISSING


def test_owner_without_a_document_number_is_never_a_counted_operator():
    signals = _signals_from_row({**ROW, "sunbiz_doc_number": None, "property_count": None, "recent_permit_count": None})
    assert signals.entity_property_count.provenance is Provenance.MISSING
    assert signals.entity_recent_permit_count.provenance is Provenance.MISSING


def test_missing_values_stay_missing_not_zero():
    signals = _signals_from_row({**ROW, "loan_amount": None, "equity_pct": None, "est_maturity_date": None})
    assert signals.loan_amount.provenance is Provenance.MISSING
    assert signals.equity_pct.provenance is Provenance.MISSING
    assert signals.maturity_date.provenance is Provenance.MISSING


def test_load_skips_the_query_for_no_properties():
    session = MagicMock()
    assert load_lead_facts(session, [], today=date(2026, 10, 5)) == {}
    session.execute.assert_not_called()


def test_load_builds_facts_keyed_by_property():
    session = MagicMock()
    session.execute.return_value.mappings.return_value.all.return_value = [ROW]
    facts = load_lead_facts(session, [7], today=date(2026, 10, 5))
    assert facts[7].lender_name == "Test Lender"
    assert facts[7].latest_permit == "Residential, 2026-08-14"
    params = session.execute.call_args.args[1]
    assert params["property_ids"] == [7]
    assert params["permit_since"] == date(2024, 10, 5)
