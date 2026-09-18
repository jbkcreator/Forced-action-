"""
Tests for Stage B — counterparty identity resolution.

Seam: extract_counterparty_candidates produces correctly shaped CandidateRecords
from mortgage deed rows; resolve_counterparty_names maps raw name strings to
buyer_entity_id via the shared graph.

Tests use mock sessions — no real DB required (consistent with the pure-unit
style of the other partner_mining tests).
"""
from dataclasses import dataclass
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest

from src.services.partner_mining.resolution import (
    extract_counterparty_candidates,
    resolve_counterparty_names,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mortgage_row(
    deed_id: int,
    grantee: str,
    county_id: str = "hillsborough",
    mortgage_amount: float = 100_000.0,
):
    row = MagicMock()
    row.id = deed_id
    row.grantee = grantee
    row.mailing_address = None
    row.county_id = county_id
    row.mortgage_amount = mortgage_amount
    return row


def _mock_session(rows):
    """Return a mock Session whose execute().yield_per() returns rows."""
    result = MagicMock()
    result.yield_per.return_value = iter(rows)
    session = MagicMock()
    session.execute.return_value = result
    return session


# ---------------------------------------------------------------------------
# extract_counterparty_candidates
# ---------------------------------------------------------------------------

def test_mortgage_row_produces_deed_lender_candidate():
    rows = [_mortgage_row(1, "WELLS FARGO BANK NA")]
    session = _mock_session(rows)

    candidates = list(extract_counterparty_candidates(session))

    assert len(candidates) == 1
    c = candidates[0]
    assert c.source_table == "deed_lender"
    assert c.source_id == 1
    assert "WELLS FARGO" in c.raw_name.upper()


def test_candidate_county_id_preserved():
    rows = [_mortgage_row(2, "ROCKET MORTGAGE LLC", county_id="pinellas")]
    session = _mock_session(rows)

    candidates = list(extract_counterparty_candidates(session, county_id="pinellas"))

    assert candidates[0].county_id == "pinellas"


def test_multiple_mortgage_rows_produce_multiple_candidates():
    rows = [
        _mortgage_row(10, "BANK A"),
        _mortgage_row(11, "BANK B"),
        _mortgage_row(12, "BANK A"),   # same lender, different deed
    ]
    session = _mock_session(rows)

    candidates = list(extract_counterparty_candidates(session))
    assert len(candidates) == 3


def test_entity_type_hint_set_to_corporate_for_bank_names():
    rows = [_mortgage_row(5, "SUNCOAST CREDIT UNION")]
    session = _mock_session(rows)

    candidates = list(extract_counterparty_candidates(session))
    # entity_type_hint should be set (not None) — lenders are institutional
    assert candidates[0].entity_type_hint is not None


def test_empty_grantee_skipped():
    rows = [_mortgage_row(6, "")]
    session = _mock_session(rows)

    candidates = list(extract_counterparty_candidates(session))
    assert candidates == []


# ---------------------------------------------------------------------------
# resolve_counterparty_names — maps raw name list → {name: buyer_entity_id}
# ---------------------------------------------------------------------------

def test_resolve_returns_entity_id_for_known_name():
    """
    If a name is already in buyer_entities (existing link), the resolver
    returns its entity ID without creating a new entity.
    """
    existing_row = MagicMock()
    existing_row.buyer_entity_id = 42
    existing_row.canonical_name = "WELLS FARGO BANK NA"

    result = MagicMock()
    result.fetchall.return_value = [existing_row]
    session = MagicMock()
    session.execute.return_value = result

    mapping = resolve_counterparty_names(session, ["WELLS FARGO BANK NA"])

    assert mapping.get("WELLS FARGO BANK NA") == 42


def test_resolve_returns_none_for_unknown_name():
    result = MagicMock()
    result.fetchall.return_value = []
    session = MagicMock()
    session.execute.return_value = result

    mapping = resolve_counterparty_names(session, ["UNKNOWN LENDER LLC"])

    assert mapping.get("UNKNOWN LENDER LLC") is None


def test_resolve_empty_name_list():
    session = MagicMock()
    mapping = resolve_counterparty_names(session, [])
    assert mapping == {}
