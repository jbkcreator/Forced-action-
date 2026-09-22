"""
Tests for Stage E — enrichment bridge.

Seam: enrich_top_partners(session, ranked_rows, county_id) resolves
buyer_entity_id → owner_ids → skip_trace_waterfall.run_cascade.
Missing contacts set needs_enrichment=True on the PartnerRow. Only top-25
('active') rows are enriched.

run_cascade is mocked — we test the bridge logic, not the waterfall itself.
"""
from dataclasses import dataclass, field
from datetime import date
from typing import Optional
from unittest.mock import MagicMock, patch, call

import pytest

from src.services.partner_mining.rank import PartnerRow
from src.services.partner_mining.enrich import enrich_top_partners


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _row(name: str, entity_id: int, status: str = "active", rank: int = 1) -> PartnerRow:
    return PartnerRow(
        buyer_entity_id=entity_id,
        canonical_name=name,
        partner_class="lender",
        observed_transaction_count=10,
        last_observed_at=date(2024, 1, 1),
        rank=rank,
        status=status,
    )


def _session_with_owner_ids(entity_owner_map: dict[int, list[int]]) -> MagicMock:
    """
    Build a mock session whose execute() returns owner_ids per buyer_entity_id.
    entity_owner_map: {buyer_entity_id: [owner_id, ...]}
    """
    def _execute(sql, params):
        beid = params.get("beid") or params.get("buyer_entity_id")
        owner_ids = entity_owner_map.get(beid, [])
        rows = [MagicMock(owner_id=oid) for oid in owner_ids]
        result = MagicMock()
        result.fetchall.return_value = rows
        return result

    session = MagicMock()
    session.execute.side_effect = _execute
    return session


# ---------------------------------------------------------------------------
# Only active (top-25) rows are enriched
# ---------------------------------------------------------------------------

def test_identified_rows_not_enriched():
    rows = [
        _row("TOP LENDER LLC", entity_id=1, status="active"),
        _row("LOW LENDER LLC", entity_id=2, status="identified", rank=30),
    ]
    session = _session_with_owner_ids({1: [101], 2: [202]})

    with patch("src.services.partner_mining.enrich.run_cascade") as mock_cascade:
        mock_cascade.return_value = MagicMock()
        enrich_top_partners(session, rows, county_id="hillsborough")

    # run_cascade should only have been called for entity_id=1 (active)
    called_owner_ids = [
        c.kwargs.get("owner_ids") or c.args[2]
        for c in mock_cascade.call_args_list
    ]
    flat = [oid for ids in called_owner_ids for oid in ids]
    assert 101 in flat
    assert 202 not in flat


def test_zero_entity_id_skipped():
    """Rows with buyer_entity_id=0 (not yet resolved) must not be enriched."""
    rows = [_row("UNRESOLVED LLC", entity_id=0, status="active")]
    session = _session_with_owner_ids({})

    with patch("src.services.partner_mining.enrich.run_cascade") as mock_cascade:
        enrich_top_partners(session, rows, county_id="hillsborough")

    mock_cascade.assert_not_called()


# ---------------------------------------------------------------------------
# needs_enrichment flag
# ---------------------------------------------------------------------------

def test_missing_owner_link_sets_needs_enrichment():
    """If no owner_ids found for the entity, flag the row as needs_enrichment."""
    row = _row("NO OWNER LLC", entity_id=99, status="active")
    session = _session_with_owner_ids({99: []})   # no linked owners

    with patch("src.services.partner_mining.enrich.run_cascade"):
        enrich_top_partners(session, [row], county_id="hillsborough")

    assert row.needs_enrichment is True


def test_resolved_owner_does_not_set_needs_enrichment():
    """If owner_ids found and cascade runs, needs_enrichment stays False."""
    row = _row("GOOD LENDER LLC", entity_id=10, status="active")
    session = _session_with_owner_ids({10: [500]})

    with patch("src.services.partner_mining.enrich.run_cascade") as mock_cascade:
        mock_cascade.return_value = MagicMock()
        enrich_top_partners(session, [row], county_id="hillsborough")

    assert row.needs_enrichment is False


# ---------------------------------------------------------------------------
# Return value
# ---------------------------------------------------------------------------

def test_returns_summary_counts():
    rows = [
        _row("LENDER A", entity_id=1, status="active"),
        _row("LENDER B", entity_id=0, status="active"),    # no entity → needs_enrichment
        _row("LENDER C", entity_id=3, status="identified"),
    ]
    session = _session_with_owner_ids({1: [101], 3: [303]})

    with patch("src.services.partner_mining.enrich.run_cascade"):
        result = enrich_top_partners(session, rows, county_id="hillsborough")

    assert result["enriched"] == 1       # entity_id=1 had owner, cascade ran
    assert result["needs_enrichment"] == 1  # entity_id=0 had no entity
    assert result["skipped_identified"] == 1
