"""
WP-8A Quote Ready deal math — pure unit tests.
Seam: compute_quote_ready(QuoteReadyInput) → QuoteReadyResult
No DB, no network, no mocks. Fixture in, object out.
"""
from decimal import Decimal

import pytest
from pydantic import ValidationError

from src.services.quote_ready import compute_quote_ready, QuoteReadyInput


# ---------------------------------------------------------------------------
# Slice 1 — full inputs: all four figures present, missing[] empty
# ---------------------------------------------------------------------------

def test_full_inputs_produces_all_figures():
    inp = QuoteReadyInput(
        property_id="prop-001",
        purchase_price=Decimal("200000"),
        rehab_estimate=Decimal("50000"),
        arv=Decimal("320000"),
        max_ltc=Decimal("0.80"),
        max_ltv=Decimal("0.70"),
    )
    result = compute_quote_ready(inp)

    assert result.project_cost.raw == Decimal("250000")
    assert result.project_cost.display == "$250,000"

    # min(0.80*250_000, 0.70*320_000) = min(200_000, 224_000) = 200_000
    assert result.proposed_loan.raw == Decimal("200000")
    assert result.proposed_loan.display == "$200,000"

    assert result.ltc.raw == Decimal("0.80")
    assert result.ltc.display == "80.0%"

    assert result.ltv.raw == Decimal("0.625")
    assert result.ltv.display == "62.5%"

    assert result.missing == []


# ---------------------------------------------------------------------------
# Slice 2 — ARV absent: loan = LTC cap only; arv + ltv in missing[]
# ---------------------------------------------------------------------------

def test_arv_absent_degrades_to_ltc_only():
    inp = QuoteReadyInput(
        property_id="prop-002",
        purchase_price=Decimal("150000"),
        rehab_estimate=Decimal("30000"),
        arv=None,
        max_ltc=Decimal("0.75"),
        max_ltv=Decimal("0.70"),
    )
    result = compute_quote_ready(inp)

    assert result.project_cost.raw == Decimal("180000")
    assert result.proposed_loan.raw == Decimal("135000")
    assert result.ltv is None
    assert result.missing == ["arv", "ltv"]


# ---------------------------------------------------------------------------
# Slice 3 — rehab absent: cost/LTC/loan/ltv all missing (complete list)
# ---------------------------------------------------------------------------

def test_rehab_absent_missing_is_complete():
    inp = QuoteReadyInput(
        property_id="prop-003",
        purchase_price=Decimal("100000"),
        rehab_estimate=None,
        arv=Decimal("200000"),
        max_ltc=Decimal("0.80"),
        max_ltv=Decimal("0.65"),
    )
    result = compute_quote_ready(inp)

    assert result.project_cost is None
    assert result.proposed_loan is None
    assert result.ltc is None
    assert result.ltv is None
    # ltv must appear even though ARV was supplied — it is still underivable
    assert result.missing == ["ltc", "ltv", "project_cost", "proposed_loan", "rehab_estimate"]


# ---------------------------------------------------------------------------
# Slice 4 — purchase fallback to estimated_value (medium confidence)
# ---------------------------------------------------------------------------

def test_purchase_fallback_to_estimated_value():
    inp = QuoteReadyInput(
        property_id="prop-004",
        estimated_value=Decimal("180000"),
        rehab_estimate=Decimal("20000"),
        arv=Decimal("250000"),
        max_ltc=Decimal("0.80"),
        max_ltv=Decimal("0.70"),
    )
    result = compute_quote_ready(inp)

    assert result.project_cost.raw == Decimal("200000")
    assert result.project_cost.source.startswith("estimated_value")
    # basis medium, rehab job_estimator medium → medium
    assert result.project_cost.confidence == "medium"


# ---------------------------------------------------------------------------
# Slice 5 — fallback to assessed_value_mkt (low confidence)
# ---------------------------------------------------------------------------

def test_purchase_fallback_to_assessed_value_mkt_low_confidence():
    inp = QuoteReadyInput(
        property_id="prop-005",
        assessed_value_mkt=Decimal("120000"),
        rehab_estimate=Decimal("15000"),
        arv=Decimal("185000"),
        max_ltc=Decimal("0.80"),
        max_ltv=Decimal("0.70"),
    )
    result = compute_quote_ready(inp)

    assert result.project_cost.source.startswith("assessed_value_mkt")
    assert result.project_cost.confidence == "low"


# ---------------------------------------------------------------------------
# Slice 6 — fallback to last_sale_price (low confidence)
# ---------------------------------------------------------------------------

def test_purchase_fallback_to_last_sale_price():
    inp = QuoteReadyInput(
        property_id="prop-006",
        last_sale_price=Decimal("110000"),
        rehab_estimate=Decimal("20000"),
        arv=Decimal("185000"),
        max_ltc=Decimal("0.80"),
        max_ltv=Decimal("0.70"),
    )
    result = compute_quote_ready(inp)

    assert result.project_cost.source.startswith("last_sale_price")
    assert result.project_cost.confidence == "low"


# ---------------------------------------------------------------------------
# Slice 7 — assessed_value_mkt takes precedence over last_sale_price
# ---------------------------------------------------------------------------

def test_assessed_value_mkt_before_last_sale_price():
    inp = QuoteReadyInput(
        property_id="prop-007",
        assessed_value_mkt=Decimal("100000"),
        last_sale_price=Decimal("90000"),
        rehab_estimate=Decimal("10000"),
        arv=Decimal("160000"),
        max_ltc=Decimal("0.80"),
        max_ltv=Decimal("0.70"),
    )
    result = compute_quote_ready(inp)

    assert result.project_cost.source.startswith("assessed_value_mkt")
    assert result.project_cost.raw == Decimal("110000")


# ---------------------------------------------------------------------------
# Slice 8 — low-confidence basis propagates to ALL derived figures
# ---------------------------------------------------------------------------

def test_low_confidence_basis_propagates_to_derived_figures():
    inp = QuoteReadyInput(
        property_id="prop-008",
        assessed_value_mkt=Decimal("80000"),
        rehab_estimate=Decimal("30000"),
        arv=Decimal("160000"),
        max_ltc=Decimal("0.80"),
        max_ltv=Decimal("0.70"),
    )
    result = compute_quote_ready(inp)

    assert result.project_cost.confidence == "low"
    assert result.proposed_loan.confidence == "low"
    assert result.ltc.confidence == "low"
    assert result.ltv.confidence == "low"


# ---------------------------------------------------------------------------
# Slice 9 — low-confidence ARV drags LTV (and loan) down, not the LTC side
# ---------------------------------------------------------------------------

def test_low_confidence_arv_propagates():
    inp = QuoteReadyInput(
        property_id="prop-009",
        purchase_price=Decimal("100000"),
        rehab_estimate=Decimal("20000"),
        arv=Decimal("300000"),
        arv_confidence="low",
        max_ltc=Decimal("0.80"),
        max_ltv=Decimal("0.70"),
    )
    result = compute_quote_ready(inp)

    # LTC binds (0.80*120_000=96_000 < 0.70*300_000=210_000) → loan=96_000
    # basis high + rehab medium → cost medium; arv low → loan min(medium,low)=low
    assert result.project_cost.confidence == "medium"
    assert result.proposed_loan.confidence == "low"
    assert result.ltv.confidence == "low"


# ---------------------------------------------------------------------------
# Slice 10 — rehab override is high confidence (distinguished from estimator)
# ---------------------------------------------------------------------------

def test_rehab_override_confidence_and_source():
    inp = QuoteReadyInput(
        property_id="prop-010",
        purchase_price=Decimal("100000"),
        rehab_estimate=Decimal("20000"),
        rehab_source="override",
        arv=Decimal("200000"),
        max_ltc=Decimal("0.80"),
        max_ltv=Decimal("0.70"),
    )
    result = compute_quote_ready(inp)

    # basis high + rehab override high → cost high
    assert result.project_cost.confidence == "high"
    assert "override" in result.project_cost.source


# ---------------------------------------------------------------------------
# Slice 11 — zero ARV: not silently ignored; arv + ltv flagged missing
# ---------------------------------------------------------------------------

def test_zero_arv_flagged_missing_not_silent():
    inp = QuoteReadyInput(
        property_id="prop-011",
        purchase_price=Decimal("100000"),
        rehab_estimate=Decimal("30000"),
        arv=Decimal("0"),
        max_ltc=Decimal("0.80"),
        max_ltv=Decimal("0.70"),
    )
    result = compute_quote_ready(inp)

    # loan still derivable from LTC cap
    assert result.proposed_loan is not None
    assert result.ltv is None
    assert "arv" in result.missing
    assert "ltv" in result.missing


# ---------------------------------------------------------------------------
# Slice 12 — zero project cost: no crash, downstream all missing
# ---------------------------------------------------------------------------

def test_zero_project_cost_downstream_all_missing():
    inp = QuoteReadyInput(
        property_id="prop-012",
        purchase_price=Decimal("0"),
        rehab_estimate=Decimal("0"),
        arv=Decimal("200000"),
        max_ltc=Decimal("0.80"),
        max_ltv=Decimal("0.70"),
    )
    result = compute_quote_ready(inp)

    assert result.project_cost is None
    assert result.proposed_loan is None
    assert result.ltc is None
    assert result.ltv is None
    for name in ("project_cost", "proposed_loan", "ltc", "ltv"):
        assert name in result.missing


# ---------------------------------------------------------------------------
# Slice 13 — negative caps / inputs rejected at construction (boundary)
# ---------------------------------------------------------------------------

def test_negative_cap_rejected():
    with pytest.raises(ValidationError):
        QuoteReadyInput(
            property_id="prop-013",
            purchase_price=Decimal("100000"),
            rehab_estimate=Decimal("20000"),
            max_ltc=Decimal("-0.80"),
            max_ltv=Decimal("0.70"),
        )


def test_negative_monetary_input_rejected():
    with pytest.raises(ValidationError):
        QuoteReadyInput(
            property_id="prop-013b",
            purchase_price=Decimal("-100000"),
            rehab_estimate=Decimal("20000"),
            max_ltc=Decimal("0.80"),
            max_ltv=Decimal("0.70"),
        )


# ---------------------------------------------------------------------------
# Slice 14 — display precision: whole dollars, 1-decimal %
# ---------------------------------------------------------------------------

def test_display_precision():
    inp = QuoteReadyInput(
        property_id="prop-014",
        purchase_price=Decimal("100000"),
        rehab_estimate=Decimal("33333"),
        arv=Decimal("200000"),
        max_ltc=Decimal("0.85"),
        max_ltv=Decimal("0.70"),
    )
    result = compute_quote_ready(inp)

    assert result.proposed_loan.display.startswith("$")
    assert "." not in result.proposed_loan.display

    assert result.ltc.display.endswith("%")
    parts = result.ltc.display.rstrip("%").split(".")
    assert len(parts) == 2 and len(parts[1]) == 1


# ---------------------------------------------------------------------------
# Slice 15 — determinism
# ---------------------------------------------------------------------------

def test_determinism():
    inp = QuoteReadyInput(
        property_id="prop-015",
        purchase_price=Decimal("175000"),
        rehab_estimate=Decimal("40000"),
        arv=Decimal("280000"),
        max_ltc=Decimal("0.80"),
        max_ltv=Decimal("0.70"),
    )
    r1 = compute_quote_ready(inp)
    r2 = compute_quote_ready(inp)

    assert r1.model_dump() == r2.model_dump()
