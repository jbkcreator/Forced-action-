"""
WP-8A Quote Ready deal math — pure unit tests.
Seam: compute_quote_ready(QuoteReadyInput) → QuoteReadyResult
No DB, no network, no mocks. Fixture in, object out.
"""
from decimal import Decimal

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

    # ltc = 200_000 / 250_000 = 0.80
    assert result.ltc.raw == Decimal("0.80")
    assert result.ltc.display == "80.0%"

    # ltv = 200_000 / 320_000 = 0.625
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
    assert "arv" in result.missing
    assert "ltv" in result.missing


# ---------------------------------------------------------------------------
# Slice 3 — rehab absent: cost/LTC/loan missing
# ---------------------------------------------------------------------------

def test_rehab_absent_missing_cost_and_ltc():
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
    assert "rehab_estimate" in result.missing
    assert "project_cost" in result.missing
    assert "ltc" in result.missing
    assert result.proposed_loan is None
    assert "proposed_loan" in result.missing
    assert result.ltv is None


# ---------------------------------------------------------------------------
# Slice 4 — purchase fallback to estimated_value (medium confidence)
# ---------------------------------------------------------------------------

def test_purchase_fallback_to_estimated_value():
    inp = QuoteReadyInput(
        property_id="prop-004",
        purchase_price=None,
        estimated_value=Decimal("180000"),
        rehab_estimate=Decimal("20000"),
        arv=Decimal("250000"),
        max_ltc=Decimal("0.80"),
        max_ltv=Decimal("0.70"),
    )
    result = compute_quote_ready(inp)

    assert result.project_cost.raw == Decimal("200000")
    assert result.project_cost.source == "estimated_value"
    assert result.project_cost.confidence == "medium"


# ---------------------------------------------------------------------------
# Slice 5 — purchase fallback to assessed_value_mkt (low confidence)
# ---------------------------------------------------------------------------

def test_purchase_fallback_to_assessed_value_mkt_low_confidence():
    inp = QuoteReadyInput(
        property_id="prop-005",
        purchase_price=None,
        estimated_value=None,
        assessed_value_mkt=Decimal("120000"),
        rehab_estimate=Decimal("15000"),
        arv=Decimal("185000"),
        max_ltc=Decimal("0.80"),
        max_ltv=Decimal("0.70"),
    )
    result = compute_quote_ready(inp)

    assert result.project_cost.source == "assessed_value_mkt"
    assert result.project_cost.confidence == "low"


# ---------------------------------------------------------------------------
# Slice 6 — purchase fallback to last_sale_price (low confidence)
# ---------------------------------------------------------------------------

def test_purchase_fallback_to_last_sale_price_low_confidence():
    inp = QuoteReadyInput(
        property_id="prop-006",
        purchase_price=None,
        estimated_value=None,
        assessed_value_mkt=None,
        last_sale_price=Decimal("110000"),
        rehab_estimate=Decimal("20000"),
        arv=Decimal("185000"),
        max_ltc=Decimal("0.80"),
        max_ltv=Decimal("0.70"),
    )
    result = compute_quote_ready(inp)

    assert result.project_cost.source == "last_sale_price"
    assert result.project_cost.confidence == "low"


# ---------------------------------------------------------------------------
# Slice 7 — assessed_value_mkt takes precedence over last_sale_price
# ---------------------------------------------------------------------------

def test_assessed_value_mkt_before_last_sale_price():
    inp = QuoteReadyInput(
        property_id="prop-007",
        purchase_price=None,
        estimated_value=None,
        assessed_value_mkt=Decimal("100000"),
        last_sale_price=Decimal("90000"),
        rehab_estimate=Decimal("10000"),
        arv=Decimal("160000"),
        max_ltc=Decimal("0.80"),
        max_ltv=Decimal("0.70"),
    )
    result = compute_quote_ready(inp)

    assert result.project_cost.source == "assessed_value_mkt"
    assert result.project_cost.raw == Decimal("110000")


# ---------------------------------------------------------------------------
# Slice 8 — confidence propagates: low-confidence basis → low-confidence
#           derived figures (loan, ltc, ltv)
# ---------------------------------------------------------------------------

def test_low_confidence_basis_propagates_to_derived_figures():
    inp = QuoteReadyInput(
        property_id="prop-008",
        purchase_price=None,
        estimated_value=None,
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
# Slice 9 — zero project cost: no division crash, goes to missing[]
# ---------------------------------------------------------------------------

def test_zero_project_cost_does_not_crash():
    inp = QuoteReadyInput(
        property_id="prop-009",
        purchase_price=Decimal("0"),
        rehab_estimate=Decimal("0"),
        arv=Decimal("200000"),
        max_ltc=Decimal("0.80"),
        max_ltv=Decimal("0.70"),
    )
    result = compute_quote_ready(inp)
    # project_cost = 0; treated as invalid denominator → proposed_loan missing
    assert result.proposed_loan is None
    assert "proposed_loan" in result.missing


# ---------------------------------------------------------------------------
# Slice 10 — zero ARV: no crash, ltv missing
# ---------------------------------------------------------------------------

def test_zero_arv_does_not_crash():
    inp = QuoteReadyInput(
        property_id="prop-010",
        purchase_price=Decimal("100000"),
        rehab_estimate=Decimal("30000"),
        arv=Decimal("0"),
        max_ltc=Decimal("0.80"),
        max_ltv=Decimal("0.70"),
    )
    result = compute_quote_ready(inp)
    # arv=0 treated as invalid; loan falls back to ltc_cap
    assert result.proposed_loan is not None
    assert result.ltv is None


# ---------------------------------------------------------------------------
# Slice 11 — display precision: whole dollars, 1-decimal %
# ---------------------------------------------------------------------------

def test_display_precision():
    inp = QuoteReadyInput(
        property_id="prop-011",
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
# Slice 12 — determinism
# ---------------------------------------------------------------------------

def test_determinism():
    inp = QuoteReadyInput(
        property_id="prop-012",
        purchase_price=Decimal("175000"),
        rehab_estimate=Decimal("40000"),
        arv=Decimal("280000"),
        max_ltc=Decimal("0.80"),
        max_ltv=Decimal("0.70"),
    )
    r1 = compute_quote_ready(inp)
    r2 = compute_quote_ready(inp)

    assert r1.project_cost.raw == r2.project_cost.raw
    assert r1.proposed_loan.raw == r2.proposed_loan.raw
    assert r1.ltc.raw == r2.ltc.raw
    assert r1.ltv.raw == r2.ltv.raw
    assert r1.missing == r2.missing
