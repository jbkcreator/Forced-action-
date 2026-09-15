"""
WP-8A Quote Ready deal math — pure stateless compute.

Entrypoint: compute_quote_ready(QuoteReadyInput) → QuoteReadyResult

No DB, no network, no program knowledge. max_ltc/max_ltv are passed in by
the caller (program-match layer); this function stays program-agnostic.
"""
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from typing import NamedTuple, Optional

from .models import Confidence, Figure, QuoteReadyInput, QuoteReadyResult, min_confidence

_ONE_CENT = Decimal("1")
_ONE_TENTH_PCT = Decimal("0.1")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fmt_dollars(v: Decimal) -> str:
    rounded = v.quantize(_ONE_CENT, rounding=ROUND_HALF_UP)
    return f"${rounded:,.0f}"


def _fmt_pct(v: Decimal) -> str:
    pct = (v * 100).quantize(_ONE_TENTH_PCT, rounding=ROUND_HALF_UP)
    return f"{pct}%"


class _PurchaseBasis(NamedTuple):
    value: Decimal
    source: str
    confidence: Confidence


def _purchase_basis(inp: QuoteReadyInput) -> Optional[_PurchaseBasis]:
    if inp.purchase_price is not None:
        return _PurchaseBasis(inp.purchase_price, "purchase_price", "high")
    if inp.estimated_value is not None:
        return _PurchaseBasis(inp.estimated_value, "estimated_value", "medium")
    if inp.assessed_value_mkt is not None:
        return _PurchaseBasis(inp.assessed_value_mkt, "assessed_value_mkt", "low")
    if inp.last_sale_price is not None:
        return _PurchaseBasis(inp.last_sale_price, "last_sale_price", "low")
    return None


def _safe_divide(numerator: Decimal, denominator: Decimal) -> Optional[Decimal]:
    """Return numerator/denominator, or None if denominator is zero/invalid."""
    try:
        if denominator <= Decimal("0"):
            return None
        return numerator / denominator
    except InvalidOperation:
        return None


# ---------------------------------------------------------------------------
# Main entrypoint
# ---------------------------------------------------------------------------

def compute_quote_ready(inp: QuoteReadyInput) -> QuoteReadyResult:
    missing: list[str] = []

    # --- purchase basis ---
    basis = _purchase_basis(inp)
    if basis is None:
        missing.append("purchase_price")

    # --- rehab ---
    rehab = inp.rehab_estimate
    if rehab is None:
        missing.append("rehab_estimate")

    # --- project cost ---
    project_cost_fig: Optional[Figure] = None
    project_cost_val: Optional[Decimal] = None
    project_cost_conf: Optional[Confidence] = None

    if basis is not None and rehab is not None:
        project_cost_val = basis.value + rehab
        project_cost_conf = basis.confidence  # rehab is a direct input, carries basis conf
        project_cost_fig = Figure(
            raw=project_cost_val,
            display=_fmt_dollars(project_cost_val),
            source=basis.source,
            confidence=project_cost_conf,
        )
    else:
        missing.append("project_cost")
        missing.append("ltc")

    # --- ARV ---
    arv = inp.arv
    if arv is None:
        missing.append("arv")
        missing.append("ltv")

    # --- proposed loan ---
    proposed_loan_fig: Optional[Figure] = None
    proposed_loan_val: Optional[Decimal] = None
    proposed_loan_conf: Optional[Confidence] = None

    if project_cost_val is not None and project_cost_val > Decimal("0"):
        ltc_cap = inp.max_ltc * project_cost_val
        if arv is not None and arv > Decimal("0"):
            ltv_cap = inp.max_ltv * arv
            proposed_loan_val = min(ltc_cap, ltv_cap)
            loan_source = "min(ltc_cap,ltv_cap)"
            # ARV is a direct input (high); loan confidence driven by weakest input
            proposed_loan_conf = project_cost_conf  # type: ignore[assignment]
        else:
            proposed_loan_val = ltc_cap
            loan_source = "ltc_cap_only"
            proposed_loan_conf = project_cost_conf  # type: ignore[assignment]

        proposed_loan_fig = Figure(
            raw=proposed_loan_val,
            display=_fmt_dollars(proposed_loan_val),
            source=loan_source,
            confidence=proposed_loan_conf,  # type: ignore[arg-type]
        )
    elif project_cost_val is not None:
        # project_cost present but zero — treat as invalid
        missing.append("proposed_loan")
    else:
        missing.append("proposed_loan")

    # --- LTC ---
    ltc_fig: Optional[Figure] = None
    if project_cost_val is not None and proposed_loan_val is not None:
        ltc_val = _safe_divide(proposed_loan_val, project_cost_val)
        if ltc_val is not None:
            ltc_fig = Figure(
                raw=ltc_val,
                display=_fmt_pct(ltc_val),
                source="computed",
                confidence=project_cost_conf,  # type: ignore[arg-type]
            )

    # --- LTV ---
    ltv_fig: Optional[Figure] = None
    if arv is not None and proposed_loan_val is not None:
        ltv_val = _safe_divide(proposed_loan_val, arv)
        if ltv_val is not None:
            ltv_conf = min_confidence(proposed_loan_conf, "high")  # type: ignore[arg-type]
            ltv_fig = Figure(
                raw=ltv_val,
                display=_fmt_pct(ltv_val),
                source="computed",
                confidence=ltv_conf,
            )

    return QuoteReadyResult(
        project_cost=project_cost_fig,
        proposed_loan=proposed_loan_fig,
        ltc=ltc_fig,
        ltv=ltv_fig,
        missing=sorted(set(missing)),
    )
