"""
WP-8A Quote Ready deal math — pure stateless compute.

Entrypoint: compute_quote_ready(QuoteReadyInput) → QuoteReadyResult

No DB, no network, no program knowledge. max_ltc/max_ltv are passed in by
the caller (program-match layer); this function stays program-agnostic.
"""
from decimal import Decimal, ROUND_HALF_UP
from typing import NamedTuple, Optional

from .models import (
    Confidence,
    Figure,
    QuoteReadyInput,
    QuoteReadyResult,
    min_confidence,
    rehab_confidence,
)

_ZERO = Decimal("0")
_ONE = Decimal("1")
_ONE_TENTH_PCT = Decimal("0.1")

# Every derivable figure — used to build missing[] from the final result state
_FIGURE_NAMES = ("project_cost", "proposed_loan", "ltc", "ltv")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fmt_dollars(v: Decimal) -> str:
    return f"${v.quantize(_ONE, rounding=ROUND_HALF_UP):,.0f}"


def _fmt_pct(v: Decimal) -> str:
    return f"{(v * 100).quantize(_ONE_TENTH_PCT, rounding=ROUND_HALF_UP)}%"


class _PurchaseBasis(NamedTuple):
    value: Decimal
    source: str
    confidence: Confidence


def _purchase_basis(inp: QuoteReadyInput) -> Optional[_PurchaseBasis]:
    """First positive value in the fallback chain, or None."""
    chain: list[tuple[Optional[Decimal], str, Confidence]] = [
        (inp.purchase_price, "purchase_price", "high"),
        (inp.estimated_value, "estimated_value", "medium"),
        (inp.assessed_value_mkt, "assessed_value_mkt", "low"),
        (inp.last_sale_price, "last_sale_price", "low"),
    ]
    for value, source, conf in chain:
        if value is not None and value > _ZERO:
            return _PurchaseBasis(value, source, conf)
    return None


# ---------------------------------------------------------------------------
# Main entrypoint
# ---------------------------------------------------------------------------

def compute_quote_ready(inp: QuoteReadyInput) -> QuoteReadyResult:
    # Input-level diagnostics — what to chase. Non-positive is unusable.
    input_missing: list[str] = []

    basis = _purchase_basis(inp)
    if basis is None:
        input_missing.append("purchase_price")

    rehab_usable = inp.rehab_estimate is not None and inp.rehab_estimate >= _ZERO
    if not rehab_usable:
        input_missing.append("rehab_estimate")

    arv_usable = inp.arv is not None and inp.arv > _ZERO
    if not arv_usable:
        input_missing.append("arv")

    # --- project cost ---
    project_cost_fig: Optional[Figure] = None
    project_cost_val: Optional[Decimal] = None
    project_cost_conf: Optional[Confidence] = None

    if basis is not None and rehab_usable:
        cost = basis.value + inp.rehab_estimate  # type: ignore[operator]
        if cost > _ZERO:
            project_cost_val = cost
            project_cost_conf = min_confidence(
                basis.confidence, rehab_confidence(inp.rehab_source)
            )
            project_cost_fig = Figure(
                raw=cost,
                display=_fmt_dollars(cost),
                source=f"{basis.source}+{inp.rehab_source}",
                confidence=project_cost_conf,
            )

    # --- proposed loan (derived) ---
    proposed_loan_fig: Optional[Figure] = None
    proposed_loan_val: Optional[Decimal] = None
    proposed_loan_conf: Optional[Confidence] = None

    if project_cost_val is not None:
        ltc_cap = inp.max_ltc * project_cost_val
        if arv_usable:
            ltv_cap = inp.max_ltv * inp.arv  # type: ignore[operator]
            proposed_loan_val = min(ltc_cap, ltv_cap)
            loan_source = "min(ltc_cap,ltv_cap)"
            proposed_loan_conf = min_confidence(project_cost_conf, inp.arv_confidence)  # type: ignore[arg-type]
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

    # --- LTC = loan / cost ---
    ltc_fig: Optional[Figure] = None
    if proposed_loan_val is not None and project_cost_val is not None and project_cost_val > _ZERO:
        ltc_val = proposed_loan_val / project_cost_val
        ltc_fig = Figure(
            raw=ltc_val,
            display=_fmt_pct(ltc_val),
            source="computed",
            confidence=proposed_loan_conf,  # type: ignore[arg-type]
        )

    # --- LTV = loan / arv ---
    ltv_fig: Optional[Figure] = None
    if proposed_loan_val is not None and arv_usable:
        ltv_val = proposed_loan_val / inp.arv  # type: ignore[operator]
        ltv_fig = Figure(
            raw=ltv_val,
            display=_fmt_pct(ltv_val),
            source="computed",
            confidence=min_confidence(proposed_loan_conf, inp.arv_confidence),  # type: ignore[arg-type]
        )

    result = QuoteReadyResult(
        project_cost=project_cost_fig,
        proposed_loan=proposed_loan_fig,
        ltc=ltc_fig,
        ltv=ltv_fig,
    )

    # missing[] = input diagnostics ∪ every figure that could not be produced.
    # Deriving figure-misses from the final state guarantees completeness.
    figure_missing = [
        name for name in _FIGURE_NAMES if getattr(result, name) is None
    ]
    result.missing = sorted(set(input_missing) | set(figure_missing))
    return result
