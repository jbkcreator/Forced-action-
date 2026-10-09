"""T-07 Minute-5 pre-qualification PDF: trigger check, letter context, delivery sink.

Stand-in types until Dev 2's T-04 contracts (BorrowerProfile / LoanRequest / LenderFitResult)
land on dev; swap the imports then. No rates, points or terms ever appear in the letter.
"""
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Protocol, Sequence


TEMPLATE = "prequal_letter.html"

_LOAN_TYPE_LABELS = {
    "FIX_AND_FLIP": "fix and flip",
    "GROUND_UP_CONSTRUCTION": "ground-up construction",
    "DSCR_RENTAL": "DSCR rental",
    "BRIDGE": "bridge",
}


@dataclass(frozen=True)
class PrequalLead:
    credit_band: Optional[str] = None
    loan_amount: Optional[int] = None
    property_state: Optional[str] = None
    loan_type: Optional[str] = None


@dataclass(frozen=True)
class FitLimits:
    """Min/max loan amount of one fitting lender (stand-in for LenderFitResult)."""
    min_amount: int
    max_amount: int


class PrequalSink(Protocol):
    def deliver(self, lead_id: int, contact_id: str, pdf: bytes) -> None: ...


def should_generate(lead: PrequalLead) -> bool:
    """All 4 core fields present (spec 4.3). Zero is not a loan amount."""
    return bool(
        lead.credit_band and lead.credit_band.strip()
        and lead.loan_amount and lead.loan_amount > 0
        and lead.property_state and lead.property_state.strip()
        and lead.loan_type and lead.loan_type.strip()
    )


def compute_range(amount: int, fits: Sequence[FitLimits], pct: int) -> Optional[tuple]:
    """Placeholder range: amount +/- pct, clamped to the widest fitting-lender limits."""
    if not fits:
        return None
    lo = int(amount * (100 - pct) / 100)
    hi = int(amount * (100 + pct) / 100)
    lo = max(lo, min(f.min_amount for f in fits))
    hi = min(hi, max(f.max_amount for f in fits))
    return (lo, hi) if lo <= hi else None


def build_context(lead: PrequalLead, fits: Sequence[FitLimits], pct: int) -> Optional[dict]:
    if not should_generate(lead):
        return None
    rng = compute_range(lead.loan_amount, fits, pct)
    if rng is None:
        return None
    return {
        "generated_date": datetime.now(timezone.utc).strftime("%B %d, %Y"),
        "loan_type_label": _LOAN_TYPE_LABELS.get(lead.loan_type, lead.loan_type.replace("_", " ").lower()),
        "property_state": lead.property_state.strip().upper(),
        "credit_band": lead.credit_band.strip(),
        "range_low": f"${rng[0]:,}",
        "range_high": f"${rng[1]:,}",
    }
