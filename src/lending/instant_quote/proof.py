"""Cora P2.2 done-when proof cases (brief: "The 4 test cases in Instant-Quote-Rules.md return the exact
documented loan amounts. A multi-guarantor test file prices off the lowest FICO with the interest reserve added").

The 4 documented cases are empty until the rules doc arrives (client Q1): fill ``request`` and ``expected_loan``
from it, verbatim. Figures in the table are indicative, subject to underwriting.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from config.lending_instant_quote import DISCLAIMER
from src.lending.instant_quote.engine import Guarantor, QuoteRequest, quote

PENDING = "awaiting Instant-Quote-Rules.md"


@dataclass(frozen=True)
class ProofCase:
    name: str
    request: Optional[QuoteRequest]
    expected_loan: Optional[Decimal]


MULTI_GUARANTOR_CASE = ProofCase(
    name="Multi-guarantor (720 + 640)",
    request=QuoteRequest(
        state="FL", zip_code="33602", borrower_type="entity",
        guarantors=(Guarantor(fico=720, completed_deals=5, exits=5), Guarantor(fico=640)),
        purpose="purchase", purchase_price=Decimal("200000"), rehab=Decimal("50000"), arv=Decimal("400000"),
    ),
    expected_loan=None,
)

DOC_CASES: tuple[ProofCase, ...] = tuple(
    ProofCase(name=f"Instant-Quote-Rules.md case {n}", request=None, expected_loan=None) for n in range(1, 5)
)


def _money(value: Optional[Decimal]) -> str:
    return "—" if value is None else f"{value:,.0f}"


def _pct(value: Decimal) -> str:
    return f"{value * 100:+.2f}%"


def render_markdown() -> str:
    lines = [
        "| Case | Status | Loan | Expected | LTC | Cuts | Fee surcharge | Conditions | Unconfirmed |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for case in (MULTI_GUARANTOR_CASE, *DOC_CASES):
        if case.request is None:
            lines.append(f"| {case.name} | {PENDING} | — | — | — | — | — | — | — |")
            continue
        r = quote(case.request)
        cuts = ", ".join(f"{c.reason} {_pct(c.pct)}" for c in r.cuts) or "—"
        conditions = ", ".join(c.code for c in r.conditions) or "—"
        ltc = "—" if r.effective_ltc is None else f"{r.effective_ltc * 100:.1f}%"
        lines.append(
            f"| {case.name} | {r.status}{f' ({r.reason})' if r.reason else ''} | {_money(r.loan_amount)} | "
            f"{_money(case.expected_loan)} | {ltc} | {cuts} | {_pct(r.fee_surcharge_pct)} | {conditions} | "
            f"{'yes' if r.unconfirmed else 'no'} |"
        )
    lines.append("")
    lines.append(f"_{DISCLAIMER}. Placeholder lender values: every figure is unconfirmed._")
    return "\n".join(lines)
