"""Synthetic loan fixtures for R1 0.10: 3 loan types x 7 edge states, deterministic and made of fake data.

Every value is derived from the loan type's index (p) and the edge state's index (c), so two calls return equal
objects and no two fixtures share a phone number or an address. Use ``loans(...)`` from other tests; see
docs/lending/synthetic-fixtures.md.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Optional

from src.lending.contracts import BorrowerRecord, EntitySchema, LoanFile, PropertySchema
from src.lending.instant_quote import Guarantor, QuoteRequest

AS_OF: date = date(2026, 11, 2)
PRODUCTS: tuple[str, ...] = ("fix_and_flip", "refinance", "ground_up")
EDGE_CASES: tuple[str, ...] = ("clean", "incomplete", "expired_documents", "conflicting_fico",
                               "cross_borrower_document", "dnc_flagged", "backflip_tagged")

# product -> (LoanFile.product, quote purpose, borrower first name, last name, dnc flag for the dnc_flagged case)
_PRODUCT_FACTS = {
    "fix_and_flip": ("flip", "purchase", "Jane", "Doe", "national_dnc"),
    "refinance": ("other", "refinance", "Carl", "Reyes", "litigator"),
    "ground_up": ("ground_up", "new_construction", "Priya", "Shah", "state_dnc"),
}


@dataclass(frozen=True)
class SyntheticDocument:
    kind: str  # "purchase_contract" | "bank_statement" | "pricing_sheet"
    owner_name: str  # the name printed on the document
    dated: date
    expires_on: Optional[date] = None


@dataclass(frozen=True)
class SyntheticLoan:
    key: str  # f"{product}/{edge_case}"
    product: str  # one of PRODUCTS
    edge_case: str  # one of EDGE_CASES
    loan_file: LoanFile
    quote_request: Optional[QuoteRequest]  # None when the file lacks the figures to quote
    documents: tuple[SyntheticDocument, ...]
    dnc_flag: Optional[str]  # None | "national_dnc" | "litigator" | "state_dnc"
    outbound_draft: str


def _build(p: int, product: str, c: int, edge_case: str) -> SyntheticLoan:
    file_product, purpose, first, last, dnc_kind = _PRODUCT_FACTS[product]
    refinance = product == "refinance"
    name = f"{first} {last}"
    address = f"{100 + 10 * p + c} Synthetic Way"

    credit_band: Optional[str] = "720-739"
    email: Optional[str] = f"{first.lower()}.{p}{c}@example.com"
    entity: Optional[EntitySchema] = EntitySchema(name=f"{last} Holdings LLC", state="FL", sunbiz_status="ACTIVE")
    amounts = dict(loan_amount=212_000, purchase_price=200_000, rehab_budget=50_000, arv=400_000,
                   payoff_balance=150_000 if refinance else None)
    guarantors = (Guarantor(720, 3, 3),)
    documents = [SyntheticDocument("purchase_contract", name, AS_OF - timedelta(days=10)),
                 SyntheticDocument("bank_statement", name, AS_OF - timedelta(days=20))]
    dnc_flag: Optional[str] = None
    draft = f"Hi {first}, following up on {address}."

    if edge_case == "incomplete":
        amounts = {key: None for key in amounts}
        email, credit_band, entity, documents = None, None, None, []
    elif edge_case == "expired_documents":
        documents = [documents[0],
                     SyntheticDocument("bank_statement", name, AS_OF - timedelta(days=75)),
                     SyntheticDocument("pricing_sheet", "Central Lending", AS_OF - timedelta(days=40),
                                       expires_on=AS_OF - timedelta(days=1))]
    elif edge_case == "conflicting_fico":
        guarantors = (Guarantor(760, 4, 2), Guarantor(640, 0, 0))
    elif edge_case == "cross_borrower_document":
        documents[1] = SyntheticDocument("bank_statement", "Mark Stone", AS_OF - timedelta(days=20))
    elif edge_case == "dnc_flagged":
        dnc_flag = dnc_kind
    elif edge_case == "backflip_tagged":
        email = f"{first.lower()}@backflip.com"
        draft += " Backflip sent this one over."

    loan_file = LoanFile(
        source="synthetic", product=file_product,
        borrower=BorrowerRecord(first_name=first, last_name=last, phone=f"+1813555{8000 + 10 * p + c:04d}",
                                email=email, credit_band=credit_band),
        property=PropertySchema(address=address, city="Tampa", state="FL", zip_code="33602"),
        entity=entity, **amounts)
    quote_request = None if edge_case == "incomplete" else QuoteRequest(
        state="FL", zip_code="33602", borrower_type="entity", guarantors=guarantors, purpose=purpose,
        purchase_price=Decimal("200000"), rehab=Decimal("50000"), arv=Decimal("400000"),
        existing_debt=Decimal("150000") if refinance else None)
    return SyntheticLoan(key=f"{product}/{edge_case}", product=product, edge_case=edge_case, loan_file=loan_file,
                         quote_request=quote_request, documents=tuple(documents), dnc_flag=dnc_flag,
                         outbound_draft=draft)


def all_loans() -> tuple[SyntheticLoan, ...]:
    """The 21 fixtures, loan type first then edge state; built fresh on every call."""
    return tuple(_build(p, product, c, edge_case)
                 for p, product in enumerate(PRODUCTS) for c, edge_case in enumerate(EDGE_CASES))


def loans(*, product: Optional[str] = None, edge_case: Optional[str] = None) -> tuple[SyntheticLoan, ...]:
    return tuple(l for l in all_loans()
                 if (product is None or l.product == product) and (edge_case is None or l.edge_case == edge_case))
