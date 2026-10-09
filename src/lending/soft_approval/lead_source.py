"""Where the borrower profile and core loan facts come from.

ASSUMPTION (not yet confirmed against David's LendingFlow schema, due Oct 10): credit band, loan type,
loan amount and state come from the borrower's LendingFlow lead, matched to the call by normalized
phone. The lead store belongs to T-11, which is not built, so the production source below is
unavailable and returns nothing; the feature stays closed until T-11 supplies a real source.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Mapping, Optional, Protocol

from sqlalchemy.orm import Session

from src.lending.contracts import BorrowerProfile, LoanType

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LeadProfile:
    lead_ref: str
    borrower: BorrowerProfile
    loan_type: Optional[LoanType] = None
    loan_amount: Optional[Decimal] = None
    state: Optional[str] = None

    def has_core_fields(self) -> bool:
        return self.loan_type is not None and self.loan_amount is not None and bool(self.state)


class LeadProfileSource(Protocol):
    def for_phone(self, db: Session, phone: str) -> Optional[LeadProfile]:
        """The lead for a normalized phone, or None."""


class UnavailableLeadProfileSource:
    """Production placeholder until T-11 stores LendingFlow leads. Finds nothing; it never guesses a profile."""

    def for_phone(self, db: Session, phone: str) -> Optional[LeadProfile]:
        logger.warning("[soft-approval] no LendingFlow lead store is available yet (T-11); no profile for the call")
        return None


class FakeLeadProfileSource:
    """Test double keyed by normalized phone."""

    def __init__(self, leads: Mapping[str, LeadProfile]) -> None:
        self._leads = dict(leads)

    def for_phone(self, db: Session, phone: str) -> Optional[LeadProfile]:
        return self._leads.get(phone)


def default_lead_source() -> LeadProfileSource:
    return UnavailableLeadProfileSource()
