"""Where the borrower profile and core loan facts come from.

Credit band, loan type, loan amount and state come from the borrower's LendingFlow lead
(``lending.lendingflow_leads``, T-11), matched to the call by normalized phone. T-11 already stores the
loan type as a ``LoanType`` value (unrecognised spellings are saved as NULL) and the phone normalized.

ASSUMPTION (field names and loan type spellings not yet confirmed against David's LendingFlow schema):
the lead's ``credit_band_min_fico``, ``loan_amount``, ``loan_type`` and ``property_state`` mean the same
as the soft approval's inputs. A missing or unrecognised value is never guessed: the lead comes back with
that field empty and the service stops with ``lead_missing_core_fields``.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Mapping, Optional, Protocol

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.lending.contracts import BorrowerProfile, LoanType
from src.lending.enrichment.fit_card import parse_band_bounds
from src.services.phone_utils import normalize

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
        """The lead for a phone, or None."""


class LendingFlowLeadProfileSource:
    """The most recent LendingFlow lead for a phone. Finds nothing rather than guessing, and never looks past a suppressed lead."""

    def for_phone(self, db: Session, phone: str) -> Optional[LeadProfile]:
        normalized = normalize(phone)
        if not normalized:
            return None
        row = db.execute(
            text(
                "SELECT lead_uuid, credit_band, credit_band_min_fico, loan_amount, loan_type, property_state, suppressed "
                "FROM lending.lendingflow_leads WHERE phone = :phone "
                "ORDER BY received_at DESC LIMIT 1"
            ),
            {"phone": normalized},
        ).first()
        if row is None or row.suppressed:
            return None
        loan_type = self._loan_type(row.loan_type)
        if row.loan_type and loan_type is None:
            logger.warning("[soft-approval] lead %s has an unrecognised loan type; treated as missing", row.lead_uuid)
        return LeadProfile(
            lead_ref=str(row.lead_uuid),
            borrower=BorrowerProfile(credit_band_min_fico=row.credit_band_min_fico,
                                     credit_band_max_fico=(parse_band_bounds(row.credit_band) or (None, None))[1],
                                     state=row.property_state),
            loan_type=loan_type,
            loan_amount=Decimal(row.loan_amount) if row.loan_amount is not None else None,
            state=row.property_state,
        )

    @staticmethod
    def _loan_type(value: Optional[str]) -> Optional[LoanType]:
        try:
            return LoanType(value) if value else None
        except ValueError:
            return None


class FakeLeadProfileSource:
    """Test double keyed by normalized phone."""

    def __init__(self, leads: Mapping[str, LeadProfile]) -> None:
        self._leads = dict(leads)

    def for_phone(self, db: Session, phone: str) -> Optional[LeadProfile]:
        return self._leads.get(phone)


def default_lead_source() -> LeadProfileSource:
    return LendingFlowLeadProfileSource()
