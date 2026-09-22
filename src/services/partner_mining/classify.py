"""
Partner classification (SPEC Stage C).

assign_partner_class maps (entity_type, role) → PartnerClass.
Builder/contractor classes are read from the WP-T2-8 buyer_entities graph
(GRILL Q9) — this module owns the mapping logic only.
"""

from __future__ import annotations

from enum import Enum


class PartnerClass(str, Enum):
    LENDER = "lender"
    WHOLESALER = "wholesaler"
    BUILDER = "builder"
    CONTRACTOR = "contractor"
    # Deferred — reserved for future MLS/agent source
    TITLE_REP = "title_rep"
    INVESTOR_AGENT = "investor_agent"
    BROKER = "broker"
    PROPERTY_MANAGER = "property_manager"
    INSURANCE_AGENT = "insurance_agent"


_ROLE_MAP: dict[str, PartnerClass] = {
    "lender": PartnerClass.LENDER,
    "wholesaler": PartnerClass.WHOLESALER,
    "builder": PartnerClass.BUILDER,
    "contractor": PartnerClass.CONTRACTOR,
}


def assign_partner_class(entity_type: str, role: str) -> PartnerClass:
    """
    Return the PartnerClass for a resolved counterparty.

    role must be one of the buildable v1 roles (lender, wholesaler, builder,
    contractor). Raises ValueError for unknown roles — callers must never
    assign a class from unverified sources (spec line 822).
    """
    cls = _ROLE_MAP.get(role.lower())
    if cls is None:
        raise ValueError(f"Unknown partner role: {role!r}. Must be one of {list(_ROLE_MAP)}")
    return cls
