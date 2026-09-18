"""
Tests for classify.assign_partner_class.
Seam: pure function — entity_type string + role string → PartnerClass literal.
"""
import pytest

from src.services.partner_mining.classify import assign_partner_class, PartnerClass


def test_lender_role_returns_lender_class():
    assert assign_partner_class(entity_type="LLC", role="lender") == PartnerClass.LENDER


def test_wholesaler_role_returns_wholesaler_class():
    assert assign_partner_class(entity_type="LLC", role="wholesaler") == PartnerClass.WHOLESALER


def test_builder_role_returns_builder_class():
    assert assign_partner_class(entity_type="Corporate", role="builder") == PartnerClass.BUILDER


def test_contractor_role_returns_contractor_class():
    assert assign_partner_class(entity_type="Individual", role="contractor") == PartnerClass.CONTRACTOR


def test_unknown_role_raises():
    with pytest.raises(ValueError):
        assign_partner_class(entity_type="LLC", role="unknown_role")


def test_partner_class_values_are_strings():
    """Enum values must be stable string literals stored in partner_class column."""
    assert PartnerClass.LENDER.value == "lender"
    assert PartnerClass.WHOLESALER.value == "wholesaler"
    assert PartnerClass.BUILDER.value == "builder"
    assert PartnerClass.CONTRACTOR.value == "contractor"
