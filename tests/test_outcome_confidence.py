"""CDE-11 — Outcome Confidence Tier hierarchy (pure, no DB)."""
import pytest

from src.services import outcome_confidence as oc


def test_tier_weights_are_ordered_by_trust():
    """founder-verified outranks subscriber-reported outranks public-record inferred."""
    assert oc.tier_weight("founder_verified") == 1.0
    assert (
        oc.tier_weight("founder_verified")
        > oc.tier_weight("subscriber_reported")
        > oc.tier_weight("public_record_inferred")
    )


def test_default_tier_for_source_maps_origin_to_trust():
    """Each outcome source defaults to its trust tier; connectors are inferred."""
    assert oc.default_tier_for_source("subscriber_tap") == "subscriber_reported"
    assert oc.default_tier_for_source("founder_import") == "founder_verified"
    for connector in ("foreclosure_auction", "deed_flip", "lis_pendens",
                      "tax_deed", "appraiser_sale", "dor_sale", "probate", "code_lien"):
        assert oc.default_tier_for_source(connector) == "public_record_inferred"
    # An unknown source is treated as the lowest trust, not an error.
    assert oc.default_tier_for_source("something_new") == "public_record_inferred"


def test_is_inferred_true_only_for_public_record():
    """is_inferred is derived from the tier — true only for public-record inferred."""
    assert oc.is_inferred("public_record_inferred") is True
    assert oc.is_inferred("subscriber_reported") is False
    assert oc.is_inferred("founder_verified") is False


def test_validate_tier_rejects_unknown_and_accepts_known():
    """A typo'd tier is rejected before it can reach the DB; valid tiers pass."""
    for tier in ("founder_verified", "subscriber_reported", "public_record_inferred"):
        oc.validate_tier(tier)  # does not raise
    with pytest.raises(ValueError):
        oc.validate_tier("founder-verified")  # hyphen, not underscore
    with pytest.raises(ValueError):
        oc.validate_tier("")
