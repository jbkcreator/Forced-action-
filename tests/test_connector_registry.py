"""
Unit tests for src/connectors/registry.py — pure Python, no DB required.
"""
from __future__ import annotations

import pytest

from src.connectors.registry import OUTCOME_CONNECTORS, ConnectorSpec, enabled_connectors, get_spec


class TestRegistryEntries:
    def test_all_entries_are_connector_specs(self):
        for spec in OUTCOME_CONNECTORS.values():
            assert isinstance(spec, ConnectorSpec)

    def test_dict_key_matches_spec_source_type(self):
        for key, spec in OUTCOME_CONNECTORS.items():
            assert key == spec.source_type

    def test_source_types_are_unique(self):
        source_types = [spec.source_type for spec in OUTCOME_CONNECTORS.values()]
        assert len(source_types) == len(set(source_types))

    def test_all_known_wave1_connectors_present(self):
        expected = {
            "foreclosure_outcomes",
            "tax_deed_outcomes",
            "appraiser_sale_outcomes",
            "dor_sale_outcomes",
        }
        assert expected.issubset(OUTCOME_CONNECTORS.keys())

    def test_still_unbuilt_connectors_disabled(self):
        # dor_sale_outcomes has no module yet.
        assert OUTCOME_CONNECTORS["dor_sale_outcomes"].enabled is False

    def test_built_connectors_enabled(self):
        # tax_deed_outcomes, appraiser_sale_outcomes, foreclosure_outcomes, and the
        # label layer modules now exist.
        assert OUTCOME_CONNECTORS["tax_deed_outcomes"].enabled is True
        assert OUTCOME_CONNECTORS["appraiser_sale_outcomes"].enabled is True
        assert OUTCOME_CONNECTORS["foreclosure_outcomes"].enabled is True
        assert OUTCOME_CONNECTORS["outcome_label_layer"].enabled is True

    def test_sla_minutes_positive(self):
        for spec in OUTCOME_CONNECTORS.values():
            assert spec.sla_minutes > 0

    def test_off_days_is_frozenset(self):
        for spec in OUTCOME_CONNECTORS.values():
            assert isinstance(spec.off_days, frozenset)


class TestGetSpec:
    def test_returns_registered_spec(self):
        spec = get_spec("foreclosure_outcomes")
        assert spec.source_type == "foreclosure_outcomes"

    def test_unregistered_source_type_raises_key_error_with_message(self):
        with pytest.raises(KeyError, match="not a registered outcome connector"):
            get_spec("nonexistent_source_xyz")


class TestEnabledConnectors:
    def test_matches_built_modules(self):
        assert set(enabled_connectors().keys()) == {
            "tax_deed_outcomes", "appraiser_sale_outcomes", "foreclosure_outcomes",
            "outcome_label_layer",
        }
