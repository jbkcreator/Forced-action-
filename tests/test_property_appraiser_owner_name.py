"""
Unit tests for _normalize_pa_owner_name — pure Python, no DB.

Regression coverage for: a pandas NaN owner_name cell is a `float`, and
`if not name` alone doesn't catch it (NaN is truthy in Python), so it used
to fall through to `name.strip()` and crash with
'float' object has no attribute 'strip'`. Confirmed live during appraiser
scraper testing (parcel 1319100000).
"""
from __future__ import annotations

import pytest

from src.loaders.property_appraiser import _normalize_pa_owner_name


class TestNormalizePaOwnerNameMissingInput:
    @pytest.mark.parametrize("value", [float("nan"), None, "", "  ", "nan", "None"])
    def test_missing_values_return_none_without_raising(self, value):
        assert _normalize_pa_owner_name(value) is None


class TestNormalizePaOwnerNameRealValues:
    def test_estate_of_prefix_stripped(self):
        assert _normalize_pa_owner_name("Estate of John Smith") == "John Smith"

    def test_last_first_reordered(self):
        assert _normalize_pa_owner_name("SMITH, JOHN") == "JOHN SMITH"

    def test_suffix_preserved(self):
        assert _normalize_pa_owner_name("JOHN SMITH, JR") == "JOHN SMITH JR"
