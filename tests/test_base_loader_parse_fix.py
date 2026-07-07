"""
Unit tests for the BaseLoader NaN-string guard fix — pure Python, no DB.

Regression coverage for: `self.parse_amount(str(raw.get("field", "")))`
turning a missing pandas cell into the literal string "nan", which the old
guard (`pd.isna(x) or not x`) failed to catch because `pd.isna("nan")` is
False and `float("nan")` succeeds instead of raising. Confirmed live in
tax_deed_auctions.sold_amount/sold_to before the fix.
"""
from __future__ import annotations

import math
from datetime import datetime

import pytest

from src.loaders.base import BaseLoader


class TestParseAmountMissingTokens:
    @pytest.mark.parametrize("value", ["nan", "NaN", "NAN", "none", "None", "null", "", "  ", None, float("nan")])
    def test_missing_tokens_return_none(self, value):
        assert BaseLoader.parse_amount(value) is None

    def test_real_number_still_parses(self):
        assert BaseLoader.parse_amount("42000.50") == 42000.50

    def test_currency_formatting_still_stripped(self):
        assert BaseLoader.parse_amount("$120,100.00") == 120100.00


class TestParseIntMissingTokens:
    @pytest.mark.parametrize("value", ["nan", "NaN", "none", "null", "", None, float("nan")])
    def test_missing_tokens_return_none(self, value):
        assert BaseLoader.parse_int(value) is None

    def test_real_int_still_parses(self):
        assert BaseLoader.parse_int("2026") == 2026


class TestParseDateMissingTokens:
    @pytest.mark.parametrize("value", ["nan", "NaN", "none", "null", "", None, float("nan")])
    def test_missing_tokens_return_none(self, value):
        assert BaseLoader.parse_date(value) is None

    def test_real_date_still_parses(self):
        assert BaseLoader.parse_date("01/21/2026") == datetime(2026, 1, 21)


class TestCleanStr:
    @pytest.mark.parametrize("value", ["nan", "NaN", "none", "null", "", "  ", None, float("nan")])
    def test_missing_tokens_return_none(self, value):
        assert BaseLoader.clean_str(value) is None

    def test_real_string_stripped(self):
        assert BaseLoader.clean_str("  3rd Party Bidder  ") == "3rd Party Bidder"

    def test_does_not_touch_legitimate_text_containing_substrings(self):
        # Guard against over-matching: a real value that merely CONTAINS "nan"
        # (e.g. a surname) must not be treated as missing.
        assert BaseLoader.clean_str("Nancy Nannerman") == "Nancy Nannerman"
