"""
Unit tests for src/utils/parcel_id.py

No DB required — county config is mocked for format dispatch tests.
"""

import pytest
from unittest.mock import patch

from src.utils.parcel_id import (
    _normalize_folio,
    _normalize_strap,
    normalize_parcel_id,
)


# ---------------------------------------------------------------------------
# _normalize_folio
# ---------------------------------------------------------------------------

class TestNormalizeFolio:

    def test_strips_whitespace(self):
        assert _normalize_folio("  086714-0100  ") == "086714-0100"

    def test_uppercases_alpha(self):
        assert _normalize_folio("u-22-28-zzz") == "U-22-28-ZZZ"

    def test_removes_special_chars(self):
        assert _normalize_folio("08671 4.0100") == "086714.0100".replace('.', '')
        # dots are stripped
        assert _normalize_folio("086714.0100") == "0867140100"

    def test_collapses_double_hyphens(self):
        assert _normalize_folio("086714--0100") == "086714-0100"

    def test_strips_leading_trailing_hyphens(self):
        assert _normalize_folio("-086714-") == "086714"

    def test_plain_digits_pass_through(self):
        assert _normalize_folio("0867140100") == "0867140100"

    def test_long_folio_with_prefix(self):
        assert _normalize_folio("U-22-28-ZZZ-000001-00000") == "U-22-28-ZZZ-000001-00000"


# ---------------------------------------------------------------------------
# _normalize_strap
# ---------------------------------------------------------------------------

class TestNormalizeStrap:

    def test_canonical_form_unchanged(self):
        assert _normalize_strap("08-31-15-00000-001-0100") == "08-31-15-00000-001-0100"

    def test_compact_18_digit(self):
        # 08 31 15 00000 001 0100 → "083115000000010100"
        assert _normalize_strap("083115000000010100") == "08-31-15-00000-001-0100"

    def test_zero_pads_single_digit_section(self):
        assert _normalize_strap("8-31-15-00000-001-0100") == "08-31-15-00000-001-0100"

    def test_zero_pads_short_subdivision(self):
        assert _normalize_strap("19-29-16-210-210-0100") == "19-29-16-00210-210-0100"

    def test_compact_with_spaces_stripped(self):
        assert _normalize_strap("  083115000000010100  ") == "08-31-15-00000-001-0100"

    def test_invalid_raises_value_error(self):
        with pytest.raises(ValueError, match="Cannot parse"):
            _normalize_strap("NOTAPARCEL")

    def test_too_short_raises_value_error(self):
        with pytest.raises(ValueError):
            _normalize_strap("12345")

    def test_real_pinellas_example(self):
        assert _normalize_strap("19-29-16-00000-210-0100") == "19-29-16-00000-210-0100"


# ---------------------------------------------------------------------------
# normalize_parcel_id — dispatch
# ---------------------------------------------------------------------------

_FOLIO_CONFIG = {"parcel_id_format": "folio"}
_STRAP_CONFIG = {"parcel_id_format": "strap"}


class TestNormalizeParcelId:

    def test_dispatches_to_folio_for_hillsborough(self):
        with patch("src.utils.parcel_id.get_county_config", return_value=_FOLIO_CONFIG):
            result = normalize_parcel_id("U-22-28-ZZZ-000001-00000", "hillsborough")
        assert result == "U-22-28-ZZZ-000001-00000"

    def test_dispatches_to_strap_for_pinellas(self):
        with patch("src.utils.parcel_id.get_county_config", return_value=_STRAP_CONFIG):
            result = normalize_parcel_id("08-31-15-00000-001-0100", "pinellas")
        assert result == "08-31-15-00000-001-0100"

    def test_compact_strap_expanded_via_dispatch(self):
        with patch("src.utils.parcel_id.get_county_config", return_value=_STRAP_CONFIG):
            result = normalize_parcel_id("083115000000010100", "pinellas")
        assert result == "08-31-15-00000-001-0100"

    def test_raises_on_empty_string(self):
        with patch("src.utils.parcel_id.get_county_config", return_value=_FOLIO_CONFIG):
            with pytest.raises(ValueError):
                normalize_parcel_id("", "hillsborough")

    def test_raises_on_none(self):
        with patch("src.utils.parcel_id.get_county_config", return_value=_FOLIO_CONFIG):
            with pytest.raises(ValueError):
                normalize_parcel_id(None, "hillsborough")

    def test_raises_on_nan_string(self):
        with patch("src.utils.parcel_id.get_county_config", return_value=_FOLIO_CONFIG):
            with pytest.raises(ValueError):
                normalize_parcel_id("nan", "hillsborough")

    def test_defaults_to_folio_if_no_format_key(self):
        with patch("src.utils.parcel_id.get_county_config", return_value={}):
            result = normalize_parcel_id("0867140100", "hillsborough")
        assert result == "0867140100"

    def test_hillsborough_folio_identical_before_after(self):
        """Existing Hillsborough data should normalize to same value it already has."""
        with patch("src.utils.parcel_id.get_county_config", return_value=_FOLIO_CONFIG):
            raw = "086714-0100"
            assert normalize_parcel_id(raw, "hillsborough") == raw
