"""Unit tests for src/utils/address_normalize.py — the shared normalizer."""
import pytest

from src.utils.address_normalize import normalize_street_address


class TestBugFixes:
    """Four bugs the new module fixes vs. the old base.py pipeline."""

    def test_way_is_way_not_wy(self):
        # USPS abbreviation for WAY is WAY (WY = Wyoming, the state)
        assert normalize_street_address("1340 HOMESTEAD WAY") == "1340 homestead way"

    def test_pre_directional_after_house_number(self):
        # Old pipeline missed " north " when at start of street name
        assert (
            normalize_street_address("1234 N DALE MABRY HWY")
            == "1234 n dale mabry hwy"
        )

    def test_post_directional_at_end_of_string(self):
        # No trailing space — old space-delimited replace missed this
        assert normalize_street_address("2448 45TH ST S") == "2448 45th st s"

    def test_compound_directional_spelled_out(self):
        assert normalize_street_address("500 NORTHEAST 5TH AVE") == "500 ne 5th ave"
        assert normalize_street_address("500 NORTHWEST 10TH ST") == "500 nw 10th st"


class TestZeroPadding:
    def test_leading_zeros_stripped(self):
        assert normalize_street_address("007 MAIN ST") == "7 main st"
        assert normalize_street_address("0100 ELM AVE") == "100 elm ave"

    def test_padded_and_unpadded_match(self):
        assert normalize_street_address("007 MAIN ST") == normalize_street_address(
            "7 MAIN ST"
        )


class TestNewSuffixes:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("123 OAK CROSSING", "123 oak xing"),
            ("5 ELM COVE", "5 elm cv"),
            ("3 PINE GROVE", "3 pine grv"),
            ("7 OAK RIDGE", "7 oak rdg"),
            ("99 MAIN TURNPIKE", "99 main tpke"),
            ("14 OAK PLAZA", "14 oak plz"),
            ("21 LAKE VIEW", "21 lake vw"),
            ("33 ELM VILLAGE", "33 elm vlg"),
            ("8 PINE TRACE", "8 pine trce"),
        ],
    )
    def test_suffix_abbreviated(self, raw, expected):
        assert normalize_street_address(raw) == expected


class TestUnitStripping:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("11308 N BLACKBARK DR APT 4", "11308 n blackbark dr"),
            ("100 MAIN ST UNIT 5B", "100 main st"),
            ("200 OAK AVE STE 101", "200 oak ave"),
            ("300 ELM BLVD #4A", "300 elm blvd"),
            ("400 PINE CT BLDG 3", "400 pine ct"),
        ],
    )
    def test_unit_designators_removed(self, raw, expected):
        assert normalize_street_address(raw) == expected


class TestInvalidAddresses:
    @pytest.mark.parametrize(
        "raw",
        [
            None,
            "",
            "   ",
            "NOT PROVIDED",
            "RIGHT OF WAY",
            "123 MAIN ST & OAK AVE",
            "INTERSECTION OF MAIN AND ELM",
            "MAIN STREET TAMPA",  # no house number
            "OAK DRIVE",
        ],
    )
    def test_returns_empty(self, raw):
        assert normalize_street_address(raw) == ""


class TestCityStateZipStripping:
    def test_comma_separated_city_state_zip(self):
        assert (
            normalize_street_address("11308 BLACKBARK DR, RIVERVIEW, FL 33579")
            == "11308 blackbark dr"
        )

    def test_trailing_zip_only(self):
        # Bare 5-digit ZIP at end is stripped
        assert normalize_street_address("100 MAIN ST 33601") == "100 main st"

    def test_zip_plus_4(self):
        assert (
            normalize_street_address("100 MAIN ST, TAMPA FL 33601-1234")
            == "100 main st"
        )

    def test_semicolon_split(self):
        assert normalize_street_address("123 MAIN ST; UNIT 4") == "123 main st"


class TestIdempotency:
    @pytest.mark.parametrize(
        "raw",
        [
            "11308 N BLACKBARK DR APT 4",
            "1340 HOMESTEAD WAY",
            "2448 45TH ST S",
            "500 NORTHEAST 5TH AVE",
            "007 MAIN ST",
        ],
    )
    def test_double_normalize_is_stable(self, raw):
        once = normalize_street_address(raw)
        twice = normalize_street_address(once)
        assert once == twice
        assert once != ""


class TestFallback:
    def test_ambiguous_input_does_not_raise(self):
        # Inputs that confuse usaddress.tag must not crash — fallback returns str
        result = normalize_street_address("123 MAIN 456 ST")
        assert isinstance(result, str)
