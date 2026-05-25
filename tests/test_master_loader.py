"""Unit tests for MasterPropertyLoader mailing-address helpers.

Tests _parse_mailing_parts() and _determine_absentee_status() in isolation —
no DB, no file I/O, no external imports required.
"""

from src.loaders.master import MasterPropertyLoader


# ---------------------------------------------------------------------------
# _parse_mailing_parts
# ---------------------------------------------------------------------------

class TestParseMailing:
    def test_format_b_four_parts(self):
        # ADDR_1, CITY, STATE, ZIP
        street, state = MasterPropertyLoader._parse_mailing_parts(
            "380 PARK PLACE BLVD STE 200, CLEARWATER, FL, 33759-4939"
        )
        assert street == "380 PARK PLACE BLVD STE 200"
        assert state == "FL"

    def test_format_b_street_with_comma(self):
        # Street itself has a comma (suite number inline)
        street, state = MasterPropertyLoader._parse_mailing_parts(
            "123 MAIN ST, APT 4, TAMPA, FL, 33601"
        )
        assert street == "123 MAIN ST, APT 4"
        assert state == "FL"

    def test_format_three_parts(self):
        # ADDR_1, CITY, STATE — no zip
        street, state = MasterPropertyLoader._parse_mailing_parts(
            "9625 WES KEARNEY WAY, TAMPA, FL"
        )
        assert street == "9625 WES KEARNEY WAY"
        assert state == "FL"

    def test_format_a_street_only(self):
        # Only street stored — state was blank in CSV
        street, state = MasterPropertyLoader._parse_mailing_parts(
            "9625 WES KEARNEY WAY"
        )
        assert street == "9625 WES KEARNEY WAY"
        assert state is None

    def test_out_of_state(self):
        street, state = MasterPropertyLoader._parse_mailing_parts(
            "100 PEACHTREE ST, ATLANTA, GA, 30303"
        )
        assert street == "100 PEACHTREE ST"
        assert state == "GA"

    def test_empty_string(self):
        street, state = MasterPropertyLoader._parse_mailing_parts("")
        assert street is None
        assert state is None

    def test_state_truncated_to_two_chars(self):
        # Malformed state column — should still take first two chars
        _, state = MasterPropertyLoader._parse_mailing_parts(
            "1 MAIN ST, CITY, FLORIDA, 99999"
        )
        assert state == "FL"


# ---------------------------------------------------------------------------
# _determine_absentee_status
# ---------------------------------------------------------------------------

class TestDetermineAbsenteeStatus:
    def _call(self, mailing, prop_addr="1949 ANCLOTE VIS", prop_state="FL"):
        return MasterPropertyLoader._determine_absentee_status(
            property_address=prop_addr,
            property_state=prop_state,
            mailing_address=mailing,
        )

    # -- None / empty guards ------------------------------------------------

    def test_none_mailing_returns_none(self):
        assert self._call(None) is None

    def test_none_property_address_returns_none(self):
        result = MasterPropertyLoader._determine_absentee_status(
            property_address=None,
            property_state="FL",
            mailing_address="1949 ANCLOTE VIS",
        )
        assert result is None

    def test_empty_mailing_returns_none(self):
        assert self._call("") is None

    # -- Out-of-State -------------------------------------------------------

    def test_out_of_state_format_b(self):
        result = self._call("100 PEACHTREE ST, ATLANTA, GA, 30303")
        assert result == "Out-of-State"

    def test_out_of_state_format_three(self):
        result = self._call("100 PEACHTREE ST, ATLANTA, GA")
        assert result == "Out-of-State"

    def test_fl_state_does_not_trigger_out_of_state(self):
        # Same state — must not return Out-of-State
        result = self._call(
            "999 DIFFERENT BLVD, CLEARWATER, FL, 33759",
            prop_addr="1949 ANCLOTE VIS",
        )
        assert result != "Out-of-State"

    # -- In-County ----------------------------------------------------------

    def test_in_county_format_b_matching_street(self):
        # Full blob, mailing street matches situs street after normalization
        result = self._call(
            "1949 ANCLOTE VIS, TARPON SPRINGS, FL, 34689",
            prop_addr="1949 ANCLOTE VIS",
        )
        assert result == "In-County"

    def test_in_county_format_a_street_only(self):
        # Street-only blob, matches
        result = self._call("1949 ANCLOTE VIS", prop_addr="1949 ANCLOTE VIS")
        assert result == "In-County"

    def test_in_county_case_insensitive(self):
        # Mixed case in mailing — normalization should handle it
        result = self._call(
            "1949 Anclote Vis, Tarpon Springs, FL, 34689",
            prop_addr="1949 ANCLOTE VIS",
        )
        assert result == "In-County"

    # -- Out-of-County (fallback) ------------------------------------------

    def test_out_of_county_different_fl_street_format_b(self):
        result = self._call(
            "380 PARK PLACE BLVD STE 200, CLEARWATER, FL, 33759",
            prop_addr="1949 ANCLOTE VIS",
        )
        assert result == "Out-of-County"

    def test_out_of_county_format_a_different_street(self):
        result = self._call("999 OTHER ROAD", prop_addr="1949 ANCLOTE VIS")
        assert result == "Out-of-County"

    def test_out_of_county_when_mailing_state_unknown(self):
        # Format A — state cannot be determined; street doesn't match → Out-of-County
        result = self._call("999 DIFFERENT ST", prop_addr="1949 ANCLOTE VIS")
        assert result == "Out-of-County"
