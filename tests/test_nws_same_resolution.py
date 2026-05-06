"""
NWS SAME/UGC → ZIP resolution tests.

Verifies the crosswalk lookup added in fa008+ (2026-05-04). Without it, raw
NWS CAP feeds (which use FIPS / UGC codes, not ZIPs) silently dispatched no
storm-pack signal for any of our subscribers.

Run:
    pytest tests/test_nws_same_resolution.py -v
"""
from src.services.nws_same_to_zip import expand_codes, SAME_TO_ZIPS, UGC_TO_ZIPS
from src.services.nws_webhook import _extract_zip_codes


def test_hillsborough_same_expands_to_known_zips():
    zips = expand_codes(["012057"], [])
    assert "33647" in zips    # Tampa Palms
    assert "33602" in zips    # Downtown Tampa
    assert "33606" in zips    # South Tampa
    # No leakage from other counties
    assert "33701" not in zips  # Pinellas / St Pete


def test_ugc_alias_maps_to_same_county():
    same_zips = set(expand_codes(["012057"], []))
    ugc_zips = set(expand_codes([], ["FLC057"]))
    assert same_zips == ugc_zips


def test_unknown_same_code_silently_drops():
    """Codes outside the active-county set must return [] — the regex /
    parameters fallback in the webhook handler picks up anything we miss."""
    zips = expand_codes(["099999"], ["XYZ999"])
    assert zips == []


def test_extract_zip_codes_resolves_same_plus_areadesc():
    """Combined: a real CAP-shaped payload with SAME + a stray ZIP in the
    prose must dedupe and return a sorted unique list."""
    payload = {
        "event": "Tornado Warning",
        "geocode": {"SAME": ["012057"], "UGC": []},
        "areaDesc": "Hillsborough County including 33647 and surrounding areas",
        "parameters": {},
    }
    out = _extract_zip_codes(payload)
    # 33647 is in both the SAME crosswalk AND the areaDesc — must appear once
    assert out.count("33647") == 1
    # Plus the rest of the Hillsborough list
    assert "33602" in out
    assert sorted(out) == out   # stable sort


def test_extract_zip_codes_explicit_relay_list_wins():
    """When a relay pre-computes affectedZips, those are unioned with SAME."""
    payload = {
        "event": "Hurricane Warning",
        "geocode": {"SAME": ["012057"]},
        "areaDesc": "",
        "parameters": {"affectedZips": ["33647", "99999"]},
    }
    out = _extract_zip_codes(payload)
    assert "33647" in out
    assert "99999" in out
    assert "33602" in out  # from SAME


def test_extract_zip_codes_no_geocode_falls_back_to_regex():
    """Backward compat — payloads without SAME/UGC keep using the regex path."""
    payload = {
        "event": "Severe Thunderstorm Warning",
        "areaDesc": "Pinellas County including 33701 and 33704",
    }
    out = _extract_zip_codes(payload)
    assert set(out) == {"33701", "33704"}


def test_all_known_counties_have_zips():
    """Crosswalk integrity: every entry must have at least 5 ZIPs."""
    for code, zips in SAME_TO_ZIPS.items():
        assert len(zips) >= 5, f"SAME {code} has too few ZIPs"
        # All ZIPs are 5-digit strings
        assert all(z.isdigit() and len(z) == 5 for z in zips), f"bad zip in {code}"
    # UGC mirrors SAME
    assert len(UGC_TO_ZIPS) == len(SAME_TO_ZIPS)
