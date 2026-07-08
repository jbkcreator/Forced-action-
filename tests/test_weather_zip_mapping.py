"""SAME/UGC → ZIP mapping used by storm_engine + flood_engine (weather scraper revival)."""

from src.services.nws_same_to_zip import alert_to_zips, expand_codes, fips_to_zips
from src.scrappers.storm.storm_engine import _extract_affected_zips


def _alert_props(same=None, ugc=None, description=""):
    return {"geocode": {"SAME": same or [], "UGC": ugc or []}, "description": description}


def test_same_code_maps_to_hillsborough_zips():
    zips = alert_to_zips(_alert_props(same=["012057"]))
    assert "33602" in zips and len(zips) > 40


def test_ugc_forecast_zone_maps_to_county():
    assert "33602" in alert_to_zips(_alert_props(ugc=["FLZ151"]))
    assert "33701" in alert_to_zips(_alert_props(ugc=["FLZ050"]))


def test_unknown_codes_ignored():
    assert alert_to_zips(_alert_props(same=["099999"], ugc=["TXZ001"])) == []
    assert alert_to_zips({"description": "no geocode key"}) == []


def test_fips_to_zips():
    assert "33701" in fips_to_zips("12103")
    assert fips_to_zips("") == []
    assert fips_to_zips("99999") == []


def test_storm_extract_uses_geocode_not_just_description():
    alert = {"properties": _alert_props(ugc=["FLZ251"], description="No zips in this text.")}
    assert "33602" in _extract_affected_zips(alert)


def test_storm_extract_description_fallback_still_works():
    alert = {"properties": _alert_props(description="Affected areas include 33547 and 33602.")}
    assert sorted(_extract_affected_zips(alert)) == ["33547", "33602"]


def test_expand_codes_dedupes_same_and_ugc():
    zips = expand_codes(["012057"], ["FLZ151", "FLZ251"])
    assert zips == sorted(set(zips))
