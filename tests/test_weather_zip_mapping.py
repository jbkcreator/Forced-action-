"""SAME/UGC → ZIP crosswalk + weather event→signal mapping (weather scraper revival)."""

from src.services.nws_same_to_zip import alert_to_zips, expand_codes, fips_to_zips, UGC_TO_ZIPS
from src.services.nws_webhook import incident_type_for_event
from src.scrappers.storm.storm_engine import STORM_EVENT_TYPES
from src.scrappers.flood.flood_engine import FLOOD_NWS_EVENTS


def _alert_props(same=None, ugc=None, description=""):
    return {"geocode": {"SAME": same or [], "UGC": ugc or []}, "description": description}


def test_same_code_maps_to_hillsborough_zips():
    zips = alert_to_zips(_alert_props(same=["012057"]))
    assert "33602" in zips and len(zips) > 40


def test_ugc_forecast_zone_maps_to_county():
    assert "33602" in alert_to_zips(_alert_props(ugc=["FLZ151"]))
    assert "33701" in alert_to_zips(_alert_props(ugc=["FLZ050"]))


def test_forecast_zones_in_poll_coverage():
    # nws_poll polls list(UGC_TO_ZIPS.keys()) — forecast zones must be present
    for zone in ("FLZ151", "FLZ251", "FLZ050"):
        assert zone in UGC_TO_ZIPS


def test_unknown_codes_ignored():
    assert alert_to_zips(_alert_props(same=["099999"], ugc=["TXZ001"])) == []
    assert alert_to_zips({"description": "no geocode key"}) == []


def test_fips_to_zips():
    assert "33701" in fips_to_zips("12103")
    assert fips_to_zips("") == []
    assert fips_to_zips("99999") == []


def test_expand_codes_dedupes_same_and_ugc():
    zips = expand_codes(["012057"], ["FLZ151", "FLZ251"])
    assert zips == sorted(set(zips))


def test_incident_type_for_event():
    assert incident_type_for_event("Flash Flood Warning") == "flood_damage"
    assert incident_type_for_event("Coastal Flood Advisory") == "flood_damage"
    assert incident_type_for_event("NFIP Claim") == "flood_damage"
    assert incident_type_for_event("Severe Thunderstorm Warning") == "storm_damage"
    assert incident_type_for_event("Tornado Warning") == "storm_damage"
    assert incident_type_for_event("") == "storm_damage"


def test_flood_events_qualify_for_nws_pipeline():
    # every flood event the flood engine backstops must map to flood_damage
    for ev in FLOOD_NWS_EVENTS:
        assert incident_type_for_event(ev) == "flood_damage"
    # storm engine's non-flood events stay storm_damage
    for ev in STORM_EVENT_TYPES:
        if "Flood" not in ev:
            assert incident_type_for_event(ev) == "storm_damage"


def test_engine_events_pass_qualifying_gate():
    # Engines route alerts through process_alert, whose _is_qualifying() gate
    # checks nws_relevant_events. Any engine event NOT passing the gate gets
    # silently dropped (status='skipped') — the "silent zero incidents"
    # failure class this fix set out to eliminate (PR #113 review, issue 1).
    from src.services.nws_webhook import _is_qualifying
    from config.settings import AppSettings

    relevant = AppSettings.model_fields["nws_relevant_events"].default
    for ev in STORM_EVENT_TYPES + FLOOD_NWS_EVENTS:
        assert _is_qualifying(ev, relevant), (
            f"{ev!r} is fetched by an engine but dropped by the "
            "process_alert gate — add it to nws_relevant_events or remove it"
        )
