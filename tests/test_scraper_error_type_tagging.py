"""
Regression tests for the error_type tagging fix (storm/flood/insurance/fire +
Hillsborough liens scrape_mode routing).

Each `_fetch_*` helper in these engines now returns (items, error) instead of
a bare list, so a real API/network failure can be distinguished from a
genuine zero-result day when calling record_scraper_stats(). These tests
cover the three states directly against the private fetch functions and the
public scrape_* entry points, with no live DB/network access.
"""
from unittest.mock import patch

from src.scrappers.storm import storm_engine
from src.scrappers.flood import flood_engine
from src.scrappers.insurance import insurance_engine
from src.scrappers.fire import fire_engine
from src.scrappers.fire import pinellas_fire_engine


_COUNTY_CONFIG = {
    "state": "FL",
    "nws_zones": ["FLZ151"],
    "fips": "12057",
    "display_name": "Hillsborough",
}


def _patched_stats():
    """Patch record_scraper_stats at its source module and return the mock."""
    return patch("src.utils.scraper_db_helper.record_scraper_stats")


# ── storm_engine ──────────────────────────────────────────────────────────
# _fetch_nws_alerts_by_zones/_fetch_nws_flood_alerts_by_zones now return
# (features, failed_zone_ids) instead of (features, error_or_None) — a
# single bad zone among several must be surfaced even when others succeed
# (PR #232 review, issue 1), so "no error" is an empty list, not None.

_TWO_ZONE_CONFIG = {**_COUNTY_CONFIG, "nws_zones": ["FLZ151", "FLZ251"]}


def test_storm_fetch_failure_reports_scraper_error(monkeypatch):
    monkeypatch.setattr(storm_engine, "get_county", lambda cid: _COUNTY_CONFIG)
    monkeypatch.setattr(storm_engine, "_fetch_nws_alerts_by_zones", lambda zones: ([], ["FLZ151: boom: connection reset"]))
    with _patched_stats() as mock_stats:
        storm_engine.scrape_storm_damage(county_id="hillsborough")
    kwargs = mock_stats.call_args.kwargs
    assert kwargs["run_success"] is False
    assert kwargs["error_type"] == "scraper_error"
    assert "boom" in kwargs["error_message"]


def test_storm_genuine_zero_result_reports_no_data(monkeypatch):
    monkeypatch.setattr(storm_engine, "get_county", lambda cid: _COUNTY_CONFIG)
    monkeypatch.setattr(storm_engine, "_fetch_nws_alerts_by_zones", lambda zones: ([], []))
    with _patched_stats() as mock_stats:
        storm_engine.scrape_storm_damage(county_id="hillsborough")
    kwargs = mock_stats.call_args.kwargs
    assert kwargs.get("run_success", True) is True
    assert kwargs["error_type"] == "no_data"


def test_storm_partial_zone_failure_reports_scraper_error_not_no_data(monkeypatch):
    # One of two zones failed — the other returned zero alerts. Must not be
    # reported as a confirmed no-data day (PR #232 review, issue 1).
    monkeypatch.setattr(storm_engine, "get_county", lambda cid: _TWO_ZONE_CONFIG)
    monkeypatch.setattr(storm_engine, "_fetch_nws_alerts_by_zones", lambda zones: ([], ["FLZ151: boom"]))
    with _patched_stats() as mock_stats:
        storm_engine.scrape_storm_damage(county_id="hillsborough")
    kwargs = mock_stats.call_args.kwargs
    assert kwargs["error_type"] == "scraper_error"
    assert kwargs["run_success"] is True  # the other zone still succeeded
    assert "FLZ151" in kwargs["error_message"]


# ── flood_engine ──────────────────────────────────────────────────────────

def test_flood_all_sources_fail_reports_scraper_error(monkeypatch):
    monkeypatch.setattr(flood_engine, "get_county", lambda cid: _COUNTY_CONFIG)
    monkeypatch.setattr(flood_engine, "_fetch_fema_declarations", lambda *a: ([], None))
    monkeypatch.setattr(flood_engine, "_fetch_nws_flood_alerts_by_zones", lambda zones: ([], ["FLZ151: NWS down"]))
    monkeypatch.setattr(flood_engine, "_fetch_nfip_claims", lambda *a: ([], "FEMA NFIP down"))
    with _patched_stats() as mock_stats:
        flood_engine.scrape_flood_damage(county_id="hillsborough")
    kwargs = mock_stats.call_args.kwargs
    assert kwargs["run_success"] is False
    assert kwargs["error_type"] == "scraper_error"


def test_flood_informational_source_failure_alone_is_still_no_data(monkeypatch):
    # FEMA disaster declarations is informational-only and never counts
    # toward total_scraped — its failure alone must NOT mask a confirmed
    # no-data day when NWS + NFIP both genuinely succeeded with zero results.
    monkeypatch.setattr(flood_engine, "get_county", lambda cid: _COUNTY_CONFIG)
    monkeypatch.setattr(flood_engine, "_fetch_fema_declarations", lambda *a: ([], "declarations API down"))
    monkeypatch.setattr(flood_engine, "_fetch_nws_flood_alerts_by_zones", lambda zones: ([], []))
    monkeypatch.setattr(flood_engine, "_fetch_nfip_claims", lambda *a: ([], None))
    with _patched_stats() as mock_stats:
        flood_engine.scrape_flood_damage(county_id="hillsborough")
    kwargs = mock_stats.call_args.kwargs
    assert kwargs.get("run_success", True) is True
    assert kwargs["error_type"] == "no_data"


def test_flood_genuine_zero_result_reports_no_data(monkeypatch):
    monkeypatch.setattr(flood_engine, "get_county", lambda cid: _COUNTY_CONFIG)
    monkeypatch.setattr(flood_engine, "_fetch_fema_declarations", lambda *a: ([], None))
    monkeypatch.setattr(flood_engine, "_fetch_nws_flood_alerts_by_zones", lambda zones: ([], []))
    monkeypatch.setattr(flood_engine, "_fetch_nfip_claims", lambda *a: ([], None))
    with _patched_stats() as mock_stats:
        flood_engine.scrape_flood_damage(county_id="hillsborough")
    kwargs = mock_stats.call_args.kwargs
    assert kwargs.get("run_success", True) is True
    assert kwargs["error_type"] == "no_data"


def test_flood_partial_zone_failure_reports_scraper_error_not_no_data(monkeypatch):
    # One of two zones failed — the other + NFIP genuinely returned zero.
    # Must not be reported as a confirmed no-data day (PR #232 review, issue 1).
    monkeypatch.setattr(flood_engine, "get_county", lambda cid: _TWO_ZONE_CONFIG)
    monkeypatch.setattr(flood_engine, "_fetch_fema_declarations", lambda *a: ([], None))
    monkeypatch.setattr(flood_engine, "_fetch_nws_flood_alerts_by_zones", lambda zones: ([], ["FLZ151: boom"]))
    monkeypatch.setattr(flood_engine, "_fetch_nfip_claims", lambda *a: ([], None))
    with _patched_stats() as mock_stats:
        flood_engine.scrape_flood_damage(county_id="hillsborough")
    kwargs = mock_stats.call_args.kwargs
    assert kwargs["error_type"] == "scraper_error"
    assert kwargs["run_success"] is True  # the other zone + NFIP still succeeded
    assert "FLZ151" in kwargs["error_message"]


def test_flood_no_nws_zones_configured_and_nfip_fails_reports_run_failure(monkeypatch):
    # No NWS zones configured (e.g. a county not yet backstopped by NWS) means
    # NFIP is the ONLY flood source. If NFIP fails, the run genuinely failed —
    # "no zones to fail" must not be conflated with "the zone source succeeded"
    # (PR #232 review, issue 3).
    no_zone_config = {**_COUNTY_CONFIG, "nws_zones": []}
    monkeypatch.setattr(flood_engine, "get_county", lambda cid: no_zone_config)
    monkeypatch.setattr(flood_engine, "_fetch_fema_declarations", lambda *a: ([], None))
    monkeypatch.setattr(flood_engine, "_fetch_nfip_claims", lambda *a: ([], "FEMA NFIP down"))
    with _patched_stats() as mock_stats:
        flood_engine.scrape_flood_damage(county_id="hillsborough")
    kwargs = mock_stats.call_args.kwargs
    assert kwargs["error_type"] == "scraper_error"
    assert kwargs["run_success"] is False


# ── insurance_engine ──────────────────────────────────────────────────────

def test_insurance_fetch_failure_reports_scraper_error(monkeypatch):
    monkeypatch.setattr(insurance_engine, "get_county", lambda cid: _COUNTY_CONFIG)
    monkeypatch.setattr(insurance_engine, "_get_insurance_permits", lambda *a, **k: [])
    monkeypatch.setattr(insurance_engine, "_fetch_fema_ia_registrants", lambda *a: ([], "FEMA IA down"))
    with _patched_stats() as mock_stats:
        insurance_engine.scrape_insurance_claims(county_id="hillsborough")
    kwargs = mock_stats.call_args.kwargs
    assert kwargs["run_success"] is False
    assert kwargs["error_type"] == "scraper_error"


def test_insurance_genuine_zero_result_reports_no_data(monkeypatch):
    monkeypatch.setattr(insurance_engine, "get_county", lambda cid: _COUNTY_CONFIG)
    monkeypatch.setattr(insurance_engine, "_get_insurance_permits", lambda *a, **k: [])
    monkeypatch.setattr(insurance_engine, "_fetch_fema_ia_registrants", lambda *a: ([], None))
    with _patched_stats() as mock_stats:
        insurance_engine.scrape_insurance_claims(county_id="hillsborough")
    kwargs = mock_stats.call_args.kwargs
    assert kwargs.get("run_success", True) is True
    assert kwargs["error_type"] == "no_data"


# ── fire_engine (Hillsborough / Tampa Fire Rescue) ───────────────────────

def test_fire_fetch_failure_reports_scraper_error(monkeypatch):
    monkeypatch.setattr(fire_engine, "_fetch_incidents", lambda: ([], "TFR API down"))
    with _patched_stats() as mock_stats:
        fire_engine.scrape_fire_incidents(county_id="hillsborough")
    kwargs = mock_stats.call_args.kwargs
    assert kwargs["run_success"] is False
    assert kwargs["error_type"] == "scraper_error"


def test_fire_genuine_zero_result_reports_no_data(monkeypatch):
    monkeypatch.setattr(fire_engine, "_fetch_incidents", lambda: ([], None))
    with _patched_stats() as mock_stats:
        fire_engine.scrape_fire_incidents(county_id="hillsborough")
    kwargs = mock_stats.call_args.kwargs
    assert kwargs.get("run_success", True) is True
    assert kwargs["error_type"] == "no_data"


# ── pinellas_fire_engine ──────────────────────────────────────────────────

def test_pinellas_fire_fetch_failure_reports_scraper_error(monkeypatch):
    monkeypatch.setattr(pinellas_fire_engine, "get_county", lambda cid: _COUNTY_CONFIG)
    monkeypatch.setattr(pinellas_fire_engine, "_fetch_active_calls", lambda: ([], "CAD feed down"))
    with _patched_stats() as mock_stats:
        pinellas_fire_engine.scrape_pinellas_fire_incidents(county_id="pinellas")
    kwargs = mock_stats.call_args.kwargs
    assert kwargs["run_success"] is False
    assert kwargs["error_type"] == "scraper_error"


def test_pinellas_fire_genuine_zero_result_reports_no_data(monkeypatch):
    monkeypatch.setattr(pinellas_fire_engine, "get_county", lambda cid: _COUNTY_CONFIG)
    monkeypatch.setattr(pinellas_fire_engine, "_fetch_active_calls", lambda: ([], None))
    with _patched_stats() as mock_stats:
        pinellas_fire_engine.scrape_pinellas_fire_incidents(county_id="pinellas")
    kwargs = mock_stats.call_args.kwargs
    assert kwargs.get("run_success", True) is True
    assert kwargs["error_type"] == "no_data"
