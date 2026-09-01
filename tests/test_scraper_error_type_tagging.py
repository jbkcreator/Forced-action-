"""
Regression tests for the error_type tagging fix (storm/flood/insurance/fire +
Hillsborough liens scrape_mode routing).

Each `_fetch_*` helper in these engines now returns (items, error) or
(items, error, exc) instead of a bare list, so a real API/network failure can
be distinguished from a genuine zero-result day when calling
record_scraper_stats(). These tests cover the three states directly against
the private fetch functions and the public scrape_* entry points, with no
live DB/network access.

The zone-fetch helpers (storm/flood) return (features, failed_zones) where
failed_zones is [(zone_id, exc), ...] — the real exception per failed zone,
not a formatted string, so record_scraper_stats(outcome=classify_exception(exc))
can classify what actually happened (src.utils.scraper_outcome_classifier)
instead of guessing from text. The single-fetch helpers (insurance/fire/
pinellas_fire) return (items, error_string, exc) for the same reason.
"""
from unittest.mock import patch

from src.scrappers.storm import storm_engine
from src.scrappers.flood import flood_engine
from src.scrappers.insurance import insurance_engine
from src.scrappers.fire import fire_engine
from src.scrappers.fire import pinellas_fire_engine
from config.scraper_outcomes import ScraperOutcome, derive_run_success, LEGACY_ERROR_TYPE_MAP


_COUNTY_CONFIG = {
    "state": "FL",
    "nws_zones": ["FLZ151"],
    "fips": "12057",
    "display_name": "Hillsborough",
}


def _patched_stats():
    """Patch record_scraper_stats at its source module and return the mock."""
    return patch("src.utils.scraper_db_helper.record_scraper_stats")


def _exc(message: str) -> Exception:
    """A generic exception for mock fetch failures — classify_exception()
    resolves any unrecognized type to UNKNOWN, which LEGACY_ERROR_TYPE_MAP
    still maps to error_type='scraper_error', so these tests don't need to
    care about the exact requests.* subtype to assert error_type/run_success."""
    return Exception(message)


def _kwargs_run_success(kwargs: dict) -> bool:
    """record_scraper_stats() is mocked out here, so its own internal
    run_success derivation from outcome= never runs — the caller only ever
    passes ONE of (a) an explicit run_success= kwarg (the deliberate
    partial-failure branches, which must NOT let outcome force it), or
    (b) outcome= alone (every total-failure/no-data branch, relying on
    record_scraper_stats to derive it — already verified live against the
    real DB in this session's classify_exception/record_scraper_stats
    tests). Mirror that same derivation here so these mocked-call tests
    check the real effective value either way."""
    if "run_success" in kwargs:
        return kwargs["run_success"]
    return derive_run_success(kwargs.get("outcome"))


def _kwargs_error_type(kwargs: dict) -> str:
    """Same rationale as _kwargs_run_success — error_type is either passed
    explicitly or must be derived from outcome= via the same
    LEGACY_ERROR_TYPE_MAP record_scraper_stats itself uses."""
    if kwargs.get("error_type"):
        return kwargs["error_type"]
    return LEGACY_ERROR_TYPE_MAP[kwargs["outcome"]]


# ── storm_engine ──────────────────────────────────────────────────────────
# _fetch_nws_alerts_by_zones/_fetch_nws_flood_alerts_by_zones now return
# (features, failed_zones) where failed_zones is [(zone_id, exc), ...] — a
# single bad zone among several must be surfaced even when others succeed
# (PR #232 review, issue 1), so "no error" is an empty list, not None.

_TWO_ZONE_CONFIG = {**_COUNTY_CONFIG, "nws_zones": ["FLZ151", "FLZ251"]}


def test_storm_fetch_failure_reports_scraper_error(monkeypatch):
    # Only one zone configured and it fails -> total failure.
    monkeypatch.setattr(storm_engine, "get_county", lambda cid: _COUNTY_CONFIG)
    monkeypatch.setattr(
        storm_engine, "_fetch_nws_alerts_by_zones",
        lambda zones: ([], [("FLZ151", _exc("boom: connection reset"))]),
    )
    with _patched_stats() as mock_stats:
        storm_engine.scrape_storm_damage(county_id="hillsborough")
    kwargs = mock_stats.call_args.kwargs
    assert _kwargs_run_success(kwargs) is False
    assert _kwargs_error_type(kwargs) == "scraper_error"
    assert "boom" in kwargs["error_message"]


def test_storm_genuine_zero_result_reports_no_data(monkeypatch):
    monkeypatch.setattr(storm_engine, "get_county", lambda cid: _COUNTY_CONFIG)
    monkeypatch.setattr(storm_engine, "_fetch_nws_alerts_by_zones", lambda zones: ([], []))
    with _patched_stats() as mock_stats:
        storm_engine.scrape_storm_damage(county_id="hillsborough")
    kwargs = mock_stats.call_args.kwargs
    assert _kwargs_run_success(kwargs) is True
    assert _kwargs_error_type(kwargs) == "no_data"


def test_storm_partial_zone_failure_reports_scraper_error_not_no_data(monkeypatch):
    # One of two zones failed — the other returned zero alerts. Must not be
    # reported as a confirmed no-data day (PR #232 review, issue 1), and
    # stays run_success=True since the other zone still produced a real,
    # confirmed (empty) result.
    monkeypatch.setattr(storm_engine, "get_county", lambda cid: _TWO_ZONE_CONFIG)
    monkeypatch.setattr(
        storm_engine, "_fetch_nws_alerts_by_zones",
        lambda zones: ([], [("FLZ151", _exc("boom"))]),
    )
    with _patched_stats() as mock_stats:
        storm_engine.scrape_storm_damage(county_id="hillsborough")
    kwargs = mock_stats.call_args.kwargs
    assert kwargs["error_type"] == "scraper_error"
    assert kwargs["run_success"] is True  # the other zone still succeeded
    assert "FLZ151" in kwargs["error_message"]


# ── flood_engine ──────────────────────────────────────────────────────────

def test_flood_all_sources_fail_reports_scraper_error(monkeypatch):
    # Single zone configured (fails) AND NFIP fails -> total failure.
    monkeypatch.setattr(flood_engine, "get_county", lambda cid: _COUNTY_CONFIG)
    monkeypatch.setattr(flood_engine, "_fetch_fema_declarations", lambda *a: ([], None))
    monkeypatch.setattr(
        flood_engine, "_fetch_nws_flood_alerts_by_zones",
        lambda zones: ([], [("FLZ151", _exc("NWS down"))]),
    )
    monkeypatch.setattr(flood_engine, "_fetch_nfip_claims", lambda *a: ([], _exc("FEMA NFIP down")))
    with _patched_stats() as mock_stats:
        flood_engine.scrape_flood_damage(county_id="hillsborough")
    kwargs = mock_stats.call_args.kwargs
    assert _kwargs_run_success(kwargs) is False
    assert _kwargs_error_type(kwargs) == "scraper_error"


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
    assert _kwargs_run_success(kwargs) is True
    assert _kwargs_error_type(kwargs) == "no_data"


def test_flood_genuine_zero_result_reports_no_data(monkeypatch):
    monkeypatch.setattr(flood_engine, "get_county", lambda cid: _COUNTY_CONFIG)
    monkeypatch.setattr(flood_engine, "_fetch_fema_declarations", lambda *a: ([], None))
    monkeypatch.setattr(flood_engine, "_fetch_nws_flood_alerts_by_zones", lambda zones: ([], []))
    monkeypatch.setattr(flood_engine, "_fetch_nfip_claims", lambda *a: ([], None))
    with _patched_stats() as mock_stats:
        flood_engine.scrape_flood_damage(county_id="hillsborough")
    kwargs = mock_stats.call_args.kwargs
    assert _kwargs_run_success(kwargs) is True
    assert _kwargs_error_type(kwargs) == "no_data"


def test_flood_partial_zone_failure_reports_scraper_error_not_no_data(monkeypatch):
    # One of two zones failed — the other + NFIP genuinely returned zero.
    # Must not be reported as a confirmed no-data day (PR #232 review, issue 1),
    # and stays run_success=True since NFIP + the other zone still produced
    # real, confirmed (empty) coverage.
    monkeypatch.setattr(flood_engine, "get_county", lambda cid: _TWO_ZONE_CONFIG)
    monkeypatch.setattr(flood_engine, "_fetch_fema_declarations", lambda *a: ([], None))
    monkeypatch.setattr(
        flood_engine, "_fetch_nws_flood_alerts_by_zones",
        lambda zones: ([], [("FLZ151", _exc("boom"))]),
    )
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
    monkeypatch.setattr(flood_engine, "_fetch_nfip_claims", lambda *a: ([], _exc("FEMA NFIP down")))
    with _patched_stats() as mock_stats:
        flood_engine.scrape_flood_damage(county_id="hillsborough")
    kwargs = mock_stats.call_args.kwargs
    assert _kwargs_error_type(kwargs) == "scraper_error"
    assert _kwargs_run_success(kwargs) is False


# ── insurance_engine ──────────────────────────────────────────────────────

def test_insurance_fetch_failure_reports_scraper_error(monkeypatch):
    # No earlier page succeeded -> total failure.
    monkeypatch.setattr(insurance_engine, "get_county", lambda cid: _COUNTY_CONFIG)
    monkeypatch.setattr(insurance_engine, "_get_insurance_permits", lambda *a, **k: [])
    monkeypatch.setattr(
        insurance_engine, "_fetch_fema_ia_registrants",
        lambda *a: ([], "FEMA IA down", _exc("FEMA IA down")),
    )
    with _patched_stats() as mock_stats:
        insurance_engine.scrape_insurance_claims(county_id="hillsborough")
    kwargs = mock_stats.call_args.kwargs
    assert _kwargs_run_success(kwargs) is False
    assert _kwargs_error_type(kwargs) == "scraper_error"


def test_insurance_genuine_zero_result_reports_no_data(monkeypatch):
    monkeypatch.setattr(insurance_engine, "get_county", lambda cid: _COUNTY_CONFIG)
    monkeypatch.setattr(insurance_engine, "_get_insurance_permits", lambda *a, **k: [])
    monkeypatch.setattr(insurance_engine, "_fetch_fema_ia_registrants", lambda *a: ([], None, None))
    with _patched_stats() as mock_stats:
        insurance_engine.scrape_insurance_claims(county_id="hillsborough")
    kwargs = mock_stats.call_args.kwargs
    assert kwargs.get("run_success", True) is True
    assert kwargs["error_type"] == "no_data"


def test_insurance_partial_page_failure_reports_scraper_error_not_none(monkeypatch):
    # Page 0 succeeded (real rows collected) before a later page failed —
    # partial, not total failure — stays run_success=True (PR #232 contract,
    # see tests/test_weather_insurance_partial_fetch_errors.py for the
    # equivalent end-to-end version of this exact scenario).
    monkeypatch.setattr(insurance_engine, "get_county", lambda cid: _COUNTY_CONFIG)
    monkeypatch.setattr(insurance_engine, "_get_insurance_permits", lambda *a, **k: [])
    monkeypatch.setattr(
        insurance_engine, "_fetch_fema_ia_registrants",
        lambda *a: ([{"zipCode": "33602"}], "page 1: timed out", _exc("timed out")),
    )
    with _patched_stats() as mock_stats:
        insurance_engine.scrape_insurance_claims(county_id="hillsborough")
    kwargs = mock_stats.call_args.kwargs
    assert kwargs["error_type"] == "scraper_error"
    assert kwargs["run_success"] is True


# ── fire_engine (Hillsborough / Tampa Fire Rescue) ───────────────────────

def test_fire_fetch_failure_reports_scraper_error(monkeypatch):
    monkeypatch.setattr(fire_engine, "_fetch_incidents", lambda: ([], "TFR API down", _exc("TFR API down")))
    with _patched_stats() as mock_stats:
        fire_engine.scrape_fire_incidents(county_id="hillsborough")
    kwargs = mock_stats.call_args.kwargs
    assert _kwargs_run_success(kwargs) is False
    assert kwargs["error_type"] == "scraper_error"


def test_fire_genuine_zero_result_reports_no_data(monkeypatch):
    monkeypatch.setattr(fire_engine, "_fetch_incidents", lambda: ([], None, None))
    with _patched_stats() as mock_stats:
        fire_engine.scrape_fire_incidents(county_id="hillsborough")
    kwargs = mock_stats.call_args.kwargs
    assert kwargs.get("run_success", True) is True
    assert kwargs["error_type"] == "no_data"


# ── pinellas_fire_engine ──────────────────────────────────────────────────

def test_pinellas_fire_fetch_failure_reports_scraper_error(monkeypatch):
    monkeypatch.setattr(pinellas_fire_engine, "get_county", lambda cid: _COUNTY_CONFIG)
    monkeypatch.setattr(
        pinellas_fire_engine, "_fetch_active_calls",
        lambda: ([], "CAD feed down", _exc("CAD feed down")),
    )
    with _patched_stats() as mock_stats:
        pinellas_fire_engine.scrape_pinellas_fire_incidents(county_id="pinellas")
    kwargs = mock_stats.call_args.kwargs
    assert _kwargs_run_success(kwargs) is False
    assert kwargs["error_type"] == "scraper_error"


def test_pinellas_fire_genuine_zero_result_reports_no_data(monkeypatch):
    monkeypatch.setattr(pinellas_fire_engine, "get_county", lambda cid: _COUNTY_CONFIG)
    monkeypatch.setattr(pinellas_fire_engine, "_fetch_active_calls", lambda: ([], None, None))
    with _patched_stats() as mock_stats:
        pinellas_fire_engine.scrape_pinellas_fire_incidents(county_id="pinellas")
    kwargs = mock_stats.call_args.kwargs
    assert kwargs.get("run_success", True) is True
    assert kwargs["error_type"] == "no_data"
