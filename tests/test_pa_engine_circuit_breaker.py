"""
Tests for the property_appraiser (pa_engine) circuit breaker and its stats
recording, mirroring the sunbiz throttling fix (see
tests/test_sunbiz_search_and_throttling.py).

Root problem: `_scrape_and_parse` runs one property at a time in a
ThreadPoolExecutor, and a page-level failure there is indistinguishable
between "this one parcel is broken" and "HCPA/PCPAO is throttling the whole
session" — previously both were just added to a flat `errors` counter with
`run_success = errors == 0`, so even one routine bad parcel among hundreds
failed the entire day with zero diagnostic detail (no error_type/message were
ever captured at all). The circuit breaker distinguishes sustained failure
(abort early, resume next cron) from occasional per-parcel noise (finish the
run, only flag the day past a real failure-rate threshold).
"""

from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

from src.scrappers.property_appraiser.pa_engine import (
    _CIRCUIT_BREAKER_WINDOW,
    _log_run_stats,
    run_pa_pipeline,
)


# ── _log_run_stats: classification ───────────────────────────────────────────

class TestLogRunStats:
    def test_rate_limited_run_is_recorded_as_informational_not_a_failure(self):
        with patch("src.utils.scraper_db_helper.record_scraper_stats") as mock_record:
            _log_run_stats(
                "hillsborough", "new-only", updated=5, skipped=0, errors=8,
                duration_s=12.3, rate_limited=True, remaining_unprocessed=42,
            )
        assert mock_record.call_count == 1
        kwargs = mock_record.call_args.kwargs
        assert kwargs["run_success"] is True
        assert kwargs["error_type"] == "rate_limited"
        assert "42" in kwargs["error_message"]

    def test_low_failure_rate_is_still_a_successful_run(self):
        """A handful of bad parcels among hundreds must not fail the day —
        this is the exact bug being fixed (previously any single failure
        flipped run_success to False)."""
        with patch("src.utils.scraper_db_helper.record_scraper_stats") as mock_record:
            _log_run_stats(
                "hillsborough", "new-only", updated=195, skipped=0, errors=1,
                duration_s=300.0, rate_limited=False, remaining_unprocessed=0,
            )
        kwargs = mock_record.call_args.kwargs
        assert kwargs["run_success"] is True
        assert kwargs["error_type"] is None

    def test_high_failure_rate_without_circuit_breaker_is_a_real_error(self):
        """If the breaker never tripped (failures weren't consecutive enough
        to look like throttling) but the overall rate is still high, that's
        a genuine problem worth flagging."""
        with patch("src.utils.scraper_db_helper.record_scraper_stats") as mock_record:
            _log_run_stats(
                "hillsborough", "new-only", updated=5, skipped=0, errors=10,
                duration_s=60.0, rate_limited=False, remaining_unprocessed=0,
            )
        kwargs = mock_record.call_args.kwargs
        assert kwargs["run_success"] is False
        assert kwargs["error_type"] == "scraper_error"
        assert "10" in kwargs["error_message"]

    def test_error_type_and_message_captured_unlike_previous_bug(self):
        """Previously _log_run_stats bypassed record_scraper_stats entirely
        via its own pg_insert and never set error_type/error_message at all."""
        with patch("src.utils.scraper_db_helper.record_scraper_stats") as mock_record:
            _log_run_stats(
                "hillsborough", "new-only", updated=0, skipped=0, errors=5,
                duration_s=10.0, rate_limited=False, remaining_unprocessed=0,
            )
        kwargs = mock_record.call_args.kwargs
        assert kwargs["error_message"] is not None
        assert kwargs["source_type"] == "property_appraiser"


# ── circuit breaker: sustained failures abort the batch early ───────────────

def _fake_scrape_fail(prop, config, county_id, headful, debug):
    time.sleep(0.03)
    return None


def _fake_scrape_ok(prop, config, county_id, headful, debug):
    time.sleep(0.03)
    import pandas as pd
    return pd.DataFrame([{"_property_id": prop["id"]}])


class TestCircuitBreaker:
    def test_sustained_failures_trip_breaker_and_abort_remaining(self):
        properties = [{"id": i, "parcel_id": f"P{i}"} for i in range(60)]

        with patch("src.scrappers.property_appraiser.pa_engine._query_properties",
                   return_value=properties), \
             patch("src.scrappers.property_appraiser.pa_engine.get_county_config",
                   return_value={"sources": {"property_appraiser": {}}}), \
             patch("src.scrappers.property_appraiser.pa_engine._scrape_and_parse",
                   side_effect=_fake_scrape_fail), \
             patch("src.scrappers.property_appraiser.pa_engine._log_run_stats") as mock_log:
            import asyncio
            asyncio.run(run_pa_pipeline(
                county_id="hillsborough", mode="all", limit=60, load_to_db=False,
            ))

        assert mock_log.call_count == 1
        kwargs = mock_log.call_args.kwargs
        assert kwargs["rate_limited"] is True
        assert kwargs["remaining_unprocessed"] > 0
        assert kwargs["remaining_unprocessed"] < 60

    def test_occasional_failures_do_not_trip_breaker(self):
        """A low, non-consecutive failure rate must run to completion —
        only sustained failure trips the breaker."""
        properties = [{"id": i, "parcel_id": f"P{i}"} for i in range(20)]

        call_count = {"n": 0}

        def mostly_ok(prop, config, county_id, headful, debug):
            call_count["n"] += 1
            if call_count["n"] % 7 == 0:
                return None
            import pandas as pd
            return pd.DataFrame([{"_property_id": prop["id"]}])

        with patch("src.scrappers.property_appraiser.pa_engine._query_properties",
                   return_value=properties), \
             patch("src.scrappers.property_appraiser.pa_engine.get_county_config",
                   return_value={"sources": {"property_appraiser": {}}}), \
             patch("src.scrappers.property_appraiser.pa_engine._scrape_and_parse",
                   side_effect=mostly_ok), \
             patch("src.scrappers.property_appraiser.pa_engine._log_run_stats") as mock_log:
            import asyncio
            asyncio.run(run_pa_pipeline(
                county_id="hillsborough", mode="all", limit=20, load_to_db=False,
            ))

        assert mock_log.call_count == 1
        kwargs = mock_log.call_args.kwargs
        assert kwargs["rate_limited"] is False
        assert kwargs["remaining_unprocessed"] == 0
