"""
PR #232 review fixes: a partial zone/page failure must never be reported as
a clean run ("none") or a confirmed no-data day ("no_data") — it has to
surface as a scraper_error so it gets investigated, even when the other
zone/earlier pages produced usable data.
"""

from unittest.mock import MagicMock, patch

import requests


def _zone_response(features):
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.json.return_value = {"features": features}
    return resp


def _mock_county(nws_zones):
    return {"state": "FL", "nws_zones": nws_zones, "fips": "12057", "display_name": "Hillsborough"}


class TestStormPartialZoneFailure:

    def test_one_zone_fails_one_zone_empty_is_scraper_error_not_no_data(self):
        """FLZ151 raises, FLZ251 returns zero alerts -> scraper_error, not no_data."""
        with patch("src.scrappers.storm.storm_engine.get_county", return_value=_mock_county(["FLZ151", "FLZ251"])), \
             patch("src.scrappers.storm.storm_engine.requests.get") as mock_get, \
             patch("src.utils.scraper_db_helper.record_scraper_stats") as mock_stats:

            def side_effect(url, **kwargs):
                if "FLZ151" in url:
                    raise requests.exceptions.ConnectionError("zone down")
                return _zone_response([])

            mock_get.side_effect = side_effect

            from src.scrappers.storm.storm_engine import scrape_storm_damage
            scrape_storm_damage(county_id="hillsborough")

            mock_stats.assert_called_once()
            kwargs = mock_stats.call_args.kwargs
            assert kwargs["error_type"] == "scraper_error"
            assert kwargs["run_success"] is True  # one zone still succeeded
            assert "FLZ151" in kwargs["error_message"]


class TestFloodPartialZoneFailure:

    def test_one_zone_fails_one_zone_empty_is_scraper_error_not_no_data(self):
        with patch("src.scrappers.flood.flood_engine.get_county", return_value=_mock_county(["FLZ151", "FLZ251"])), \
             patch("src.scrappers.flood.flood_engine._fetch_fema_declarations", return_value=([], None)), \
             patch("src.scrappers.flood.flood_engine._fetch_nfip_claims", return_value=([], None)), \
             patch("src.scrappers.flood.flood_engine.requests.get") as mock_get, \
             patch("src.utils.scraper_db_helper.record_scraper_stats") as mock_stats:

            def side_effect(url, **kwargs):
                if "FLZ151" in url:
                    raise requests.exceptions.ConnectionError("zone down")
                return _zone_response([])

            mock_get.side_effect = side_effect

            from src.scrappers.flood.flood_engine import scrape_flood_damage
            scrape_flood_damage(county_id="hillsborough")

            mock_stats.assert_called_once()
            kwargs = mock_stats.call_args.kwargs
            assert kwargs["error_type"] == "scraper_error"
            assert kwargs["run_success"] is True  # NFIP succeeded (empty), so not a total failure
            assert "FLZ151" in kwargs["error_message"]


class TestInsurancePartialPageFailure:

    def test_page_two_failure_after_page_one_success_is_scraper_error_not_none(self):
        """Page 1 returns a full batch (triggers page 2), page 2 raises."""
        full_batch = [{"zipCode": "33602"}] * 1000

        with patch("src.scrappers.insurance.insurance_engine.get_county",
                   return_value=_mock_county([])), \
             patch("src.scrappers.insurance.insurance_engine.requests_get_with_retry") as mock_get, \
             patch("src.scrappers.insurance.insurance_engine._get_insurance_permits", return_value=[]), \
             patch("src.scrappers.insurance.insurance_engine.get_db_context") as mock_db_ctx, \
             patch("src.utils.scraper_db_helper.record_scraper_stats") as mock_stats:

            page1_resp = MagicMock()
            page1_resp.raise_for_status.return_value = None
            page1_resp.json.return_value = {"HousingAssistanceOwners": full_batch}

            mock_get.side_effect = [page1_resp, requests.exceptions.Timeout("page 2 timed out")]

            db = MagicMock()
            db.execute.return_value.scalars.return_value.all.return_value = []
            mock_db_ctx.return_value.__enter__.return_value = db

            from src.scrappers.insurance.insurance_engine import scrape_insurance_claims
            scrape_insurance_claims(county_id="hillsborough")

            mock_stats.assert_called_once()
            kwargs = mock_stats.call_args.kwargs
            assert kwargs["error_type"] == "scraper_error"
            assert "page 1" in kwargs["error_message"]
            # Partial data (page 1) is still usable, but the run isn't "none"/"no_data".
            assert kwargs["run_success"] is True
