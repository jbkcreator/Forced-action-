"""
Tests for NWS alert poll pipeline.

Covers: process_alert idempotency, qualifying filter, ZIP resolution,
feature flags, and storm-pack gating.

Run:
    pytest tests/test_nws_poll.py -v
    pytest tests/test_nws_poll.py -m integration -v   # live NWS API
"""

import pytest
from unittest.mock import MagicMock, patch


# ──────────────────────────────────────────────────────────────────────────────
# Shared helpers
# ──────────────────────────────────────────────────────────────────────────────

def _qualifying_payload(alert_id="https://api.weather.gov/alerts/urn:test:001"):
    return {
        "id": alert_id,
        "event": "Severe Thunderstorm Warning",
        "severity": "Severe",
        "urgency": "Immediate",
        "certainty": "Observed",
        "headline": "Severe Thunderstorm Warning issued",
        "description": "A severe thunderstorm warning is in effect.",
        "areaDesc": "Hillsborough County",
        "geocode": {"SAME": ["012057"], "UGC": ["FLC057"]},
        "expires": "2026-05-14T06:00:00+00:00",
    }


def _mock_settings(
    nws_weather_enabled=True,
    storm_pack_enabled=True,
    nws_cora_urgency_enabled=True,
    nws_revenue_polling_enabled=True,
):
    s = MagicMock()
    s.nws_weather_enabled = nws_weather_enabled
    s.storm_pack_enabled = storm_pack_enabled
    s.nws_cora_urgency_enabled = nws_cora_urgency_enabled
    s.nws_revenue_polling_enabled = nws_revenue_polling_enabled
    s.nws_relevant_events = [
        "Tornado Warning", "Tornado Watch",
        "Severe Thunderstorm Warning", "Severe Thunderstorm Watch",
        "Hurricane Warning", "Hurricane Watch",
        "Tropical Storm Warning", "Tropical Storm Watch",
        "Flash Flood Warning", "Flood Warning",
        "High Wind Warning", "Wind Advisory",
        "Storm Surge Warning", "Storm Surge Watch",
        "Special Weather Statement",
    ]
    return s


def _db_no_existing():
    """Mock DB session reporting no pre-existing NWSAlert (idempotency miss)."""
    db = MagicMock()
    db.execute.return_value.scalar_one_or_none.return_value = None
    return db


# ──────────────────────────────────────────────────────────────────────────────
# process_alert — idempotency
# ──────────────────────────────────────────────────────────────────────────────

class TestProcessAlertIdempotency:

    def test_new_alert_stored_idempotently(self):
        """Same alert_id processed twice → first 'processed', second 'duplicate'."""
        from src.services.nws_webhook import process_alert
        from src.core.models import NWSAlert

        payload = _qualifying_payload()

        db_first = _db_no_existing()

        existing_mock = MagicMock(spec=NWSAlert)
        existing_mock.affected_zips = ["33602", "33647"]
        existing_mock.subscriber_count = 0
        db_second = MagicMock()
        db_second.execute.return_value.scalar_one_or_none.return_value = existing_mock

        with (
            patch("src.services.nws_webhook.get_settings", return_value=_mock_settings()),
            patch("src.services.nws_webhook._activate_storm_packs", return_value=0),
            patch("src.services.nws_webhook._log_event"),
        ):
            first = process_alert(payload, db_first)
            second = process_alert(payload, db_second)

        assert first["status"] == "processed"
        assert first["alert_id"] == payload["id"]
        assert second["status"] == "duplicate"
        assert second["alert_id"] == payload["id"]
        # DB row written on first call only
        db_first.add.assert_called_once()
        db_second.add.assert_not_called()


# ──────────────────────────────────────────────────────────────────────────────
# process_alert — qualifying filter
# ──────────────────────────────────────────────────────────────────────────────

class TestProcessAlertQualifyingFilter:

    def test_non_qualifying_event_skipped(self):
        """A non-qualifying event type returns 'skipped' and writes nothing to DB."""
        from src.services.nws_webhook import process_alert

        payload = {
            "id": "https://api.weather.gov/alerts/urn:test:frost:001",
            "event": "Frost Advisory",
            "areaDesc": "Hillsborough County",
            "geocode": {"SAME": ["012057"], "UGC": []},
        }
        db = _db_no_existing()

        with patch("src.services.nws_webhook.get_settings", return_value=_mock_settings()):
            result = process_alert(payload, db)

        assert result["status"] == "skipped"
        assert result.get("reason") == "non_qualifying_event"
        db.add.assert_not_called()
        db.commit.assert_not_called()


# ──────────────────────────────────────────────────────────────────────────────
# process_alert — SAME/UGC ZIP resolution
# ──────────────────────────────────────────────────────────────────────────────

class TestProcessAlertZIPResolution:

    def test_fips_same_matched_to_hillsborough(self):
        """SAME code 012057 resolves to Hillsborough ZIPs; Pinellas must not appear."""
        from src.services.nws_webhook import process_alert

        payload = _qualifying_payload()
        db = _db_no_existing()

        with (
            patch("src.services.nws_webhook.get_settings", return_value=_mock_settings()),
            patch("src.services.nws_webhook._activate_storm_packs", return_value=0),
            patch("src.services.nws_webhook._log_event"),
        ):
            result = process_alert(payload, db)

        assert result["status"] == "processed"
        zips = result["affected_zips"]
        assert "33647" in zips    # Tampa Palms
        assert "33602" in zips    # Downtown Tampa
        assert "33701" not in zips  # Pinellas — must not leak


# ──────────────────────────────────────────────────────────────────────────────
# process_alert — feature flags
# ──────────────────────────────────────────────────────────────────────────────

class TestProcessAlertFeatureFlags:

    def test_nws_weather_disabled_returns_skipped_without_db_writes(self):
        """nws_weather_enabled=False → returns early, no DB calls at all."""
        from src.services.nws_webhook import process_alert

        db = MagicMock()

        with patch(
            "src.services.nws_webhook.get_settings",
            return_value=_mock_settings(nws_weather_enabled=False),
        ):
            result = process_alert(_qualifying_payload(), db)

        assert result["status"] == "skipped"
        db.execute.assert_not_called()
        db.add.assert_not_called()

    def test_storm_pack_flag_disabled_skips_offer(self):
        """storm_pack_enabled=False → NWSAlert inserted, _activate_storm_packs NOT called."""
        from src.services.nws_webhook import process_alert

        db = _db_no_existing()
        captured = []
        db.add.side_effect = captured.append

        with (
            patch("src.services.nws_webhook.get_settings",
                  return_value=_mock_settings(storm_pack_enabled=False)),
            patch("src.services.nws_webhook._activate_storm_packs") as mock_activate,
            patch("src.services.nws_webhook._log_event"),
        ):
            result = process_alert(_qualifying_payload(), db)

        assert result["status"] == "processed"
        mock_activate.assert_not_called()
        # Row was still inserted
        assert len(captured) == 1
        # storm_pack_triggered must be False (notified=0, _activate never ran)
        assert captured[0].storm_pack_triggered is False


# ──────────────────────────────────────────────────────────────────────────────
# run_nws_poll — top-level runner
# ──────────────────────────────────────────────────────────────────────────────

class TestNWSPollRunner:

    def test_feature_flag_disabled_poll_returns_zeros(self):
        """nws_weather_enabled=False → run_nws_poll returns all-zero stats immediately."""
        from src.tasks.nws_poll import run_nws_poll

        with patch("src.tasks.nws_poll.get_settings",
                   return_value=_mock_settings(nws_weather_enabled=False)):
            stats = run_nws_poll(county_id="hillsborough")

        assert stats["polled"] == 0
        assert stats["new_alerts"] == 0
        assert stats["duplicates_skipped"] == 0
        assert stats["cora_dispatched"] == 0

    def test_dry_run_fetches_but_does_not_enter_db(self):
        """dry_run=True logs features but never opens a DB session."""
        from src.tasks.nws_poll import run_nws_poll

        fake_features = [
            {
                "id": "https://api.weather.gov/alerts/urn:test:dry:001",
                "properties": {
                    "event": "Severe Thunderstorm Warning",
                    "areaDesc": "Hillsborough County",
                    "geocode": {"SAME": ["012057"], "UGC": []},
                },
            }
        ]

        with (
            patch("src.tasks.nws_poll.get_settings", return_value=_mock_settings()),
            patch("src.tasks.nws_poll._fetch_alerts_for_zones", return_value=fake_features),
            patch("src.tasks.nws_poll.get_db_context") as mock_ctx,
        ):
            stats = run_nws_poll(dry_run=True)

        assert stats["polled"] == 1
        mock_ctx.assert_not_called()


# ──────────────────────────────────────────────────────────────────────────────
# Integration — live NWS API (opt-in only)
# ──────────────────────────────────────────────────────────────────────────────

@pytest.mark.integration
def test_real_nws_api_returns_valid_structure():
    """
    Live smoke test against api.weather.gov.

    Checks HTTP 200, top-level 'features' key, and that every feature carries
    a non-empty string event in its properties.  No DB writes.

    Run with: pytest tests/test_nws_poll.py -m integration -v
    """
    import requests

    resp = requests.get(
        "https://api.weather.gov/alerts/active",
        params={"area": "FL", "status": "actual"},
        headers={
            "User-Agent": "ForcedAction/1.0 (distressed-property-intelligence)",
            "Accept": "application/geo+json",
        },
        timeout=20,
    )
    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"

    data = resp.json()
    assert "features" in data, "Response missing 'features' key"

    for feature in data["features"]:
        props = feature.get("properties", {})
        event = props.get("event", "")
        assert isinstance(event, str) and event, (
            f"Feature {feature.get('id', '?')!r} has missing/non-string event: {event!r}"
        )
