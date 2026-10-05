"""WP-GL-5: push to the GHL "Booked Calls" pipeline (Booked / Nurture stages)."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.services.calendar import ghl_pipeline

PIPELINE_ID = "OzuRH2ELgAQJZy3bBVDr"
BOOKED_STAGE = "c937b97a-c46b-4107-8658-3948acd0ad88"
NURTURE_STAGE = "1cfb8e57-8c60-4e2e-b77c-c885021c3522"
LOCATION_ID = "9qCy9RMDxsrTh2FOkdDc"


def _configured_settings(**overrides):
    s = MagicMock()
    s.lending_ghl_api_key = MagicMock(get_secret_value=lambda: "test-key")
    s.lending_ghl_location_id = LOCATION_ID
    s.lending_ghl_pipeline_id = PIPELINE_ID
    s.lending_ghl_stage_booked = BOOKED_STAGE
    s.lending_ghl_stage_nurture = NURTURE_STAGE
    for k, v in overrides.items():
        setattr(s, k, v)
    return s


def _response(status_code=200, json_body=None, text=""):
    resp = MagicMock()
    resp.status_code = status_code
    resp.ok = 200 <= status_code < 300
    resp.json.return_value = json_body or {}
    resp.text = text
    return resp


class TestIsConfigured:
    def test_false_when_api_key_missing(self):
        with patch("config.settings.get_settings", return_value=_configured_settings(lending_ghl_api_key=None)):
            assert ghl_pipeline._is_configured() is False

    def test_false_when_pipeline_id_missing(self):
        with patch("config.settings.get_settings", return_value=_configured_settings(lending_ghl_pipeline_id=None)):
            assert ghl_pipeline._is_configured() is False

    def test_true_when_fully_configured(self):
        with patch("config.settings.get_settings", return_value=_configured_settings()):
            assert ghl_pipeline._is_configured() is True


class TestPushToStage:
    def test_not_configured_skips_without_raising(self):
        with patch("config.settings.get_settings", return_value=_configured_settings(lending_ghl_pipeline_id=None)):
            result = ghl_pipeline.push_to_stage(
                phone="+18135551234", email=None, first_name="Maria",
                stage_id=BOOKED_STAGE, opportunity_name="test",
            )
        assert result is False

    def test_no_contact_method_skips_without_raising(self):
        with patch("config.settings.get_settings", return_value=_configured_settings()):
            result = ghl_pipeline.push_to_stage(
                phone=None, email=None, first_name="Maria",
                stage_id=BOOKED_STAGE, opportunity_name="test",
            )
        assert result is False

    def test_creates_contact_then_opportunity(self):
        contact_resp = _response(json_body={"contact": {"id": "contact_1"}})
        search_resp = _response(json_body={"opportunities": []})
        create_opp_resp = _response(status_code=201, json_body={"opportunity": {"id": "opp_1"}})

        with (
            patch("config.settings.get_settings", return_value=_configured_settings()),
            patch("requests.request", side_effect=[contact_resp, search_resp, create_opp_resp]) as mock_req,
        ):
            result = ghl_pipeline.push_to_stage(
                phone="+18135551234", email=None, first_name="Maria",
                stage_id=BOOKED_STAGE, opportunity_name="Intro call (ref_1)",
            )

        assert result is True
        # First call creates the contact
        first_call = mock_req.call_args_list[0]
        assert "/contacts/" in first_call.args[1]
        assert first_call.kwargs["json"]["phone"] == "+18135551234"
        # Last call creates the opportunity in the right pipeline/stage
        last_call = mock_req.call_args_list[-1]
        assert "/opportunities/" in last_call.args[1]
        assert last_call.kwargs["json"]["pipelineId"] == PIPELINE_ID
        assert last_call.kwargs["json"]["pipelineStageId"] == BOOKED_STAGE
        assert last_call.kwargs["json"]["contactId"] == "contact_1"

    def test_updates_existing_opportunity_instead_of_creating_a_second_one(self):
        contact_resp = _response(json_body={"contact": {"id": "contact_1"}})
        search_resp = _response(json_body={"opportunities": [{"id": "existing_opp"}]})
        update_resp = _response(json_body={"opportunity": {"id": "existing_opp"}})

        with (
            patch("config.settings.get_settings", return_value=_configured_settings()),
            patch("requests.request", side_effect=[contact_resp, search_resp, update_resp]) as mock_req,
        ):
            result = ghl_pipeline.push_to_stage(
                phone="+18135551234", email=None, first_name="Maria",
                stage_id=NURTURE_STAGE, opportunity_name="Gate fail (gate_1)",
            )

        assert result is True
        last_call = mock_req.call_args_list[-1]
        assert last_call.args[0] == "PUT"
        assert "existing_opp" in last_call.args[1]

    def test_contact_upsert_failure_does_not_raise(self):
        with (
            patch("config.settings.get_settings", return_value=_configured_settings()),
            patch("requests.request", return_value=_response(status_code=500, text="boom")),
        ):
            result = ghl_pipeline.push_to_stage(
                phone="+18135551234", email=None, first_name="Maria",
                stage_id=BOOKED_STAGE, opportunity_name="test",
            )
        assert result is False

    def test_duplicate_contact_400_resolves_to_existing_id(self):
        dup_resp = _response(status_code=400, json_body={"meta": {"contactId": "dup_contact"}})
        search_resp = _response(json_body={"opportunities": []})
        create_opp_resp = _response(status_code=201, json_body={"opportunity": {"id": "opp_1"}})

        with (
            patch("config.settings.get_settings", return_value=_configured_settings()),
            patch("requests.request", side_effect=[dup_resp, search_resp, create_opp_resp]) as mock_req,
        ):
            result = ghl_pipeline.push_to_stage(
                phone="+18135551234", email=None, first_name="Maria",
                stage_id=BOOKED_STAGE, opportunity_name="test",
            )
        assert result is True
        last_call = mock_req.call_args_list[-1]
        assert last_call.kwargs["json"]["contactId"] == "dup_contact"


class TestStageHelpers:
    def test_push_booking_to_booked_stage_uses_the_booked_stage_id(self):
        with (
            patch("config.settings.get_settings", return_value=_configured_settings()),
            patch("src.services.calendar.ghl_pipeline.push_to_stage", return_value=True) as mock_push,
        ):
            ghl_pipeline.push_booking_to_booked_stage(
                phone="+18135551234", email=None, first_name="Maria", opportunity_name="x",
            )
        assert mock_push.call_args.kwargs["stage_id"] == BOOKED_STAGE

    def test_push_gate_fail_uses_the_nurture_stage_id(self):
        with (
            patch("config.settings.get_settings", return_value=_configured_settings()),
            patch("src.services.calendar.ghl_pipeline.push_to_stage", return_value=True) as mock_push,
        ):
            ghl_pipeline.push_gate_fail_to_nurture_stage(
                phone="+18135551234", email=None, first_name="Maria", opportunity_name="x",
            )
        assert mock_push.call_args.kwargs["stage_id"] == NURTURE_STAGE

    def test_booked_stage_missing_skips_without_raising(self):
        with patch("config.settings.get_settings", return_value=_configured_settings(lending_ghl_stage_booked=None)):
            result = ghl_pipeline.push_booking_to_booked_stage(
                phone="+18135551234", email=None, first_name="Maria", opportunity_name="x",
            )
        assert result is False

    def test_nurture_stage_missing_skips_without_raising(self):
        with patch("config.settings.get_settings", return_value=_configured_settings(lending_ghl_stage_nurture=None)):
            result = ghl_pipeline.push_gate_fail_to_nurture_stage(
                phone="+18135551234", email=None, first_name="Maria", opportunity_name="x",
            )
        assert result is False
