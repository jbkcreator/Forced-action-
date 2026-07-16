"""
T-B3-01 — freemium funnel master launch toggle.

Unit tests only (no DB): each funnel leg goes dark when
FREEMIUM_FUNNEL_ENABLED is off, and the admin status endpoint reports
effective state per leg.

Run:
    pytest tests/test_freemium_toggle.py -v
"""
from unittest.mock import MagicMock, patch

import pytest

from config.settings import get_settings


@pytest.fixture
def funnel_off(monkeypatch):
    monkeypatch.setattr(get_settings(), "freemium_funnel_enabled", False)


@pytest.fixture
def funnel_on(monkeypatch):
    monkeypatch.setattr(get_settings(), "freemium_funnel_enabled", True)


def test_flag_defaults_on():
    assert get_settings().freemium_funnel_enabled is True


def test_proof_leads_empty_when_off(funnel_off):
    from src.services.proof_moment import get_proof_leads

    result = get_proof_leads(vertical="roofing", county_id="hillsborough", db=MagicMock())
    assert result == {
        "revealed": None, "blurred": [],
        "county_id": "hillsborough", "vertical": "roofing",
    }


def test_blurred_stack_empty_when_off(funnel_off):
    from src.services.proof_moment import get_blurred_stack

    db = MagicMock()
    assert get_blurred_stack(1, "roofing", "hillsborough", db) == []
    db.execute.assert_not_called()


def test_flash_scarcity_noop_when_off(funnel_off):
    from src.services.flash_scarcity import open_window_if_spike

    db = MagicMock()
    assert open_window_if_spike(db, lead_id=1, zip_code="33607", vertical="roofing") is False
    db.execute.assert_not_called()


def test_abandonment_waves_skip_when_off(funnel_off):
    from src.agents.graphs import abandonment

    with patch.object(abandonment, "build_wave1_graph") as w1, \
         patch.object(abandonment, "build_wave2_graph") as w2:
        r1 = abandonment.run_wave1({}, subscriber_id=1)
        r2 = abandonment.run_wave2({}, subscriber_id=1, decision_id="d-1")
    assert r1 == {"skipped": "freemium_funnel_disabled", "wave": "wave1"}
    assert r2 == {"skipped": "freemium_funnel_disabled", "wave": "wave2"}
    w1.assert_not_called()
    w2.assert_not_called()


def test_free_signup_403_when_off(funnel_off):
    from fastapi import HTTPException

    from src.api.main import free_signup

    with pytest.raises(HTTPException) as exc:
        free_signup(req=MagicMock(), request=MagicMock(), db=MagicMock())
    assert exc.value.status_code == 403


def test_wall_session_403_when_off(funnel_off):
    from fastapi import HTTPException

    from src.api.main import create_wall_session

    with pytest.raises(HTTPException) as exc:
        create_wall_session(req=MagicMock(), db=MagicMock())
    assert exc.value.status_code == 403


def test_status_endpoint_off(funnel_off):
    from src.api import admin_router

    result = admin_router.freemium_funnel_status()
    assert result["master"] is False
    assert all(leg["effective"] == "OFF" for leg in result["legs"].values())


def test_status_endpoint_on_reports_kill_switch(funnel_on):
    from src.api import admin_router

    with patch("src.services.kill_switch_service.get_cached_metric", return_value=None), \
         patch("src.services.kill_switch_service.get_kill_switch_status",
               return_value={"color": "red", "feature": "first_payment_rate"}):
        result = admin_router.freemium_funnel_status()

    assert result["master"] is True
    assert result["legs"]["free_signup"]["effective"] == "ON"
    assert result["legs"]["abandonment"]["effective"] == "DEGRADED"
    assert result["legs"]["abandonment"]["gates"]["first_payment_rate"] == "red"


def test_status_endpoint_survives_kill_switch_failure(funnel_on):
    from src.api import admin_router

    with patch("src.services.kill_switch_service.get_cached_metric", side_effect=RuntimeError("redis down")):
        result = admin_router.freemium_funnel_status()

    assert result["legs"]["abandonment"]["gates"]["first_payment_rate"] == "unknown"
    assert result["legs"]["abandonment"]["effective"] == "DEGRADED"
