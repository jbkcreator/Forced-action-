"""
Unit tests for POST /api/clay/resolve-linkedin-url (Clay HTTP API enrichment).

The LLM call (call_claude) is mocked so tests are deterministic and offline.
Auth secret is patched via get_settings.
"""

import json
from unittest.mock import patch

import pytest
from pydantic import SecretStr
from fastapi.testclient import TestClient

from src.api.main import app

client = TestClient(app)

_SECRET = "test-clay-secret"
_URL = "/api/clay/resolve-linkedin-url"


def _auth_header(token: str = _SECRET) -> dict:
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture(autouse=True)
def _configure_secret():
    """Point clay_http_api_secret at a known value for every test, then restore."""
    from config.settings import get_settings
    s = get_settings()
    original = s.clay_http_api_secret
    s.clay_http_api_secret = SecretStr(_SECRET)
    try:
        yield
    finally:
        s.clay_http_api_secret = original


def _mock_llm(payload: dict):
    """Patch call_claude in the router to return a JSON string payload."""
    return patch("src.api.clay_router.call_claude", return_value=json.dumps(payload))


# ── Auth ────────────────────────────────────────────────────────────────────

def test_missing_token_returns_401():
    resp = client.post(_URL, json={"company_name": "Acme"})
    assert resp.status_code == 401


def test_wrong_token_returns_401():
    resp = client.post(_URL, json={"company_name": "Acme"}, headers=_auth_header("nope"))
    assert resp.status_code == 401


# ── Happy path ──────────────────────────────────────────────────────────────

def test_strong_company_match_returns_url():
    payload = {
        "company_linkedin_url": "https://www.linkedin.com/company/acme-roofing",
        "linkedin_confidence": 0.93,
        "linkedin_match_type": "company_page",
        "linkedin_reason": "Name and domain match the company page.",
        "linkedin_candidates": [
            {"url": "https://www.linkedin.com/company/acme-roofing",
             "title": "Acme Roofing | LinkedIn", "confidence": 0.93},
        ],
    }
    with _mock_llm(payload):
        resp = client.post(_URL, json={
            "company_name": "Acme Roofing",
            "domain": "acmeroofing.com",
            "google_search_results": "Acme Roofing LinkedIn company page ...",
            "city": "Tampa", "state": "FL",
        }, headers=_auth_header())
    assert resp.status_code == 200
    body = resp.json()
    assert body["company_linkedin_url"] == "https://www.linkedin.com/company/acme-roofing"
    assert body["linkedin_match_type"] == "company_page"
    assert body["linkedin_confidence"] == 0.93
    assert len(body["linkedin_candidates"]) == 1


# ── Rejection rules ───────────────────────────────────────────────────────────

def test_people_profile_is_rejected():
    """A /in/ people profile must never become company_linkedin_url."""
    payload = {
        "company_linkedin_url": "https://www.linkedin.com/in/john-doe",
        "linkedin_confidence": 0.95,
        "linkedin_match_type": "company_page",
        "linkedin_reason": "LLM mistakenly picked a person.",
        "linkedin_candidates": [],
    }
    with _mock_llm(payload):
        resp = client.post(_URL, json={
            "company_name": "Acme",
            "google_search_results": "John Doe LinkedIn profile",
        }, headers=_auth_header())
    assert resp.status_code == 200
    body = resp.json()
    assert body["company_linkedin_url"] is None
    assert body["linkedin_match_type"] == "no_match"


def test_low_confidence_returns_null_url():
    payload = {
        "company_linkedin_url": "https://www.linkedin.com/company/maybe-acme",
        "linkedin_confidence": 0.42,
        "linkedin_match_type": "company_page",
        "linkedin_reason": "Weak match.",
        "linkedin_candidates": [
            {"url": "https://www.linkedin.com/company/maybe-acme", "title": None, "confidence": 0.42},
        ],
    }
    with _mock_llm(payload):
        resp = client.post(_URL, json={
            "company_name": "Acme",
            "google_search_results": "ambiguous results",
        }, headers=_auth_header())
    assert resp.status_code == 200
    body = resp.json()
    assert body["company_linkedin_url"] is None
    assert body["linkedin_match_type"] == "uncertain"
    # Candidate list is still surfaced for human/Clay review.
    assert body["linkedin_candidates"][0]["url"].endswith("/company/maybe-acme")


# ── Robustness ────────────────────────────────────────────────────────────────

def test_missing_search_results_does_not_crash():
    """No google_search_results + no identifiers → deterministic no_match, no LLM call."""
    with patch("src.api.clay_router.call_claude") as mock_llm:
        resp = client.post(_URL, json={"lead_id": "abc"}, headers=_auth_header())
    assert resp.status_code == 200
    body = resp.json()
    assert body["company_linkedin_url"] is None
    assert body["linkedin_match_type"] == "no_match"
    mock_llm.assert_not_called()


def test_search_results_as_array_is_accepted():
    """Clay may send an array of result objects — must be normalized, not rejected."""
    payload = {
        "company_linkedin_url": "https://www.linkedin.com/company/acme",
        "linkedin_confidence": 0.88,
        "linkedin_match_type": "company_page",
        "linkedin_reason": "Match.",
        "linkedin_candidates": [],
    }
    with _mock_llm(payload):
        resp = client.post(_URL, json={
            "company_name": "Acme",
            "google_search_results": [
                {"title": "Acme | LinkedIn", "link": "https://www.linkedin.com/company/acme"},
                {"title": "Acme Inc", "link": "https://acme.com"},
            ],
        }, headers=_auth_header())
    assert resp.status_code == 200
    assert resp.json()["company_linkedin_url"] == "https://www.linkedin.com/company/acme"


def test_unparseable_llm_response_returns_no_match():
    with patch("src.api.clay_router.call_claude", return_value="not json at all"):
        resp = client.post(_URL, json={
            "company_name": "Acme",
            "google_search_results": "something",
        }, headers=_auth_header())
    assert resp.status_code == 200
    assert resp.json()["linkedin_match_type"] == "no_match"
