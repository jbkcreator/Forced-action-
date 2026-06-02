"""
Unit tests for src/services/instantly_service.py (Phase B0).

All HTTP is mocked — no real Instantly API calls.
"""
import pytest
from unittest.mock import MagicMock, patch, call
import requests


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_resp(status: int, body: dict | list | None = None) -> MagicMock:
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status
    resp.json.return_value = body or {}
    resp.text = str(body or {})
    if status >= 400:
        resp.raise_for_status.side_effect = requests.HTTPError(response=resp)
    else:
        resp.raise_for_status.return_value = None
    return resp


# ---------------------------------------------------------------------------
# _is_configured
# ---------------------------------------------------------------------------

class TestIsConfigured:
    def test_returns_false_when_key_absent(self, monkeypatch):
        monkeypatch.setenv("INSTANTLY_API_KEY", "")
        monkeypatch.setenv("INSTANTLY_ENABLED", "true")
        from importlib import reload
        import config.settings as cs
        reload(cs)
        import src.services.instantly_service as svc
        reload(svc)
        assert not svc._is_configured()

    def test_returns_false_when_disabled(self, monkeypatch):
        monkeypatch.setenv("INSTANTLY_API_KEY", "test-key")
        monkeypatch.setenv("INSTANTLY_ENABLED", "false")
        from importlib import reload
        import config.settings as cs
        reload(cs)
        import src.services.instantly_service as svc
        reload(svc)
        assert not svc._is_configured()

    def test_returns_true_when_configured(self, monkeypatch):
        monkeypatch.setenv("INSTANTLY_API_KEY", "test-key")
        monkeypatch.setenv("INSTANTLY_ENABLED", "true")
        from importlib import reload
        import config.settings as cs
        reload(cs)
        import src.services.instantly_service as svc
        reload(svc)
        assert svc._is_configured()


# ---------------------------------------------------------------------------
# _request — throttle, 429 retry, 401 raise
# ---------------------------------------------------------------------------

class TestRequest:
    @patch("src.services.instantly_service.time.sleep")
    @patch("src.services.instantly_service.requests.request")
    def test_200_returns_immediately(self, mock_req, mock_sleep, monkeypatch):
        monkeypatch.setenv("INSTANTLY_API_KEY", "k")
        monkeypatch.setenv("INSTANTLY_ENABLED", "true")
        from importlib import reload
        import config.settings as cs; reload(cs)
        import src.services.instantly_service as svc; reload(svc)

        mock_req.return_value = _mock_resp(200, {"ok": True})
        resp = svc._request("GET", "/api/v2/campaigns")
        assert resp.status_code == 200
        # baseline throttle sleep called once
        mock_sleep.assert_called_once_with(svc._BASE_THROTTLE)

    @patch("src.services.instantly_service.time.sleep")
    @patch("src.services.instantly_service.requests.request")
    def test_429_retries_then_succeeds(self, mock_req, mock_sleep, monkeypatch):
        monkeypatch.setenv("INSTANTLY_API_KEY", "k")
        monkeypatch.setenv("INSTANTLY_ENABLED", "true")
        from importlib import reload
        import config.settings as cs; reload(cs)
        import src.services.instantly_service as svc; reload(svc)

        mock_req.side_effect = [
            _mock_resp(429),
            _mock_resp(429),
            _mock_resp(200, {"id": "abc"}),
        ]
        resp = svc._request("POST", "/api/v2/campaigns")
        assert resp.status_code == 200
        assert mock_req.call_count == 3

    @patch("src.services.instantly_service.time.sleep")
    @patch("src.services.instantly_service.requests.request")
    def test_401_raises_runtime_error(self, mock_req, mock_sleep, monkeypatch):
        monkeypatch.setenv("INSTANTLY_API_KEY", "k")
        monkeypatch.setenv("INSTANTLY_ENABLED", "true")
        from importlib import reload
        import config.settings as cs; reload(cs)
        import src.services.instantly_service as svc; reload(svc)

        mock_req.return_value = _mock_resp(401)
        with pytest.raises(RuntimeError, match="Auth/billing error 401"):
            svc._request("GET", "/api/v2/campaigns")

    @patch("src.services.instantly_service.time.sleep")
    @patch("src.services.instantly_service.requests.request")
    def test_402_raises_runtime_error(self, mock_req, mock_sleep, monkeypatch):
        monkeypatch.setenv("INSTANTLY_API_KEY", "k")
        monkeypatch.setenv("INSTANTLY_ENABLED", "true")
        from importlib import reload
        import config.settings as cs; reload(cs)
        import src.services.instantly_service as svc; reload(svc)

        mock_req.return_value = _mock_resp(402)
        with pytest.raises(RuntimeError, match="Auth/billing error 402"):
            svc._request("POST", "/api/v2/leads/add")


# ---------------------------------------------------------------------------
# No-op when not configured
# ---------------------------------------------------------------------------

class TestNoopWhenNotConfigured:
    def _reload_unconfigured(self, monkeypatch):
        monkeypatch.setenv("INSTANTLY_API_KEY", "")
        monkeypatch.setenv("INSTANTLY_ENABLED", "true")
        from importlib import reload
        import config.settings as cs; reload(cs)
        import src.services.instantly_service as svc; reload(svc)
        return svc

    def test_create_campaign_returns_none(self, monkeypatch):
        svc = self._reload_unconfigured(monkeypatch)
        assert svc.create_campaign("x", {}, []) is None

    def test_add_leads_returns_none(self, monkeypatch):
        svc = self._reload_unconfigured(monkeypatch)
        assert svc.add_leads("cid", [{"email": "a@b.com"}]) is None

    def test_list_accounts_returns_empty(self, monkeypatch):
        svc = self._reload_unconfigured(monkeypatch)
        assert svc.list_accounts() == []

    def test_get_daily_analytics_returns_empty(self, monkeypatch):
        svc = self._reload_unconfigured(monkeypatch)
        assert svc.get_daily_analytics(["id1"], "2026-01-01", "2026-01-31") == []

    def test_get_warmup_analytics_returns_empty(self, monkeypatch):
        svc = self._reload_unconfigured(monkeypatch)
        assert svc.get_warmup_analytics(["a@b.com"]) == []


# ---------------------------------------------------------------------------
# Status + analytics mapping helpers
# ---------------------------------------------------------------------------

class TestMapping:
    def test_map_lead_status_known(self):
        from src.services import instantly_service as svc
        assert svc.map_lead_status("unsubscribed") == "unsubscribed"
        assert svc.map_lead_status("Interested") == "interested"
        assert svc.map_lead_status("not interested") == "not_interested"
        assert svc.map_lead_status("completed") == "completed"

    def test_map_lead_status_unknown_defaults_active(self):
        from src.services import instantly_service as svc
        assert svc.map_lead_status("something_weird") == "active"

    def test_map_analytics_computes_rates(self):
        from src.services import instantly_service as svc
        raw = {
            "emails_sent_count": 100,
            "open_count_unique": 30,
            "reply_count_unique": 10,
            "link_click_count": 5,
            "bounced_count": 2,
            "unsubscribed_count": 1,
        }
        out = svc.map_analytics(raw)
        assert out["emails_sent"] == 100
        assert out["opens"] == 30
        assert out["replies"] == 10
        assert out["open_rate"] == pytest.approx(0.3)
        assert out["reply_rate"] == pytest.approx(0.1)

    def test_map_analytics_zero_sent(self):
        from src.services import instantly_service as svc
        out = svc.map_analytics({"emails_sent_count": 0})
        assert out["open_rate"] == 0.0
        assert out["reply_rate"] == 0.0


# ---------------------------------------------------------------------------
# Campaign methods (mocked HTTP)
# ---------------------------------------------------------------------------

class TestCampaignMethods:
    def _setup(self, monkeypatch):
        monkeypatch.setenv("INSTANTLY_API_KEY", "test-key")
        monkeypatch.setenv("INSTANTLY_ENABLED", "true")
        from importlib import reload
        import config.settings as cs; reload(cs)
        import src.services.instantly_service as svc; reload(svc)
        return svc

    @patch("src.services.instantly_service.time.sleep")
    @patch("src.services.instantly_service.requests.request")
    def test_create_campaign_success(self, mock_req, mock_sleep, monkeypatch):
        svc = self._setup(monkeypatch)
        mock_req.return_value = _mock_resp(200, {"id": "camp-123", "name": "Test"})
        result = svc.create_campaign(
            name="Test",
            schedule={"schedules": [{"name": "S", "timing": {"from": "09:00", "to": "17:00"},
                                      "days": {}, "timezone": "America/New_York"}],
                      "start_date": "2026-07-01", "end_date": "2026-08-01"},
            sequence_steps=[{"step_number": 1, "subject": "Hi", "body": "Hello", "delay_days": 0}],
        )
        assert result is not None
        assert result["id"] == "camp-123"
        call_kwargs = mock_req.call_args
        assert call_kwargs[0][0] == "POST"
        assert "/api/v2/campaigns" in call_kwargs[0][1]

    @patch("src.services.instantly_service.time.sleep")
    @patch("src.services.instantly_service.requests.request")
    def test_activate_campaign(self, mock_req, mock_sleep, monkeypatch):
        svc = self._setup(monkeypatch)
        mock_req.return_value = _mock_resp(200, {})
        assert svc.activate_campaign("camp-123") is True
        assert "/activate" in mock_req.call_args[0][1]

    @patch("src.services.instantly_service.time.sleep")
    @patch("src.services.instantly_service.requests.request")
    def test_pause_campaign(self, mock_req, mock_sleep, monkeypatch):
        svc = self._setup(monkeypatch)
        mock_req.return_value = _mock_resp(200, {})
        assert svc.pause_campaign("camp-123") is True
        assert "/pause" in mock_req.call_args[0][1]

    @patch("src.services.instantly_service.time.sleep")
    @patch("src.services.instantly_service.requests.request")
    def test_duplicate_campaign(self, mock_req, mock_sleep, monkeypatch):
        svc = self._setup(monkeypatch)
        mock_req.return_value = _mock_resp(200, {"id": "camp-456"})
        result = svc.duplicate_campaign("camp-123")
        assert result["id"] == "camp-456"
        assert "/duplicate" in mock_req.call_args[0][1]


# ---------------------------------------------------------------------------
# Lead methods (mocked HTTP)
# ---------------------------------------------------------------------------

class TestLeadMethods:
    def _setup(self, monkeypatch):
        monkeypatch.setenv("INSTANTLY_API_KEY", "test-key")
        monkeypatch.setenv("INSTANTLY_ENABLED", "true")
        from importlib import reload
        import config.settings as cs; reload(cs)
        import src.services.instantly_service as svc; reload(svc)
        return svc

    @patch("src.services.instantly_service.time.sleep")
    @patch("src.services.instantly_service.requests.request")
    def test_add_leads_posts_to_correct_path(self, mock_req, mock_sleep, monkeypatch):
        svc = self._setup(monkeypatch)
        mock_req.return_value = _mock_resp(200, {"leads_created": 2, "leads_skipped": 0})
        leads = [{"email": "a@test.com"}, {"email": "b@test.com"}]
        result = svc.add_leads("camp-123", leads)
        assert result["leads_created"] == 2
        call_args = mock_req.call_args
        assert "/api/v2/leads/add" in call_args[0][1]
        body = call_args[1]["json"]
        assert body["campaign_id"] == "camp-123"
        assert len(body["leads"]) == 2

    @patch("src.services.instantly_service.time.sleep")
    @patch("src.services.instantly_service.requests.request")
    def test_add_leads_empty_returns_early(self, mock_req, mock_sleep, monkeypatch):
        svc = self._setup(monkeypatch)
        result = svc.add_leads("camp-123", [])
        mock_req.assert_not_called()
        assert result == {"leads_created": 0, "leads_skipped": 0}

    @patch("src.services.instantly_service.time.sleep")
    @patch("src.services.instantly_service.requests.request")
    def test_list_leads_uses_post(self, mock_req, mock_sleep, monkeypatch):
        svc = self._setup(monkeypatch)
        mock_req.return_value = _mock_resp(200, {"leads": [], "next_starting_after": None})
        result = svc.list_leads("camp-123")
        assert result is not None
        call_args = mock_req.call_args
        assert call_args[0][0] == "POST"
        assert "/api/v2/leads/list" in call_args[0][1]
        assert call_args[1]["json"]["campaign"] == "camp-123"

    @patch("src.services.instantly_service.time.sleep")
    @patch("src.services.instantly_service.requests.request")
    def test_list_leads_passes_cursor(self, mock_req, mock_sleep, monkeypatch):
        svc = self._setup(monkeypatch)
        mock_req.return_value = _mock_resp(200, {"leads": [], "next_starting_after": None})
        svc.list_leads("camp-123", cursor="abc123")
        body = mock_req.call_args[1]["json"]
        assert body["starting_after"] == "abc123"


# ---------------------------------------------------------------------------
# Inbox / warmup
# ---------------------------------------------------------------------------

class TestInboxMethods:
    def _setup(self, monkeypatch):
        monkeypatch.setenv("INSTANTLY_API_KEY", "test-key")
        monkeypatch.setenv("INSTANTLY_ENABLED", "true")
        from importlib import reload
        import config.settings as cs; reload(cs)
        import src.services.instantly_service as svc; reload(svc)
        return svc

    @patch("src.services.instantly_service.time.sleep")
    @patch("src.services.instantly_service.requests.request")
    def test_list_accounts(self, mock_req, mock_sleep, monkeypatch):
        svc = self._setup(monkeypatch)
        mock_req.return_value = _mock_resp(200, [{"email": "sender@domain.com"}])
        result = svc.list_accounts()
        assert len(result) == 1
        assert result[0]["email"] == "sender@domain.com"

    @patch("src.services.instantly_service.time.sleep")
    @patch("src.services.instantly_service.requests.request")
    def test_get_warmup_analytics_posts(self, mock_req, mock_sleep, monkeypatch):
        svc = self._setup(monkeypatch)
        mock_req.return_value = _mock_resp(200, [{"email": "s@d.com", "health_score": 85}])
        result = svc.get_warmup_analytics(["s@d.com"])
        assert result[0]["health_score"] == 85
        call_args = mock_req.call_args
        assert call_args[0][0] == "POST"
        assert "/api/v2/accounts/warmup-analytics" in call_args[0][1]
        assert call_args[1]["json"]["emails"] == ["s@d.com"]

    @patch("src.services.instantly_service.time.sleep")
    @patch("src.services.instantly_service.requests.request")
    def test_get_warmup_analytics_empty_input(self, mock_req, mock_sleep, monkeypatch):
        svc = self._setup(monkeypatch)
        result = svc.get_warmup_analytics([])
        mock_req.assert_not_called()
        assert result == []
