"""NDL cold email through NDL's own Instantly workspace (P1.1, decisions D16 to D22). HTTP is faked; no network."""
from __future__ import annotations

from types import SimpleNamespace

import pytest
from pydantic import SecretStr

from src.lending import instantly_email
from src.lending.instantly_email import InstantlySendError, NdlInstantly, NdlInstantlySender

ACCOUNT = NdlInstantly(api_key="ndl-key", base_url="https://api.instantly.ai", campaign_id="camp-ndl",
                       sender_email="josh@ndl-mail.com", footer_brand="Next Deal Lending",
                       footer_postal_address="1 Main St, Tampa, FL 33602", footer_phone="(656) 221-1100")


class FakeResponse:
    def __init__(self, status: int, body: dict) -> None:
        self.status_code, self._body = status, body

    def json(self) -> dict:
        return self._body


class FakeInstantly:
    def __init__(self, routes: dict[tuple[str, str], tuple[int, dict]]) -> None:
        self.routes, self.calls = routes, []

    def __call__(self, method: str, url: str, **kwargs):
        path = url.removeprefix(ACCOUNT.base_url)
        self.calls.append((method, path, kwargs))
        status, body = self.routes[(method, path)]
        return FakeResponse(status, body)


def _routes(existing: list[dict] | None = None, added: dict | None = None) -> dict:
    return {
        ("POST", "/api/v2/leads/list"): (200, {"items": existing or []}),
        ("DELETE", "/api/v2/leads/lead-old"): (200, {}),
        ("POST", "/api/v2/leads/add"): (200, added if added is not None else
                                        {"leads_uploaded": 1, "created_leads": [{"id": "lead-new"}]}),
    }


@pytest.fixture(autouse=True)
def _not_halted(monkeypatch):
    monkeypatch.setattr(instantly_email, "is_globally_halted", lambda: False)


def test_send_adds_lead_with_footer_and_unsubscribe() -> None:
    fake = FakeInstantly(_routes())
    NdlInstantlySender(ACCOUNT, fake).send("mark@example.com", "Your refinance", "Hi Mark,\nquick question")
    method, path, kwargs = fake.calls[-1]
    assert (method, path) == ("POST", "/api/v2/leads/add")
    assert kwargs["headers"]["Authorization"] == "Bearer ndl-key"
    lead = kwargs["json"]["leads"][0]
    assert kwargs["json"]["campaign_id"] == "camp-ndl"
    assert lead["email"] == "mark@example.com"
    body = lead["custom_variables"]["ra_body"]
    assert lead["custom_variables"]["ra_subject"] == "Your refinance"
    assert "Hi Mark,<br>" in body
    assert "Next Deal Lending" in body and "1 Main St, Tampa, FL 33602" in body
    assert "/api/email/unsubscribe?token=" in body


def test_send_returns_lead_id() -> None:
    assert NdlInstantlySender(ACCOUNT, FakeInstantly(_routes())).send("m@example.com", "S", "B") == "lead-new"


def test_send_without_lead_id_returns_campaign_ref() -> None:
    fake = FakeInstantly(_routes(added={"leads_uploaded": 1}))
    assert NdlInstantlySender(ACCOUNT, fake).send("m@example.com", "S", "B") == "instantly:camp-ndl"


def test_duplicate_skip_is_error() -> None:
    fake = FakeInstantly(_routes(added={"leads_uploaded": 0, "duplicated_leads": 1}))
    with pytest.raises(InstantlySendError):
        NdlInstantlySender(ACCOUNT, fake).send("m@example.com", "S", "B")


def test_repeat_send_removes_previous_lead_first() -> None:
    existing = [{"id": "lead-other", "email": "someone@else.com"}, {"id": "lead-old", "email": "M@Example.com"}]
    fake = FakeInstantly(_routes(existing=existing))
    NdlInstantlySender(ACCOUNT, fake).send("m@example.com", "S", "B")
    calls = [(method, path) for method, path, _ in fake.calls]
    assert calls == [("POST", "/api/v2/leads/list"), ("DELETE", "/api/v2/leads/lead-old"),
                     ("POST", "/api/v2/leads/add")]
    assert fake.calls[0][2]["json"] == {"campaign": "camp-ndl", "search": "m@example.com", "limit": 50}


def test_refused_while_halted(monkeypatch) -> None:
    monkeypatch.setattr(instantly_email, "is_globally_halted", lambda: True)
    fake = FakeInstantly(_routes())
    with pytest.raises(InstantlySendError):
        NdlInstantlySender(ACCOUNT, fake).send("m@example.com", "S", "B")
    assert fake.calls == []


def test_error_message_has_no_email() -> None:
    fake = FakeInstantly({("POST", "/api/v2/leads/list"): (200, {"items": []}),
                          ("POST", "/api/v2/leads/add"): (500, {})})
    with pytest.raises(InstantlySendError) as err:
        NdlInstantlySender(ACCOUNT, fake).send("mark@example.com", "S", "B")
    assert "mark" not in str(err.value)
    assert "500" in str(err.value)


def _settings(**overrides) -> SimpleNamespace:
    values = dict(lending_instantly_api_key=SecretStr("ndl-key"), lending_instantly_base_url="https://api.instantly.ai",
                  lending_instantly_campaign_id="camp-ndl", lending_instantly_sender_email="josh@ndl-mail.com",
                  lending_email_footer_brand="Next Deal Lending",
                  lending_email_footer_postal_address="1 Main St, Tampa, FL 33602",
                  lending_email_footer_phone="(656) 221-1100", instantly_api_key=SecretStr("fa-key"))
    values.update(overrides)
    return SimpleNamespace(**values)


def test_configured_account(monkeypatch) -> None:
    monkeypatch.setattr(instantly_email, "get_settings", lambda: _settings())
    assert instantly_email.ndl_instantly() == ACCOUNT


def test_not_configured_without_postal_address(monkeypatch) -> None:
    monkeypatch.setattr(instantly_email, "get_settings", lambda: _settings(lending_email_footer_postal_address=""))
    assert instantly_email.ndl_instantly() is None
    assert instantly_email.get_ndl_instantly_sender() is None


def test_never_uses_fa_instantly_key(monkeypatch) -> None:
    monkeypatch.setattr(instantly_email, "get_settings", lambda: _settings(lending_instantly_api_key=None))
    assert instantly_email.ndl_instantly() is None


def test_pause_and_resume_call_campaign_endpoints() -> None:
    fake = FakeInstantly({("POST", "/api/v2/campaigns/camp-ndl/pause"): (200, {}),
                          ("POST", "/api/v2/campaigns/camp-ndl/activate"): (200, {})})
    sender = NdlInstantlySender(ACCOUNT, fake)
    assert sender.pause_campaign() is True
    assert sender.resume_campaign() is True
    assert [path for _, path, _ in fake.calls] == ["/api/v2/campaigns/camp-ndl/pause",
                                                     "/api/v2/campaigns/camp-ndl/activate"]


def test_pause_failure_returns_false() -> None:
    fake = FakeInstantly({("POST", "/api/v2/campaigns/camp-ndl/pause"): (503, {})})
    assert NdlInstantlySender(ACCOUNT, fake).pause_campaign() is False


def test_setup_campaign_schedule_is_9_to_20_detroit() -> None:
    fake = FakeInstantly({("GET", "/api/v2/campaigns"): (200, {"items": []}),
                          ("POST", "/api/v2/campaigns"): (200, {"id": "camp-new"}),
                          ("POST", "/api/v2/campaigns/camp-new/activate"): (200, {})})
    campaign_id = instantly_email.setup_campaign("ndl-key", "https://api.instantly.ai", "josh@ndl-mail.com", fake)
    assert campaign_id == "camp-new"
    created = next(kwargs["json"] for method, path, kwargs in fake.calls if (method, path) == ("POST", "/api/v2/campaigns"))
    assert created["name"] == "NDL Cora Passthrough"
    schedule = created["campaign_schedule"]["schedules"][0]
    assert schedule["timing"] == {"from": "09:00", "to": "20:00"}
    assert schedule["timezone"] == "America/Detroit"
    assert created["sequences"][0]["steps"][0]["variants"][0] == {"subject": "{{ra_subject}}", "body": "{{ra_body}}"}
    assert created["email_list"] == ["josh@ndl-mail.com"]


def test_setup_campaign_reuses_existing() -> None:
    fake = FakeInstantly({("GET", "/api/v2/campaigns"): (200, {"items": [{"id": "camp-1", "name": "NDL Cora Passthrough"}]})})
    assert instantly_email.setup_campaign("ndl-key", "https://api.instantly.ai", "josh@ndl-mail.com", fake) == "camp-1"
    assert [method for method, _, _ in fake.calls] == ["GET"]
