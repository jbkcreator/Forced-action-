"""NDL cold email through NDL's own Instantly workspace (P1.1, decisions D16 to D22). HTTP is faked; no network."""
from __future__ import annotations

from types import SimpleNamespace

import pytest
from pydantic import SecretStr

from src.lending import instantly_email
from src.lending.email_identity import LendingEmailIdentity
from src.lending.instantly_email import InstantlySendError, NdlInstantly, NdlInstantlySender

DAVID, JOSH = "david.willis@nextdeallending.com", "josh@nextdeallending.com"
IDENTITY = LendingEmailIdentity(from_address=DAVID, bcc=(JOSH,))
ACCOUNT = NdlInstantly(api_key="ndl-key", base_url="https://api.instantly.ai", campaign_id="camp-ndl",
                       identity=IDENTITY, footer_brand="Next Deal Lending",
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


CAMPAIGN_OK = {"id": "camp-ndl", "email_list": [DAVID], "bcc_list": [JOSH]}


def _routes(existing: list[dict] | None = None, added: dict | None = None, campaign: dict | None = None) -> dict:
    return {
        ("GET", "/api/v2/campaigns/camp-ndl"): (200, campaign if campaign is not None else CAMPAIGN_OK),
        ("POST", "/api/v2/leads/list"): (200, {"items": existing or []}),
        ("DELETE", "/api/v2/leads/lead-old"): (200, {}),
        ("POST", "/api/v2/leads/add"): (200, added if added is not None else
                                        {"leads_uploaded": 1, "created_leads": [{"id": "lead-new"}]}),
    }


@pytest.fixture(autouse=True)
def _not_halted(monkeypatch):
    monkeypatch.setattr(instantly_email, "is_globally_halted", lambda: False)


@pytest.fixture(autouse=True)
def _unsubscribe_link(monkeypatch):
    """The real link is signed with SUBSCRIBER_JWT_SECRET, which CI does not set; the footer only needs a link."""
    from src.services.relay import channels_email

    monkeypatch.setattr(channels_email, "unsubscribe_url",
                        lambda email: "https://app.example.test/api/email/unsubscribe?token=test-token")


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
    assert calls == [("GET", "/api/v2/campaigns/camp-ndl"), ("POST", "/api/v2/leads/list"),
                     ("DELETE", "/api/v2/leads/lead-old"), ("POST", "/api/v2/leads/add")]
    assert fake.calls[1][2]["json"] == {"campaign": "camp-ndl", "contacts": ["m@example.com"], "limit": 50}


def test_refused_while_halted(monkeypatch) -> None:
    monkeypatch.setattr(instantly_email, "is_globally_halted", lambda: True)
    fake = FakeInstantly(_routes())
    with pytest.raises(InstantlySendError):
        NdlInstantlySender(ACCOUNT, fake).send("m@example.com", "S", "B")
    assert fake.calls == []


def test_error_message_has_no_email() -> None:
    fake = FakeInstantly({("GET", "/api/v2/campaigns/camp-ndl"): (200, CAMPAIGN_OK),
                          ("POST", "/api/v2/leads/list"): (200, {"items": []}),
                          ("POST", "/api/v2/leads/add"): (500, {})})
    with pytest.raises(InstantlySendError) as err:
        NdlInstantlySender(ACCOUNT, fake).send("mark@example.com", "S", "B")
    assert "mark" not in str(err.value)
    assert "500" in str(err.value)


def _settings(**overrides) -> SimpleNamespace:
    values = dict(lending_instantly_api_key=SecretStr("ndl-key"), lending_instantly_base_url="https://api.instantly.ai",
                  lending_instantly_campaign_id="camp-ndl", lending_email_from=DAVID, lending_email_bcc=JOSH,
                  lending_email_footer_brand="Next Deal Lending",
                  lending_email_footer_postal_address="1 Main St, Tampa, FL 33602",
                  lending_email_footer_phone="(656) 221-1100", instantly_api_key=SecretStr("fa-key"))
    values.update(overrides)
    return SimpleNamespace(**values)


def _use(monkeypatch, settings) -> None:
    from src.lending import email_identity, instantly_client

    monkeypatch.setattr(instantly_email, "get_settings", lambda: settings)
    monkeypatch.setattr(instantly_client, "get_settings", lambda: settings)
    monkeypatch.setattr(email_identity, "get_settings", lambda: settings)


def test_configured_account(monkeypatch) -> None:
    _use(monkeypatch, _settings())
    assert instantly_email.ndl_instantly() == ACCOUNT


def test_not_configured_without_postal_address(monkeypatch) -> None:
    _use(monkeypatch, _settings(lending_email_footer_postal_address=""))
    assert instantly_email.ndl_instantly() is None
    assert instantly_email.get_ndl_instantly_sender() is None


@pytest.mark.parametrize("override", [{"lending_email_from": None}, {"lending_email_bcc": ""},
                                      {"lending_email_bcc": "not-an-address"}])
def test_without_david_and_josh_sends_are_refused_but_the_halt_can_still_pause(monkeypatch, override) -> None:
    _use(monkeypatch, _settings(**override))
    sender = instantly_email.get_ndl_instantly_sender()
    assert sender is not None and sender.can_send is False
    fake = FakeInstantly({**_routes(), ("POST", "/api/v2/campaigns/camp-ndl/pause"): (200, {})})
    sender._request = fake
    with pytest.raises(InstantlySendError, match="LENDING_EMAIL_FROM"):
        sender.send("m@example.com", "S", "B")
    assert fake.calls == []
    assert sender.pause_campaign() is True


def test_never_uses_fa_instantly_key(monkeypatch) -> None:
    _use(monkeypatch, _settings(lending_instantly_api_key=None))
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
    campaign_id = instantly_email.setup_campaign("ndl-key", "https://api.instantly.ai", IDENTITY, fake)
    assert campaign_id == "camp-new"
    created = next(kwargs["json"] for method, path, kwargs in fake.calls if (method, path) == ("POST", "/api/v2/campaigns"))
    assert created["name"] == "NDL Cora Passthrough"
    schedule = created["campaign_schedule"]["schedules"][0]
    assert schedule["timing"] == {"from": "09:00", "to": "20:00"}
    assert schedule["timezone"] == "America/Detroit"
    assert created["sequences"][0]["steps"][0]["variants"][0] == {"subject": "{{ra_subject}}", "body": "{{ra_body}}"}
    assert created["email_list"] == [DAVID]
    assert created["bcc_list"] == [JOSH]
    assert "cc_list" not in created




def test_bodyless_calls_send_no_json_content_type() -> None:
    """Instantly (Fastify) rejects a JSON content-type with an empty body (FST_ERR_CTP_EMPTY_JSON_BODY)."""
    fake = FakeInstantly({**_routes(existing=[{"id": "lead-old", "email": "m@example.com"}]),
                          ("POST", "/api/v2/campaigns/camp-ndl/pause"): (200, {})})
    sender = NdlInstantlySender(ACCOUNT, fake)
    sender.send("m@example.com", "S", "B")
    sender.pause_campaign()
    for method, path, kwargs in fake.calls:
        has_body = "json" in kwargs
        assert ("Content-Type" in kwargs["headers"]) is has_body, (method, path)


def test_setup_campaign_sets_david_and_josh_bcc_on_an_existing_campaign_then_activates_it() -> None:
    fake = FakeInstantly({("GET", "/api/v2/campaigns"): (200, {"items": [{"id": "camp-1", "name": "NDL Cora Passthrough"}]}),
                          ("PATCH", "/api/v2/campaigns/camp-1"): (200, {}),
                          ("POST", "/api/v2/campaigns/camp-1/activate"): (200, {})})
    assert instantly_email.setup_campaign("ndl-key", "https://api.instantly.ai", IDENTITY, fake) == "camp-1"
    assert [(m, p) for m, p, _ in fake.calls] == [("GET", "/api/v2/campaigns"), ("PATCH", "/api/v2/campaigns/camp-1"),
                                                  ("POST", "/api/v2/campaigns/camp-1/activate")]
    assert fake.calls[1][2]["json"] == {"email_list": [DAVID], "bcc_list": [JOSH]}


@pytest.mark.parametrize("campaign", [
    {"email_list": ["hello@nextdeallending.com"], "bcc_list": [JOSH]},
    {"email_list": [DAVID, "hello@nextdeallending.com"], "bcc_list": [JOSH]},
    {"email_list": [DAVID], "bcc_list": []},
    {"email_list": [DAVID]},
])
def test_send_refused_unless_campaign_sends_from_david_with_josh_in_bcc(campaign: dict) -> None:
    fake = FakeInstantly(_routes(campaign=campaign))
    with pytest.raises(InstantlySendError, match="setup-campaign"):
        NdlInstantlySender(ACCOUNT, fake).send("m@example.com", "S", "B")
    assert [(m, p) for m, p, _ in fake.calls] == [("GET", "/api/v2/campaigns/camp-ndl")]


def test_send_accepts_campaign_addresses_in_any_case() -> None:
    fake = FakeInstantly(_routes(campaign={"email_list": [DAVID.upper()], "bcc_list": [JOSH.upper(), "x@y.com"]}))
    assert NdlInstantlySender(ACCOUNT, fake).send("m@example.com", "S", "B") == "lead-new"



@pytest.mark.parametrize("result", [{"leads_uploaded": 0, "skipped_count": 1}, {"leads_uploaded": 0, "in_blocklist": 1}])
def test_skipped_or_blocklisted_lead_is_not_sent(result: dict) -> None:
    with pytest.raises(InstantlySendError):
        NdlInstantlySender(ACCOUNT, FakeInstantly(_routes(added=result))).send("m@example.com", "S", "B")


def test_blocklisted_error_says_so() -> None:
    with pytest.raises(InstantlySendError) as err:
        NdlInstantlySender(ACCOUNT, FakeInstantly(_routes(added={"leads_uploaded": 0, "in_blocklist": 1}))).send(
            "m@example.com", "S", "B")
    assert "block list" in str(err.value)


def test_setup_campaign_adds_the_unsubscribe_header() -> None:
    fake = FakeInstantly({("GET", "/api/v2/campaigns"): (200, {"items": []}),
                          ("POST", "/api/v2/campaigns"): (200, {"id": "camp-new"}),
                          ("POST", "/api/v2/campaigns/camp-new/activate"): (200, {})})
    instantly_email.setup_campaign("ndl-key", "https://api.instantly.ai", IDENTITY, fake)
    created = next(kwargs["json"] for method, path, kwargs in fake.calls if (method, path) == ("POST", "/api/v2/campaigns"))
    assert created["insert_unsubscribe_header"] is True
