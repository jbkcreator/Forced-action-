"""NDL cold email through NDL's own Instantly workspace (P1.1, decisions D16 to D22). HTTP is faked; no network."""
from __future__ import annotations

from types import SimpleNamespace

import pytest
from pydantic import SecretStr

from src.lending import instantly_email
from src.lending.instantly_email import InstantlySendError, NdlInstantly, NdlInstantlySender

ACCOUNT = NdlInstantly(api_key="ndl-key", base_url="https://api.instantly.ai", campaign_id="camp-ndl",
                       sender_email="josh@ndl-mail.com", footer_brand="Next Deal Lending",
                       footer_postal_address="1 Main St, Tampa, FL 33602", footer_phone="(656) 221-1100",
                       unsubscribe_base_url="https://api.ndl.example.test")


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


@pytest.fixture(autouse=True)
def _unsubscribe_link(monkeypatch):
    """The real link is signed with SUBSCRIBER_JWT_SECRET, which CI does not set; the footer only needs a link."""
    monkeypatch.setattr(instantly_email, "unsubscribe_url",
                        lambda email, base_url=None, path="": f"{base_url}{path}?token=test-token")


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
    assert 'href="https://api.ndl.example.test/api/lending/email/unsubscribe?token=test-token"' in body
    assert "forcedactionleads" not in body


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
    assert fake.calls[0][2]["json"] == {"campaign": "camp-ndl", "contacts": ["m@example.com"], "limit": 50}


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
                  lending_email_footer_phone="(656) 221-1100", instantly_api_key=SecretStr("fa-key"),
                  lending_unsubscribe_base_url="https://api.ndl.example.test")
    values.update(overrides)
    return SimpleNamespace(**values)


def _use(monkeypatch, settings) -> None:
    from src.lending import instantly_client

    monkeypatch.setattr(instantly_email, "get_settings", lambda: settings)
    monkeypatch.setattr(instantly_client, "get_settings", lambda: settings)


def test_configured_account(monkeypatch) -> None:
    _use(monkeypatch, _settings())
    assert instantly_email.ndl_instantly() == ACCOUNT


def test_not_configured_without_postal_address(monkeypatch) -> None:
    _use(monkeypatch, _settings(lending_email_footer_postal_address=""))
    assert instantly_email.ndl_instantly() is None
    assert instantly_email.get_ndl_instantly_sender() is None


def test_cold_email_is_off_without_an_unsubscribe_host(monkeypatch) -> None:
    _use(monkeypatch, _settings(lending_unsubscribe_base_url=""))
    assert instantly_email.ndl_instantly() is None


def test_a_send_without_an_unsubscribe_host_is_refused_and_never_uses_the_fa_host() -> None:
    account = NdlInstantly(**{**ACCOUNT.__dict__, "unsubscribe_base_url": ""})
    fake = FakeInstantly(_routes())
    with pytest.raises(InstantlySendError, match="unsubscribe host"):
        NdlInstantlySender(account, fake).send("m@example.com", "S", "B")
    assert fake.calls == []


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
    campaign_id = instantly_email.setup_campaign("ndl-key", "https://api.instantly.ai", "josh@ndl-mail.com", fake)
    assert campaign_id == "camp-new"
    created = next(kwargs["json"] for method, path, kwargs in fake.calls if (method, path) == ("POST", "/api/v2/campaigns"))
    assert created["name"] == "NDL Cora Passthrough"
    schedule = created["campaign_schedule"]["schedules"][0]
    assert schedule["timing"] == {"from": "09:00", "to": "20:00"}
    assert schedule["timezone"] == "America/Detroit"
    assert created["sequences"][0]["steps"][0]["variants"][0] == {"subject": "{{ra_subject}}", "body": "{{ra_body}}"}
    assert created["email_list"] == ["josh@ndl-mail.com"]




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


def test_setup_campaign_activates_an_existing_campaign() -> None:
    fake = FakeInstantly({("GET", "/api/v2/campaigns"): (200, {"items": [{"id": "camp-1", "name": "NDL Cora Passthrough"}]}),
                          ("POST", "/api/v2/campaigns/camp-1/activate"): (200, {})})
    assert instantly_email.setup_campaign("ndl-key", "https://api.instantly.ai", "josh@ndl-mail.com", fake) == "camp-1"
    assert [(m, p) for m, p, _ in fake.calls] == [("GET", "/api/v2/campaigns"), ("POST", "/api/v2/campaigns/camp-1/activate")]



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
    instantly_email.setup_campaign("ndl-key", "https://api.instantly.ai", "josh@ndl-mail.com", fake)
    created = next(kwargs["json"] for method, path, kwargs in fake.calls if (method, path) == ("POST", "/api/v2/campaigns"))
    assert created["insert_unsubscribe_header"] is True


def test_a_footer_naming_backflip_stops_the_send() -> None:
    account = NdlInstantly(**{**ACCOUNT.__dict__, "footer_brand": "Backflip Lending"}) if hasattr(ACCOUNT, "__dict__") \
        else ACCOUNT._replace(footer_brand="Backflip Lending")
    fake = FakeInstantly(_routes())
    with pytest.raises(InstantlySendError, match="Backflip"):
        NdlInstantlySender(account, fake).send("m@example.com", "S", "clean body")
    assert fake.calls == []


def test_a_footer_block_is_a_lane_block_the_relay_stores_as_blocked_and_the_digest_finds() -> None:
    from packages.agent_core.lane_guard import LaneViolation

    account = NdlInstantly(**{**ACCOUNT.__dict__, "footer_brand": "Backflip Lending"})
    fake = FakeInstantly(_routes())
    with pytest.raises(instantly_email.InstantlyLaneBlock) as raised:
        NdlInstantlySender(account, fake).send("m@example.com", "S", "clean body")
    assert isinstance(raised.value, InstantlySendError) and isinstance(raised.value, LaneViolation)
    assert fake.calls == []  # no request went to Instantly


# ── R1 0.4: the Instantly channel breaker ─────────────────────────────────────

@pytest.fixture
def breaker(monkeypatch):
    from src.lending.dispatch import circuit_breaker

    seen = SimpleNamespace(admitted=[], failures=[], successes=[], refuse=None)

    def admit(channel):
        if seen.refuse is not None:
            raise seen.refuse(channel, "test")
        seen.admitted.append(channel)

    monkeypatch.setattr(circuit_breaker, "admit", admit)
    monkeypatch.setattr(circuit_breaker, "record_failure", lambda channel, error, *, ambiguous=False:
                        seen.failures.append((channel, error, ambiguous)) or False)
    monkeypatch.setattr(circuit_breaker, "record_success", lambda channel: seen.successes.append(channel))
    return seen


def test_a_good_cold_email_asks_the_breaker_and_resets_the_run(breaker) -> None:
    NdlInstantlySender(ACCOUNT, FakeInstantly(_routes())).send("mark@example.com", "Hi", "Body")
    assert breaker.admitted == ["instantly"] and breaker.successes == ["instantly"] and breaker.failures == []


def test_a_paused_instantly_channel_refuses_before_any_request(breaker) -> None:
    from src.lending.dispatch import circuit_breaker

    breaker.refuse = circuit_breaker.RateLimited
    fake = FakeInstantly(_routes())
    with pytest.raises(instantly_email.InstantlySendPaused) as err:
        NdlInstantlySender(ACCOUNT, fake).send("mark@example.com", "Hi", "Body")
    assert err.value.rate_limited is True and err.value.ambiguous is False and fake.calls == []


def test_a_5xx_on_the_send_counts_and_is_ambiguous(breaker) -> None:
    routes = _routes()
    routes[("POST", "/api/v2/leads/add")] = (503, {})
    with pytest.raises(InstantlySendError) as err:
        NdlInstantlySender(ACCOUNT, FakeInstantly(routes)).send("mark@example.com", "Hi", "Body")
    assert err.value.ambiguous is True and breaker.failures == [("instantly", "HTTP 503", True)]


def test_a_failed_lookup_counts_but_nothing_had_been_sent(breaker) -> None:
    routes = _routes()
    routes[("POST", "/api/v2/leads/list")] = (500, {})
    with pytest.raises(InstantlySendError) as err:
        NdlInstantlySender(ACCOUNT, FakeInstantly(routes)).send("mark@example.com", "Hi", "Body")
    assert err.value.ambiguous is False and breaker.failures == [("instantly", "HTTP 500", False)]


def test_a_recipient_the_provider_declines_is_not_a_channel_failure(breaker) -> None:
    routes = _routes(added={"in_blocklist": True})
    with pytest.raises(InstantlySendError):
        NdlInstantlySender(ACCOUNT, FakeInstantly(routes)).send("mark@example.com", "Hi", "Body")
    assert breaker.failures == [] and breaker.successes == []


def test_pausing_the_campaign_does_not_count_toward_the_send_breaker(breaker) -> None:
    routes = {("POST", f"/api/v2/campaigns/{ACCOUNT.campaign_id}/pause"): (500, {})}
    NdlInstantlySender(ACCOUNT, FakeInstantly(routes)).pause_campaign()
    assert breaker.failures == []


def test_an_instantly_reply_is_counted_and_a_5xx_is_ambiguous(breaker, monkeypatch) -> None:
    from src.lending import instantly_client

    account = instantly_client.InstantlyAccount("k", "https://api.instantly.ai")
    monkeypatch.setattr("src.lending.global_halt.is_globally_halted", lambda: False)
    monkeypatch.setattr(instantly_client.requests, "post", lambda *a, **k: FakeResponse(502, {}))
    with pytest.raises(instantly_client.InstantlyError) as err:
        instantly_client.send_reply(account, eaccount="a@x.test", reply_to_uuid="u", subject="s", body="b")
    assert err.value.ambiguous is True and breaker.failures == [("instantly", "InstantlyError", True)]
    monkeypatch.setattr(instantly_client.requests, "post", lambda *a, **k: FakeResponse(200, {"id": "e1"}))
    assert instantly_client.send_reply(account, eaccount="a@x.test", reply_to_uuid="u", subject="s", body="b") == "e1"
    assert breaker.successes == ["instantly"]
