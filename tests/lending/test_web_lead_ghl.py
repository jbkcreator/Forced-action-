"""WP-GL-11: delivery of a website lead into the Next Deal Lending GHL sub-account (HTTP mocked)."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from config.settings import get_settings
from src.lending.ghl_account import GhlAccount
from src.lending.web_lead_ghl import GhlLeadSink
from src.lending.web_leads import DeliveryError

ACCOUNT = GhlAccount(api_key="test-key", location_id="loc_1")


class FakeResponse:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}

    def json(self):
        return self._payload


class FakeGhl:
    """Records every request and answers from a small route table."""

    def __init__(self, monkeypatch, routes):
        self.calls = []
        self.routes = routes
        monkeypatch.setattr("src.services.ghl_webhook._ghl_request", self._request)

    def _request(self, method, url, **kwargs):
        self.calls.append((method, url.split("leadconnectorhq.com")[-1], kwargs))
        handler = self.routes.get((method, self.calls[-1][1].split("?")[0]))
        if handler is None:
            for (m, prefix), value in self.routes.items():
                if m == method and self.calls[-1][1].startswith(prefix):
                    handler = value
        if isinstance(handler, Exception):
            raise handler
        return handler or FakeResponse(404)

    def paths(self):
        return [(m, p) for m, p, _ in self.calls]

    def body(self, method, path):
        return next(k for m, p, k in self.calls if (m, p) == (method, path))


def _lead(**overrides):
    lead = {"id": 7, "name": "Dana Builder", "phone": "+18135550142", "email": "dana@example.com",
            "property_city": "Tampa", "deal_type": "Fix and flip", "completed_projects_3y": "1 to 2",
            "sms_consent": False, "deal_drop_optin": False, "suppressed": False,
            "received_at": datetime(2026, 10, 5, 15, 30, tzinfo=timezone.utc), "ghl_attempts": 0}
    lead.update(overrides)
    return lead


@pytest.fixture(autouse=True)
def _unconfigured(monkeypatch):
    settings = get_settings()
    for field in ("lending_ghl_pipeline_id", "lending_ghl_stage_new_lead",
                  "lending_ghl_cf_sms_consent", "lending_ghl_cf_deal_drop_optin"):
        monkeypatch.setattr(settings, field, None, raising=False)


def _configure_pipeline(monkeypatch):
    settings = get_settings()
    monkeypatch.setattr(settings, "lending_ghl_pipeline_id", "pipe_1", raising=False)
    monkeypatch.setattr(settings, "lending_ghl_stage_new_lead", "stage_new", raising=False)


def _routes(opportunities=None, note=None, is_new=True, tags=None):
    return {
        ("POST", "/contacts/upsert"): FakeResponse(200, {"new": is_new, "contact": {"id": "c_1"}}),
        ("POST", "/contacts/c_1/tags"): tags or FakeResponse(200, {}),
        ("DELETE", "/contacts/c_1/tags"): FakeResponse(200, {}),
        ("GET", "/opportunities/search"): FakeResponse(200, {"opportunities": opportunities or []}),
        ("POST", "/opportunities/"): FakeResponse(201, {"opportunity": {"id": "o_1"}}),
        ("POST", "/contacts/c_1/notes"): note or FakeResponse(201, {}),
    }


def test_contact_is_upserted_and_never_carries_dnd_tags_or_source(monkeypatch):
    ghl = FakeGhl(monkeypatch, _routes())
    result = GhlLeadSink(ACCOUNT).push(_lead())
    body = ghl.body("POST", "/contacts/upsert")["json"]
    assert "dnd" not in body and "dndSettings" not in body  # an earlier STOP must stay in force
    # GHL's upsert REPLACES tags and source (verified live): sending them would wipe an earlier
    # consent tag, the opt-out tag or the dialer's tags, and the contact's original source.
    assert "tags" not in body and "source" not in body
    assert body["locationId"] == "loc_1" and body["phone"] == "+18135550142" and body["email"] == "dana@example.com"
    assert (body["firstName"], body["lastName"]) == ("Dana", "Builder")
    assert result.contact_id == "c_1"


@pytest.mark.parametrize("lead,is_new,expected", [
    ({}, True, ["web-lead", "web-sms-consent-no"]),
    ({"sms_consent": True}, True, ["web-lead", "web-sms-consent-yes"]),
    ({"sms_consent": True, "deal_drop_optin": True}, True, ["web-lead", "web-sms-consent-yes", "web-deal-drop-optin"]),
    ({"sms_consent": True, "suppressed": True, "deal_drop_optin": True}, True, ["web-lead", "web-sms-consent-no"]),
    ({"sms_consent": True}, False, ["web-lead", "web-sms-consent-yes"]),
    # an existing contact who leaves the boxes unticked must never gain a "no" tag next to an earlier "yes"
    ({}, False, ["web-lead"]),
    ({"suppressed": True, "sms_consent": True}, False, ["web-lead"]),
])
def test_consent_values_are_added_as_tags_never_replaced(monkeypatch, lead, is_new, expected):
    ghl = FakeGhl(monkeypatch, _routes(is_new=is_new))
    GhlLeadSink(ACCOUNT).push(_lead(**lead))
    assert ghl.body("POST", "/contacts/c_1/tags")["json"] == {"tags": expected}


def test_giving_consent_removes_only_our_own_stale_no_tag(monkeypatch):
    ghl = FakeGhl(monkeypatch, _routes(is_new=False))
    GhlLeadSink(ACCOUNT).push(_lead(sms_consent=True))
    assert ghl.body("DELETE", "/contacts/c_1/tags")["json"] == {"tags": ["web-sms-consent-no"]}


def test_no_tag_is_removed_when_consent_was_not_given(monkeypatch):
    ghl = FakeGhl(monkeypatch, _routes())
    GhlLeadSink(ACCOUNT).push(_lead())
    GhlLeadSink(ACCOUNT).push(_lead(sms_consent=True, suppressed=True))
    assert not any(m == "DELETE" for m, _ in ghl.paths())


def test_a_tags_failure_fails_the_delivery_so_it_is_retried(monkeypatch):
    FakeGhl(monkeypatch, _routes(tags=FakeResponse(500)))
    with pytest.raises(DeliveryError, match="add tags: HTTP 500"):
        GhlLeadSink(ACCOUNT).push(_lead())


def test_custom_fields_are_sent_only_when_their_ids_are_configured_and_only_for_yes(monkeypatch):
    ghl = FakeGhl(monkeypatch, _routes())
    GhlLeadSink(ACCOUNT).push(_lead(sms_consent=True))
    assert "customFields" not in ghl.body("POST", "/contacts/upsert")["json"]

    settings = get_settings()
    monkeypatch.setattr(settings, "lending_ghl_cf_sms_consent", "cf_sms", raising=False)
    monkeypatch.setattr(settings, "lending_ghl_cf_deal_drop_optin", "cf_drop", raising=False)
    ghl = FakeGhl(monkeypatch, _routes())
    GhlLeadSink(ACCOUNT).push(_lead(sms_consent=True))
    assert ghl.body("POST", "/contacts/upsert")["json"]["customFields"] == [{"id": "cf_sms", "value": "yes"}]

    ghl = FakeGhl(monkeypatch, _routes())
    GhlLeadSink(ACCOUNT).push(_lead())  # unticked: never overwrite an earlier "yes" with "no"
    assert "customFields" not in ghl.body("POST", "/contacts/upsert")["json"]


def test_without_a_new_lead_stage_the_lead_is_contact_only(monkeypatch):
    ghl = FakeGhl(monkeypatch, _routes())
    result = GhlLeadSink(ACCOUNT).push(_lead())
    assert result.pipeline_card is False
    assert [p for _, p in ghl.paths()] == ["/contacts/upsert", "/contacts/c_1/tags", "/contacts/c_1/notes"]


def test_a_pipeline_card_is_created_when_the_contact_has_none(monkeypatch):
    _configure_pipeline(monkeypatch)
    ghl = FakeGhl(monkeypatch, _routes())
    result = GhlLeadSink(ACCOUNT).push(_lead())
    assert result.pipeline_card is True
    search = next(k for m, p, k in ghl.calls if p == "/opportunities/search")
    assert search["params"]["pipeline_id"] == "pipe_1"  # scoped to Booked Calls, not any pipeline
    created = ghl.body("POST", "/opportunities/")["json"]
    assert created["pipelineStageId"] == "stage_new" and created["contactId"] == "c_1" and created["status"] == "open"


def test_an_existing_card_is_never_moved_or_duplicated(monkeypatch):
    _configure_pipeline(monkeypatch)
    ghl = FakeGhl(monkeypatch, _routes(opportunities=[{"id": "o_9"}]))
    result = GhlLeadSink(ACCOUNT).push(_lead())
    assert result.pipeline_card is True
    assert not any(m in ("POST", "PUT") and p.startswith("/opportunities") for m, p in ghl.paths())


def test_the_note_records_the_ticked_values_and_the_utc_time(monkeypatch):
    ghl = FakeGhl(monkeypatch, _routes())
    GhlLeadSink(ACCOUNT).push(_lead(sms_consent=True, deal_drop_optin=True))
    note = ghl.body("POST", "/contacts/c_1/notes")["json"]["body"]
    assert "2026-10-05 15:30:00 UTC" in note and "consent box: ticked" in note and "Deal Drop opt-in box: ticked" in note
    assert "dana@example.com" not in note and "+18135550142" not in note


def test_a_failed_note_does_not_fail_the_delivery(monkeypatch):
    FakeGhl(monkeypatch, _routes(note=FakeResponse(500)))
    assert GhlLeadSink(ACCOUNT).push(_lead()).contact_id == "c_1"


@pytest.mark.parametrize("failing,reason", [
    (("POST", "/contacts/upsert"), "contact upsert: HTTP 500"),
])
def test_contact_failure_raises_a_pii_free_delivery_error(monkeypatch, failing, reason):
    routes = _routes()
    routes[failing] = FakeResponse(500, {"message": "leaks +18135550142"})
    FakeGhl(monkeypatch, routes)
    with pytest.raises(DeliveryError) as err:
        GhlLeadSink(ACCOUNT).push(_lead())
    assert str(err.value) == reason


def test_a_network_error_becomes_a_delivery_error_without_the_message(monkeypatch):
    routes = _routes()
    routes[("POST", "/contacts/upsert")] = ConnectionError("dana@example.com unreachable")
    FakeGhl(monkeypatch, routes)
    with pytest.raises(DeliveryError) as err:
        GhlLeadSink(ACCOUNT).push(_lead())
    assert str(err.value) == "contact upsert: ConnectionError"


def test_an_opportunity_failure_fails_the_delivery_so_it_is_retried(monkeypatch):
    _configure_pipeline(monkeypatch)
    routes = _routes()
    routes[("POST", "/opportunities/")] = FakeResponse(422)
    FakeGhl(monkeypatch, routes)
    with pytest.raises(DeliveryError, match="opportunity create: HTTP 422"):
        GhlLeadSink(ACCOUNT).push(_lead())


def test_no_live_sink_without_the_lending_account(monkeypatch):
    from src.lending import web_lead_ghl

    monkeypatch.undo()  # the suite-wide guard replaces get_live_sink; test the real one
    monkeypatch.setattr("src.lending.web_lead_ghl.lending_ghl_account", lambda: None)
    assert web_lead_ghl.get_live_sink() is None
