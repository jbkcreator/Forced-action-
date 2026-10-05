"""Dialer-neutral interface: the rules code never names a vendor (Go Live G1)."""
from __future__ import annotations

import pytest

from config.lending_compliance import RemovalReason
from src.lending.dialer_port import BatchDialerAdapter, InMemoryDialer, UnconfirmedCapability
from src.lending.dialer_removal import DialerRemovalUndecided

PHONE = "+18135558201"
PHONE_10 = "8135558201"  # what BatchDialer accepts on the wire


class FakeHttp:
    def __init__(self, body=None):
        self.calls, self.body = [], body or {}

    def __call__(self, method, path, *, json=None):
        self.calls.append((method, path, json))
        return self.body


ENDPOINTS = {
    "contact_upsert": ("POST", "/contact"),
    "contact_update": ("PUT", "/contact/{id}"),
    "contacts_add_to_campaign": ("POST", "/contacts"),
    "campaign_remove": ("POST", "/campaign/remove"),
    "campaign_restore": ("POST", "/campaign/add"),
    "dnc_add": None,
    "contact_delete": ("DELETE", "/contact/{id}"),
}


def test_an_opt_out_deletes_every_dialer_contact_loaded_for_that_phone():
    http = FakeHttp()
    adapter = BatchDialerAdapter(http=http, endpoints=ENDPOINTS, contact_ids=lambda phone: ["77", "78"])
    adapter.remove(PHONE, reason=RemovalReason.OPT_OUT.value)
    assert [(c[0], c[1]) for c in http.calls] == [("DELETE", "/contact/77"), ("DELETE", "/contact/78")]


def test_an_opt_out_for_a_number_never_loaded_calls_nothing():
    http = FakeHttp()
    BatchDialerAdapter(http=http, endpoints=ENDPOINTS, contact_ids=lambda phone: []).remove(
        PHONE, reason=RemovalReason.OPT_OUT.value)
    assert http.calls == []


@pytest.mark.parametrize("reason", [RemovalReason.CALL_WINDOW.value, RemovalReason.ATTEMPT_CAP.value])
def test_temporary_holds_leave_the_campaign_and_return_on_restore(reason):
    http = FakeHttp()
    adapter = BatchDialerAdapter(http=http, endpoints=ENDPOINTS)
    adapter.remove(PHONE, reason=reason)
    adapter.restore(PHONE)
    assert [c[1] for c in http.calls] == ["/campaign/remove", "/campaign/add"]


def test_unconfirmed_capability_raises_the_undecided_error_the_rules_code_already_handles():
    adapter = BatchDialerAdapter(http=FakeHttp(), endpoints={**ENDPOINTS, "campaign_remove": None})
    with pytest.raises(DialerRemovalUndecided):
        adapter.remove(PHONE, reason=RemovalReason.ATTEMPT_CAP.value)
    assert issubclass(UnconfirmedCapability, DialerRemovalUndecided)


def test_upsert_returns_the_dialer_contact_id():
    http = FakeHttp(body={"id": 42})
    contact_id = BatchDialerAdapter(http=http, endpoints=ENDPOINTS).upsert_contact({"phone": PHONE, "name": "A"})
    assert contact_id == "42"


def test_in_memory_dialer_records_the_same_calls():
    d = InMemoryDialer()
    d.upsert_contact({"phone": PHONE})
    d.remove(PHONE, reason="opt_out")
    d.restore(PHONE)
    assert d.events == [("upsert", PHONE, None), ("remove", PHONE, "opt_out"), ("restore", PHONE, None)]


def test_rules_code_defaults_use_the_configured_dialer(monkeypatch):
    from src.lending import compliance, dialer_port

    dialer = InMemoryDialer()
    monkeypatch.setattr(dialer_port, "get_dialer", lambda: dialer)
    compliance._default_dialer_remover()(PHONE, reason="attempt_cap")
    compliance._default_dialer_restorer()(PHONE)
    assert dialer.events == [("remove", PHONE, "attempt_cap"), ("restore", PHONE, None)]


# ── Loader surface (the dialer load pushes contacts through the same adapter) ──

from src.lending.dialer_port import DialerRequestError
from src.lending.dialer_port import DialerContactFields

FIELDS = DialerContactFields(first_name="Jane", last_name="Roe", company_name="Roe LLC",
                              information="Property: 1 St", email="j@example.com")


class CampaignHttp(FakeHttp):
    def __init__(self, campaigns, body=None):
        super().__init__(body=body)
        self.campaigns = campaigns

    def __call__(self, method, path, *, json=None):
        if (method, path) == ("GET", "/campaigns"):
            self.calls.append((method, path, None))
            return self.campaigns
        return super().__call__(method, path, json=json)


def test_loader_upsert_adds_the_contact_into_the_campaign_then_sets_the_card_fields():
    http = CampaignHttp([{"id": 7, "name": "Builders"}], body={"ids": [55], "success": True})
    result = BatchDialerAdapter(http=http, endpoints=ENDPOINTS).upsert_contact(
        PHONE, FIELDS, campaign="Builders", vendor_contact_id="staging:9")
    assert result.contact_id == 55 and result.created is True
    add, card = http.calls[-2], http.calls[-1]
    assert add[:2] == ("POST", "/contacts")
    assert add[2]["campaignids"] == [7]
    contact = add[2]["contacts"][0]
    assert (contact["firstname"], contact["lastname"], contact["phonenumber1"]) == ("Jane", "Roe", PHONE_10)
    assert contact["vendorcontactid"] == "staging:9" and contact["email"] == "j@example.com"
    assert card[:2] == ("PUT", "/contact/55")
    assert card[2]["customfields"]["entity_name"] == "Roe LLC" and card[2]["phonenumbers"] == [{"phonenumber": PHONE_10}]


def test_a_failed_campaign_import_is_a_request_error():
    http = CampaignHttp([{"id": 7, "name": "Builders"}], body={"success": False, "msg": "bad phone"})
    with pytest.raises(DialerRequestError):
        BatchDialerAdapter(http=http, endpoints=ENDPOINTS).upsert_contact(PHONE, FIELDS, campaign="Builders")


def test_an_unconfirmed_campaign_step_refuses_before_any_contact_is_created():
    http = CampaignHttp([{"id": 7, "name": "Builders"}], body={"ids": [55], "success": True})
    adapter = BatchDialerAdapter(http=http, endpoints={**ENDPOINTS, "contacts_add_to_campaign": None})
    with pytest.raises(UnconfirmedCapability):
        adapter.upsert_contact(PHONE, FIELDS, campaign="Builders")
    assert not [c for c in http.calls if c[0] != "GET"]


class _PutFailsHttp(CampaignHttp):
    """POST (add to campaign) succeeds; the follow-up PUT (card fields) always fails."""

    def __call__(self, method, path, *, json=None):
        if method == "PUT":
            self.calls.append((method, path, json))
            raise DialerRequestError("BatchDialer PUT /contact/55 HTTP 500", status=500)
        return super().__call__(method, path, json=json)


def test_a_failed_card_field_update_still_returns_the_created_contact_id(caplog):
    """Finding #2: the contact is already live in BatchDialer's campaign once the POST
    succeeds, so a failing field-update PUT must not make upsert_contact raise — that
    would make the loader drop the record and leave a dialable contact with no
    lending.dialer_load_records row to track or retry it."""
    http = _PutFailsHttp([{"id": 7, "name": "Builders"}], body={"ids": [55], "success": True})
    with caplog.at_level("WARNING"):
        result = BatchDialerAdapter(http=http, endpoints=ENDPOINTS).upsert_contact(
            PHONE, FIELDS, campaign="Builders", vendor_contact_id="staging:9")
    assert result.contact_id == 55 and result.created is True
    assert any("field update failed" in r.getMessage() for r in caplog.records)


def test_update_is_a_full_put_that_keeps_the_phone():
    http = FakeHttp(body={})
    BatchDialerAdapter(http=http, endpoints=ENDPOINTS).update_contact("55", FIELDS, phone=PHONE)
    method, path, body = http.calls[-1]
    assert (method, path) == ("PUT", "/contact/55") and body["phonenumbers"] == [{"phonenumber": PHONE_10}]


def test_missing_for_load_names_every_unconfirmed_load_endpoint():
    adapter = BatchDialerAdapter(http=FakeHttp(), endpoints={**ENDPOINTS, "contacts_add_to_campaign": None})
    assert adapter.missing_for_load() == ["contacts_add_to_campaign"]
    assert BatchDialerAdapter(http=FakeHttp(), endpoints=ENDPOINTS).missing_for_load() == []


def test_confirmed_contact_endpoints_are_set_in_config():
    from config.lending_dialer import BATCHDIALER_ENDPOINTS
    assert BATCHDIALER_ENDPOINTS["contact_upsert"] == ("POST", "/contact")
    assert BATCHDIALER_ENDPOINTS["contact_update"] == ("PUT", "/contact/{id}")
    assert BATCHDIALER_ENDPOINTS["contacts_add_to_campaign"] == ("POST", "/contacts")   # docs: "Add contacts"


def test_campaign_ids_are_looked_up_once():
    http = CampaignHttp([{"id": 7, "name": "Builders"}], body={"ids": [1], "success": True})
    adapter = BatchDialerAdapter(http=http, endpoints=ENDPOINTS)
    adapter.upsert_contact(PHONE, FIELDS, campaign="Builders")
    adapter.upsert_contact("+18135558202", FIELDS, campaign="Builders")
    assert [c for c in http.calls if c[1] == "/campaigns"] == [("GET", "/campaigns", None)]


def test_an_unknown_campaign_is_a_request_error_not_a_silent_load():
    http = CampaignHttp([{"id": 7, "name": "Builders"}])
    with pytest.raises(DialerRequestError):
        BatchDialerAdapter(http=http, endpoints=ENDPOINTS).upsert_contact(PHONE, FIELDS, campaign="Nope")


def test_record_style_upsert_still_works_for_the_rules_code():
    http = FakeHttp(body={"id": 42})
    assert BatchDialerAdapter(http=http, endpoints=ENDPOINTS).upsert_contact({"phone": PHONE}) == "42"


def test_pool_campaign_tags_name_the_four_ranked_queues_plus_nurture():
    from config import lending_queues as q
    from config.lending_dialer import POOL_CAMPAIGN_TAGS
    assert POOL_CAMPAIGN_TAGS == {q.VERIFIED_MATURITY: "Verified maturity",
                                  q.TRANSACTION_READY: "Transaction ready", q.BUILDERS: "Builders",
                                  q.PARTNERS: "Partners", q.NURTURE: "Nurture"}


# ── HTTP transport ──

class _Resp:
    def __init__(self, status, body=b"{}"):
        self.status_code, self.content = status, body
        self.ok = status < 400

    def json(self):
        return {"id": 9}

    def raise_for_status(self):
        if not self.ok:
            import requests
            raise requests.HTTPError(response=self)


def test_transport_uses_the_retry_helpers_and_logs_safe_failure_context(monkeypatch, caplog):
    import requests
    from src.lending import dialer_port

    calls = []

    def fake_post(url, **kwargs):
        calls.append(("POST", url, kwargs["headers"]["X-ApiKey"]))
        raise requests.HTTPError(response=_Resp(422))

    monkeypatch.setattr(dialer_port, "requests_post_with_retry", fake_post)
    http = dialer_port._requests_http("secret-key")
    with caplog.at_level("WARNING"), pytest.raises(DialerRequestError) as err:
        http("POST", "/dnclist", json={"phone": PHONE})
    assert err.value.status == 422 and calls[0][0] == "POST" and calls[0][1].endswith("/dnclist")
    assert "/dnclist" in caplog.text and "422" in caplog.text
    assert PHONE not in caplog.text and "secret-key" not in caplog.text


def test_transport_get_goes_through_the_get_retry_helper(monkeypatch):
    from src.lending import dialer_port

    monkeypatch.setattr(dialer_port, "requests_get_with_retry", lambda url, **kw: _Resp(200, b"[]"))
    assert dialer_port._requests_http("k")("GET", "/campaigns") == {"id": 9}


# ── DNC safety: holds never use the DNC list, restores never delete a DNC entry ──

@pytest.mark.parametrize("reason", [RemovalReason.CALL_WINDOW.value, RemovalReason.ATTEMPT_CAP.value])
def test_a_temporary_hold_never_touches_the_dnc_list(reason):
    http = FakeHttp()
    BatchDialerAdapter(http=http, endpoints=ENDPOINTS).remove(PHONE, reason=reason)
    assert [c[1] for c in http.calls] == ["/campaign/remove"]


def test_restore_never_deletes_a_dnc_entry():
    http = FakeHttp()
    endpoints = {**ENDPOINTS, "dnc_delete": ("DELETE", "/dnc")}
    BatchDialerAdapter(http=http, endpoints=endpoints).restore(PHONE)
    assert [c[1] for c in http.calls] == ["/campaign/add"]


def test_the_adapter_has_no_dnc_delete_capability_at_all():
    from config.lending_dialer import BATCHDIALER_ENDPOINTS
    assert "dnc_delete" not in BATCHDIALER_ENDPOINTS
    assert not any("dnc" in name and name != "dnc_add" for name in BATCHDIALER_ENDPOINTS)


def test_an_unconfirmed_campaign_removal_keeps_the_hold_pending_not_a_dnc_fallback():
    http = FakeHttp()
    with pytest.raises(UnconfirmedCapability):
        BatchDialerAdapter(http=http, endpoints={**ENDPOINTS, "campaign_remove": None}).remove(
            PHONE, reason=RemovalReason.CALL_WINDOW.value)
    assert http.calls == []


def test_an_opt_out_for_a_contact_already_deleted_still_completes():
    from src.lending.dialer_port import DialerRequestError

    def http(method, path, *, json=None):
        raise DialerRequestError("gone", status=404)
    BatchDialerAdapter(http=http, endpoints=ENDPOINTS, contact_ids=lambda p: ["77"]).remove(
        PHONE, reason=RemovalReason.OPT_OUT.value)


def test_the_property_address_fills_the_standard_fields_the_agent_script_can_show():
    from dataclasses import replace
    fields = replace(FIELDS, address="123 Main St", city="Tampa", state="FL", postal_code="33602")
    http = CampaignHttp([{"id": 7, "name": "Builders"}], body={"ids": [55], "success": True})
    BatchDialerAdapter(http=http, endpoints=ENDPOINTS).upsert_contact(PHONE, fields, campaign="Builders")
    add, card = http.calls[-2], http.calls[-1]
    imported = add[2]["contacts"][0]
    assert (imported["addressline1"], imported["city"], imported["state"], imported["postalcode"]) == (
        "123 Main St", "Tampa", "FL", "33602")
    assert (card[2]["address"], card[2]["city"], card[2]["state"], card[2]["postalcode"]) == (
        "123 Main St", "Tampa", "FL", "33602")


def test_phones_are_sent_as_ten_digits_because_batchdialer_rejects_e164_on_update():
    http = CampaignHttp([{"id": 7, "name": "Builders"}], body={"ids": [55], "success": True})
    BatchDialerAdapter(http=http, endpoints=ENDPOINTS).upsert_contact("+18135558201", FIELDS, campaign="Builders")
    add, card = http.calls[-2], http.calls[-1]
    assert add[2]["contacts"][0]["phonenumber1"] == "8135558201"
    assert card[2]["phonenumbers"] == [{"phonenumber": "8135558201"}]


def test_the_card_update_keeps_our_vendor_contact_id():
    # BatchDialer's PUT is a full replace: without it, the link back to our record is wiped.
    http = CampaignHttp([{"id": 7, "name": "Builders"}], body={"ids": [55], "success": True})
    BatchDialerAdapter(http=http, endpoints=ENDPOINTS).upsert_contact(
        PHONE, FIELDS, campaign="Builders", vendor_contact_id="staging:9")
    assert http.calls[-1][2]["vendorcontactid"] == "staging:9"
