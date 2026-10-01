"""Dialer-neutral interface: the rules code never names a vendor (Go Live G1)."""
from __future__ import annotations

import pytest

from config.lending_compliance import RemovalReason
from src.lending.dialer_port import BatchDialerAdapter, InMemoryDialer, UnconfirmedCapability
from src.lending.dialer_removal import DialerRemovalUndecided

PHONE = "+18135558201"


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
    "dnc_add": ("POST", "/dnc"),
    "contact_get": ("GET", "/contact/{id}"),
}


def test_opt_out_goes_to_the_permanent_dnc_list():
    http = FakeHttp()
    BatchDialerAdapter(http=http, endpoints=ENDPOINTS).remove(PHONE, reason=RemovalReason.OPT_OUT.value)
    assert http.calls == [("POST", "/dnc", {"phone": PHONE})]


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
    add, read, card = http.calls[-3], http.calls[-2], http.calls[-1]
    assert read[:2] == ("GET", "/contact/55")
    assert add[:2] == ("POST", "/contacts")
    assert add[2]["campaignids"] == [7]
    contact = add[2]["contacts"][0]
    assert (contact["firstname"], contact["lastname"], contact["phonenumber1"]) == ("Jane", "Roe", PHONE)
    assert contact["vendorcontactid"] == "staging:9" and contact["email"] == "j@example.com"
    assert card[:2] == ("PUT", "/contact/55")
    assert card[2]["customfields"]["entity_name"] == "Roe LLC" and card[2]["phonenumbers"] == [{"phonenumber": PHONE}]


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


def test_update_is_a_full_put_that_keeps_the_phone():
    http = FakeHttp(body={})
    BatchDialerAdapter(http=http, endpoints=ENDPOINTS).update_contact("55", FIELDS, phone=PHONE)
    method, path, body = http.calls[-1]
    assert (method, path) == ("PUT", "/contact/55") and body["phonenumbers"] == [{"phonenumber": PHONE}]


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


def test_pool_campaign_tags_name_the_three_launch_queues():
    from config import lending_queues as q
    from config.lending_dialer import POOL_CAMPAIGN_TAGS
    assert POOL_CAMPAIGN_TAGS == {q.VERIFIED_MATURITY: "Verified maturity",
                                  q.TRANSACTION_READY: "Transaction ready", q.BUILDERS: "Builders",
                                  q.NURTURE: "Nurture"}


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


# ── Contact read + custom-field protection (client Q27: text_consent) ──


class ContactHttp:
    """GET /contact/7 returns stored custom fields; everything else is recorded."""

    def __init__(self, customfields=None, get_error=None):
        self.calls, self.customfields, self.get_error = [], customfields or {}, get_error

    def __call__(self, method, path, *, json=None):
        self.calls.append((method, path, json))
        if method == "GET":
            if self.get_error:
                raise self.get_error
            return {"customfields": self.customfields}
        return {}


READ_ENDPOINTS = ENDPOINTS


def test_get_contact_customfields_reads_without_a_body():
    http = ContactHttp({"text_consent": "yes"})
    assert BatchDialerAdapter(http=http, endpoints=READ_ENDPOINTS).get_contact_customfields(7) == {"text_consent": "yes"}
    assert http.calls == [("GET", "/contact/7", None)]


def test_contact_get_is_a_confirmed_read_endpoint():
    from config.lending_dialer import BATCHDIALER_ENDPOINTS
    assert BATCHDIALER_ENDPOINTS["contact_get"] == ("GET", "/contact/{id}")


def test_update_keeps_text_consent_and_caller_values_win():
    http = ContactHttp({"text_consent": "yes", "queue": "Builders"})
    adapter = BatchDialerAdapter(http=http, endpoints=READ_ENDPOINTS)
    adapter.update_contact(7, DialerContactFields(information="x"))
    assert http.calls[-1][2]["customfields"]["text_consent"] == "yes"
    adapter.update_contact(7, DialerContactFields(information="x", customfields={"queue": "Nurture"}))
    sent = http.calls[-1][2]["customfields"]
    assert sent["queue"] == "Nurture" and sent["text_consent"] == "yes"


def test_update_raises_instead_of_wiping_when_the_read_fails():
    http = ContactHttp(get_error=DialerRequestError("down"))
    with pytest.raises(DialerRequestError):
        BatchDialerAdapter(http=http, endpoints=READ_ENDPOINTS).update_contact(7, DialerContactFields())
    assert all(c[0] == "GET" for c in http.calls)


def test_in_memory_dialer_returns_empty_customfields():
    assert InMemoryDialer().get_contact_customfields(7) == {}
