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
    "contact_upsert": ("POST", "/contacts"),
    "campaign_remove": ("POST", "/campaign/remove"),
    "campaign_restore": ("POST", "/campaign/add"),
    "dnc_add": ("POST", "/dnc"),
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


def test_loader_upsert_adds_the_contact_to_the_named_campaign():
    http = CampaignHttp([{"id": 7, "name": "Builders"}], body={"id": 55})
    result = BatchDialerAdapter(http=http, endpoints=ENDPOINTS).upsert_contact(PHONE, FIELDS, campaign="Builders")
    assert result.contact_id == 55 and result.created is True
    method, path, body = http.calls[-1]
    assert (method, path) == ("POST", "/contacts") and body["campaignId"] == 7 and body["phone"] == PHONE


def test_campaign_ids_are_looked_up_once():
    http = CampaignHttp([{"id": 7, "name": "Builders"}], body={"id": 1})
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
                                  q.TRANSACTION_READY: "Transaction ready", q.BUILDERS: "Builders"}
