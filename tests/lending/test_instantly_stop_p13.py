"""P1.3 gaps: an email-only STOP from a cold contact held only in GHL and the dialer reaches both, and a STOP is
decided in the webhook, before Cora's sweep or the model."""
from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from config.lending_compliance import OptOutChannel
from src.lending import compliance, ghl_email_lookup, instantly_replies
from src.services import email_suppression
from tests.lending.test_email_opt_out_links_phone import FakeDb, _Rows

EMAIL = "cold@example.com"
PHONE = "+18135550177"


@pytest.mark.parametrize("reply", [
    "Please remove my name from this", "this is spam", "leave us alone", "please stop this",
    "we no longer want to receive these", "never email me again", "STOP", "Please delete my email address",
])
def test_wording_that_could_mean_stop_is_a_stop_in_the_webhook(reply):
    assert instantly_replies.could_mean_stop(reply)


@pytest.mark.parametrize("reply", ["Interested, can we talk Tuesday?", "What rates do you offer on a bridge loan?",
                                   "Already have a lender, thanks", "We no longer have that property, got another deal",
                                   "Please stop by Tuesday", "I can remove the prepayment penalty",
                                   "Out of office: no longer with the company"])
def test_ordinary_replies_are_not_a_stop(reply):
    assert not instantly_replies.could_mean_stop(reply)


def test_a_quoted_earlier_email_does_not_make_a_reply_a_stop():
    reply = "Sounds good, call me.\n\nOn Mon, Nov 9 Next Deal Lending wrote:\n> reply STOP to unsubscribe"
    assert not instantly_replies.could_mean_stop(reply)


def test_record_event_marks_a_possible_stop_as_suppressed():
    seen = {}

    class Db:
        def execute(self, stmt, params):
            seen.update(params)
            return SimpleNamespace(scalar=lambda: 5)

    event = instantly_replies.InstantlyEvent(
        dedup_key="k", event_type=instantly_replies.EVENT_REPLY, lead_email=EMAIL,
        reply_text="please remove me from your list of contacts")
    assert instantly_replies.record_event(Db(), event).stop_request is True
    assert seen["stop"] is True and seen["status"] == "suppressed"


class _LookupDb:
    def __init__(self, loads):
        self.loads, self.params = loads, []

    def execute(self, stmt, params=None):
        self.params.append(params)
        return _Rows(self.loads)


def _ghl(monkeypatch, contacts):
    monkeypatch.setattr(ghl_email_lookup, "lending_ghl_account",
                        lambda: SimpleNamespace(api_key="k", location_id="loc"))
    response = SimpleNamespace(raise_for_status=lambda: None, json=lambda: {"contacts": contacts})
    monkeypatch.setattr(ghl_email_lookup.requests, "request", lambda *a, **k: response)


def test_ghl_contact_and_its_dialer_loads_give_the_phones(monkeypatch):
    _ghl(monkeypatch, [{"id": "g1", "email": "Cold@Example.com", "phone": "(813) 555-0177"},
                       {"id": "g2", "email": "someone@else.com", "phone": "+18135550999"}])
    db = _LookupDb([("g1", "+18135550188")])
    assert ghl_email_lookup.phones_for_emails(db, [EMAIL]) == {EMAIL: {PHONE, "+18135550188"}}
    assert db.params == [{"ids": ["g1"]}]


def test_ghl_failure_costs_only_the_phone_link(monkeypatch):
    monkeypatch.setattr(ghl_email_lookup, "lending_ghl_account",
                        lambda: SimpleNamespace(api_key="k", location_id="loc"))

    def boom(*a, **k):
        raise ConnectionError("no route")

    monkeypatch.setattr(ghl_email_lookup.requests, "request", boom)
    assert ghl_email_lookup.phones_for_emails(_LookupDb([]), [EMAIL]) == {}


def test_no_ghl_account_means_no_lookup(monkeypatch):
    monkeypatch.setattr(ghl_email_lookup, "lending_ghl_account", lambda: None)
    assert ghl_email_lookup.phones_for_emails(_LookupDb([]), [EMAIL]) == {}


def test_cold_contact_only_in_ghl_and_the_dialer_is_suppressed_and_removed(monkeypatch):
    writes = []
    monkeypatch.setattr(email_suppression, "suppress_contact",
                        lambda db, email=None, phone=None, source="manual": writes.append((email, phone)))
    _ghl(monkeypatch, [{"id": "g1", "email": EMAIL, "phone": "+18135550177"}])
    db = FakeDb([])  # on no lending table
    removed = []
    opt_out = compliance._OptOut(OptOutChannel.EMAIL, None, EMAIL, "instantly:k", "instantly",
                                 datetime.now(timezone.utc))
    monkeypatch.setattr(db, "execute", _with_loads(db, [("g1", "+18135550188")]))
    compliance._propagate(db, [opt_out], lambda phone, reason: removed.append(phone))

    assert sorted(removed) == [PHONE, "+18135550188"]
    (event,) = db.params_for("INSERT INTO lending.opt_out_events")
    assert event["hashes"] == [compliance.phone_hash(PHONE), compliance.phone_hash("+18135550188")]
    (contacts,) = db.params_for("INSERT INTO lending.contacts")
    assert set(contacts["phones"]) == {PHONE, "+18135550188"}
    assert {p for _, p in writes} == {PHONE, "+18135550188"}


def test_traced_cold_list_email_is_searched_for_phones():
    assert "lending.calling_pool_staging" in str(compliance._LINKED_PHONES)


def _with_loads(db, loads):
    original = db.execute

    def execute(stmt, params=None):
        if "dialer_load_records" in str(stmt):
            db.calls.append((str(stmt), params))
            return _Rows(loads)
        return original(stmt, params)

    return execute


def test_failed_ghl_lookup_is_raised_to_the_inbox(monkeypatch):
    from src.lending import compliance, ghl_email_lookup

    raised: list[dict] = []
    monkeypatch.setattr(ghl_email_lookup, "phones_for_emails", lambda db, emails, failed=None: (failed.extend(emails), {})[1])
    monkeypatch.setattr("src.lending.cora_inbox.add_to_inbox_safely", lambda db, **event: raised.append(event))
    compliance._alert_unlinked_stop(None, [EMAIL])
    assert raised[0]["event_type"] == "stop_phone_link_failed"
    assert EMAIL not in str(raised[0])
