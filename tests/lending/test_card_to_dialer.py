"""Context card sent to BatchDialer, vendor id kept on reload, and call transcripts."""
from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from src.lending import dialer_load
from src.lending.context_card import NO_PRIOR_CONTACT
from src.lending.dialer_contact import display_from_record
from src.lending.dialer_display import NOT_AVAILABLE
from src.lending.dialer_load import _cards, _Loadable, _push_contact
from src.lending.dialer_port import BatchDialerAdapter, DialerContactFields, DialerRequestError, _contact_body
from src.lending.lead_facts import LeadFacts
from src.lending.lead_scoring import LeadSignals, Signal

RECORD = {"source_record_ref": "staging:11", "phone": "+18135550142", "pool": "verified_maturity",
          "borrower_name": "Ann Lee", "property_address": "1 Test St, Tampa, FL 33602",
          "county": "Hillsborough", "property_id": 7}


def _item(record=RECORD, campaign="Verified maturity"):
    return _Loadable(record=record, record_ref=record["source_record_ref"], pool=record["pool"],
                     phone=record["phone"], display=display_from_record(record, campaign))


class TestCardPayload:
    def test_card_values_go_into_custom_fields_next_to_the_existing_ones(self):
        fields = DialerContactFields(first_name="Ann", information="Property: 1 Test St",
                                     card={"maturity": "Around Nov 2026", "score": "8/10"})
        custom = _contact_body("+18135550142", fields)["customfields"]
        assert custom["card_maturity"] == "Around Nov 2026"
        assert custom["card_score"] == "8/10"
        assert custom["details"] == "Property: 1 Test St"

    def test_no_card_sends_only_the_existing_custom_fields(self):
        custom = _contact_body(None, DialerContactFields())["customfields"]
        assert set(custom) == {"entity_name", "details", "email"}


class TestCardsForALoad:
    def test_one_facts_query_builds_a_card_per_record(self):
        facts = LeadFacts(7, "1 Test St", "Test Lender", None,
                          LeadSignals(maturity_date=Signal.estimated(date(2026, 11, 20)),
                                      loan_amount=Signal.known(Decimal("400000"))))
        with patch.object(dialer_load, "load_lead_facts", return_value={7: facts}) as load:
            cards = _cards(MagicMock(), [_item()])
        load.assert_called_once()
        assert load.call_args.args[1] == [7]
        card = cards["staging:11"]
        assert card["first_name"] == "Ann"
        assert card["county"] == "Hillsborough"
        assert card["campaign"] == "Verified maturity"
        assert card["lender"] == "Test Lender"
        assert card["hook"].startswith("It looks like the loan")

    def test_record_without_a_property_gets_a_card_with_inputs_missing(self):
        record = {**RECORD, "property_id": None}
        with patch.object(dialer_load, "load_lead_facts") as load:
            cards = _cards(MagicMock(), [_item(record)])
        load.assert_not_called()
        assert cards["staging:11"]["score"] == "1/10"
        assert cards["staging:11"]["property"] == "1 Test St, Tampa, FL 33602"

    def test_facts_failure_loads_without_cards(self):
        with patch.object(dialer_load, "load_lead_facts", side_effect=RuntimeError("db")):
            assert _cards(MagicMock(), [_item()]) == {}


def _history_db(rows):
    db = MagicMock()
    db.execute.return_value.mappings.return_value.all.return_value = rows
    return db


class TestPriorContact:
    def test_reloaded_phone_with_logged_calls_shows_its_history(self):
        db = _history_db([{"phone": RECORD["phone"], "calls": 2, "last_disposition": "callback_requested",
                           "last_ended": datetime(2026, 10, 2, 1, 30, tzinfo=timezone.utc)}])
        with patch.object(dialer_load, "load_lead_facts", return_value={}):
            card = _cards(db, [_item()])["staging:11"]
        assert card["prior_contact"] == "2 calls, last 2026-10-01; last outcome: callback requested"
        assert db.execute.call_args.args[1] == {"phones": [RECORD["phone"]]}

    def test_call_without_a_disposition_omits_the_outcome(self):
        db = _history_db([{"phone": RECORD["phone"], "calls": 1, "last_disposition": None,
                           "last_ended": datetime(2026, 10, 3, 15, 0, tzinfo=timezone.utc)}])
        with patch.object(dialer_load, "load_lead_facts", return_value={}):
            card = _cards(db, [_item()])["staging:11"]
        assert card["prior_contact"] == "1 call, last 2026-10-03"

    def test_phone_never_called_shows_no_prior_contact(self):
        with patch.object(dialer_load, "load_lead_facts", return_value={}):
            card = _cards(_history_db([]), [_item()])["staging:11"]
        assert card["prior_contact"] == NO_PRIOR_CONTACT

    def test_unreadable_history_is_not_available_never_no_prior_contact(self):
        db = MagicMock()
        db.execute.side_effect = RuntimeError("db")
        with patch.object(dialer_load, "load_lead_facts", return_value={}):
            card = _cards(db, [_item()])["staging:11"]
        assert card["prior_contact"] == NOT_AVAILABLE


class TestReloadKeepsTheVendorId:
    def test_update_by_known_contact_resends_the_vendor_contact_id(self):
        dialer = MagicMock()
        _push_contact(dialer, _item(), "555", DialerContactFields())
        assert dialer.update_contact.call_args.kwargs["vendor_contact_id"] == "staging:11"


class TestCallTranscript:
    def _adapter(self, http):
        return BatchDialerAdapter(http=http, endpoints={"call_transcription": ("GET", "/cdrs/{id}/transcription")})

    def test_segments_become_role_lines(self):
        http = MagicMock(return_value=[{"time": 0, "role": "agent", "text": "Hi, calling about Main St."},
                                       {"time": 4, "role": "customer", "text": "  We closed  already. "},
                                       {"time": 9, "role": "customer", "text": ""}])
        assert self._adapter(http).call_transcript(991) == "agent: Hi, calling about Main St.\ncustomer: We closed already."
        assert http.call_args.args[:2] == ("GET", "/cdrs/991/transcription")

    def test_missing_transcript_is_none(self):
        http = MagicMock(side_effect=DialerRequestError("not found", status=404))
        assert self._adapter(http).call_transcript(991) is None

    def test_empty_transcript_is_none(self):
        assert self._adapter(MagicMock(return_value=[])).call_transcript(991) is None

    def test_other_errors_propagate(self):
        http = MagicMock(side_effect=DialerRequestError("server", status=500))
        with pytest.raises(DialerRequestError):
            self._adapter(http).call_transcript(991)
