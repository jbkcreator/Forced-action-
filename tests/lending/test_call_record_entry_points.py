"""The single-call entry points the call-record intake uses: snapshot, extraction, cause."""
from __future__ import annotations

import json
from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from src.lending import first_contact
from src.lending.call_extraction import CallExtraction
from src.lending.call_extraction_store import extract_and_store_call, load_call_extraction, save_call_extraction
from src.lending.lead_facts import LeadFacts
from src.lending.lead_scoring import LeadSignals, Signal
from src.lending.outcome_causes import UnknownCause, record_unfunded_cause

# 01:30 UTC on Oct 7 is still Oct 6 in Eastern time.
CALL_AT = datetime(2026, 10, 7, 1, 30, tzinfo=timezone.utc)
PHONE = "(813) 555-0142"


def _snapshot(session, property_id=7):
    return first_contact.snapshot_first_contact(
        session, phone=PHONE, property_id=property_id, contacted_at=CALL_AT, caller_seat="seat-a",
        script_version=None, source_tag="list_1", queue="verified_maturity", warm=False,
    )


class TestSnapshotEntryPoint:
    def test_existing_snapshot_skips_loading_facts(self):
        session = MagicMock()
        with patch.object(first_contact, "has_first_contact", return_value=True), \
             patch.object(first_contact, "load_lead_facts") as load:
            assert _snapshot(session) is False
        load.assert_not_called()

    def test_first_call_scores_on_the_eastern_day_and_records(self):
        facts = LeadFacts(7, "1 Test St", "Test Lender", None,
                          LeadSignals(maturity_date=Signal.estimated(date(2026, 11, 20)),
                                      loan_amount=Signal.known(Decimal("400000"))))
        with patch.object(first_contact, "has_first_contact", return_value=False), \
             patch.object(first_contact, "load_lead_facts", return_value={7: facts}) as load, \
             patch.object(first_contact, "record_first_contact", return_value=True) as record:
            assert _snapshot(MagicMock()) is True
        assert load.call_args.kwargs["today"] == date(2026, 10, 6)
        kwargs = record.call_args.kwargs
        assert kwargs["signals"] is facts.signals
        assert kwargs["score"].rank == 2
        assert kwargs["queue"] == "verified_maturity"

    def test_call_without_a_property_is_snapshotted_with_inputs_missing(self):
        with patch.object(first_contact, "has_first_contact", return_value=False), \
             patch.object(first_contact, "load_lead_facts") as load, \
             patch.object(first_contact, "record_first_contact", return_value=True) as record:
            assert _snapshot(MagicMock(), property_id=None) is True
        load.assert_not_called()
        assert record.call_args.kwargs["score"].rank == 1

    def test_naive_time_is_refused(self):
        with patch.object(first_contact, "has_first_contact", return_value=False), pytest.raises(ValueError):
            first_contact.snapshot_first_contact(
                MagicMock(), phone=PHONE, property_id=7, contacted_at=datetime(2026, 10, 6, 14, 0),
                caller_seat=None, script_version=None, source_tag=None, queue=None, warm=False)

    def test_has_first_contact_checks_the_normalized_phone(self):
        session = MagicMock()
        session.execute.return_value.first.return_value = (1,)
        assert first_contact.has_first_contact(session, PHONE) is True
        assert session.execute.call_args.args[1] == {"phone": "+18135550142"}
        assert first_contact.has_first_contact(MagicMock(), "not-a-phone") is False


class TestExtractionStore:
    def test_save_upserts_by_call_id(self):
        session = MagicMock()
        save_call_extraction(session, dialer_call_id=" 991 ", phone=PHONE,
                             extraction=CallExtraction(objection="Timing", credit_band="below_640"),
                             extracted_at=CALL_AT)
        sql, params = session.execute.call_args.args
        assert "ON CONFLICT (dialer_call_id) DO UPDATE" in str(sql)
        assert params["dialer_call_id"] == "991"
        assert params["phone"] == "+18135550142"
        assert json.loads(params["fields"])["credit_band"] == "below_640"

    def test_save_requires_a_call_id(self):
        with pytest.raises(ValueError):
            save_call_extraction(MagicMock(), dialer_call_id=" ", phone=None,
                                 extraction=CallExtraction(), extracted_at=CALL_AT)

    def test_blank_transcript_stores_nothing(self):
        session = MagicMock()
        with patch("src.lending.call_extraction_store.extract_call_fields") as extract:
            assert extract_and_store_call(session, dialer_call_id="991", phone=PHONE,
                                          transcript="  ", extracted_at=CALL_AT) is None
        extract.assert_not_called()
        session.execute.assert_not_called()

    def test_extracts_on_the_eastern_day_and_saves(self):
        session = MagicMock()
        with patch("src.lending.call_extraction_store.extract_call_fields",
                   return_value=CallExtraction(deal_status="under_contract")) as extract:
            result = extract_and_store_call(session, dialer_call_id="991", phone=PHONE,
                                            transcript="We are under contract on Main St.", extracted_at=CALL_AT)
        assert result.deal_status == "under_contract"
        assert extract.call_args.kwargs["today"] == date(2026, 10, 6)
        session.execute.assert_called_once()

    def test_load_round_trips_the_stored_fields(self):
        session = MagicMock()
        session.execute.return_value.scalar.return_value = {"objection": "Rates", "completed_projects_3y": 3}
        loaded = load_call_extraction(session, "991")
        assert (loaded.objection, loaded.completed_projects_3y) == ("Rates", 3)
        session.execute.return_value.scalar.return_value = None
        assert load_call_extraction(session, "missing") is None


class TestCauseCorrection:
    def test_correction_replaces_the_provisional_cause(self):
        session = MagicMock()
        session.execute.return_value.rowcount = 1
        assert record_unfunded_cause(session, dialer_call_id="991", cause=" Our_Execution ") is True
        sql, params = session.execute.call_args.args
        assert "unfunded_cause_provisional = false" in str(sql)
        assert params == {"cause": "our_execution", "call_id": "991"}

    def test_unknown_call_returns_false(self):
        session = MagicMock()
        session.execute.return_value.rowcount = 0
        assert record_unfunded_cause(session, dialer_call_id="nope", cause="timing") is False

    def test_invalid_cause_never_reaches_the_database(self):
        session = MagicMock()
        with pytest.raises(UnknownCause):
            record_unfunded_cause(session, dialer_call_id="991", cause="bad_source")
        session.execute.assert_not_called()
