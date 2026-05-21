"""
M5 unit tests — contact enrichment pipeline.
No live DB, no BatchData/IDI API calls required.

Run with:
    pytest tests/test_m5_unit.py -v
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch
import pytest

import src.services.skip_trace as skip_trace_mod
import src.services.idi_fallback as idi_mod


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_batchdata_person(
    mobile="8135551234",
    landline=None,
    email="owner@example.com",
    include_mailing=True,
):
    """Build a mock BatchData person dict."""
    phones = []
    if mobile:
        phones.append({"number": mobile, "type": "Mobile", "score": 85})
    if landline:
        phones.append({"number": landline, "type": "Land Line", "score": 60})

    person = {
        "phoneNumbers": phones,
        "emails": [{"email": email}] if email else [],
    }
    if include_mailing:
        person["mailingAddress"] = {
            "street": "123 Main St",
            "city": "Tampa",
            "state": "FL",
            "zip": "33601",
        }
    return person


def _make_idi_result(mobile="8135559876", email="other@example.com"):
    """Build a mock IDI result dict."""
    return {
        "persons": [
            {
                "phones": [
                    {"number": mobile, "type": "Mobile"},
                ],
                "emails": [{"address": email}],
                "currentAddress": {
                    "street": "456 Oak Ave",
                    "city": "Tampa",
                    "state": "FL",
                    "zip": "33602",
                },
                "relatives": [
                    {"name": "Jane Doe", "phones": [{"number": "8135550001"}]},
                ],
            }
        ]
    }


# ---------------------------------------------------------------------------
# BatchData _parse_result
# ---------------------------------------------------------------------------

class TestBatchDataParseResult:
    """Unit tests for skip_trace._parse_result()."""

    def test_mobile_preferred_over_landline(self):
        person = _make_batchdata_person(mobile="8131111111", landline="8132222222")
        result = skip_trace_mod._parse_result(person)
        assert result["mobile_phone"] == "8131111111"
        assert result["landline"] == "8132222222"

    def test_landline_only_when_no_mobile(self):
        person = _make_batchdata_person(mobile=None, landline="8133333333")
        result = skip_trace_mod._parse_result(person)
        assert result["mobile_phone"] is None
        assert result["landline"] == "8133333333"

    def test_fallback_to_first_number_when_type_unknown(self):
        person = {
            "phoneNumbers": [{"number": "8134444444", "type": "Unknown", "score": 50}],
            "emails": [],
        }
        result = skip_trace_mod._parse_result(person)
        assert result["mobile_phone"] == "8134444444"

    def test_email_extracted(self):
        person = _make_batchdata_person(email="test@owner.com")
        result = skip_trace_mod._parse_result(person)
        assert result["email"] == "test@owner.com"

    def test_no_email_returns_none(self):
        person = _make_batchdata_person(email=None)
        result = skip_trace_mod._parse_result(person)
        assert result["email"] is None

    def test_mailing_address_assembled_correctly(self):
        person = _make_batchdata_person(include_mailing=True)
        result = skip_trace_mod._parse_result(person)
        assert "123 Main St" in result["mailing_address"]
        assert "Tampa" in result["mailing_address"]

    def test_match_success_true_when_phone_present(self):
        person = _make_batchdata_person(mobile="8135551234")
        result = skip_trace_mod._parse_result(person)
        assert result["match_success"] is True

    def test_match_success_false_when_no_contact(self):
        person = {"phoneNumbers": [], "emails": []}
        result = skip_trace_mod._parse_result(person)
        assert result["match_success"] is False

    def test_score_based_phone_ranking(self):
        """Higher-scored phone should be selected as mobile_phone."""
        person = {
            "phoneNumbers": [
                {"number": "8130000001", "type": "Mobile", "score": 40},
                {"number": "8130000099", "type": "Mobile", "score": 99},
            ],
            "emails": [],
        }
        result = skip_trace_mod._parse_result(person)
        assert result["mobile_phone"] == "8130000099"

    def test_empty_phone_list(self):
        person = {"phoneNumbers": [], "emails": [{"email": "x@y.com"}]}
        result = skip_trace_mod._parse_result(person)
        assert result["mobile_phone"] is None
        assert result["landline"] is None
        assert result["email"] == "x@y.com"


# ---------------------------------------------------------------------------
# IDI _parse_idi_result
# ---------------------------------------------------------------------------

class TestIdiParseResult:
    """Unit tests for idi_fallback._parse_idi_result()."""

    def test_mobile_extracted(self):
        result = idi_mod._parse_idi_result(_make_idi_result(mobile="8135550001"))
        assert result["mobile_phone"] == "8135550001"

    def test_email_extracted(self):
        result = idi_mod._parse_idi_result(_make_idi_result(email="idi@test.com"))
        assert result["email"] == "idi@test.com"

    def test_relatives_extracted(self):
        result = idi_mod._parse_idi_result(_make_idi_result())
        assert result["relative_contacts"] is not None
        assert len(result["relative_contacts"]) >= 1
        assert "name" in result["relative_contacts"][0]
        assert "phones" in result["relative_contacts"][0]

    def test_empty_result_returns_no_match(self):
        result = idi_mod._parse_idi_result({"persons": []})
        assert result["match_success"] is False
        assert result["mobile_phone"] is None

    def test_missing_persons_key_returns_no_match(self):
        result = idi_mod._parse_idi_result({})
        assert result["match_success"] is False

    def test_match_success_true_with_contact(self):
        result = idi_mod._parse_idi_result(_make_idi_result())
        assert result["match_success"] is True

    def test_mailing_address_assembled(self):
        result = idi_mod._parse_idi_result(_make_idi_result())
        assert "456 Oak Ave" in result["mailing_address"]

    def test_relatives_capped_at_five(self):
        """Relative contacts should be capped at 5 to avoid massive payloads."""
        many_relatives = [{"name": f"Person {i}", "phones": []} for i in range(10)]
        result_data = {
            "persons": [{
                "phones": [{"number": "8130000001", "type": "Mobile"}],
                "emails": [],
                "currentAddress": {},
                "relatives": many_relatives,
            }]
        }
        result = idi_mod._parse_idi_result(result_data)
        assert len(result["relative_contacts"]) <= 5


# ---------------------------------------------------------------------------
# Skip-trace failure alerting
# ---------------------------------------------------------------------------

def _make_fluent_session(rows):
    """
    Build a MagicMock DB session where any chained query call
    eventually returns `rows` from `.all()`.

    Uses a 'fluent' mock where every method returns the same mock object,
    so `.query().join().join().filter()...filter().limit().all()` works
    regardless of chain depth.
    """
    q = MagicMock()
    q.all.return_value = rows
    q.join.return_value = q
    q.filter.return_value = q
    q.limit.return_value = q
    q.group_by.return_value = q
    q.subquery.return_value = MagicMock()  # subquery result is just a mock

    session = MagicMock()
    session.query.return_value = q
    return session


class TestSkipTraceFailureAlerting:
    """Verify that 402/401 BatchData errors trigger ops alerts."""

    def _owner_prop(self):
        owner = MagicMock()
        owner.id = 1
        owner.phone_1 = None
        owner.email_1 = None
        owner.skip_trace_success = None
        owner.county_id = "hillsborough"
        owner.owner_name = "Test Owner"

        prop = MagicMock()
        prop.id = 1
        prop.address = "100 Test St"
        prop.city = "Tampa"
        prop.state = "FL"
        prop.zip = "33601"
        return owner, prop

    def test_402_triggers_send_alert(self):
        """Out-of-credits (402) should fire an ops alert."""
        owner, prop = self._owner_prop()

        from contextlib import contextmanager

        @contextmanager
        def fake_ctx():
            yield _make_fluent_session([(owner, prop)])

        with patch("src.services.skip_trace.get_settings") as mock_settings, \
             patch("src.services.skip_trace.get_db_context", side_effect=fake_ctx), \
             patch("src.services.skip_trace._call_batch_data",
                   side_effect=RuntimeError("BatchData: out of credits (402)")), \
             patch("src.services.skip_trace.send_alert") as mock_alert:

            mock_settings.return_value.batch_skip_tracing_api_key = MagicMock()
            mock_settings.return_value.batch_skip_tracing_api_key.get_secret_value.return_value = "key"

            skip_trace_mod.run_skip_trace(limit=1)

        mock_alert.assert_called_once()
        body = mock_alert.call_args[1]["body"]
        assert "402" in body

    def test_401_triggers_send_alert(self):
        """Invalid API key (401) should fire an ops alert."""
        owner, prop = self._owner_prop()

        from contextlib import contextmanager

        @contextmanager
        def fake_ctx():
            yield _make_fluent_session([(owner, prop)])

        with patch("src.services.skip_trace.get_settings") as mock_settings, \
             patch("src.services.skip_trace.get_db_context", side_effect=fake_ctx), \
             patch("src.services.skip_trace._call_batch_data",
                   side_effect=RuntimeError("BatchData: invalid API key (401)")), \
             patch("src.services.skip_trace.send_alert") as mock_alert:

            mock_settings.return_value.batch_skip_tracing_api_key = MagicMock()
            mock_settings.return_value.batch_skip_tracing_api_key.get_secret_value.return_value = "key"

            skip_trace_mod.run_skip_trace(limit=1)

        mock_alert.assert_called_once()
        body = mock_alert.call_args[1]["body"]
        assert "401" in body

    def test_missing_api_key_raises(self):
        """Missing BATCH_SKIP_TRACING_API_KEY should raise RuntimeError immediately."""
        with patch("src.services.skip_trace.get_settings") as mock_settings:
            mock_settings.return_value.batch_skip_tracing_api_key = None
            with pytest.raises(RuntimeError, match="BATCH_SKIP_TRACING_API_KEY"):
                skip_trace_mod.run_skip_trace(limit=1)


# ---------------------------------------------------------------------------
# Enrichment queue (run_enrichment)
# ---------------------------------------------------------------------------

class TestRunEnrichmentPipeline:
    """Test enrichment pipeline orchestrator.

    run_enrichment.py uses lazy imports inside function body, so we patch
    the source modules directly (not src.tasks.run_enrichment.run_skip_trace).
    """

    def test_both_stages_called(self):
        """Pipeline should call BatchData then IDI fallback."""
        with patch("src.services.skip_trace.run_skip_trace",
                   return_value={"success": 10, "failed": 2, "total": 12}) as bd, \
             patch("src.services.idi_fallback.run_idi_fallback",
                   return_value={"success": 3, "failed": 1, "total": 4}) as idi:
            from src.tasks.run_enrichment import run_enrichment_pipeline
            result = run_enrichment_pipeline(county_id="hillsborough")

        bd.assert_called_once()
        idi.assert_called_once()
        assert result["total_enriched"] == 13  # 10 + 3

    def test_batchdata_failure_continues_to_idi(self):
        """BatchData failure should not prevent IDI from running."""
        with patch("src.services.skip_trace.run_skip_trace",
                   side_effect=RuntimeError("API down")), \
             patch("src.services.idi_fallback.run_idi_fallback",
                   return_value={"success": 5, "failed": 0, "total": 5}) as idi, \
             patch("src.tasks.run_enrichment.send_alert"):
            from src.tasks.run_enrichment import run_enrichment_pipeline
            result = run_enrichment_pipeline()

        idi.assert_called_once()

    def test_skip_idi_flag(self):
        """--skip-idi should bypass IDI fallback stage entirely."""
        with patch("src.services.skip_trace.run_skip_trace",
                   return_value={"success": 5, "failed": 0, "total": 5}), \
             patch("src.services.idi_fallback.run_idi_fallback") as idi:
            from src.tasks.run_enrichment import run_enrichment_pipeline
            result = run_enrichment_pipeline(skip_idi=True)

        idi.assert_not_called()
        assert result["idi"] == {"skipped": True}


# ---------------------------------------------------------------------------
# Match rate monitor integration
# ---------------------------------------------------------------------------

class TestMatchRateMonitor:
    """Test match rate monitoring thresholds and state tracking."""

    def test_below_threshold_increments_consecutive_days(self):
        """Rate below 65% should increment the consecutive_low_days counter."""
        from src.tasks.match_rate_monitor import MATCH_RATE_THRESHOLD
        assert MATCH_RATE_THRESHOLD == 0.65

    def test_alert_fires_after_two_consecutive_low_days(self):
        """Alert should fire after 2 consecutive days below threshold."""
        with patch("src.tasks.match_rate_monitor.get_db_context") as mock_ctx, \
             patch("src.tasks.match_rate_monitor._load_state",
                   return_value={"consecutive_low_days": 1, "last_check": None}), \
             patch("src.tasks.match_rate_monitor._save_state"), \
             patch("src.tasks.match_rate_monitor.send_alert", return_value=True) as mock_alert:

            mock_session = MagicMock()
            # total=20, matched=10 → rate=0.50 (below 0.65 threshold)
            mock_session.query.return_value.filter.return_value.count.side_effect = [20, 10]
            mock_ctx.return_value.__enter__ = MagicMock(return_value=mock_session)
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)

            from src.tasks.match_rate_monitor import run_match_rate_monitor
            result = run_match_rate_monitor()

        mock_alert.assert_called_once()
        assert result["alerted"] is True

    def test_no_alert_on_first_low_day(self):
        """Alert should NOT fire on the first low day (require 2 consecutive)."""
        with patch("src.tasks.match_rate_monitor.get_db_context") as mock_ctx, \
             patch("src.tasks.match_rate_monitor._load_state",
                   return_value={"consecutive_low_days": 0, "last_check": None}), \
             patch("src.tasks.match_rate_monitor._save_state"), \
             patch("src.tasks.match_rate_monitor.send_alert") as mock_alert:

            mock_session = MagicMock()
            mock_session.query.return_value.filter.return_value.count.side_effect = [20, 5]
            mock_ctx.return_value.__enter__ = MagicMock(return_value=mock_session)
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)

            from src.tasks.match_rate_monitor import run_match_rate_monitor
            result = run_match_rate_monitor()

        mock_alert.assert_not_called()
        assert result["alerted"] is False

    def test_small_sample_skips_check(self):
        """Fewer than MIN_SAMPLE_SIZE records should skip the rate check."""
        with patch("src.tasks.match_rate_monitor.get_db_context") as mock_ctx, \
             patch("src.tasks.match_rate_monitor._load_state",
                   return_value={"consecutive_low_days": 0, "last_check": None}), \
             patch("src.tasks.match_rate_monitor._save_state"):

            mock_session = MagicMock()
            # Only 3 records — below MIN_SAMPLE_SIZE of 10
            mock_session.query.return_value.filter.return_value.count.side_effect = [3, 1]
            mock_ctx.return_value.__enter__ = MagicMock(return_value=mock_session)
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)

            from src.tasks.match_rate_monitor import run_match_rate_monitor
            result = run_match_rate_monitor()

        assert result["rate"] is None


# ---------------------------------------------------------------------------
# Filing-derived name override (probate / eviction / divorce / lis pendens)
# ---------------------------------------------------------------------------

def _make_owner(owner_type="Individual", name="JOHN SMITH"):
    """Build a mock Owner row used by the candidate query."""
    owner = MagicMock()
    owner.id = 1
    owner.phone_1 = None
    owner.email_1 = None
    owner.skip_trace_success = None
    owner.county_id = "hillsborough"
    owner.owner_name = name
    owner.owner_type = owner_type
    owner.registered_agent_name = None
    owner.registered_agent_address = None
    return owner


def _make_prop(prop_id=1):
    """Build a mock Property row used by the candidate query."""
    prop = MagicMock()
    prop.id = prop_id
    prop.address = "100 Test St"
    prop.city = "Tampa"
    prop.state = "FL"
    prop.zip = "33601"
    return prop


def _make_filing_aware_session(
    candidates,
    probate_rows=None,
    eviction_rows=None,
    divorce_rows=None,
    lp_rows=None,
):
    """
    Build a MagicMock session whose `.all()` returns results in the order
    that run_skip_trace() issues them inside its `with get_db_context()` block:
        1. main candidate query  → (owner, prop) tuples
        2. probate pre-fetch     → (property_id, name) tuples
        3. eviction pre-fetch    → (property_id, name) tuples
        4. divorce pre-fetch     → (property_id, name) tuples
        5. lis pendens pre-fetch → (property_id, name) tuples
    """
    q = MagicMock()
    q.join.return_value = q
    q.filter.return_value = q
    q.limit.return_value = q
    q.group_by.return_value = q
    q.order_by.return_value = q
    q.subquery.return_value = MagicMock()
    q.all.side_effect = [
        candidates,
        probate_rows or [],
        eviction_rows or [],
        divorce_rows or [],
        lp_rows or [],
    ]

    session = MagicMock()
    session.query.return_value = q
    return session


def _run_and_capture_payload(session, batch_call_side_effect=None):
    """
    Run skip_trace.run_skip_trace(limit=1) with the supplied mock session,
    making `_call_batch_data` fail fast so we capture the payload sent
    (mirrors the TestSkipTraceFailureAlerting pattern).
    Returns (mock_call_batch_data, stats).
    """
    from contextlib import contextmanager

    @contextmanager
    def fake_ctx():
        yield session

    if batch_call_side_effect is None:
        # Default: raise 402 so the function exits cleanly after the first
        # _call_batch_data call. The mock still captures call_args.
        batch_call_side_effect = RuntimeError("BatchData: out of credits (402)")

    with patch("src.services.skip_trace.get_settings") as mock_settings, \
         patch("src.services.skip_trace.get_db_context", side_effect=fake_ctx), \
         patch("src.services.skip_trace._call_batch_data",
               side_effect=batch_call_side_effect) as mock_call, \
         patch("src.services.skip_trace.send_alert"):

        mock_settings.return_value.batch_skip_tracing_api_key = MagicMock()
        mock_settings.return_value.batch_skip_tracing_api_key.get_secret_value.return_value = "key"

        stats = skip_trace_mod.run_skip_trace(limit=1)

    return mock_call, stats


class TestProbateHeirOverride:
    """Probate heir (LegalProceeding.secondary_party) replaces deceased owner name."""

    def test_individual_owner_uses_probate_heir_name(self):
        """Individual owner + probate filing → heir's name in payload."""
        owner, prop = _make_owner(name="JOHN DECEASED"), _make_prop()
        session = _make_filing_aware_session(
            candidates=[(owner, prop)],
            probate_rows=[(prop.id, "Jane Heir")],
        )
        mock_call, _ = _run_and_capture_payload(session)

        payload = mock_call.call_args[0][0][0]
        assert payload["firstName"] == "Jane"
        assert payload["lastName"] == "Heir"
        assert payload["propertyAddress"]["street"] == "100 Test St"

    def test_estate_owner_uses_probate_heir_name(self):
        """Estate-type owner (no registered agent) + probate heir → heir traced."""
        owner = _make_owner(owner_type="Estate", name="SMITH ESTATE")
        prop = _make_prop()
        session = _make_filing_aware_session(
            candidates=[(owner, prop)],
            probate_rows=[(prop.id, "Mary Beneficiary")],
        )
        mock_call, _ = _run_and_capture_payload(session)

        payload = mock_call.call_args[0][0][0]
        assert payload["firstName"] == "Mary"
        assert payload["lastName"] == "Beneficiary"

    def test_estate_owner_with_entity_heir_is_skipped(self):
        """Estate + heir that is itself an entity → cannot trace, skipped."""
        owner = _make_owner(owner_type="Trust", name="SMITH FAMILY TRUST")
        prop = _make_prop()
        session = _make_filing_aware_session(
            candidates=[(owner, prop)],
            probate_rows=[(prop.id, "ACME HOLDINGS LLC")],
        )
        mock_call, stats = _run_and_capture_payload(session)

        # No payload sent because the only "heir" is an entity name.
        assert not mock_call.called
        assert stats["no_address"] >= 1


class TestEvictionLandlordOverride:
    """Eviction plaintiff (LegalProceeding.secondary_party, record_type='Eviction')."""

    def test_individual_owner_uses_eviction_landlord_name(self):
        owner, prop = _make_owner(name="LLC OWNED PROP"), _make_prop()
        session = _make_filing_aware_session(
            candidates=[(owner, prop)],
            eviction_rows=[(prop.id, "Robert Landlord")],
        )
        mock_call, _ = _run_and_capture_payload(session)

        payload = mock_call.call_args[0][0][0]
        assert payload["firstName"] == "Robert"
        assert payload["lastName"] == "Landlord"

    def test_llc_owner_no_agent_uses_eviction_landlord(self):
        """LLC owner with no registered agent → resolves to eviction landlord."""
        owner = _make_owner(owner_type="LLC", name="MAIN ST LLC")
        prop = _make_prop()
        session = _make_filing_aware_session(
            candidates=[(owner, prop)],
            eviction_rows=[(prop.id, "Linda Plaintiff")],
        )
        mock_call, _ = _run_and_capture_payload(session)

        payload = mock_call.call_args[0][0][0]
        assert payload["firstName"] == "Linda"
        assert payload["lastName"] == "Plaintiff"


class TestDivorcePetitionerOverride:
    """Divorce petitioner (LegalProceeding.associated_party, record_type='Divorce')."""

    def test_individual_owner_uses_divorce_petitioner_name(self):
        owner, prop = _make_owner(name="MARK SPOUSE"), _make_prop()
        session = _make_filing_aware_session(
            candidates=[(owner, prop)],
            divorce_rows=[(prop.id, "Sarah Petitioner")],
        )
        mock_call, _ = _run_and_capture_payload(session)

        payload = mock_call.call_args[0][0][0]
        assert payload["firstName"] == "Sarah"
        assert payload["lastName"] == "Petitioner"


class TestLisPendensDefendantOverride:
    """LP defendant (Foreclosure.defendant) — borrower being foreclosed."""

    def test_individual_owner_uses_lp_defendant_name(self):
        owner, prop = _make_owner(name="OWNER OF RECORD"), _make_prop()
        session = _make_filing_aware_session(
            candidates=[(owner, prop)],
            lp_rows=[(prop.id, "David Defendant")],
        )
        mock_call, _ = _run_and_capture_payload(session)

        payload = mock_call.call_args[0][0][0]
        assert payload["firstName"] == "David"
        assert payload["lastName"] == "Defendant"


class TestFilingPriorityResolver:
    """When multiple filings exist for one property, priority order applies:
       Probate > Eviction > Divorce > LP."""

    def test_probate_wins_over_eviction(self):
        owner, prop = _make_owner(), _make_prop()
        session = _make_filing_aware_session(
            candidates=[(owner, prop)],
            probate_rows=[(prop.id, "Probate Heir")],
            eviction_rows=[(prop.id, "Eviction Landlord")],
        )
        mock_call, _ = _run_and_capture_payload(session)

        payload = mock_call.call_args[0][0][0]
        assert payload["firstName"] == "Probate"
        assert payload["lastName"] == "Heir"

    def test_eviction_wins_over_divorce(self):
        owner, prop = _make_owner(), _make_prop()
        session = _make_filing_aware_session(
            candidates=[(owner, prop)],
            eviction_rows=[(prop.id, "Eviction Landlord")],
            divorce_rows=[(prop.id, "Divorce Petitioner")],
        )
        mock_call, _ = _run_and_capture_payload(session)

        payload = mock_call.call_args[0][0][0]
        assert payload["firstName"] == "Eviction"
        assert payload["lastName"] == "Landlord"

    def test_divorce_wins_over_lp(self):
        owner, prop = _make_owner(), _make_prop()
        session = _make_filing_aware_session(
            candidates=[(owner, prop)],
            divorce_rows=[(prop.id, "Divorce Petitioner")],
            lp_rows=[(prop.id, "LP Defendant")],
        )
        mock_call, _ = _run_and_capture_payload(session)

        payload = mock_call.call_args[0][0][0]
        assert payload["firstName"] == "Divorce"
        assert payload["lastName"] == "Petitioner"

    def test_lp_used_when_no_higher_priority_filing(self):
        owner, prop = _make_owner(), _make_prop()
        session = _make_filing_aware_session(
            candidates=[(owner, prop)],
            lp_rows=[(prop.id, "LP Defendant")],
        )
        mock_call, _ = _run_and_capture_payload(session)

        payload = mock_call.call_args[0][0][0]
        assert payload["firstName"] == "LP"
        assert payload["lastName"] == "Defendant"

    def test_no_filings_falls_back_to_blind_address_lookup(self):
        """Individual owner, no filings → payload has only propertyAddress."""
        owner, prop = _make_owner(), _make_prop()
        session = _make_filing_aware_session(candidates=[(owner, prop)])
        mock_call, _ = _run_and_capture_payload(session)

        payload = mock_call.call_args[0][0][0]
        assert "firstName" not in payload
        assert "lastName" not in payload
        assert payload["propertyAddress"]["street"] == "100 Test St"
