"""Tests for PropertyRadar ingestion adapter — Developer 1 scope.

All tests use FakePropertyRadarPort; no network calls are made.

Covers:
  - Normalizer: field extraction, long-term exclusion, est_maturity_date,
    principal_name extraction, Unknown term handling.
  - Port: FakePropertyRadarPort records calls, count() returns len(canned).
  - Budget guard: allowance check and per-run cap raise RuntimeError.
  - Campaign criteria: build_campaign_criteria raises on unknown campaign;
    FL and GA criteria include expected keys.
  - FIPS: Miami-Dade → 12025, Hillsborough → 12057, unknown → None.
  - county_fips: case-insensitive lookup.
"""
from __future__ import annotations

import json
from dataclasses import replace
from datetime import date
from typing import Iterator
from unittest.mock import patch

import pytest

from config.property_radar_campaigns import build_campaign_criteria, DEFAULT_CAMPAIGN
from config.property_radar_fips import county_fips, FIPS_BY_STATE
from src.services.property_radar_normalizer import (
    PropertyRadarNormalized,
    _compute_maturity,
    _extract_principal,
    _parse_term,
    normalize,
)
from src.services.property_radar_port import (
    AllowanceInfo,
    FakePropertyRadarPort,
    PropertyRadarRecord,
)


# ---------------------------------------------------------------------------
# Fixtures — raw record builders
# ---------------------------------------------------------------------------

def _raw_record(
    radar_id: str = "PDA00001",
    *,
    state: str = "FL",
    county: str = "HILLSBOROUGH",
    lender: str = "KIAVI FNDG INC",
    loan_date: str = "2025-12-09",
    term: object = "2",
    persons: list | None = None,
    ownership_type: str = "Corporate",
) -> dict:
    return {
        "RadarID": radar_id,
        "APN": "APN_001",
        "State": state,
        "County": county,
        "Address": "100 MAIN ST",
        "City": "TAMPA",
        "ZipFive": "33601",
        "PType": "SFR",
        "Owner": "TEST LLC",
        "OwnershipType": ownership_type,
        "OwnerAddress": "500 MAILING AVE",
        "OwnerCity": "MIAMI",
        "OwnerState": "FL",
        "OwnerZipFive": "33101",
        "FirstLenderOriginal": lender,
        "FirstDate": loan_date,
        "FirstAmount": 300000,
        "FirstTermInYears": term,
        "Persons": persons or [],
    }


def _record(radar_id: str = "PDA00001", **kwargs) -> PropertyRadarRecord:
    return PropertyRadarRecord(radar_id=radar_id, raw=_raw_record(radar_id=radar_id, **kwargs))


# ---------------------------------------------------------------------------
# Normalizer tests
# ---------------------------------------------------------------------------

class TestNormalizer:
    def test_basic_normalization(self):
        result = normalize(_record(), state="FL", campaign="maturity_target_lender")
        assert result is not None
        assert result.radar_id == "PDA00001"
        assert result.state == "FL"
        assert result.county_name == "HILLSBOROUGH"
        assert result.county_fips == "12057"
        assert result.state_fips == "12"
        assert result.loan_recorded_date == date(2025, 12, 9)
        assert result.loan_term_years == 2
        assert result.loan_doc_number is None
        assert result.raw["RadarID"] == "PDA00001"
        assert result.property_address == "100 MAIN ST"
        assert result.zip == "33601"
        assert result.lender_name == "KIAVI FNDG INC"

    def test_long_term_exclusion_filters_record(self):
        result = normalize(_record(term="30"), state="FL", campaign="maturity_target_lender")
        assert result is None

    def test_long_term_exclusion_threshold_is_20(self):
        assert normalize(_record(term="19"), state="FL", campaign="x") is not None
        assert normalize(_record(term="20"), state="FL", campaign="x") is None

    def test_unknown_term_is_kept(self):
        result = normalize(_record(term="Unknown"), state="FL", campaign="x")
        assert result is not None
        assert result.loan_term_years is None

    def test_unknown_term_has_no_maturity_date(self):
        result = normalize(_record(term="Unknown"), state="FL", campaign="x")
        assert result.est_maturity_date is None

    def test_numeric_term_computes_maturity(self):
        result = normalize(_record(term="2", loan_date="2025-01-01"), state="FL", campaign="x")
        assert result.est_maturity_date == date(2027, 1, 1)

    def test_missing_loan_date_yields_no_maturity(self):
        rec = _record(term="2")
        rec.raw["FirstDate"] = None
        result = normalize(rec, state="FL", campaign="x")
        assert result.loan_recorded_date is None
        assert result.est_maturity_date is None

    def test_mailing_address_extracted_from_owner_fields(self):
        """Dev 2's staging schema (property_radar_records) has dedicated
        mailing_address/city/state/zip columns sourced from PropertyRadar's
        OwnerAddress/OwnerCity/OwnerState/OwnerZipFive — this is the owner's
        mailing address, not the property's own address."""
        result = normalize(_record(), state="FL", campaign="x")
        assert result.mailing_address == "500 MAILING AVE"
        assert result.mailing_city == "MIAMI"
        assert result.mailing_state == "FL"
        assert result.mailing_zip == "33101"

    def test_missing_mailing_fields_are_none(self):
        rec = _record()
        for key in ("OwnerAddress", "OwnerCity", "OwnerState", "OwnerZipFive"):
            rec.raw[key] = None
        result = normalize(rec, state="FL", campaign="x")
        assert result.mailing_address is None
        assert result.mailing_city is None
        assert result.mailing_state is None
        assert result.mailing_zip is None

    def test_principal_name_extracted_from_persons(self):
        persons = [
            {
                "RadarID": "X",
                "OwnershipRole": "Principal",
                "PersonType": "Person",
                "FirstName": "Jane",
                "LastName": "Doe",
            }
        ]
        result = normalize(_record(persons=persons), state="FL", campaign="x")
        assert result.principal_name == "Jane Doe"

    def test_company_principal_yields_no_name(self):
        persons = [
            {
                "RadarID": "X",
                "OwnershipRole": "Principal",
                "PersonType": "Company",
                "EntityName": "ACME LLC",
            }
        ]
        result = normalize(_record(persons=persons), state="FL", campaign="x")
        assert result.principal_name is None

    def test_no_persons_yields_no_principal(self):
        result = normalize(_record(persons=[]), state="FL", campaign="x")
        assert result.principal_name is None

    def test_county_normalized_uppercase(self):
        rec = _record()
        rec.raw["County"] = "hillsborough"
        result = normalize(rec, state="FL", campaign="x")
        assert result.county_name == "HILLSBOROUGH"

    def test_miami_dade_fips_is_12025(self):
        rec = _record(county="MIAMI-DADE")
        result = normalize(rec, state="FL", campaign="x")
        assert result.county_fips == "12025"

    def test_unknown_county_is_skipped_missing_required_fips(self):
        """Dev 2's contract: records missing county_fips (unresolvable county
        name) are skipped, not passed through with county_fips=None."""
        rec = _record(county="MADE-UP-COUNTY")
        result = normalize(rec, state="FL", campaign="x")
        assert result is None

    def test_missing_apn_is_skipped(self):
        rec = _record()
        rec.raw["APN"] = None
        result = normalize(rec, state="FL", campaign="x")
        assert result is None

    def test_unresolvable_state_is_skipped(self):
        rec = _record(state="ZZ")
        result = normalize(rec, state="ZZ", campaign="x")
        assert result is None


# ---------------------------------------------------------------------------
# FIPS mapping tests
# ---------------------------------------------------------------------------

class TestFIPSMapping:
    def test_hillsborough_fips(self):
        assert county_fips("FL", "HILLSBOROUGH") == "12057"

    def test_pinellas_fips(self):
        assert county_fips("FL", "PINELLAS") == "12103"

    def test_miami_dade_legacy_fips(self):
        assert county_fips("FL", "MIAMI-DADE") == "12025"

    def test_case_insensitive(self):
        assert county_fips("fl", "hillsborough") == "12057"

    def test_unknown_county_returns_none(self):
        assert county_fips("FL", "ATLANTIS") is None

    def test_unknown_state_returns_none(self):
        assert county_fips("ZZ", "ANYWHERE") is None

    def test_fl_has_67_counties(self):
        assert len(FIPS_BY_STATE["FL"]) == 67

    def test_ga_cobb_fips(self):
        assert county_fips("GA", "COBB") == "13067"


# ---------------------------------------------------------------------------
# FakePropertyRadarPort tests
# ---------------------------------------------------------------------------

class TestFakePort:
    def test_count_returns_len_of_canned(self):
        port = FakePropertyRadarPort(
            canned_records=[_raw_record("A"), _raw_record("B")]
        )
        assert port.count([]) == 2

    def test_count_records_call(self):
        port = FakePropertyRadarPort()
        criteria = [{"name": "State", "value": ["FL"]}]
        port.count(criteria)
        assert port.count_calls == [criteria]

    def test_purchase_yields_all_canned(self):
        port = FakePropertyRadarPort(
            canned_records=[_raw_record("R1"), _raw_record("R2")]
        )
        results = list(port.purchase([]))
        assert [r.radar_id for r in results] == ["R1", "R2"]

    def test_purchase_records_call(self):
        port = FakePropertyRadarPort(canned_records=[_raw_record()])
        criteria = [{"name": "State", "value": ["FL"]}]
        list(port.purchase(criteria))
        assert port.purchase_calls == [criteria]

    def test_allowance_returns_canned(self):
        port = FakePropertyRadarPort(
            canned_allowance=AllowanceInfo(
                quantity_free_remaining=500,
                quantity_purchased_remaining=9500,
            )
        )
        a = port.allowance()
        assert a.total_remaining == 10000


# ---------------------------------------------------------------------------
# Budget guard tests
# ---------------------------------------------------------------------------

class TestBudgetGuard:
    """Test the budget guard by calling _check_budget directly."""

    def _port_with(self, count: int, remaining: int) -> FakePropertyRadarPort:
        return FakePropertyRadarPort(
            canned_records=[_raw_record(str(i)) for i in range(count)],
            canned_allowance=AllowanceInfo(
                quantity_free_remaining=remaining,
                quantity_purchased_remaining=0,
            ),
        )

    def test_within_budget_returns_count(self):
        from src.tasks.property_radar_maturity_pull import _check_budget
        from unittest.mock import patch
        port = self._port_with(count=100, remaining=5000)
        with patch("src.tasks.property_radar_maturity_pull.settings") as mock_settings:
            mock_settings.property_radar_per_run_cap = 5000
            result = _check_budget(port, [], "FL", "maturity_target_lender")
        assert result == 100

    def test_over_allowance_raises(self):
        from src.tasks.property_radar_maturity_pull import _check_budget
        from unittest.mock import patch
        port = self._port_with(count=200, remaining=50)
        with patch("src.tasks.property_radar_maturity_pull.settings") as mock_settings:
            mock_settings.property_radar_per_run_cap = 5000
            with pytest.raises(RuntimeError, match="budget guard"):
                _check_budget(port, [], "FL", "maturity_target_lender")

    def test_over_per_run_cap_raises(self):
        from src.tasks.property_radar_maturity_pull import _check_budget
        from unittest.mock import patch
        port = self._port_with(count=2000, remaining=10000)
        with patch("src.tasks.property_radar_maturity_pull.settings") as mock_settings:
            mock_settings.property_radar_per_run_cap = 500
            with pytest.raises(RuntimeError, match="per-run cap"):
                _check_budget(port, [], "FL", "maturity_target_lender")

    def test_zero_count_skips_allowance_check(self):
        from src.tasks.property_radar_maturity_pull import _check_budget
        from unittest.mock import patch
        port = FakePropertyRadarPort(
            canned_records=[],
            canned_allowance=AllowanceInfo(
                quantity_free_remaining=0, quantity_purchased_remaining=0
            ),
        )
        with patch("src.tasks.property_radar_maturity_pull.settings") as mock_settings:
            mock_settings.property_radar_per_run_cap = 500
            result = _check_budget(port, [], "FL", "maturity_target_lender")
        assert result == 0

    def test_unverified_allowance_skips_over_allowance_check_but_enforces_cap(self):
        """LivePropertyRadarPort.allowance() has no free quota endpoint to call —
        it returns verified=False. _check_budget must not treat that as a real
        remaining-balance of 0 (which would wrongly block every live run); it
        must still enforce per_run_cap, the only real pre-flight guard left."""
        from src.tasks.property_radar_maturity_pull import _check_budget
        from unittest.mock import patch
        port = FakePropertyRadarPort(
            canned_records=[_raw_record(str(i)) for i in range(100)],
            canned_allowance=AllowanceInfo(
                quantity_free_remaining=0, quantity_purchased_remaining=0, verified=False
            ),
        )
        with patch("src.tasks.property_radar_maturity_pull.settings") as mock_settings:
            mock_settings.property_radar_per_run_cap = 5000
            result = _check_budget(port, [], "FL", "maturity_target_lender")
        assert result == 100

    def test_unverified_allowance_still_enforces_per_run_cap(self):
        from src.tasks.property_radar_maturity_pull import _check_budget
        from unittest.mock import patch
        port = FakePropertyRadarPort(
            canned_records=[_raw_record(str(i)) for i in range(2000)],
            canned_allowance=AllowanceInfo(
                quantity_free_remaining=0, quantity_purchased_remaining=0, verified=False
            ),
        )
        with patch("src.tasks.property_radar_maturity_pull.settings") as mock_settings:
            mock_settings.property_radar_per_run_cap = 500
            with pytest.raises(RuntimeError, match="per-run cap"):
                _check_budget(port, [], "FL", "maturity_target_lender")


# ---------------------------------------------------------------------------
# Campaign criteria tests
# ---------------------------------------------------------------------------

class TestCampaignCriteria:
    def test_fl_maturity_target_lender_has_required_keys(self):
        criteria = build_campaign_criteria("FL", "maturity_target_lender")
        names = {c["name"] for c in criteria}
        assert "OwnershipType" in names
        assert "isListedForSale" in names
        assert "FirstDate" in names
        assert "FirstLenderOriginal" in names

    def test_fl_criteria_ownership_is_corporate(self):
        criteria = build_campaign_criteria("FL", "maturity_target_lender")
        ownership = next(c for c in criteria if c["name"] == "OwnershipType")
        assert ownership["value"] == ["Corporate"]

    def test_fl_criteria_no_firsttermsinyears(self):
        # TermInYears is export-only — must not appear as a criterion
        criteria = build_campaign_criteria("FL", "maturity_target_lender")
        names = [c["name"] for c in criteria]
        assert "FirstTermInYears" not in names

    def test_ga_criteria_state_value_is_ga(self):
        criteria = build_campaign_criteria("GA", "maturity_target_lender")
        state_c = next(c for c in criteria if c["name"] == "State")
        assert state_c["value"] == ["GA"]

    def test_fl_lender_includes_kiavi(self):
        criteria = build_campaign_criteria("FL", "maturity_target_lender")
        lender_c = next(c for c in criteria if c["name"] == "FirstLenderOriginal")
        assert "Kiavi" in lender_c["value"]

    def test_unknown_campaign_raises(self):
        with pytest.raises(ValueError, match="Unknown campaign"):
            build_campaign_criteria("FL", "nonexistent_campaign_xyz")


# ---------------------------------------------------------------------------
# Daily incremental-window tests (§2.3: daily pull must not re-query the
# full backlog window — otherwise every daily run re-bills ~700+ records)
# ---------------------------------------------------------------------------

class TestDailyIncrementalWindow:
    def test_no_daily_since_uses_full_backlog_window(self):
        """Backlog mode (daily_since=None) must keep querying the full
        8-15 month window — this is the existing, unchanged behavior."""
        from datetime import date, timedelta
        full = build_campaign_criteria("FL", "maturity_target_lender")
        narrowed = build_campaign_criteria(
            "FL", "maturity_target_lender", daily_since=date.today() - timedelta(days=1)
        )
        full_date = next(c for c in full if c["name"] == "FirstDate")["value"][0]
        narrowed_date = next(c for c in narrowed if c["name"] == "FirstDate")["value"][0]
        assert full_date != narrowed_date

    def test_daily_since_narrows_to_since_last_run(self):
        """A daily_since of N days ago must produce a FirstDate window whose
        start is N days before today's window edge, not the full 15-month
        backlog start — this is what keeps a daily run cheap."""
        from datetime import date, timedelta
        since = date.today() - timedelta(days=3)
        criteria = build_campaign_criteria("FL", "maturity_target_lender", daily_since=since)
        first_date = next(c for c in criteria if c["name"] == "FirstDate")["value"][0]
        # window spans only a few days, not ~7 months (15m - 8m)
        start_str, end_str = first_date.replace("from: ", "").split(" to: ")
        from datetime import datetime as dt
        start_d = dt.strptime(start_str, "%m/%d/%Y").date()
        end_d = dt.strptime(end_str, "%m/%d/%Y").date()
        assert (end_d - start_d).days <= 4

    def test_daily_since_today_or_future_degenerates_to_empty_not_wider(self):
        """If daily_since is today (two runs same day) the window must not
        widen back out to the full backlog range."""
        from datetime import date
        criteria = build_campaign_criteria(
            "FL", "maturity_target_lender", daily_since=date.today()
        )
        first_date = next(c for c in criteria if c["name"] == "FirstDate")["value"][0]
        start_str, end_str = first_date.replace("from: ", "").split(" to: ")
        assert start_str == end_str

    def test_ga_supports_daily_since_too(self):
        from datetime import date, timedelta
        since = date.today() - timedelta(days=2)
        criteria = build_campaign_criteria("GA", "maturity_target_lender", daily_since=since)
        first_date = next(c for c in criteria if c["name"] == "FirstDate")["value"][0]
        start_str, end_str = first_date.replace("from: ", "").split(" to: ")
        from datetime import datetime as dt
        start_d = dt.strptime(start_str, "%m/%d/%Y").date()
        end_d = dt.strptime(end_str, "%m/%d/%Y").date()
        assert (end_d - start_d).days <= 4


class TestLastSuccessfulRunDate:
    def test_no_prior_run_returns_none(self):
        from unittest.mock import MagicMock
        from src.tasks.property_radar_maturity_pull import _last_successful_run_date
        session = MagicMock()
        session.execute.return_value.first.return_value = None
        result = _last_successful_run_date(session, "FL", "maturity_target_lender")
        assert result is None

    def test_prior_run_returns_its_date(self):
        from unittest.mock import MagicMock
        from datetime import datetime, timezone
        from src.tasks.property_radar_maturity_pull import _last_successful_run_date
        session = MagicMock()
        ts = datetime(2026, 9, 20, tzinfo=timezone.utc)
        session.execute.return_value.first.return_value = (ts,)
        result = _last_successful_run_date(session, "FL", "maturity_target_lender")
        assert result == ts.date()


# ---------------------------------------------------------------------------
# main() PROPERTY_RADAR_ENABLED gate (PR #313 review finding: the cron job
# was documented as "disabled by default" but nothing actually checked the
# flag, so it silently wrote real rows to production tables every night even
# in mode="fake". main() must short-circuit on property_radar_enabled=False
# regardless of property_radar_mode.)
# ---------------------------------------------------------------------------

class TestMainRespectsEnabledFlag:
    def _run_main(self, argv):
        import sys
        from src.tasks.property_radar_maturity_pull import main
        old_argv = sys.argv
        sys.argv = ["property_radar_maturity_pull.py"] + argv
        try:
            main()
        finally:
            sys.argv = old_argv

    def test_disabled_short_circuits_before_any_db_session(self):
        """Regardless of property_radar_mode, PROPERTY_RADAR_ENABLED=false
        must no-op main() before get_db_context()/_run_pull() ever run —
        this is what the cron job's "disabled by default" promise depends on."""
        with patch("src.tasks.property_radar_maturity_pull.settings") as mock_settings, \
             patch("src.tasks.property_radar_maturity_pull.get_db_context") as mock_db, \
             patch("src.tasks.property_radar_maturity_pull.ENABLED_STATES", frozenset({"FL"})):
            mock_settings.property_radar_enabled = False
            mock_settings.property_radar_mode = "fake"  # the default — must still be gated
            self._run_main(["--mode", "daily", "--state", "FL"])
            mock_db.assert_not_called()

    def test_disabled_short_circuits_even_in_live_mode(self):
        with patch("src.tasks.property_radar_maturity_pull.settings") as mock_settings, \
             patch("src.tasks.property_radar_maturity_pull.get_db_context") as mock_db, \
             patch("src.tasks.property_radar_maturity_pull.ENABLED_STATES", frozenset({"FL"})):
            mock_settings.property_radar_enabled = False
            mock_settings.property_radar_mode = "live"
            self._run_main(["--mode", "backlog", "--state", "FL"])
            mock_db.assert_not_called()

    def test_enabled_proceeds_to_db_session(self):
        with patch("src.tasks.property_radar_maturity_pull.settings") as mock_settings, \
             patch("src.tasks.property_radar_maturity_pull.get_db_context") as mock_db, \
             patch("src.tasks.property_radar_maturity_pull._run_pull") as mock_run_pull, \
             patch("src.tasks.property_radar_maturity_pull.property_radar_lead_handoff") as mock_handoff, \
             patch("src.tasks.property_radar_maturity_pull.ENABLED_STATES", frozenset({"FL"})):
            mock_settings.property_radar_enabled = True
            mock_settings.property_radar_mode = "fake"
            mock_run_pull.return_value = {"run_id": 1}
            mock_handoff.run.return_value.summary.return_value = "ok"
            self._run_main(["--mode", "daily", "--state", "FL"])
            mock_db.assert_called_once()

    def test_dry_run_is_exempt_from_the_enabled_gate(self):
        """--dry-run only makes free count() calls and writes nothing, so it
        must stay usable for verification even when the job is disabled."""
        with patch("src.tasks.property_radar_maturity_pull.settings") as mock_settings, \
             patch("src.tasks.property_radar_maturity_pull.get_db_context") as mock_db, \
             patch("src.tasks.property_radar_maturity_pull._dry_run_county_report") as mock_report, \
             patch("src.tasks.property_radar_maturity_pull.ENABLED_STATES", frozenset({"FL"})):
            mock_settings.property_radar_enabled = False
            mock_settings.property_radar_mode = "fake"
            self._run_main(["--dry-run", "--state", "FL"])
            mock_report.assert_called_once()
            mock_db.assert_not_called()


# ---------------------------------------------------------------------------
# Pull -> stage -> handoff chaining (Dev 3 integration): main() must call
# property_radar_lead_handoff.run() after a successful pull, matching the
# repo's dry-run-unless-apply sweep convention, without letting a handoff
# failure retroactively affect the pull's own already-committed success.
# ---------------------------------------------------------------------------

class TestMainChainsHandoff:
    def _run_main(self, argv):
        import sys
        from src.tasks.property_radar_maturity_pull import main
        old_argv = sys.argv
        sys.argv = ["property_radar_maturity_pull.py"] + argv
        try:
            main()
        finally:
            sys.argv = old_argv

    def _enabled_settings(self, mock_settings):
        mock_settings.property_radar_enabled = True
        mock_settings.property_radar_mode = "fake"

    def test_handoff_runs_after_a_successful_pull_dry_run_by_default(self):
        with patch("src.tasks.property_radar_maturity_pull.settings") as mock_settings, \
             patch("src.tasks.property_radar_maturity_pull.get_db_context"), \
             patch("src.tasks.property_radar_maturity_pull._run_pull") as mock_run_pull, \
             patch("src.tasks.property_radar_maturity_pull.property_radar_lead_handoff") as mock_handoff, \
             patch("src.tasks.property_radar_maturity_pull.ENABLED_STATES", frozenset({"FL"})):
            self._enabled_settings(mock_settings)
            mock_run_pull.return_value = {"run_id": 1}
            mock_handoff.run.return_value.summary.return_value = "ok"
            self._run_main(["--mode", "daily", "--state", "FL", "--campaign", "maturity_target_lender"])
            mock_handoff.run.assert_called_once_with(campaign="maturity_target_lender", apply=False)

    def test_apply_handoff_flag_passes_through(self):
        with patch("src.tasks.property_radar_maturity_pull.settings") as mock_settings, \
             patch("src.tasks.property_radar_maturity_pull.get_db_context"), \
             patch("src.tasks.property_radar_maturity_pull._run_pull") as mock_run_pull, \
             patch("src.tasks.property_radar_maturity_pull.property_radar_lead_handoff") as mock_handoff, \
             patch("src.tasks.property_radar_maturity_pull.ENABLED_STATES", frozenset({"FL"})):
            self._enabled_settings(mock_settings)
            mock_run_pull.return_value = {"run_id": 1}
            mock_handoff.run.return_value.summary.return_value = "ok"
            self._run_main([
                "--mode", "daily", "--state", "FL",
                "--campaign", "maturity_target_lender", "--apply-handoff",
            ])
            mock_handoff.run.assert_called_once_with(campaign="maturity_target_lender", apply=True)

    def test_skip_handoff_flag_prevents_the_call(self):
        with patch("src.tasks.property_radar_maturity_pull.settings") as mock_settings, \
             patch("src.tasks.property_radar_maturity_pull.get_db_context"), \
             patch("src.tasks.property_radar_maturity_pull._run_pull") as mock_run_pull, \
             patch("src.tasks.property_radar_maturity_pull.property_radar_lead_handoff") as mock_handoff, \
             patch("src.tasks.property_radar_maturity_pull.ENABLED_STATES", frozenset({"FL"})):
            self._enabled_settings(mock_settings)
            mock_run_pull.return_value = {"run_id": 1}
            self._run_main(["--mode", "daily", "--state", "FL", "--skip-handoff"])
            mock_handoff.run.assert_not_called()

    def test_handoff_failure_does_not_propagate(self):
        """A handoff-layer exception must not crash main() or be raised to the
        cron wrapper -- the pull_run row is already committed as 'done' by the
        time this runs, and that success must stand regardless."""
        with patch("src.tasks.property_radar_maturity_pull.settings") as mock_settings, \
             patch("src.tasks.property_radar_maturity_pull.get_db_context"), \
             patch("src.tasks.property_radar_maturity_pull._run_pull") as mock_run_pull, \
             patch("src.tasks.property_radar_maturity_pull.property_radar_lead_handoff") as mock_handoff, \
             patch("src.tasks.property_radar_maturity_pull.ENABLED_STATES", frozenset({"FL"})):
            self._enabled_settings(mock_settings)
            mock_run_pull.return_value = {"run_id": 1}
            mock_handoff.run.side_effect = RuntimeError("handoff DB blew up")
            self._run_main(["--mode", "daily", "--state", "FL"])  # must not raise


# ---------------------------------------------------------------------------
# End-to-end integration: the pull must actually write into Dev 2's staging
# table, not just print to stdout. This is what earlier silently regressed —
# _run_pull() only emitted JSON to stdout and never called upsert_records(),
# so nothing landed in property_radar_records regardless of how correct the
# normalizer's output looked. Real Postgres, both migrations applied.
# ---------------------------------------------------------------------------

class TestPullWritesToStagingTable:
    # Must be a real registered campaign key (build_campaign_criteria raises
    # otherwise) — isolate test data by radar_id prefix instead of a fake
    # campaign name.
    CAMPAIGN = "maturity_target_lender"
    RADAR_ID_PREFIX = "INTTEST"

    def _cleanup(self, session):
        from sqlalchemy import text
        session.execute(text("DELETE FROM property_radar_records WHERE radar_id LIKE :p"), {"p": f"{self.RADAR_ID_PREFIX}%"})
        session.execute(text("DELETE FROM property_radar_seen_ids WHERE radar_id LIKE :p"), {"p": f"{self.RADAR_ID_PREFIX}%"})
        session.commit()

    def test_backlog_pull_actually_inserts_into_property_radar_records(self):
        from sqlalchemy import text
        from unittest.mock import patch
        from src.core.database import get_db_context
        from src.tasks.property_radar_maturity_pull import _run_pull

        raw_records = [
            _raw_record("INTTEST0001", county="HILLSBOROUGH"),
            _raw_record("INTTEST0002", county="PINELLAS"),
        ]
        fake_port = FakePropertyRadarPort(canned_records=raw_records)

        with get_db_context() as session:
            self._cleanup(session)
            run_id = None
            try:
                with patch(
                    "src.tasks.property_radar_maturity_pull.get_property_radar_port",
                    return_value=fake_port,
                ):
                    result = _run_pull(
                        mode="backlog", state="FL", campaign=self.CAMPAIGN,
                        dry_run=False, session=session,
                    )
                run_id = result["run_id"]
                assert result["records_fetched"] == 2

                rows = session.execute(
                    text(
                        "SELECT radar_id, property_address, zip, lender_name, "
                        "loan_recorded_date, mailing_address, county_name "
                        "FROM property_radar_records WHERE radar_id LIKE :p ORDER BY radar_id"
                    ),
                    {"p": f"{self.RADAR_ID_PREFIX}%"},
                ).mappings().all()
                assert len(rows) == 2
                # The exact bug this test guards against: a field-name mismatch
                # between the normalizer and staging._COLUMNS silently lands as
                # NULL here even though the normalizer's own output looked correct.
                for row in rows:
                    assert row["property_address"] == "100 MAIN ST"
                    assert row["zip"] == "33601"
                    assert row["lender_name"] == "KIAVI FNDG INC"
                    assert row["loan_recorded_date"] is not None
                    assert row["mailing_address"] == "500 MAILING AVE"
                    assert row["county_name"] in ("HILLSBOROUGH", "PINELLAS")
            finally:
                self._cleanup(session)
                if run_id is not None:
                    session.execute(text("DELETE FROM property_radar_pull_runs WHERE id = :id"), {"id": run_id})
                    session.commit()

    def test_rerun_is_idempotent_no_duplicate_rows(self):
        """upsert_records() dedupes by (state_fips, county_fips, apn); running
        the same batch twice must not create duplicate property_radar_records rows."""
        from sqlalchemy import text
        from unittest.mock import patch
        from src.core.database import get_db_context
        from src.services.property_radar.staging import upsert_records
        from src.tasks.property_radar_maturity_pull import _to_staging_dict
        from src.services.property_radar_normalizer import normalize
        from src.services.property_radar_port import PropertyRadarRecord

        raw = _raw_record("INTTEST0003", county="HILLSBOROUGH")
        record = PropertyRadarRecord(radar_id="INTTEST0003", raw=raw)
        normalized = normalize(record, state="FL", campaign=self.CAMPAIGN)
        staging_dict = _to_staging_dict(normalized)

        with get_db_context() as session:
            self._cleanup(session)
            try:
                upsert_records(session, [staging_dict])
                upsert_records(session, [staging_dict])
                session.commit()
                count = session.execute(
                    text("SELECT COUNT(*) FROM property_radar_records WHERE radar_id LIKE :p"),
                    {"p": f"{self.RADAR_ID_PREFIX}%"},
                ).scalar()
                assert count == 1
            finally:
                self._cleanup(session)
