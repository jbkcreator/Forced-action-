"""Unit tests for the PropertyRadar -> calling-pool bridge (pure mapping)."""
from decimal import Decimal

import pytest

from config.lending_compliance import GEORGIA_ALLOWED_ENTITY_TYPES
from src.lending.pr_maturity_bridge import (
    CAMPAIGN_TAG,
    POOL_NAME,
    SOURCE_TABLE,
    _compose_address,
    _first_phone,
    _to_decimal,
    build_pool_row,
    map_entity_status,
)


class TestMapEntityStatus:
    @pytest.mark.parametrize("owner, otype, expected", [
        ("ACME HOLDINGS LLC", "Company", "LLC"),
        ("SMITH FAMILY TRUST", "Trust", "TRUST"),
        ("BIG CAPITAL INC", "Company", "CORPORATION"),
        ("REDSTONE CORP", None, "CORPORATION"),
        ("JOHN Q PUBLIC", "Individual", "NATURAL_PERSON"),
        (None, "Corporate", "CORPORATION"),
        (None, "Individual", "NATURAL_PERSON"),
    ])
    def test_classification(self, owner, otype, expected):
        assert map_entity_status(otype, owner) == expected

    @pytest.mark.parametrize("name", [
        "RALPH SMITH",       # contains "LP" as a substring
        "ALPHONSE GREEN",    # contains "LP"
        "VINCENT ROBINSON",  # contains "INC"
        "PRINCESS WARE",     # contains "INC"
    ])
    def test_natural_person_names_with_entity_substrings_are_not_businesses(self, name):
        # Must match on word boundaries, not substrings: a GA natural person
        # misread as CORPORATION would fail the GA cold-calling gate open.
        assert map_entity_status("Individual", name) == "NATURAL_PERSON"

    def test_nothing_to_classify_is_none(self):
        assert map_entity_status(None, None) is None
        assert map_entity_status("", "  ") is None

    def test_ga_dialable_types_are_a_subset_of_the_compliance_allow_list(self):
        # LLC + CORPORATION must both pass the GA gate; TRUST / NATURAL_PERSON must not.
        assert {"LLC", "CORPORATION"} <= GEORGIA_ALLOWED_ENTITY_TYPES
        assert "TRUST" not in GEORGIA_ALLOWED_ENTITY_TYPES
        assert "NATURAL_PERSON" not in GEORGIA_ALLOWED_ENTITY_TYPES


class TestHelpers:
    def test_compose_address_skips_blanks(self):
        assert _compose_address("1 Main St", None, "GA", "30301") == "1 Main St, GA, 30301"
        assert _compose_address(None, None, None, None) is None

    def test_to_decimal(self):
        assert _to_decimal(250000) == Decimal(250000)
        assert _to_decimal(None) is None
        assert _to_decimal("oops") is None

    def test_first_phone_normalizes_and_skips_junk(self):
        assert _first_phone(["not-a-number", "(813) 555-0142"]) == "+18135550142"
        assert _first_phone([]) is None


class TestBuildPoolRow:
    def _record(self, **over):
        base = {
            "radar_id": "R1", "apn": "12-34", "state": "GA", "county_name": "FULTON",
            "property_address": "5 Oak Ave", "city": "Atlanta", "zip": "30301",
            "owner_name": "ACME LLC", "ownership_type": "Company",
            "principal_name": "Jane Doe", "loan_amount": 300000,
            "campaign": "private_maturity", "property_id": 42,
        }
        base.update(over)
        return base

    def test_ga_corporate_with_phone_is_dialable(self):
        row = build_pool_row(self._record(), run_id="run-1", phone="+14045550100", email="a@b.com")
        assert row["pool_name"] == POOL_NAME
        assert row["source_table"] == SOURCE_TABLE
        assert row["aircall_campaign_tag"] == CAMPAIGN_TAG
        assert row["entity_status"] == "LLC"          # GA-allowed
        assert row["phone_available"] is True
        assert row["borrower_name"] == "Jane Doe"     # principal preferred over owner
        assert row["entity_name"] == "ACME LLC"
        assert row["estimated_loan_value"] == Decimal(300000)
        assert row["source_property_id"] == 42
        assert row["source_tag"] == "list_8"  # private_maturity -> list_8 (Verified maturity queue)

    def test_campaign_maps_to_brief_source_list(self):
        for campaign, tag in [("private_maturity", "list_8"),
                              ("stalled_flip", "list_9"), ("auction_winner", "list_6")]:
            row = build_pool_row(self._record(campaign=campaign), run_id="r", phone="+14045550100", email=None)
            assert row["source_tag"] == tag

    def test_maturity_target_lender_splits_by_state(self):
        ga = build_pool_row(self._record(campaign="maturity_target_lender", state="GA"),
                            run_id="r", phone="+14045550100", email=None)
        fl = build_pool_row(self._record(campaign="maturity_target_lender", state="FL"),
                            run_id="r", phone="+18135550100", email=None)
        assert ga["source_tag"] == "list_5"  # GA maturity
        assert fl["source_tag"] == "list_1"  # FL maturity

    def test_ga_natural_person_status_is_set_so_gate_can_block(self):
        row = build_pool_row(
            self._record(owner_name="JOHN SMITH", ownership_type="Individual", principal_name=None),
            run_id="run-1", phone="+14045550100", email=None,
        )
        assert row["entity_status"] == "NATURAL_PERSON"
        assert row["entity_status"] not in GEORGIA_ALLOWED_ENTITY_TYPES
        assert row["entity_name"] is None

    def test_no_phone_still_builds_row(self):
        row = build_pool_row(self._record(), run_id="run-1", phone=None, email=None)
        assert row["phone_available"] is False
        assert row["normalized_phone"] is None


class TestDialerCompatibility:
    """Review #329 finding 1: bridged rows must survive the dialer's queue assignment."""

    def test_bridged_rows_are_not_dropped_by_launch_queue_records(self):
        from src.tasks.lending_dialer_load import launch_queue_records

        records = []
        for campaign, state in [("private_maturity", "FL"), ("private_maturity", "GA"),
                                ("maturity_target_lender", "FL"), ("maturity_target_lender", "GA"),
                                ("stalled_flip", "FL"), ("auction_winner", "GA")]:
            row = build_pool_row(
                TestBuildPoolRow()._record(campaign=campaign, state=state),
                run_id="r", phone="+14045550100", email=None,
            )
            records.append({"source_tag": row["source_tag"], "phone": row["normalized_phone"]})
        queued = launch_queue_records(records)
        assert len(queued) == len(records)
        assert {r["queue"] for r in queued} == {"verified_maturity", "transaction_ready"}


class TestRunSelection:
    """Review #329 finding 2: the bridge must not start a run that crowds out other pools."""

    def _patch(self, monkeypatch, latest):
        import src.lending.pr_maturity_bridge as bridge

        calls = {"cleared": [], "written": []}
        record = TestBuildPoolRow()._record()
        monkeypatch.setattr(bridge, "latest_run_id", lambda session: latest)
        monkeypatch.setattr(bridge, "_iter_record_pages", lambda session, state: iter([[record]]))
        monkeypatch.setattr(bridge, "_load_trace_contacts", lambda session, keys: {})
        monkeypatch.setattr(bridge, "_clear_previous_rows",
                            lambda session, run_id, state: calls["cleared"].append(run_id))
        monkeypatch.setattr(bridge, "_write_rows",
                            lambda session, rows: calls["written"].extend(rows) or len(rows))
        return bridge, calls

    def test_rows_join_the_newest_existing_run(self, monkeypatch):
        bridge, calls = self._patch(monkeypatch, latest="extract-run-1")
        summary = bridge.extract_pr_maturity_pool(session=None, dry_run=False)
        assert summary["run_id"] == "extract-run-1"
        assert calls["cleared"] == ["extract-run-1"]  # previous pr_maturity rows replaced
        assert {r["run_id"] for r in calls["written"]} == {"extract-run-1"}

    def test_new_run_only_when_none_exists(self, monkeypatch):
        bridge, calls = self._patch(monkeypatch, latest=None)
        summary = bridge.extract_pr_maturity_pool(session=None, dry_run=False)
        assert summary["run_id"] and summary["run_id"] != "extract-run-1"

    def test_dry_run_clears_nothing(self, monkeypatch):
        bridge, calls = self._patch(monkeypatch, latest="extract-run-1")
        bridge.extract_pr_maturity_pool(session=None, dry_run=True)
        assert calls["cleared"] == [] and calls["written"] == []
