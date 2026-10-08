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
    _phones_in_other_pools,
    _pick_phone,
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
        monkeypatch.setattr(bridge, "_phones_in_other_pools", lambda session, run_id, state=None: set())
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

    def test_shared_traced_phone_goes_to_one_row_only(self, monkeypatch):
        bridge, calls = self._patch(monkeypatch, latest="extract-run-1")
        r1, r2 = TestBuildPoolRow()._record(radar_id="R1"), TestBuildPoolRow()._record(radar_id="R2")
        monkeypatch.setattr(bridge, "_iter_record_pages", lambda session, state: iter([[r1], [r2]]))
        shared = {"phones": ["8135550142"], "emails": []}
        monkeypatch.setattr(bridge, "_load_trace_contacts",
                            lambda session, keys: {k: shared for k in keys})
        bridge.extract_pr_maturity_pool(session=None, dry_run=False)
        assert [r["phone_available"] for r in calls["written"]] == [True, False]

    def test_dry_run_clears_nothing(self, monkeypatch):
        bridge, calls = self._patch(monkeypatch, latest="extract-run-1")
        bridge.extract_pr_maturity_pool(session=None, dry_run=True)
        assert calls["cleared"] == [] and calls["written"] == []


def test_traced_contacts_are_read_from_the_table_the_live_trace_writes_by_radar_id():
    """The live trace writes property_radar_traced_contacts keyed by radar_id; the bridge must
    read that table, not an address-keyed copy nobody writes."""
    from unittest.mock import MagicMock

    from src.lending import pr_maturity_bridge as bridge

    session = MagicMock()
    session.execute.return_value.mappings.return_value.all.return_value = [
        {"radar_id": "R1", "phones": ["8135550111"], "emails": ["a@b.co"]},
        {"radar_id": "R2", "phones": None, "emails": None},
    ]
    got = bridge._load_trace_contacts(session, ["R1", "R2"])
    sql = str(session.execute.call_args.args[0])
    assert "property_radar_traced_contacts" in sql and "radar_id" in sql and "trace_key" not in sql
    assert got == {"R1": {"phones": ["8135550111"], "emails": ["a@b.co"]}, "R2": {"phones": [], "emails": []}}
    assert bridge._load_trace_contacts(session, []) == {}


class TestPickPhone:
    def test_first_free_phone_is_taken_and_reserved(self):
        taken: set[str] = set()
        assert _pick_phone(["(813) 555-0142"], taken) == "+18135550142"
        assert taken == {"+18135550142"}

    def test_same_phone_on_a_second_record_is_not_reused(self):
        taken = {"+18135550142"}
        assert _pick_phone(["813-555-0142"], taken) is None

    def test_second_phone_is_used_when_first_is_taken(self):
        taken = {"+18135550142"}
        assert _pick_phone(["8135550142", "8135550199"], taken) == "+18135550199"
        assert "+18135550199" in taken

    def test_junk_and_empty_lists_give_none(self):
        assert _pick_phone(["nope"], set()) is None
        assert _pick_phone([], set()) is None


class _PhoneSession:
    def __init__(self, phones):
        self._phones, self.params = phones, None

    def execute(self, _stmt, params=None):
        self.params = params
        phones = self._phones

        class _R:
            def scalars(self_inner):
                return phones
        return _R()


def test_phones_in_other_pools_excludes_our_pool_and_nulls():
    session = _PhoneSession(["+18135550142", None, "junk", "(813) 555-0199"])
    assert _phones_in_other_pools(session, "run-1") == {"+18135550142", "+18135550199"}
    assert session.params == {"run_id": "run-1", "pool": "pr_maturity"}


def test_state_scoped_run_also_reserves_other_states_of_our_pool():
    session = _PhoneSession([])
    _phones_in_other_pools(session, "run-1", "fl")
    assert session.params == {"run_id": "run-1", "pool": "pr_maturity", "state": "FL"}
