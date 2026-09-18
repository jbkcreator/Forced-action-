"""WP-T2-8 Stage A/A′ — loader enrichment + staging tests (mock-based, no DB)."""
from datetime import date
from types import MethodType, SimpleNamespace
from unittest.mock import Mock

import pandas as pd

from src.loaders.permits import (
    BuildingPermitLoader,
    _clean_str,
    _normalize_completion_status,
    _parse_job_value,
)


# ── pure helpers ──────────────────────────────────────────────────────────────

def test_parse_job_value_strips_currency_formatting():
    assert _parse_job_value("$250,000") == 250000.0
    assert _parse_job_value("1000") == 1000.0


def test_parse_job_value_rejects_zero_and_junk():
    assert _parse_job_value("0") is None
    assert _parse_job_value("$0.00") is None
    assert _parse_job_value("N/A") is None
    assert _parse_job_value(None) is None


def test_normalize_completion_status_maps_synonyms():
    assert _normalize_completion_status("Issued") == "issued"
    assert _normalize_completion_status("Finaled") == "completed"
    assert _normalize_completion_status("Open") == "active"
    assert _normalize_completion_status("Revoked") == "expired"


def test_normalize_completion_status_unknown_is_none():
    assert _normalize_completion_status("banana") is None
    assert _normalize_completion_status(None) is None


def test_clean_str_guards_nan():
    import numpy as np
    assert _clean_str(np.nan) is None          # blank CSV cell → NaN, not "nan"
    assert _clean_str(float("nan")) is None
    assert _clean_str(None) is None
    assert _clean_str("  ") is None
    assert _clean_str("  ACME  ") == "ACME"


# ── matched permit gets enrichment fields ─────────────────────────────────────

def test_matched_permit_populates_enrichment_fields():
    session = Mock()
    session.execute.return_value.fetchone.return_value = None  # no duplicate
    prop = SimpleNamespace(id=44)
    loader = SimpleNamespace(
        session=session,
        county_id="hillsborough",
        _thresholds=SimpleNamespace(address_floor=75),
        find_property_by_address=Mock(return_value=(prop, 100)),
        parse_date=Mock(return_value=date(2026, 9, 1)),
        safe_add=Mock(return_value=True),
        _promote_from_staging=Mock(),
    )
    frame = pd.DataFrame([{
        "Record Number": "P-enrich",
        "Record Type": "New Single Family",
        "Status": "Issued",
        "Address": "1 Test Way, Tampa, FL 33602",
        "Date": "2026-09-01",
        "Expiration Date": "2027-09-01",
        "Holder Name": "ACME HOMES LLC",
        "Contractor Name": "BUILDPRO INC",
        "Job Value": "$250,000",
    }])

    matched, unmatched, skipped = BuildingPermitLoader.load_from_dataframe(loader, frame)

    assert (matched, unmatched, skipped) == (1, 0, 0)
    permit = loader.safe_add.call_args.args[0]
    assert permit.holder_name == "ACME HOMES LLC"
    assert permit.contractor_name == "BUILDPRO INC"
    assert float(permit.job_value) == 250000.0
    assert permit.completion_status == "issued"


# ── unmatched permit is staged, not dropped ───────────────────────────────────

def test_unmatched_permit_persists_to_staging():
    session = Mock()
    session.execute.return_value.fetchone.return_value = None  # no duplicate
    loader = SimpleNamespace(
        session=session,
        county_id="pinellas",
        _thresholds=SimpleNamespace(address_floor=75),
        find_property_by_address=Mock(return_value=None),  # NO property match
        parse_date=Mock(return_value=date(2026, 9, 1)),
        safe_add=Mock(return_value=True),
        _promote_from_staging=Mock(),
        quarantine_unmatched=Mock(),
    )
    # bind the real _persist_to_staging so it actually runs
    loader._persist_to_staging = MethodType(BuildingPermitLoader._persist_to_staging, loader)
    frame = pd.DataFrame([{
        "Record Number": "P-vacant-lot",
        "Record Type": "New Construction",
        "Status": "Issued",
        "Address": "999 Empty Lot Rd, Largo, FL 33770",
        "Date": "2026-09-01",
        "Expiration Date": "2027-09-01",
        "Holder Name": "SPEC BUILDERS LLC",
        "Contractor Name": "SPEC BUILDERS LLC",
        "Job Value": "$400,000",
    }])

    matched, unmatched, skipped = BuildingPermitLoader.load_from_dataframe(loader, frame)

    assert (matched, unmatched, skipped) == (0, 1, 0)
    loader.quarantine_unmatched.assert_called_once()
    # 2nd execute = the staging INSERT (1st was the duplicate-check SELECT)
    insert_call = session.execute.call_args_list[1]
    sql = str(insert_call.args[0])
    params = insert_call.args[1]
    assert "INSERT INTO permit_staging" in sql
    assert params["permit_number"] == "P-vacant-lot"
    assert params["holder_name"] == "SPEC BUILDERS LLC"
    assert params["job_value"] == 400000.0
    assert params["county_id"] == "pinellas"


# ── upsert idempotency: duplicate backfills enrichment when NULL ───────────────

def test_duplicate_permit_backfills_enrichment_columns():
    existing = SimpleNamespace(
        id=12, description="roof", status="Issued",
        holder_name=None, contractor_name=None,
        job_value=None, completion_status=None,
    )
    session = Mock()
    session.execute.return_value.fetchone.return_value = existing
    loader = SimpleNamespace(session=session)
    frame = pd.DataFrame([{
        "Record Number": "P-12",
        "Description": "roof",
        "Status": "Complete",
        "Holder Name": "LATE ARRIVING LLC",
        "Contractor Name": "FINISH CO",
        "Job Value": "$150,000",
    }])

    matched, unmatched, skipped = BuildingPermitLoader.load_from_dataframe(loader, frame)

    assert (matched, unmatched, skipped) == (0, 0, 1)
    update_call = session.execute.call_args_list[1]
    sql = str(update_call.args[0])
    params = update_call.args[1]
    assert "holder_name       = COALESCE(holder_name, :holder_name)" in sql
    assert params["holder_name"] == "LATE ARRIVING LLC"
    assert params["contractor_name"] == "FINISH CO"
    assert params["job_value"] == 150000.0
    assert params["completion_status"] == "completed"


def test_duplicate_backfills_enrichment_when_status_and_description_unchanged():
    # existing permit already has description + status; a LATER scrape adds
    # enrichment fields. Backfill must fire even though desc/status are unchanged.
    existing = SimpleNamespace(
        id=77, description="roof", status="Issued",
        holder_name=None, contractor_name=None,
        job_value=None, completion_status=None,
    )
    session = Mock()
    session.execute.return_value.fetchone.return_value = existing
    loader = SimpleNamespace(session=session)
    frame = pd.DataFrame([{
        "Record Number": "P-77",
        "Description": "roof",       # unchanged
        "Status": "Issued",          # unchanged
        "Holder Name": "NEWLY SEEN LLC",
        "Job Value": "$220,000",
    }])

    matched, unmatched, skipped = BuildingPermitLoader.load_from_dataframe(loader, frame)

    assert (matched, unmatched, skipped) == (0, 0, 1)
    # an UPDATE was still issued (2nd execute call)
    assert len(session.execute.call_args_list) == 2
    params = session.execute.call_args_list[1].args[1]
    assert params["holder_name"] == "NEWLY SEEN LLC"
    assert params["job_value"] == 220000.0


def test_blank_holder_cell_does_not_create_nan_identity():
    session = Mock()
    session.execute.return_value.fetchone.return_value = None
    prop = SimpleNamespace(id=55)
    loader = SimpleNamespace(
        session=session,
        county_id="hillsborough",
        _thresholds=SimpleNamespace(address_floor=75),
        find_property_by_address=Mock(return_value=(prop, 100)),
        parse_date=Mock(return_value=date(2026, 9, 1)),
        safe_add=Mock(return_value=True),
        _promote_from_staging=Mock(),
    )
    # blank Holder/Contractor cells → pandas NaN under read_csv(dtype=str)
    frame = pd.DataFrame({
        "Record Number": ["P-blank"],
        "Record Type": ["New Construction"],
        "Status": ["Issued"],
        "Address": ["1 Test Way, Tampa, FL 33602"],
        "Date": ["2026-09-01"],
        "Expiration Date": ["2027-09-01"],
        "Holder Name": [None],
        "Contractor Name": [None],
        "Job Value": [None],
    }).astype(object)
    frame.loc[0, ["Holder Name", "Contractor Name", "Job Value"]] = float("nan")

    BuildingPermitLoader.load_from_dataframe(loader, frame)

    permit = loader.safe_add.call_args.args[0]
    assert permit.holder_name is None       # not the string "nan"
    assert permit.contractor_name is None
    assert permit.job_value is None
