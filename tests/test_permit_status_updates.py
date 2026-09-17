from datetime import date
from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd

from src.loaders.permits import BuildingPermitLoader


def test_existing_permit_status_is_refreshed_when_source_closes_it():
    existing = SimpleNamespace(id=12, description="roof", status="Issued")
    session = Mock()
    session.execute.return_value.fetchone.return_value = existing
    loader = SimpleNamespace(session=session)
    frame = pd.DataFrame([{
        "Record Number": "P-12",
        "Description": "roof",
        "Status": "Complete",
    }])

    matched, unmatched, skipped = BuildingPermitLoader.load_from_dataframe(loader, frame)

    assert (matched, unmatched, skipped) == (0, 0, 1)
    update_sql = str(session.execute.call_args_list[1].args[0])
    assert "CASE WHEN :status IS NOT NULL" in update_sql
    assert session.execute.call_args_list[1].args[1]["status"] == "Complete"


def test_new_completed_permit_is_persisted_for_ledger_closure():
    session = Mock()
    session.execute.return_value.fetchone.return_value = None
    prop = SimpleNamespace(id=44)
    loader = SimpleNamespace(
        session=session,
        county_id="hillsborough",
        _thresholds=SimpleNamespace(address_floor=75),
        find_property_by_address=Mock(return_value=(prop, 100)),
        parse_date=Mock(return_value=date(2026, 9, 16)),
        safe_add=Mock(return_value=True),
    )
    frame = pd.DataFrame([{
        "Record Number": "P-closed",
        "Description": "roof",
        "Status": "Complete",
        "Record Type": "Roofing",
        "Address": "1 Test Way, Tampa, FL 33602",
        "Date": "2026-09-01",
        "Expiration Date": "2026-09-16",
    }])

    matched, unmatched, skipped = BuildingPermitLoader.load_from_dataframe(loader, frame)

    assert (matched, unmatched, skipped) == (1, 0, 0)
    permit = loader.safe_add.call_args.args[0]
    assert permit.status == "Complete"
