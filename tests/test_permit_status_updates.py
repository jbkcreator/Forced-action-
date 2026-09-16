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
    assert "status = CASE WHEN :status IS NOT NULL" in update_sql
    assert session.execute.call_args_list[1].args[1]["status"] == "Complete"
