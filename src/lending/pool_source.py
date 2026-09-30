"""Pool records from one calling-pool staging run.

The staging table accumulates every extraction run, so readers always name a
run (default: the newest) and never mix runs. Output matches the JSON the dialer
load and queue report take.
"""
from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import text

_COLUMNS = (
    "id, run_id, pool_name, source_tag, normalized_phone, email, borrower_name, entity_name, "
    "target_property_address, estimated_loan_value, recent_permit_details, parcel_id, state, "
    "entity_status, zip"
)


def latest_run_id(db) -> Optional[str]:
    run_id = db.execute(text(
        "SELECT run_id FROM lending_calling_pool_staging GROUP BY run_id ORDER BY max(created_at) DESC LIMIT 1"
    )).scalar()
    return str(run_id) if run_id is not None else None


def staged_pool_records(db, *, run_id: Optional[str] = None) -> list[dict[str, Any]]:
    run_id = run_id or latest_run_id(db)
    if run_id is None:
        return []
    rows = db.execute(
        text(f"SELECT {_COLUMNS} FROM lending_calling_pool_staging WHERE run_id = CAST(:r AS uuid) ORDER BY id"),
        {"r": run_id},
    ).mappings().all()
    return [
        {
            "source_record_ref": f"staging:{row['id']}",
            "run_id": str(row["run_id"]),
            "staging_pool": row["pool_name"],
            "source_tag": row["source_tag"],
            "phone": row["normalized_phone"],
            "email": row["email"],
            "borrower_name": row["borrower_name"],
            "entity_name": row["entity_name"],
            "property_address": row["target_property_address"],
            "estimated_loan_value": row["estimated_loan_value"],
            "recent_permit_details": row["recent_permit_details"],
            "parcel_id": row["parcel_id"],
            "state": row["state"],
            "entity_status": row["entity_status"],
            "zip": row["zip"],
        }
        for row in rows
    ]
