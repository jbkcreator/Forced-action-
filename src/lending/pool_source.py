"""Pool records from one calling-pool staging run.

The staging table accumulates every extraction run, so readers always name a
run (default: the newest) and never mix runs. Output matches the JSON the dialer
load and queue report take.

R1 0.6: PropertyRadar records are loaded only while the newest successful PropertyRadar pull is younger than
``PROPERTYRADAR_MAX_AGE_HOURS`` (``propertyradar_staleness``): after a failed overnight pull the morning load loads
nothing and says how old the data is, instead of re-loading an old list as if it were new.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from sqlalchemy import text

from config.lending_fallbacks import PROPERTYRADAR_MAX_AGE_HOURS

# Both pull paths (property_radar_maturity_pull and property_radar_runner) close their run row 'done' on success.
_NEWEST_PULL = text("SELECT max(finished_at) FROM property_radar_pull_runs WHERE status = 'done'")


@dataclass(frozen=True)
class Staleness:
    """Why PropertyRadar data may not be loaded: its age in whole hours (None = no successful pull at all)."""

    age_hours: Optional[int]

    def message(self) -> str:
        if self.age_hours is None:
            return "PropertyRadar data: no successful pull on record — nothing loaded"
        return f"PropertyRadar data is {self.age_hours} h old — nothing loaded"


def propertyradar_staleness(db, now: Optional[datetime] = None, *,
                            max_age_hours: int = PROPERTYRADAR_MAX_AGE_HOURS) -> Optional[Staleness]:
    """None when the newest successful PropertyRadar pull is fresh enough to load; otherwise why not."""
    finished = db.execute(_NEWEST_PULL).scalar()
    if finished is None:
        return Staleness(None)
    age = (now or datetime.now(timezone.utc)) - finished
    if age > timedelta(hours=max_age_hours):
        return Staleness(int(age.total_seconds() // 3600))
    return None

_COLUMNS = (
    "id, run_id, pool_name, source_tag, normalized_phone, email, borrower_name, entity_name, "
    "target_property_address, estimated_loan_value, recent_permit_details, parcel_id, state, "
    "entity_status, zip, county_name, homestead_exempt, source_property_id"
)


def latest_run_id(db) -> Optional[str]:
    run_id = db.execute(text(
        "SELECT run_id FROM lending.calling_pool_staging GROUP BY run_id ORDER BY max(created_at) DESC LIMIT 1"
    )).scalar()
    return str(run_id) if run_id is not None else None


def staged_pool_records(db, *, run_id: Optional[str] = None) -> list[dict[str, Any]]:
    run_id = run_id or latest_run_id(db)
    if run_id is None:
        return []
    rows = db.execute(
        text(f"SELECT {_COLUMNS} FROM lending.calling_pool_staging WHERE run_id = CAST(:r AS uuid) ORDER BY id"),
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
            "county": row["county_name"],
            "homestead_exempt": row["homestead_exempt"],
            "property_id": row["source_property_id"],
        }
        for row in rows
    ]
