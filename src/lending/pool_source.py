"""Pool records from one calling-pool staging run.

The staging table accumulates every extraction run, so readers always name a
run (default: the newest) and never mix runs. Output matches the JSON the dialer
load and queue report take.

R1 0.6: PropertyRadar freshness is judged per (state, campaign) segment, because the nightly runner pulls each one
separately: a segment with a nightly (``daily``) pull whose newest successful pull is older than
``PROPERTYRADAR_MAX_AGE_HOURS`` is left out of staging and loading and reported (``stale_segments``), while the fresh
segments still load. A segment only ever pulled as a one-time backlog has no nightly feed that can fail, so its age is
not judged.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from sqlalchemy import text

from config.lending_fallbacks import PROPERTYRADAR_MAX_AGE_HOURS

# Both pull paths (property_radar_maturity_pull and property_radar_runner) close their run row 'done' on success.
_SCHEDULED_SEGMENTS = text("""
    SELECT state, campaign, max(finished_at) FILTER (WHERE status = 'done') AS newest
      FROM property_radar_pull_runs
     GROUP BY state, campaign
    HAVING bool_or(run_type = 'daily')
""")

Segment = tuple[str, str]  # (state, campaign)


@dataclass(frozen=True)
class Staleness:
    """Why one PropertyRadar segment may not be loaded: its age in whole hours (None = no successful pull)."""

    state: str
    campaign: str
    age_hours: Optional[int]

    def message(self) -> str:
        name = f"PropertyRadar {self.state} {self.campaign}"
        if self.age_hours is None:
            return f"{name}: no successful pull on record — not loaded"
        return f"{name} data is {self.age_hours} h old — not loaded"


def stale_segments(db, now: Optional[datetime] = None, *,
                   max_age_hours: int = PROPERTYRADAR_MAX_AGE_HOURS) -> dict[Segment, Staleness]:
    """The nightly-pulled segments whose newest successful pull is too old (or missing) to load."""
    now = now or datetime.now(timezone.utc)
    stale: dict[Segment, Staleness] = {}
    for state, campaign, newest in db.execute(_SCHEDULED_SEGMENTS):
        if newest is None:
            stale[(state, campaign)] = Staleness(state, campaign, None)
        elif now - newest > timedelta(hours=max_age_hours):
            stale[(state, campaign)] = Staleness(state, campaign, int((now - newest).total_seconds() // 3600))
    return stale


def stale_message(stale: dict[Segment, Staleness]) -> str:
    return "; ".join(s.message() for _, s in sorted(stale.items()))

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
