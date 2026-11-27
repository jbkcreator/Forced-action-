"""Pool records from one calling-pool staging run.

The staging table accumulates every extraction run, so readers always name a
run (default: the newest) and never mix runs. Output matches the JSON the dialer
load and queue report take.

R1 0.6: PropertyRadar freshness is judged per (state, campaign) segment, because the nightly runner pulls each one
separately. The judged segments come from the configured nightly schedule (``scheduled_segments``), not from run
history, so a segment seeded by an old backlog pull whose first nightly pull failed is still stale. One whose newest
successful pull is older than ``PROPERTYRADAR_MAX_AGE_HOURS``, or that has none, is left out of staging and loading and
reported (``stale_segments``); the fresh segments still load. Segments outside the schedule are not judged.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from sqlalchemy import text

from config.lending_fallbacks import PROPERTYRADAR_MAX_AGE_HOURS, PROPERTYRADAR_NIGHTLY_CAMPAIGNS

# Both pull paths (property_radar_maturity_pull and property_radar_runner) close their run row 'done' on success.
_NEWEST_PULLS = text("""
    SELECT state, campaign, max(finished_at) AS newest
      FROM property_radar_pull_runs
     WHERE status = 'done'
     GROUP BY state, campaign
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


def scheduled_segments() -> frozenset[Segment]:
    """The (state, campaign) pairs the nightly cron pulls under the current switches, from configuration, not from
    run history: a segment whose first nightly pull failed has no history yet and must still be judged."""
    from config.property_radar_campaigns import ENABLED_STATES
    from config.settings import get_settings

    settings = get_settings()
    if not settings.property_radar_enabled:
        return frozenset()
    if not settings.property_radar_statewide_enabled:
        return frozenset({("FL", "maturity_target_lender")})
    return frozenset((state, campaign) for state in ENABLED_STATES for campaign in PROPERTYRADAR_NIGHTLY_CAMPAIGNS)


def stale_segments(db, now: Optional[datetime] = None, *, max_age_hours: int = PROPERTYRADAR_MAX_AGE_HOURS,
                   scheduled: Optional[frozenset[Segment]] = None) -> dict[Segment, Staleness]:
    """The scheduled segments whose newest successful pull is too old, or that never had one, to load."""
    now = now or datetime.now(timezone.utc)
    scheduled = scheduled_segments() if scheduled is None else scheduled
    newest = {(state, campaign): at for state, campaign, at in db.execute(_NEWEST_PULLS)}
    stale: dict[Segment, Staleness] = {}
    for segment in scheduled:
        at = newest.get(segment)
        if at is None:
            stale[segment] = Staleness(*segment, None)
        elif now - at > timedelta(hours=max_age_hours):
            stale[segment] = Staleness(*segment, int((now - at).total_seconds() // 3600))
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
