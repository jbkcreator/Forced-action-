"""
County launch T+24 Revenue Pulse.

Fires once per launched county, exactly after 24 hours have elapsed since
expansion_candidates.launched_at. Idempotent: revenue_pulse_sent_at is
stamped on success and acts as a permanent guard — subsequent cron ticks
are no-ops for that county.

Reuses run_daily_pulse(county_id=...) from revenue_pulse.py — no changes
to the pulse task itself.

Cron:
    */15 * * * *   $PROJECT/scripts/cron/run.sh src.tasks.county_launch_pulse

Usage:
    python -m src.tasks.county_launch_pulse [--dry-run]
"""
import logging
import sys
from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.core.database import get_db_context
from src.core.models import CountyLaunchAudit, ExpansionCandidate
from src.tasks.revenue_pulse import run_daily_pulse

logger = logging.getLogger(__name__)

T24 = timedelta(hours=24)


def run_county_launch_pulse(dry_run: bool = False) -> dict:
    """Send the T+24 county-specific Revenue Pulse for newly launched counties."""
    now = datetime.now(timezone.utc)
    cutoff = now - T24

    with get_db_context() as db:
        candidates = db.execute(
            select(ExpansionCandidate).where(
                ExpansionCandidate.status == "launched",
                ExpansionCandidate.launched_at <= cutoff,
                ExpansionCandidate.revenue_pulse_sent_at.is_(None),
            )
        ).scalars().all()

        if not candidates:
            logger.info("[CountyLaunchPulse] no counties due for T+24 pulse")
            return {"no_pending_counties": True}

        results = []
        for candidate in candidates:
            result = _send_pulse(db, candidate, dry_run)
            results.append(result)
            logger.info(
                "[CountyLaunchPulse] county=%s sent=%s dry_run=%s",
                candidate.county_id, result["sent"], dry_run,
            )

    return {"processed": results}


def _send_pulse(db: Session, candidate: ExpansionCandidate, dry_run: bool) -> dict:
    county_id = candidate.county_id
    sent = False

    try:
        result = run_daily_pulse(county_id=county_id, dry_run=dry_run)
        sent = result.get("sent", False) or dry_run
    except Exception as exc:
        logger.error("[CountyLaunchPulse] pulse failed county=%s: %s", county_id, exc)
        _write_audit(db, county_id, detail={"sent": False, "error": str(exc), "dry_run": dry_run})
        return {"county_id": county_id, "sent": False, "dry_run": dry_run}

    if not dry_run:
        candidate.revenue_pulse_sent_at = datetime.now(timezone.utc)
        db.commit()

    _write_audit(db, county_id, detail={"sent": sent, "dry_run": dry_run})
    return {"county_id": county_id, "sent": sent, "dry_run": dry_run}


def _write_audit(db: Session, county_id: str, detail: dict | None = None) -> None:
    row = CountyLaunchAudit(
        county_id=county_id,
        event_type="revenue_pulse_sent",
        actor="county_launch_pulse",
        detail=detail,
    )
    db.add(row)
    db.commit()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    dry = "--dry-run" in sys.argv
    import json
    result = run_county_launch_pulse(dry_run=dry)
    print(json.dumps(result, indent=2, default=str))
