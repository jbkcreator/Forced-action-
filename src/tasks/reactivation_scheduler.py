"""
reactivation_scheduler — Daily orchestrator for Sprint S0 Dormant Reactivation.

Two cohorts:
  county_live  — subscribers dormant/churned with geo interest in a county that
                 launched in the last COUNTY_LIVE_LAUNCH_WINDOW_DAYS days.
  sold_out     — subscribers dormant/churned with ZIP interest where
                 gold_plus_zip_snapshots recorded new Gold+ leads today.

CLI:
    python -m src.tasks.reactivation_scheduler --dry-run --cohort all --limit 5
    python -m src.tasks.reactivation_scheduler --cohort county_live --county-id pinellas
    python -m src.tasks.reactivation_scheduler --cohort sold_out --zip-code 33701
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.agents.events.ingestion import publish_cora_event
from src.core.database import get_db_context
from src.services.geo_interest import (
    get_subscribers_interested_in_county,
    get_subscribers_interested_in_zip,
)
from src.services.reactivation_eligibility import (
    check_county_live_eligibility,
    check_sold_out_zip_eligibility,
    check_tier3_winback_eligibility,
)

logger = logging.getLogger(__name__)

COUNTY_LIVE_LAUNCH_WINDOW_DAYS = 2
CAMPAIGN_COUNTY_LIVE = "reactivation_county_live"
CAMPAIGN_SOLD_OUT = "reactivation_sold_out_zip"
CAMPAIGN_TIER3_WINBACK = "reactivation_tier3_winback"

# Reasons that are lifecycle/contact gates — no value trying a different vertical.
_LIFECYCLE_STOP_REASONS = {"on_cooldown", "not_dormant_or_lapsed", "no_contact_info"}


# ── Dispatch ─────────────────────────────────────────────────────────────────

def _dispatch(
    sub,
    cohort: str,
    dry_run: bool,
    *,
    county_id: Optional[str] = None,
    zip_code: Optional[str] = None,
    vertical: Optional[str] = None,
    winback_branch: Optional[str] = None,
) -> bool:
    """
    Publish a reactivation_outreach Cora event for a subscriber.
    Cora's reactivation graph selects the channel, composes, sends, and logs.
    Returns True if the event was published (or would have been in dry_run).
    """
    if dry_run:
        channel_hint = "sms" if sub.phone else "email"
        logger.info(
            "reactivation_scheduler: [dry-run] would publish cohort=%s channel=%s sub_id=%s branch=%s",
            cohort, channel_hint, sub.id, winback_branch,
        )
        return True

    try:
        publish_cora_event({
            "event_type": "reactivation_outreach",
            "subscriber_id": sub.id,
            "payload": {
                "cohort": cohort,
                "county_id": county_id,
                "zip_code": zip_code,
                "vertical": vertical,
                "winback_branch": winback_branch,
            },
        })
        logger.info(
            "reactivation_scheduler: published cohort=%s sub_id=%s county=%s zip=%s",
            cohort, sub.id, county_id, zip_code,
        )
        return True
    except Exception:
        logger.exception(
            "reactivation_scheduler: publish error sub_id=%s cohort=%s", sub.id, cohort
        )
        return False


# ── Shared helpers ────────────────────────────────────────────────────────────

def _fetch_subscribers(sub_ids: list[int], db: Session) -> list:
    """Batch SELECT for a list of subscriber IDs. One round trip."""
    if not sub_ids:
        return []
    return db.execute(
        text("SELECT * FROM subscribers WHERE id = ANY(:ids)"),
        {"ids": sub_ids},
    ).all()


def _limit_reached(result: dict, limit: Optional[int]) -> bool:
    return limit is not None and result["sent"] >= limit


# ── County-Live cohort ────────────────────────────────────────────────────────

def _run_county_live(
    db: Session,
    dry_run: bool,
    limit: Optional[int],
    county_id_filter: Optional[str],
) -> dict:
    result: dict = {"checked": 0, "eligible": 0, "sent": 0, "skipped": 0, "errors": 0}
    sent_this_run: set[int] = set()
    cutoff = datetime.now(timezone.utc) - timedelta(days=COUNTY_LIVE_LAUNCH_WINDOW_DAYS)

    counties = db.execute(text("""
        SELECT county_id
        FROM expansion_candidates
        WHERE status = 'launched'
          AND launched_at >= :cutoff
          AND (:county_filter IS NULL OR county_id = :county_filter)
        ORDER BY launched_at DESC
    """), {"cutoff": cutoff, "county_filter": county_id_filter}).scalars().all()

    for county_id in counties:
        sub_ids = get_subscribers_interested_in_county(county_id, db)
        new_ids = [i for i in sub_ids if i not in sent_this_run]
        if not new_ids:
            continue

        subs = _fetch_subscribers(new_ids, db)
        for sub in subs:
            if _limit_reached(result, limit):
                return result

            result["checked"] += 1
            eligible, reason = check_county_live_eligibility(sub, county_id, db)
            if not eligible:
                logger.debug(
                    "reactivation_scheduler: skip sub_id=%s reason=%s cohort=county_live",
                    sub.id, reason,
                )
                result["skipped"] += 1
                continue

            result["eligible"] += 1
            ok = _dispatch(sub, "county_live", dry_run, county_id=county_id)
            if ok:
                result["sent"] += 1
                sent_this_run.add(sub.id)
            else:
                result["errors"] += 1

    return result


# ── Sold-Out cohort ───────────────────────────────────────────────────────────

def _get_zip_verticals(zip_code: str, county_id: str, db: Session) -> list[str]:
    """Distinct verticals with active interest in a ZIP (waitlist + territories)."""
    return db.execute(text("""
        SELECT vertical
        FROM waitlist_entries
        WHERE zip_code = :zip AND county_id = :county
          AND status IN ('waiting', 'notified', 'converted')
          AND vertical IS NOT NULL
        UNION
        SELECT vertical
        FROM zip_territories
        WHERE zip_code = :zip AND county_id = :county
          AND vertical IS NOT NULL
    """), {"zip": zip_code, "county": county_id}).scalars().all()


def _run_sold_out(
    db: Session,
    dry_run: bool,
    limit: Optional[int],
    zip_code_filter: Optional[str],
) -> dict:
    result: dict = {"checked": 0, "eligible": 0, "sent": 0, "skipped": 0, "errors": 0}
    sent_this_run: set[int] = set()

    zip_rows = db.execute(text("""
        SELECT zip_code, county_id
        FROM gold_plus_zip_snapshots
        WHERE snapshot_date >= CURRENT_DATE - 1
          AND gold_plus_lead_count > 0
          AND (:zip_filter IS NULL OR zip_code = :zip_filter)
        ORDER BY gold_plus_lead_count DESC
    """), {"zip_filter": zip_code_filter}).all()

    for row in zip_rows:
        if _limit_reached(result, limit):
            break

        zip_code: str = row.zip_code
        county_id: str = row.county_id

        verticals = _get_zip_verticals(zip_code, county_id, db)
        if not verticals:
            continue

        # Build sub_id → [verticals] map — dedup across verticals within ZIP and globally
        sub_verticals: dict[int, list[str]] = {}
        for vertical in verticals:
            for sid in get_subscribers_interested_in_zip(zip_code, vertical, county_id, db):
                if sid not in sent_this_run:
                    sub_verticals.setdefault(sid, []).append(vertical)

        if not sub_verticals:
            continue

        subs = _fetch_subscribers(list(sub_verticals), db)
        for sub in subs:
            if _limit_reached(result, limit):
                break

            result["checked"] += 1
            sub_vert_list = sub_verticals.get(sub.id, verticals)

            dispatched = False
            for vertical in sub_vert_list:
                eligible, reason = check_sold_out_zip_eligibility(
                    sub, zip_code, vertical, county_id, db
                )
                if eligible:
                    result["eligible"] += 1
                    ok = _dispatch(
                        sub, "sold_out", dry_run,
                        county_id=county_id, zip_code=zip_code, vertical=vertical,
                    )
                    if ok:
                        result["sent"] += 1
                        sent_this_run.add(sub.id)
                    else:
                        result["errors"] += 1
                    dispatched = True
                    break

                if reason in _LIFECYCLE_STOP_REASONS:
                    break  # lifecycle/contact failures are vertical-agnostic

                # no_zip_interest or no_supply — try the next vertical

            if not dispatched:
                logger.debug(
                    "reactivation_scheduler: skip sub_id=%s zip=%s cohort=sold_out",
                    sub.id, zip_code,
                )
                result["skipped"] += 1

    return result


# ── Tier3 win-back cohort ─────────────────────────────────────────────────────

def _lapsed_subscriber_ids(db: Session) -> list[int]:
    """Lapsed = churned_at set. Candidate pool for the win-back cohort."""
    return list(db.execute(text("""
        SELECT id FROM subscribers
        WHERE churned_at IS NOT NULL
        ORDER BY churned_at DESC
    """)).scalars().all())


def _run_tier3_winback(
    db: Session,
    dry_run: bool,
    limit: Optional[int],
) -> dict:
    """
    Two lapsed-tier branches (client-locked copy/mechanics, T-B12-07):
      zip_held     — <30d lapsed, territory still theirs: 50% off return month.
      zip_released — >=30d lapsed, territory released: 5 free credits on reactivation.
    """
    result: dict = {"checked": 0, "eligible": 0, "sent": 0, "skipped": 0, "errors": 0}

    sub_ids = _lapsed_subscriber_ids(db)
    subs = _fetch_subscribers(sub_ids, db)

    for sub in subs:
        if _limit_reached(result, limit):
            break

        result["checked"] += 1
        eligible, reason, branch = check_tier3_winback_eligibility(sub, db)
        if not eligible:
            logger.debug(
                "reactivation_scheduler: skip sub_id=%s reason=%s cohort=tier3_winback",
                sub.id, reason,
            )
            result["skipped"] += 1
            continue

        result["eligible"] += 1
        ok = _dispatch(sub, "tier3_winback", dry_run, winback_branch=branch)
        if ok:
            result["sent"] += 1
        else:
            result["errors"] += 1

    return result


# ── Public entry point ────────────────────────────────────────────────────────

def run(
    cohort: str = "all",
    dry_run: bool = False,
    limit: Optional[int] = None,
    county_id: Optional[str] = None,
    zip_code: Optional[str] = None,
    db: Optional[Session] = None,
) -> dict:
    """
    Run the daily reactivation scheduler.

    Args:
        cohort:    "county_live" | "sold_out" | "tier3_winback" | "all"
        dry_run:   Log what would be sent without actually sending.
        limit:     Cap total messages sent across both cohorts.
        county_id: Restrict county_live cohort to a single county.
        zip_code:  Restrict sold_out cohort to a single ZIP.
        db:        Injected session (used in tests). If None, opens get_db_context().

    Returns:
        Result dict with keys: cohort, checked, eligible, sent, skipped, errors, dry_run.
    """
    totals: dict = {"checked": 0, "eligible": 0, "sent": 0, "skipped": 0, "errors": 0}

    def _merge(partial: dict) -> None:
        for k in totals:
            totals[k] += partial.get(k, 0)

    if db is not None:
        _execute(db, cohort, dry_run, limit, county_id, zip_code, totals, _merge)
    else:
        with get_db_context() as session:
            _execute(session, cohort, dry_run, limit, county_id, zip_code, totals, _merge)

    return {"cohort": cohort, **totals, "dry_run": dry_run}


def _execute(db, cohort, dry_run, limit, county_id, zip_code, totals, _merge):
    if cohort in ("county_live", "all"):
        _merge(_run_county_live(db, dry_run, limit, county_id))

    remaining_limit = None
    if limit is not None:
        remaining_limit = max(0, limit - totals["sent"])

    if cohort in ("sold_out", "all"):
        _merge(_run_sold_out(db, dry_run, remaining_limit, zip_code))
        remaining_limit = None if limit is None else max(0, limit - totals["sent"])

    if cohort in ("tier3_winback", "all"):
        _merge(_run_tier3_winback(db, dry_run, remaining_limit))


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
        stream=sys.stdout,
    )

    parser = argparse.ArgumentParser(description="Sprint S0 reactivation scheduler")
    parser.add_argument(
        "--cohort",
        choices=["county_live", "sold_out", "tier3_winback", "all"],
        default="all",
        help="Which cohort to run (default: all)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Log without sending")
    parser.add_argument("--limit", type=int, default=None, help="Max messages to send")
    parser.add_argument("--county-id", default=None, help="Filter to one county")
    parser.add_argument("--zip-code", default=None, help="Filter to one ZIP")
    args = parser.parse_args()

    result = run(
        cohort=args.cohort,
        dry_run=args.dry_run,
        limit=args.limit,
        county_id=args.county_id,
        zip_code=args.zip_code,
    )
    print(result)
    sys.exit(0)
