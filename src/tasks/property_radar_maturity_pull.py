"""PropertyRadar target-lender maturity pull (Developer 1 entry point).

Two modes:
  backlog  — one-time sweep of all Florida records currently in the maturity
             window. property_radar_seen_ids prevents a re-run from EMITTING
             duplicate normalized records, but it does NOT reduce export
             cost: purchase() always re-fetches (and re-bills) the ENTIRE
             matching set on every backlog call, because backlog mode uses
             the full, static 8-15 month window every time. A crashed or
             re-run backlog pull re-spends credits on every record PropertyRadar
             returns, seen or not — confirmed empirically (2026-09-28): a
             resumed backlog run re-billed the full ~708-record set even
             with 100 records already in seen_ids. Never assume "re-run is
             free because of seen_ids" for backlog mode.
  daily    — fetches only records that newly entered the maturity window since
             the last successful run (via the FirstDate window itself narrowing,
             not via seen_ids) — THIS is the mode that's actually cheap to
             re-run same-day, because the free count() for an already-covered
             window returns 0 before any purchase() call happens.

Budget guard (Requirement 6):
  1. Free count() first — refuses if count > remaining allowance or per-run cap.
  2. Exports are purchased page-by-page; each page's radar_ids are inserted
     into seen_ids immediately so a mid-run crash does not re-purchase them.
  3. The pull_run row is updated to 'done' with the final counts on success,
     or 'failed' on any unhandled exception.

Wiring / production entry point:
  Add to scripts/cron/crontab.txt (example, run at 02:00 UTC daily):
    0 2 * * * /path/to/scripts/cron/run.sh python -m src.tasks.property_radar_maturity_pull --mode daily

  For the one-time backlog run, invoke manually:
    PYTHONPATH=. python -m src.tasks.property_radar_maturity_pull --mode backlog [--dry-run]

Dev 2 integration note (open question #2, deferred to Monday meeting):
  This task writes normalised records to stdout (JSON lines) and inserts
  dedup checkpoints into property_radar_seen_ids. If the contract meeting
  resolves that Dev 2's staging table owns seen radar_ids instead, remove
  the seen_ids INSERT/query here and replace with a NOT IN against Dev 2's
  staging table. The normalised-record output contract (PropertyRadarNormalized)
  does not change.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, datetime, timezone
from typing import Optional

from sqlalchemy import text

from config.property_radar_campaigns import (
    DEFAULT_CAMPAIGN,
    ENABLED_STATES,
    build_campaign_criteria,
)
from config.settings import settings
from src.core.database import get_db_context
from src.services.property_radar_normalizer import normalize
from src.services.property_radar_port import get_property_radar_port
from src.utils.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Budget guard
# ---------------------------------------------------------------------------

def _check_budget(port, criteria: list[dict], state: str, campaign: str) -> int:
    """Return the safe export count, or raise RuntimeError if over budget.

    Calls count() (free) first, then checks remaining allowance and per-run cap.
    """
    total = port.count(criteria)
    logger.info("PropertyRadar count [%s/%s]: %d records match", state, campaign, total)
    if total == 0:
        return 0

    allowance = port.allowance()
    per_run_cap = settings.property_radar_per_run_cap
    if allowance.verified and total > allowance.total_remaining:
        raise RuntimeError(
            f"PropertyRadar budget guard: {total} matching records but only "
            f"{allowance.total_remaining} export credits remaining — aborting"
        )
    if not allowance.verified:
        logger.warning(
            "PropertyRadar remaining allowance cannot be verified before purchase "
            "(no free quota endpoint exists) — relying on per-run cap (%d) as the "
            "only pre-flight guard; PropertyRadar's own quota-exceeded error is the "
            "real backstop if the account runs out of credits mid-purchase",
            per_run_cap,
        )
    if total > per_run_cap:
        raise RuntimeError(
            f"PropertyRadar budget guard: {total} matching records exceeds "
            f"per-run cap of {per_run_cap} — aborting (raise PROPERTY_RADAR_PER_RUN_CAP or "
            f"split the pull into county-scoped runs)"
        )
    logger.info(
        "Budget OK: %d records, cap %d%s",
        total, per_run_cap,
        f", {allowance.total_remaining} remaining" if allowance.verified else " (allowance unverified)",
    )
    return total


# ---------------------------------------------------------------------------
# Seen-IDs helpers
# ---------------------------------------------------------------------------

def _load_seen_ids(session, state: str, campaign: str) -> frozenset[str]:
    rows = session.execute(
        text(
            "SELECT radar_id FROM property_radar_seen_ids "
            "WHERE state = :s AND campaign = :c"
        ),
        {"s": state, "c": campaign},
    ).fetchall()
    return frozenset(r[0] for r in rows)


def _mark_seen(session, state: str, campaign: str, radar_ids: list[str]) -> None:
    if not radar_ids:
        return
    # NOTE: ":ids::text[]" (a bind param immediately followed by a "::" cast)
    # is not parsed correctly by SQLAlchemy's text() tokenizer — the bind
    # param is silently left unbound and psycopg2 raises a syntax error on
    # the literal ":ids". CAST(:ids AS ...) avoids the adjacency entirely.
    session.execute(
        text(
            "INSERT INTO property_radar_seen_ids (state, campaign, radar_id) "
            "SELECT :s, :c, unnest(CAST(:ids AS text[])) "
            "ON CONFLICT (state, campaign, radar_id) DO NOTHING"
        ),
        {"s": state, "c": campaign, "ids": radar_ids},
    )


def _last_successful_run_date(session, state: str, campaign: str) -> Optional[date]:
    """Return the calendar date of the most recent 'done' run for (state,
    campaign), or None if there isn't one yet. This is the watermark daily
    mode uses to narrow FirstDate to only newly-matured loans (§2.3) instead
    of re-querying the full 8-15 month backlog window every day."""
    row = session.execute(
        text(
            "SELECT started_at FROM property_radar_pull_runs "
            "WHERE state = :s AND campaign = :c AND status = 'done' "
            "ORDER BY started_at DESC LIMIT 1"
        ),
        {"s": state, "c": campaign},
    ).first()
    if row is None:
        return None
    return row[0].date()


# ---------------------------------------------------------------------------
# Pull logic
# ---------------------------------------------------------------------------

def _run_pull(
    *,
    mode: str,
    state: str,
    campaign: str,
    dry_run: bool,
    session,
) -> dict:
    port = get_property_radar_port()

    daily_since = None
    if mode == "daily":
        daily_since = _last_successful_run_date(session, state, campaign)
        if daily_since is None:
            logger.warning(
                "PropertyRadar daily pull [%s/%s]: no prior successful run found — "
                "this run will use the FULL 8-15 month backlog window, not an "
                "incremental one. Run --mode backlog first to establish a watermark, "
                "otherwise every daily run re-queries and re-bills the whole matching set.",
                state, campaign,
            )
        else:
            logger.info(
                "PropertyRadar daily pull [%s/%s]: narrowing to loans newly "
                "matured since %s (last successful run)",
                state, campaign, daily_since.isoformat(),
            )

    criteria = build_campaign_criteria(state, campaign, daily_since=daily_since)

    # Budget guard — free count first
    if not dry_run:
        _check_budget(port, criteria, state, campaign)
    else:
        count = port.count(criteria)
        logger.info("[dry-run] Would fetch %d records from PropertyRadar", count)
        return {"mode": mode, "state": state, "campaign": campaign, "dry_run": True, "count": count}

    # Load already-seen radar_ids to skip re-purchase
    seen = _load_seen_ids(session, state, campaign)
    logger.info("Skipping %d already-seen radar_ids", len(seen))

    # Insert pull_run row
    run_row = session.execute(
        text(
            "INSERT INTO property_radar_pull_runs "
            "(run_type, state, campaign, started_at, status) "
            "VALUES (:rt, :s, :c, :ts, 'running') RETURNING id"
        ),
        {"rt": mode, "s": state, "c": campaign, "ts": datetime.now(timezone.utc)},
    ).scalar()
    session.commit()

    records_fetched = 0
    exports_consumed = 0
    excluded = 0
    batch_ids: list[str] = []

    try:
        for record in port.purchase(criteria):
            exports_consumed += 1
            if record.radar_id in seen:
                continue

            normalized = normalize(record, state=state, campaign=campaign)
            if normalized is None:
                excluded += 1
                continue

            records_fetched += 1
            batch_ids.append(record.radar_id)

            # Emit normalised record to stdout (JSON line) for Dev 2 to consume
            # — field names match the §3 shared contract exactly.
            print(json.dumps({
                "radar_id": normalized.radar_id,
                "state_fips": normalized.state_fips,
                "county_fips": normalized.county_fips,
                "apn": normalized.apn,
                "state": normalized.state,
                "county_name": normalized.county_name,
                "address": normalized.address,
                "city": normalized.city,
                "zip_code": normalized.zip_code,
                "property_type": normalized.property_type,
                "owner_name": normalized.owner_name,
                "ownership_type": normalized.ownership_type,
                "lender_original": normalized.lender_original,
                "loan_date": normalized.loan_date.isoformat() if normalized.loan_date else None,
                "loan_amount": normalized.loan_amount,
                "loan_term_years": normalized.loan_term_years,
                "est_maturity_date": (
                    normalized.est_maturity_date.isoformat()
                    if normalized.est_maturity_date else None
                ),
                "loan_doc_number": None,
                "principal_name": normalized.principal_name,
                "campaign": normalized.campaign,
                "raw": normalized.raw,
            }), flush=True)

            # Flush seen-id batch every 100 records to survive mid-run crashes
            if len(batch_ids) >= 100:
                _mark_seen(session, state, campaign, batch_ids)
                session.commit()
                batch_ids = []

        # Final batch
        if batch_ids:
            _mark_seen(session, state, campaign, batch_ids)
            session.commit()

        # Mark run done
        session.execute(
            text(
                "UPDATE property_radar_pull_runs "
                "SET finished_at = :ts, records_fetched = :rf, "
                "    exports_consumed = :ec, status = 'done' "
                "WHERE id = :id"
            ),
            {
                "ts": datetime.now(timezone.utc),
                "rf": records_fetched,
                "ec": exports_consumed,
                "id": run_row,
            },
        )
        session.commit()

    except Exception:
        session.rollback()
        session.execute(
            text(
                "UPDATE property_radar_pull_runs SET status = 'failed', "
                "finished_at = :ts WHERE id = :id"
            ),
            {"ts": datetime.now(timezone.utc), "id": run_row},
        )
        session.commit()
        raise

    result = {
        "run_id": run_row,
        "mode": mode,
        "state": state,
        "campaign": campaign,
        "records_fetched": records_fetched,
        "exports_consumed": exports_consumed,
        "excluded_long_term": excluded,
    }
    logger.info("PropertyRadar pull complete: %s", result)
    return result


def _dry_run_county_report(state: str, campaign: str) -> None:
    """Print per-county counts using Purchase=0 (free). Satisfies the dry-run
    definition-of-done: 'dry run prints the Florida target-lender maturity
    count by county, using no exports.'"""
    from config.property_radar_fips import FIPS_BY_STATE

    port = get_property_radar_port()
    counties = FIPS_BY_STATE.get(state.upper(), {})

    total = 0
    rows = []
    for county_name, fips in sorted(counties.items()):
        base_criteria = build_campaign_criteria(state, campaign)
        # PropertyRadar's real criterion name is "County" (not "CountyFIPS"),
        # value is the 5-digit FIPS code as a string — confirmed against the
        # live API (400 "Unexpected Criterion: CountyFIPS" otherwise).
        county_criteria = base_criteria + [{"name": "County", "value": [fips]}]
        count = port.count(county_criteria)
        rows.append((county_name, fips, count))
        total += count

    print(f"\nPropertyRadar dry-run count — {state} / {campaign}")
    print(f"{'County':<30} {'FIPS':<8} {'Count':>8}")
    print("-" * 50)
    for county_name, fips, count in rows:
        print(f"{county_name:<30} {fips:<8} {count:>8}")
    print("-" * 50)
    print(f"{'TOTAL':<30} {'':8} {total:>8}\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="PropertyRadar target-lender maturity pull",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--mode",
        choices=["backlog", "daily"],
        default="daily",
        help="backlog=full sweep, daily=new records only",
    )
    parser.add_argument(
        "--state",
        default="FL",
        help="Two-letter state code (must be in ENABLED_STATES)",
    )
    parser.add_argument(
        "--campaign",
        default=DEFAULT_CAMPAIGN,
        help="Campaign key from config/property_radar_campaigns.py",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print county-level counts using free API calls; do not purchase exports",
    )
    parser.add_argument(
        "--county-report",
        action="store_true",
        help="Print per-county breakdown (uses Purchase=0, always free)",
    )
    args = parser.parse_args()

    state = args.state.upper()
    if state not in ENABLED_STATES and not args.dry_run:
        logger.error(
            "State %s is not in ENABLED_STATES %s — "
            "add it to config/property_radar_campaigns.ENABLED_STATES to enable",
            state, ENABLED_STATES,
        )
        sys.exit(1)

    if args.county_report or args.dry_run:
        _dry_run_county_report(state, args.campaign)
        if args.dry_run:
            return

    # Fail-closed: CLAUDE.md and the cron entry both document this job as
    # "disabled until PROPERTY_RADAR_ENABLED=true". Nothing else in the code
    # path actually enforced that — get_property_radar_port() only reads this
    # flag on the mode=="live" branch, so with the default mode=="fake" the
    # job would otherwise run daily against the real DB (inserting real rows
    # into property_radar_pull_runs/property_radar_seen_ids) even while
    # "disabled". --dry-run/--county-report are exempt: they only make free
    # count() calls and write nothing, so they stay usable for verification
    # regardless of this flag.
    if not settings.property_radar_enabled:
        logger.info(
            "PropertyRadar pull is disabled (PROPERTY_RADAR_ENABLED=false) — "
            "no-op. Set PROPERTY_RADAR_ENABLED=true to enable real runs."
        )
        return

    logger.info(
        "Starting PropertyRadar pull: mode=%s state=%s campaign=%s",
        args.mode, state, args.campaign,
    )

    with get_db_context() as session:
        result = _run_pull(
            mode=args.mode,
            state=state,
            campaign=args.campaign,
            dry_run=False,
            session=session,
        )

    print(json.dumps(result), file=sys.stderr)


if __name__ == "__main__":
    main()
