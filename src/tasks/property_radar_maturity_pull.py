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

Dev 2 integration:
  Every normalized record is written into Dev 2's staging table
  (property_radar_records) via src.services.property_radar.staging.upsert_records(),
  once per ~100-record batch, followed by a single link_unlinked() call after
  the whole pull completes — this is the real interchange point with Dev 2's
  storage layer. The stdout JSON emission is retained only for manual
  inspection/debugging; nothing downstream should rely on parsing it.
  property_radar_seen_ids remains Dev 1's own dedup checkpoint (separate
  from, and does not replace, Dev 2's own dedupe-by-key in upsert_records).

Dev 3 integration (pull -> stage -> handoff):
  After the pull's own session commits (staging + linking done), main() calls
  src.tasks.property_radar_lead_handoff.run() once, in its OWN session, to
  walk every staged record and decide handoff/suppress/skip. This follows
  the build-split spec's own stated default (PROPERTYRADAR_BUILD_SPLIT.md:
  "one CLI (--dry-run by default, --apply)"): dry run by default, real
  writes to FA Max only with --apply-handoff. --skip-handoff opts out of the
  handoff step entirely. A handoff failure is logged but never flips an
  already-successful pull_run row to 'failed' -- staging and handoff are
  separate concerns with separate outcomes.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, datetime, timezone
from pathlib import Path
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
from src.services.property_radar.linking import link_unlinked
from src.services.property_radar.staging import upsert_records
from src.tasks import property_radar_lead_handoff
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


def _to_staging_dict(normalized) -> dict:
    """PropertyRadarNormalized -> the exact dict shape upsert_records() expects
    (src.services.property_radar.staging._COLUMNS). Field names must match
    that module's _COLUMN_TYPES map exactly — a renamed field here lands as
    a silent NULL in property_radar_records, not an error."""
    return {
        "radar_id": normalized.radar_id,
        "state_fips": normalized.state_fips,
        "county_fips": normalized.county_fips,
        "apn": normalized.apn,
        "state": normalized.state,
        "county_name": normalized.county_name,
        "property_address": normalized.property_address,
        "city": normalized.city,
        "zip": normalized.zip,
        "property_type": normalized.property_type,
        "owner_name": normalized.owner_name,
        "ownership_type": normalized.ownership_type,
        "mailing_address": normalized.mailing_address,
        "mailing_city": normalized.mailing_city,
        "mailing_state": normalized.mailing_state,
        "mailing_zip": normalized.mailing_zip,
        "principal_name": normalized.principal_name,
        "lender_name": normalized.lender_name,
        "loan_amount": normalized.loan_amount,
        "loan_recorded_date": (
            normalized.loan_recorded_date.isoformat() if normalized.loan_recorded_date else None
        ),
        "loan_term_years": normalized.loan_term_years,
        "est_maturity_date": (
            normalized.est_maturity_date.isoformat() if normalized.est_maturity_date else None
        ),
        "loan_doc_number": normalized.loan_doc_number,
        "campaign": normalized.campaign,
        "raw": normalized.raw,
    }


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
    staging_batch: list[dict] = []
    inserted_total = updated_total = skipped_total = 0

    def _flush_staging_batch() -> None:
        nonlocal inserted_total, updated_total, skipped_total, staging_batch
        if not staging_batch:
            return
        ins, upd, skp = upsert_records(session, staging_batch)
        inserted_total += ins
        updated_total += upd
        skipped_total += skp
        staging_batch = []

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
            staging_batch.append(_to_staging_dict(normalized))

            # Emit normalised record to stdout (JSON line) — useful for manual
            # inspection/debugging; the real interchange point with Dev 2's
            # storage is the upsert_records() call below, not this print.
            print(json.dumps(_to_staging_dict(normalized)), flush=True)

            # Flush every 100 records: write to Dev 2's staging table, mark
            # seen, commit. Both survive a mid-run crash at this boundary.
            if len(batch_ids) >= 100:
                _flush_staging_batch()
                _mark_seen(session, state, campaign, batch_ids)
                session.commit()
                batch_ids = []

        # Final partial batch
        _flush_staging_batch()
        if batch_ids:
            _mark_seen(session, state, campaign, batch_ids)
            session.commit()

        # Link newly-staged records to FA properties where the county is
        # loaded (Dev 2's contract: call once after all pages are upserted,
        # not per page).
        link_counts = link_unlinked(session)
        session.commit()
        logger.info(
            "PropertyRadar staging: %d inserted, %d updated, %d skipped; link: %s",
            inserted_total, updated_total, skipped_total, link_counts,
        )

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
    parser.add_argument(
        "--skip-handoff",
        action="store_true",
        help="Do not run the FA Max lead handoff step after staging completes",
    )
    parser.add_argument(
        "--trace-results",
        type=Path,
        default=None,
        help="Tracerfy results CSV to attach contacts from during handoff "
             "(no new trace spend — reads the existing file only). Without "
             "this, every staged record has no contact data and the handoff "
             "always skips it regardless of --apply-handoff.",
    )
    parser.add_argument(
        "--apply-handoff",
        action="store_true",
        help="Actually write handed-off leads to FA Max (default: dry run, matching "
             "the build-split spec's own stated runner default — prints the decision "
             "summary, writes nothing)",
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

    if args.skip_handoff:
        return

    # Staging (upsert_records + link_unlinked) already committed inside
    # _run_pull() above. The handoff walks property_radar_records itself
    # (independent read, its own session) to decide handoff/suppress/skip
    # for every staged record — not just the ones this run touched, so a
    # record staged by an earlier run that was previously skipped (e.g. for
    # a missing contact) is reconsidered too.
    try:
        report = property_radar_lead_handoff.run(
            campaign=args.campaign,
            trace_results=args.trace_results,
            apply=args.apply_handoff,
        )
        logger.info("PropertyRadar handoff: %s", report.summary().replace("\n", " | "))
    except Exception:
        # A handoff failure must never retroactively mark the pull_run
        # above as failed -- staging succeeded and is durable regardless
        # of what the handoff step does with it.
        logger.exception(
            "PropertyRadar handoff step failed after a successful pull "
            "(run_id=%s) -- staged records are safe, handoff can be "
            "retried independently via property_radar_lead_handoff",
            result.get("run_id"),
        )


if __name__ == "__main__":
    main()
