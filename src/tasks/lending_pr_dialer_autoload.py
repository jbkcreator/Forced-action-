"""Morning BatchDialer load of the PropertyRadar leads the bridge staged overnight (pool ``pr_maturity``).

The nightly PropertyRadar runner pulls and traces Florida and Georgia records, the bridge (03:00 UTC) stages them
with their traced phones into the newest calling-pool staging run, and this job loads them into BatchDialer at
09:xx ET, so no one has to run ``lending_dialer_load`` by hand. GHL follows on its own: the GHL load sync pushes
every new BatchDialer load into the NDL GHL account.

What it loads: the staged ``pr_maturity`` records in a launch queue (their source tag decides the campaign), one per
phone, leaving out phones already active in BatchDialer and Georgia owners who are not companies. Everything else
is the dialer load's own gates (src/lending/dialer_load.py): suppression, homestead, attempt limits, do-not-call
and litigator, calling hours, emergency halt.

Do-not-call spend: a phone with a fresh check costs nothing; any other phone costs one Tracerfy credit.
LENDING_PR_DIALER_AUTOLOAD_MAX_SCRUB_CREDITS caps those credits per ET day. Before each Tracerfy call the job writes
how many phones it sends (``data/pr_dialer_autoload/scrubs-<ET day>.log``), and the cap counts from that file, so a
run that fails and is retried by run.sh never spends the day's credits twice. Phones over the cap wait for the next
morning.

Usage (cron 13:05 and 14:05 UTC; acts only during 09:xx ET):
    python -m src.tasks.lending_pr_dialer_autoload                 # dry run: what would load, nothing sent
    python -m src.tasks.lending_pr_dialer_autoload --apply [--any-hour]

Exit codes: 0 ok (also when switched off or outside 09:xx ET), 1 some loads failed (safe to run again), 3 refused.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo

from sqlalchemy import text

logger = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")
LOAD_HOUR_ET = 9  # two UTC cron lines cover daylight saving; only the one landing in 09:xx ET acts
SPEND_ROOT = Path(__file__).resolve().parents[2] / "data" / "pr_dialer_autoload"
EXIT_OK, EXIT_FAILED, EXIT_REFUSED = 0, 1, 3

_ACTIVE_PHONES_SQL = text("SELECT DISTINCT phone FROM lending.dialer_load_records WHERE active AND phone = ANY(:p)")


def day_of(now: datetime) -> date:
    return now.astimezone(ET).date()


def in_load_hour(now: datetime) -> bool:
    return now.astimezone(ET).hour == LOAD_HOUR_ET


# ── the day's do-not-call credits ───────────────────────────────────────────

def _spend_log(day: date, root: Optional[Path] = None) -> Path:
    return (root or SPEND_ROOT) / f"scrubs-{day.isoformat()}.log"


def credits_spent(day: date, root: Optional[Path] = None) -> int:
    """Phones this job sent to the do-not-call check on ``day`` (1 credit each), across every run that day."""
    path = _spend_log(day, root)
    if not path.exists():
        return 0
    return sum(int(line) for line in path.read_text(encoding="utf-8").split() if line.isdigit())


def _record_sent(day: date, phones: int, root: Optional[Path] = None) -> None:
    path = _spend_log(day, root)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(f"{phones}\n")
        handle.flush()
        os.fsync(handle.fileno())


def capped_scrubber(scrubber: Callable[[list[str]], list[dict]], *, day: date, cap: int,
                    root: Optional[Path] = None) -> Callable[[list[str]], list[dict]]:
    """The scrubber, never sending more phones than the day's credits left. The count is written before the call,
    so a run that dies mid-call still counts what it sent. Phones over the cap are not checked, and the dialer
    load blocks an unchecked phone, so it is never loaded."""

    def scrub(phones: list[str]) -> list[dict]:
        left = max(0, cap - credits_spent(day, root))
        sending = list(phones)[:left]
        if len(sending) < len(phones):
            logger.warning("[pr-autoload] %d phone(s) over the day's do-not-call credits; left for tomorrow",
                           len(phones) - len(sending))
        if not sending:
            return []
        _record_sent(day, len(sending), root)
        return scrubber(sending)

    return scrub


# ── choosing what to load ───────────────────────────────────────────────────

def select_records(records: Iterable[Mapping[str, Any]], *, active_phones: set[str], fresh_phones: set[str],
                   credits_left: int, georgia_blocked: Callable[[Mapping[str, Any]], bool]) -> tuple[list[dict], dict]:
    """Records to load, in dial order: one per phone, never a phone already loaded or a Georgia owner who may not be
    called. A phone with a fresh do-not-call check always goes (it costs nothing); others only while the day's
    credits last."""
    from src.lending.dial_order import sort_for_load

    counts = {"staged": 0, "no_phone": 0, "already_loaded": 0, "georgia_not_a_company": 0, "duplicate_phone": 0,
              "needs_check": 0, "waiting_for_credits": 0}
    chosen: list[dict] = []
    seen: set[str] = set()
    for record in sort_for_load([dict(r) for r in records]):
        counts["staged"] += 1
        phone = record.get("phone")
        if not phone:
            counts["no_phone"] += 1
        elif phone in active_phones:
            counts["already_loaded"] += 1
        elif georgia_blocked(record):
            counts["georgia_not_a_company"] += 1
        elif phone in seen:
            counts["duplicate_phone"] += 1
        elif phone in fresh_phones:
            seen.add(phone)
            chosen.append(record)
        elif counts["needs_check"] < credits_left:
            counts["needs_check"] += 1
            seen.add(phone)
            chosen.append(record)
        else:
            counts["waiting_for_credits"] += 1
    counts["to_load"] = len(chosen)
    return chosen, counts


def staged_pr_records(db) -> list[dict]:
    """The bridge's records in the newest staging run, assigned to their launch queue."""
    from src.lending.pool_source import staged_pool_records
    from src.lending.pr_maturity_bridge import POOL_NAME
    from src.tasks.lending_dialer_load import launch_queue_records

    return launch_queue_records([r for r in staged_pool_records(db) if r["staging_pool"] == POOL_NAME])


def plan_load(db, *, day: date, cap: int, root: Optional[Path] = None) -> tuple[list[dict], dict]:
    from src.lending.compliance import _georgia_blocked, fresh_scrubs

    records = staged_pr_records(db)
    phones = sorted({r["phone"] for r in records if r.get("phone")})
    active = {row[0] for row in db.execute(_ACTIVE_PHONES_SQL, {"p": phones})} if phones else set()
    fresh = set(fresh_scrubs(db, phones)) if phones else set()
    left = max(0, cap - credits_spent(day, root))
    chosen, counts = select_records(records, active_phones=active, fresh_phones=fresh, credits_left=left,
                                    georgia_blocked=_georgia_blocked)
    counts["credits_left_today"] = left
    return chosen, counts


# ── command line ────────────────────────────────────────────────────────────

def main(argv: Optional[list[str]] = None) -> int:
    from config.settings import get_settings

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Morning BatchDialer load of the staged PropertyRadar leads.")
    parser.add_argument("--apply", action="store_true", help="load BatchDialer (default: dry run)")
    parser.add_argument("--any-hour", action="store_true",
                        help="run outside 09:xx ET (the dialer load still enforces calling hours)")
    args = parser.parse_args(argv)
    settings = get_settings()
    now = datetime.now(timezone.utc)
    if args.apply and not settings.lending_pr_dialer_autoload_enabled:
        logger.info("[pr-autoload] off (LENDING_PR_DIALER_AUTOLOAD_ENABLED=false)")
        return EXIT_OK
    if args.apply and not args.any_hour and not in_load_hour(now):
        logger.info("[pr-autoload] outside %02d:xx ET; the other cron line runs it", LOAD_HOUR_ET)
        return EXIT_OK
    cap = max(0, settings.lending_pr_dialer_autoload_max_scrub_credits)
    summary, code = _run(now, cap, apply=args.apply)
    message = f"PropertyRadar morning dialer load{'' if args.apply else ' (dry run)'}: {json.dumps(summary, default=str)}"
    logger.info("[pr-autoload] %s", message)
    if args.apply and (summary.get("to_load") or code != EXIT_OK):
        _notify(message, failed=code != EXIT_OK)
    return code


def _run(now: datetime, cap: int, *, apply: bool) -> tuple[dict[str, Any], int]:
    from src.core.database import get_db_context

    day = day_of(now)
    with get_db_context() as db:
        records, counts = plan_load(db, day=day, cap=cap)
        if not apply or not records:
            db.rollback()
            return counts, EXIT_OK
        return _load(db, records, counts, now=now, day=day, cap=cap)


def _load(db, records: Sequence[dict], counts: dict, *, now: datetime, day: date, cap: int) -> tuple[dict, int]:
    from src.lending.compliance import tracerfy_scrub
    from src.lending.dialer_load import LoadRefused, run_dialer_load
    from src.lending.dialer_port import get_dialer

    dialer = get_dialer()
    if dialer is None:
        return {**counts, "refused": "BatchDialer is not configured (BATCHDIALER_API_KEY)"}, EXIT_REFUSED
    missing = dialer.missing_for_load() if hasattr(dialer, "missing_for_load") else []
    if missing:
        return {**counts, "refused": f"unconfirmed dialer endpoint(s): {', '.join(missing)}"}, EXIT_REFUSED
    try:
        report = run_dialer_load(list(records), db, run_id=f"pr-autoload-{now:%Y%m%dT%H%M%SZ}", dry_run=False,
                                 scrubber=capped_scrubber(tracerfy_scrub, day=day, cap=cap), dialer=dialer,
                                 commit=db.commit, now=now)
    except LoadRefused as exc:
        db.rollback()
        return {**counts, "refused": str(exc)}, EXIT_REFUSED
    loaded = {k: v for k, v in report.as_dict().items() if not isinstance(v, (list, dict))}  # counts, never phones
    return {**counts, "load": loaded}, EXIT_FAILED if report.failed else EXIT_OK


def _notify(message: str, *, failed: bool) -> None:
    from src.lending.ops_alerts import post_ops_alert, post_ops_notice

    (post_ops_alert if failed else post_ops_notice)(message[:3000])


if __name__ == "__main__":
    raise SystemExit(main())
