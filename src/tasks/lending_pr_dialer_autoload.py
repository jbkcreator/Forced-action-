"""Morning BatchDialer load of the PropertyRadar leads the bridge staged overnight (pool ``pr_maturity``).

The nightly PropertyRadar runner pulls and traces Florida and Georgia records, the bridge (03:00 UTC) stages them
with their traced phones into the newest calling-pool staging run, and this job loads them into BatchDialer at
09:xx ET, so no one has to run ``lending_dialer_load`` by hand. GHL follows on its own: the GHL load sync pushes
every new BatchDialer load into the NDL GHL account.

What it loads: the staged ``pr_maturity`` records in a launch queue (their source tag decides the campaign), one per
phone, leaving out phones already active in BatchDialer. The dialer load's checks that cost nothing (Georgia
owners who are not companies, homestead, suppression, attempt history, numbers worked from GHL, a fresh do-not-call
result) run first, so a number that can never load never takes a do-not-call credit; the dialer load
(src/lending/dialer_load.py) then runs every gate again: do-not-call and litigator, calling hours, emergency halt.

Do-not-call spend: a phone with a fresh check costs nothing; any other phone costs one Tracerfy credit.
LENDING_PR_DIALER_AUTOLOAD_MAX_SCRUB_CREDITS caps those credits per ET day. Before each Tracerfy call the job writes
how many phones it sends (``data/pr_dialer_autoload/scrubs-<ET day>.log``), and the cap counts from that file, so a
run that fails and is retried by run.sh never spends the day's credits twice. Phones over the cap wait for the next
morning. A do-not-call check that fails (Tracerfy timeout or error) fails the run and alerts; the credits it
may have used stay counted.

Two passes a day, 09:xx and 12:xx ET: the dialer load only loads a number inside its own calling hours (8am
recipient local), so a Mountain or Pacific number blocked at 09:xx loads at 12:xx (Hawaii numbers still never do).

Usage (cron 13:05, 14:05, 16:05 and 17:05 UTC; acts only during 09:xx and 12:xx ET):
    python -m src.tasks.lending_pr_dialer_autoload                 # dry run: what would load, nothing sent
    python -m src.tasks.lending_pr_dialer_autoload --apply [--any-hour]

Exit codes: 0 ok (also when switched off or outside the load hours), 1 some loads or do-not-call checks failed (safe
to run again), 3 refused.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo

from sqlalchemy import text

logger = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")
LOAD_HOURS_ET = frozenset({9, 12})  # two UTC cron lines per pass cover daylight saving; only those landing here act
SPEND_ROOT = Path(__file__).resolve().parents[2] / "data" / "pr_dialer_autoload"
EXIT_OK, EXIT_FAILED, EXIT_REFUSED = 0, 1, 3

_ACTIVE_PHONES_SQL = text("SELECT DISTINCT phone FROM lending.dialer_load_records WHERE active AND phone = ANY(:p)")


def day_of(now: datetime) -> date:
    return now.astimezone(ET).date()


def in_load_hour(now: datetime) -> bool:
    return now.astimezone(ET).hour in LOAD_HOURS_ET


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


class CappedScrubber:
    """The scrubber, never sending more phones than the day's credits left. The count is written before the call,
    so a run that dies mid-call still counts what it sent. Phones over the cap are not checked, and the dialer
    load blocks an unchecked phone, so it is never loaded. ``failed`` counts phones sent but not answered (the
    call raised, or Tracerfy returned no row for them): the dialer load swallows that error, so the run reads it
    here."""

    def __init__(self, scrubber: Callable[[list[str]], list[dict]], *, day: date, cap: int,
                 root: Optional[Path] = None) -> None:
        self._scrubber, self._day, self._cap, self._root = scrubber, day, cap, root
        self.failed = 0

    def __call__(self, phones: list[str]) -> list[dict]:
        from src.services.phone_utils import normalize as normalize_phone

        left = max(0, self._cap - credits_spent(self._day, self._root))
        sending = list(phones)[:left]
        if len(sending) < len(phones):
            logger.warning("[pr-autoload] %d phone(s) over the day's do-not-call credits; left for tomorrow",
                           len(phones) - len(sending))
        if not sending:
            return []
        _record_sent(self._day, len(sending), self._root)
        try:
            rows = self._scrubber(sending)
        except Exception:
            self.failed += len(sending)
            raise
        answered = {normalize_phone(str(row.get("phone") or "")) for row in rows}
        self.failed += sum(1 for phone in sending if normalize_phone(phone) not in answered)
        return rows


# ── choosing what to load ───────────────────────────────────────────────────

_VERDICT = "_pre_check_verdict"


def select_records(records: Sequence[Mapping[str, Any]], verdicts: Sequence[Optional[str]], *,
                   active_phones: set[str], credits_left: int) -> tuple[list[dict], dict]:
    """Records to load, in dial order: one per phone, never a phone already loaded. ``verdicts`` (one per record)
    is the dialer load's free checks: None = allowed on a fresh do-not-call result (always goes, it costs
    nothing), SCRUB_FAILED = needs a do-not-call check (goes while the day's credits last), anything else = blocked
    (never takes a credit)."""
    from src.lending.dial_order import sort_for_load
    from src.lending.dialer_load import REASON_SCRUB_FAILED

    counts: dict[str, Any] = {"staged": 0, "no_phone": 0, "already_loaded": 0, "blocked_before_check": 0,
                              "duplicate_phone": 0, "needs_check": 0, "waiting_for_credits": 0}
    blocked_by_reason: Counter = Counter()
    chosen: list[dict] = []
    seen: set[str] = set()
    for record in sort_for_load([{**r, _VERDICT: v} for r, v in zip(records, verdicts, strict=True)]):
        verdict = record.pop(_VERDICT)
        counts["staged"] += 1
        phone = record.get("phone")
        if not phone:
            counts["no_phone"] += 1
        elif phone in active_phones:
            counts["already_loaded"] += 1
        elif verdict is not None and verdict != REASON_SCRUB_FAILED:
            counts["blocked_before_check"] += 1
            blocked_by_reason[verdict] += 1
        elif phone in seen:
            counts["duplicate_phone"] += 1
        elif verdict is None:
            seen.add(phone)
            chosen.append(record)
        elif counts["needs_check"] < credits_left:
            counts["needs_check"] += 1
            seen.add(phone)
            chosen.append(record)
        else:
            counts["waiting_for_credits"] += 1
    counts["blocked_by_reason"] = dict(blocked_by_reason)
    counts["to_load"] = len(chosen)
    return chosen, counts


def staged_pr_records(db) -> list[dict]:
    """The bridge's records in the newest staging run, assigned to their launch queue."""
    from src.lending.pool_source import staged_pool_records
    from src.lending.pr_maturity_bridge import POOL_NAME
    from src.tasks.lending_dialer_load import launch_queue_records

    return launch_queue_records([r for r in staged_pool_records(db) if r["staging_pool"] == POOL_NAME])


def pre_check_verdicts(db, records: Sequence[Mapping[str, Any]]) -> list[Optional[str]]:
    """The dialer load's gates up to the do-not-call check, without calling Tracerfy (as its own pre-scrub refusal
    check does): None = allowed, else the block reason (SCRUB_FAILED = not checked yet)."""
    from src.lending.compliance import filter_loadable
    from src.lending.dialer_load import no_scrub

    if not records:
        return []
    results = filter_loadable([dict(r) for r in records], db, scrubber=no_scrub)
    return [None if r.allowed else r.reason.value for r in results]


def plan_load(db, *, day: date, cap: int, root: Optional[Path] = None) -> tuple[list[dict], dict]:
    records = staged_pr_records(db)
    phones = sorted({r["phone"] for r in records if r.get("phone")})
    active = {row[0] for row in db.execute(_ACTIVE_PHONES_SQL, {"p": phones})} if phones else set()
    left = max(0, cap - credits_spent(day, root))
    chosen, counts = select_records(records, pre_check_verdicts(db, records), active_phones=active,
                                    credits_left=left)
    counts["credits_left_today"] = left
    return chosen, counts


# ── command line ────────────────────────────────────────────────────────────

def main(argv: Optional[list[str]] = None) -> int:
    from config.settings import get_settings

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Morning BatchDialer load of the staged PropertyRadar leads.")
    parser.add_argument("--apply", action="store_true", help="load BatchDialer (default: dry run)")
    parser.add_argument("--any-hour", action="store_true",
                        help="run outside the load hours (the dialer load still enforces calling hours)")
    args = parser.parse_args(argv)
    settings = get_settings()
    now = datetime.now(timezone.utc)
    if args.apply and not settings.lending_pr_dialer_autoload_enabled:
        logger.info("[pr-autoload] off (LENDING_PR_DIALER_AUTOLOAD_ENABLED=false)")
        return EXIT_OK
    if args.apply and not args.any_hour and not in_load_hour(now):
        logger.info("[pr-autoload] outside the load hours (%s ET); another cron line runs it",
                    ", ".join(f"{h:02d}:xx" for h in sorted(LOAD_HOURS_ET)))
        return EXIT_OK
    cap = max(0, settings.lending_pr_dialer_autoload_max_scrub_credits)
    summary, code = _run(now, cap, apply=args.apply)
    if summary.get("stale_data"):  # R1 0.6: a failed overnight pull; say so plainly, never load the old list
        logger.warning("[pr-autoload] %s", summary["stale_data"])
        if args.apply:
            _notify(summary["stale_data"], failed=True)
        return code
    message = f"PropertyRadar morning dialer load{'' if args.apply else ' (dry run)'}: {json.dumps(summary, default=str)}"
    logger.info("[pr-autoload] %s", message)
    if args.apply and (summary.get("to_load") or code != EXIT_OK):
        _notify(message, failed=code != EXIT_OK)
    return code


def _run(now: datetime, cap: int, *, apply: bool) -> tuple[dict[str, Any], int]:
    from src.core.database import get_db_context

    from src.lending.pool_source import propertyradar_staleness

    day = day_of(now)
    with get_db_context() as db:
        stale = propertyradar_staleness(db, now)
        if stale is not None:
            db.rollback()
            return {"stale_data": stale.message(), "propertyradar_age_hours": stale.age_hours}, EXIT_REFUSED
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
    scrubber = CappedScrubber(tracerfy_scrub, day=day, cap=cap)
    try:
        report = run_dialer_load(list(records), db, run_id=f"pr-autoload-{now:%Y%m%dT%H%M%SZ}", dry_run=False,
                                 scrubber=scrubber, dialer=dialer, commit=db.commit, now=now)
    except LoadRefused as exc:
        db.rollback()
        return {**counts, "refused": str(exc)}, EXIT_REFUSED
    loaded = {k: v for k, v in report.as_dict().items() if not isinstance(v, (list, dict))}  # counts, never phones
    summary = {**counts, "load": loaded}
    if scrubber.failed:
        summary["do_not_call_check_failed"] = scrubber.failed
    return summary, EXIT_FAILED if report.failed or scrubber.failed else EXIT_OK


def _notify(message: str, *, failed: bool) -> None:
    from src.lending.ops_alerts import post_ops_alert, post_ops_notice

    (post_ops_alert if failed else post_ops_notice)(message[:3000])


if __name__ == "__main__":
    raise SystemExit(main())
