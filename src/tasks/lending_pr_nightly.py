"""Nightly PropertyRadar calling-list pipeline: pull -> link -> trace -> DNC overnight, BatchDialer load in the morning.

Lists (config/lending_calling_lists.NIGHTLY_LISTS, in buying order): Verified maturity (loans whose estimated
maturity newly came within 165 days), Cash buyers and Investor purchases, for each state in
LENDING_PR_NIGHTLY_STATES. GHL follows on its own: the GHL load sync (src/lending/ghl_load_sync.py) pushes every
new BatchDialer load into the NDL GHL account.

Stages (``--stage``), each reusing the calling-list tools:

* ``pull``: free RadarID listing, then buys only what is new (src/tasks/lending_pr_calling_list_pull.py), under
  LENDING_PR_NIGHTLY_MAX_EXPORTS for the night and never past the free allowance unless
  LENDING_PR_NIGHTLY_MAX_PAID_EXPORTS allows it. Records the cash list matched are left to it, never bought
  again as investor purchases. Then links new records to Forced Action properties.
* ``trace``: resumes and replays any paid Tracerfy queue first, then traces new people under
  LENDING_PR_NIGHTLY_TRACE_MAX_CREDITS (src/tasks/lending_pr_calling_list_trace.py).
* ``dnc``: do-not-call checks traced phones never checked, under LENDING_PR_NIGHTLY_DNC_MAX_CREDITS.
* ``load``: at 09:xx ET only, loads every DNC-clean person not already in BatchDialer (the dialer load's own
  gates, global halt and calling hours included).

Spend safety: every cap is per ET night and counts what the night already spent (export counts from
``lending.pr_list_pulls``; Tracerfy rows sent, read from the night's landing files, counted as if every one was a
paid hit), so a run that fails and is retried by run.sh never spends a cap twice. One run at a time: the
calling-list pull lock, shared with the manual tools.

Dry run by default (free listing and plans only). ``--apply`` spends within the caps.

Usage:
    python -m src.tasks.lending_pr_nightly --stage pull [--apply]
    python -m src.tasks.lending_pr_nightly --stage trace --apply
    python -m src.tasks.lending_pr_nightly --stage dnc --apply
    python -m src.tasks.lending_pr_nightly --stage load --apply [--any-hour]

Exit codes: 0 ok (also when switched off), 1 a stage failed part-way (safe to run again), 3 refused.
"""
from __future__ import annotations

import argparse
import json
import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field, replace
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo

from sqlalchemy import text

from config.lending_calling_lists import CASH_BUYER, INVESTOR_PURCHASE, LANDING_ROOT, NIGHTLY_LISTS, PULL_STATES

logger = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")
EXIT_OK, EXIT_FAILED, EXIT_REFUSED = 0, 1, 3
STAGES = ("pull", "trace", "dnc", "load")
LOAD_HOUR_ET = 9  # the morning load acts only during this Eastern hour, so two UTC cron lines cover daylight saving
_NIGHTLY_FOLDER_PREFIX = "nightly-"


# ── the night ────────────────────────────────────────────────────────────────

def night_of(now: datetime) -> date:
    """The ET date a run belongs to: every stage of one night (01:30 ET pull to 09:xx ET load) shares it."""
    return now.astimezone(ET).date()


def night_start(night: date) -> datetime:
    return datetime.combine(night, time(), ET).astimezone(timezone.utc)


def nightly_folder(list_key: str, night: date, root: Optional[Path] = None) -> Path:
    return (root or LANDING_ROOT) / list_key / "tracerfy" / f"{_NIGHTLY_FOLDER_PREFIX}{night.isoformat()}"


def _rows_sent(folder: Path, prefixes: Sequence[str]) -> int:
    sent = 0
    for path in folder.glob("*.json"):
        if path.name.startswith(tuple(prefixes)):
            sent += len(json.loads(path.read_text(encoding="utf-8")).get("sent") or [])
    return sent


def trace_credits_sent(night: date, root: Optional[Path] = None) -> int:
    """Tracerfy trace rows the night's runs sent, across the nightly lists: the most they can have cost (a miss is
    free, a hit 1 credit; an address-only retry 2, counted twice). Read from the landing files, which are written
    before each queue is polled, so a crash still counts what it sent."""
    total = 0
    for list_key in NIGHTLY_LISTS:
        folder = nightly_folder(list_key, night, root)
        if folder.exists():
            total += _rows_sent(folder, ("trace_", "pending_trace_"))
            total += 2 * _rows_sent(folder, ("advanced_", "pending_advanced_"))
    return total


def dnc_credits_sent(night: date, root: Optional[Path] = None) -> int:
    """Phones the night's runs sent to the do-not-call check (1 credit each), across the nightly lists."""
    return sum(_rows_sent(nightly_folder(k, night, root), ("dnc_",)) for k in NIGHTLY_LISTS
               if nightly_folder(k, night, root).exists())


def remaining(cap: int, spent: int) -> int:
    return max(0, cap - spent)


# ── settings ────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class NightlyCaps:
    states: tuple[str, ...]
    max_exports: int
    max_paid_exports: int
    trace_credits: int
    dnc_credits: int


def nightly_caps(settings: Any) -> NightlyCaps:
    states = tuple(s.strip().upper() for s in (settings.lending_pr_nightly_states or "").split(",") if s.strip())
    unknown = [s for s in states if s not in PULL_STATES]
    if unknown:
        raise ValueError(f"LENDING_PR_NIGHTLY_STATES has unsupported state(s): {', '.join(unknown)}")
    return NightlyCaps(states=states, max_exports=max(0, settings.lending_pr_nightly_max_exports),
                       max_paid_exports=max(0, settings.lending_pr_nightly_max_paid_exports),
                       trace_credits=max(0, settings.lending_pr_nightly_trace_max_credits),
                       dnc_credits=max(0, settings.lending_pr_nightly_dnc_max_credits))


# ── pull ────────────────────────────────────────────────────────────────────

def trim_plan(plan, limit: int):
    """The plan cut to its first ``limit`` purchases, in search order (copies from another list are free and kept).
    What is cut is bought on a later night: the nightly window looks back long enough to find it again."""
    kept: dict[str, list[str]] = {}
    left = max(0, limit)
    for label, ids in plan.to_buy.items():
        kept[label] = list(ids[:left])
        left -= len(kept[label])
    return replace(plan, to_buy=kept)


@dataclass
class PullOutcome:
    bought: int = 0
    stored: int = 0
    left_for_later: int = 0
    by_list: dict[str, dict] = field(default_factory=dict)
    stopped: Optional[str] = None


def run_pull(db, port, *, caps: NightlyCaps, night: date, apply: bool,
             make_plan: Optional[Callable] = None, buy_and_store: Optional[Callable] = None,
             store=None) -> PullOutcome:
    """Every nightly list in buying order, for each state, within the night's export cap."""
    from src.lending import pr_calling_list_store as default_store
    from src.lending.pr_calling_lists import build_criteria_sets, nightly_window
    from src.tasks import lending_pr_calling_list_pull as pull

    store = store or default_store
    make_plan = make_plan or pull.make_plan
    buy_and_store = buy_and_store or pull.buy_and_store
    outcome = PullOutcome()
    exports_left = remaining(caps.max_exports, store.nightly_exports_since(db, night_start(night)))
    paid_left = caps.max_paid_exports
    cash_matched: dict[str, frozenset[str]] = {}
    for list_key in NIGHTLY_LISTS:
        for state in caps.states:
            window = nightly_window(list_key, as_of=night)
            sets = build_criteria_sets(list_key, as_of=night, window=window, state=state)
            exclude = frozenset()
            if list_key == INVESTOR_PURCHASE:  # left to the cash list: matched tonight or already bought there
                exclude = cash_matched.get(state, frozenset()) | frozenset(store.stored_radar_ids(db, CASH_BUYER))
            plan = make_plan(db, port, list_key, sets, window, exclude=exclude)
            if list_key == CASH_BUYER:
                cash_matched[state] = plan.matched_ids
            allowed = min(exports_left, plan.free_remaining + paid_left)
            trimmed = trim_plan(plan, allowed)
            key = f"{list_key}:{state}"
            outcome.by_list[key] = {**plan.summary(), "buying_tonight": trimmed.total}
            outcome.left_for_later += plan.total - trimmed.total
            if not apply or (trimmed.total == 0 and not trimmed.reused):
                continue
            bought, stored, error = _buy(db, port, store, buy_and_store, list_key, state, window, sets, trimmed,
                                         night=night, max_paid=paid_left)
            outcome.bought += bought
            outcome.stored += stored
            exports_left = remaining(exports_left, bought)
            paid_left = remaining(paid_left, max(0, bought - trimmed.free_remaining))
            outcome.by_list[key].update(bought=bought, stored=stored)
            if error:
                outcome.stopped = f"{key}: {error}"
                return outcome
    return outcome


def _buy(db, port, store, buy_and_store, list_key: str, state: str, window, sets, plan, *, night: date,
         max_paid: int) -> tuple[int, int, Optional[str]]:
    from src.tasks.lending_pr_calling_list_pull import PullProgress

    pull_id = store.start_pull(db, list_key, window.start, window.end,
                               [{"label": s.label, "criteria": s.criteria} for s in sets], plan.total,
                               state=state, run_kind="nightly")
    db.commit()
    progress = PullProgress()
    status, error = "done", None
    try:
        buy_and_store(db, port, pull_id, list_key, plan, as_of=night, progress=progress, max_paid=max_paid,
                      balance_criteria=sets[0].criteria)
    except Exception as exc:  # class only: provider and SQL errors can carry record data
        db.rollback()
        status, error = "failed", type(exc).__name__
        logger.error("[pr-nightly] %s %s pull %s stopped (%d bought, all on disk): %s", list_key, state, pull_id,
                     progress.bought, error)
    store.update_pull(db, pull_id, exports_consumed=progress.bought, records_stored=progress.stored,
                      status=status, error=error)
    db.commit()
    return progress.bought, progress.stored, error


# ── trace and DNC ───────────────────────────────────────────────────────────

@dataclass
class TraceOutcome:
    credits_left_before: int = 0
    by_list: dict[str, dict] = field(default_factory=dict)


def run_trace_stage(db, *, caps: NightlyCaps, night: date, balance: Callable[[], int], submit_for: Callable,
                    fetch: Callable, write_ledger, write_contacts, apply: bool,
                    root: Optional[Path] = None) -> TraceOutcome:
    """Resume and replay paid queues first (free), then trace new people within the night's credits."""
    from src.tasks import lending_pr_calling_list_trace as trace

    outcome = TraceOutcome(credits_left_before=remaining(caps.trace_credits, trace_credits_sent(night, root)))
    for list_key in NIGHTLY_LISTS:
        folder = nightly_folder(list_key, night, root)
        leads = trace.load_leads(db, list_key)
        if not apply:
            outcome.by_list[list_key] = {"people_to_trace": len(leads)}
            continue
        resumed = trace.resume_pending(list_key, fetch=fetch)
        replay = trace.replay_landed(db, list_key, write_ledger=write_ledger, write_contacts=write_contacts)
        db.commit()
        leads = trace.load_leads(db, list_key)  # replay may have found some of them
        credits_left = min(remaining(caps.trace_credits, trace_credits_sent(night, root)), max(0, balance()))
        result = {"resumed_queues": resumed, "replayed": replay, "people_to_trace": len(leads)}
        if credits_left and leads:
            result["trace"] = trace.run_trace(db, leads, folder=folder, trace_limit=credits_left, balance=balance,
                                              submit=submit_for(folder), write_ledger=write_ledger,
                                              write_contacts=write_contacts)
            db.commit()
            result["reconciled"] = trace.reconcile_queues(db, folder, fetch=fetch, write_ledger=write_ledger,
                                                          write_contacts=write_contacts)
            db.commit()
        outcome.by_list[list_key] = result
    return outcome


_QUALIFYING_SQL = text("SELECT radar_id FROM lending.pr_list_records WHERE list_key = :k AND qualifies")


def run_dnc_stage(db, *, caps: NightlyCaps, night: date, balance: Callable[[], int], scrubber, apply: bool,
                  root: Optional[Path] = None) -> dict[str, dict]:
    """Do-not-call check of traced phones never checked (a fresh check is reused, free), within the night's
    credits."""
    from src.tasks import lending_pr_calling_list_trace as trace

    results: dict[str, dict] = {}
    for list_key in NIGHTLY_LISTS:
        radar_ids = [r[0] for r in db.execute(_QUALIFYING_SQL, {"k": list_key})]
        if not apply:
            results[list_key] = {"qualifying": len(radar_ids)}
            continue
        credits_left = min(remaining(caps.dnc_credits, dnc_credits_sent(night, root)), max(0, balance()))
        if not credits_left:
            results[list_key] = {"qualifying": len(radar_ids), "skipped": "no do-not-call credits left tonight"}
            continue
        results[list_key] = trace.run_dnc(db, radar_ids, folder=nightly_folder(list_key, night, root),
                                          run_id=f"pr-nightly-{list_key}-{night.isoformat()}", scrubber=scrubber,
                                          max_phones=credits_left, skip_fresh=True)
        db.commit()
    return results


# ── load ────────────────────────────────────────────────────────────────────

_ACTIVE_PHONES_SQL = text("SELECT DISTINCT phone FROM lending.dialer_load_records WHERE active AND phone = ANY(:p)")


def load_ready_records(db) -> tuple[list[dict], dict[str, Any]]:
    """Every nightly list's DNC-clean people (one per phone, earlier lists win), minus phones already loaded."""
    from src.tasks.lending_pr_calling_list_export import build

    records: list[dict] = []
    counts: dict[str, Any] = {}
    seen: set[str] = set()
    for list_key in NIGHTLY_LISTS:
        built, list_counts = build(db, list_key)
        fresh = [r for r in built if r["phone"] not in seen]
        seen.update(r["phone"] for r in fresh)
        records.extend(fresh)
        counts[list_key] = {**list_counts, "on_an_earlier_list": len(built) - len(fresh)}
    active = {r[0] for r in db.execute(_ACTIVE_PHONES_SQL, {"p": sorted(seen)})} if seen else set()
    counts["already_loaded"] = sum(1 for r in records if r["phone"] in active)
    return [r for r in records if r["phone"] not in active], counts


def in_load_hour(now: datetime) -> bool:
    return now.astimezone(ET).hour == LOAD_HOUR_ET


# ── command line ────────────────────────────────────────────────────────────

def _parse_args(argv: Optional[list[str]]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Nightly PropertyRadar calling-list pipeline (FL + GA).")
    parser.add_argument("--stage", choices=STAGES, required=True)
    parser.add_argument("--apply", action="store_true", help="spend within the nightly caps (default: plan only)")
    parser.add_argument("--any-hour", action="store_true",
                        help="load: run outside 09:xx ET (the dialer load still enforces calling hours)")
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    from config.settings import get_settings

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = _parse_args(argv)
    settings = get_settings()
    if args.apply and not settings.lending_pr_nightly_enabled:
        logger.info("[pr-nightly] off (LENDING_PR_NIGHTLY_ENABLED=false)")
        return EXIT_OK
    try:
        caps = nightly_caps(settings)
    except ValueError as exc:
        logger.error("[pr-nightly] refused: %s", exc)
        return EXIT_REFUSED
    now = datetime.now(timezone.utc)
    if args.stage == "load" and args.apply and not args.any_hour and not in_load_hour(now):
        logger.info("[pr-nightly] load: outside %02d:xx ET; the other cron line runs it", LOAD_HOUR_ET)
        return EXIT_OK

    from src.lending import pr_list_landing as landing

    try:
        lock = landing.acquire_pull_lock()
    except landing.PullLocked as exc:
        logger.error("[pr-nightly] refused: %s", exc)
        return EXIT_REFUSED
    try:
        return _run_stage(args, settings, caps, now)
    finally:
        landing.release_pull_lock(lock)


def _run_stage(args: argparse.Namespace, settings: Any, caps: NightlyCaps, now: datetime) -> int:
    from src.core.database import get_db_context

    night = night_of(now)
    with get_db_context() as db:
        if args.stage == "pull":
            summary, failed = _stage_pull(db, settings, caps, night, args.apply)
        elif args.stage == "trace":
            summary, failed = _stage_trace(db, settings, caps, night, args.apply)
        elif args.stage == "dnc":
            summary, failed = _stage_dnc(db, caps, night, args.apply)
        else:
            summary, failed = _stage_load(db, now, args.apply)
    message = f"PropertyRadar nightly {args.stage} ({night.isoformat()}){'' if args.apply else ' dry run'}: " \
              f"{json.dumps(summary, default=str)}"
    logger.info("[pr-nightly] %s", message)
    if args.apply:
        _notify(message, failed)
    if failed and args.stage == "load" and summary.get("refused"):
        return EXIT_REFUSED
    return EXIT_FAILED if failed else EXIT_OK


def _notify(message: str, failed: bool) -> None:
    from src.lending.ops_alerts import post_ops_alert, post_ops_notice

    (post_ops_alert if failed else post_ops_notice)(message[:3000])


def _stage_pull(db, settings: Any, caps: NightlyCaps, night: date, apply: bool) -> tuple[dict, bool]:
    from src.services.property_radar_port import get_property_radar_port

    if apply and settings.property_radar_mode != "live":
        return {"refused": "PROPERTY_RADAR_MODE is not live, so nothing real can be bought"}, True
    outcome = run_pull(db, get_property_radar_port(), caps=caps, night=night, apply=apply)
    summary: dict[str, Any] = {"exports_bought": outcome.bought, "records_stored": outcome.stored,
                               "left_for_a_later_night": outcome.left_for_later, "lists": outcome.by_list}
    if outcome.stopped:
        summary["stopped"] = outcome.stopped
    if apply:
        from src.lending.pr_list_linking import link_unlinked_records

        summary["linking"] = link_unlinked_records(db)
        db.commit()
    return summary, outcome.stopped is not None


def _tracerfy_key(settings: Any) -> Optional[str]:
    secret = settings.tracerfy_api_key
    return secret.get_secret_value() if secret else None


def _stage_trace(db, settings: Any, caps: NightlyCaps, night: date, apply: bool) -> tuple[dict, bool]:
    from src.tasks import lending_pr_calling_list_trace as trace
    from src.tasks.property_radar_lead_handoff import _write_traced_contacts, _write_usage_ledger

    api_key = _tracerfy_key(settings)
    if apply and not api_key:
        return {"refused": "TRACERFY_API_KEY is not set"}, True
    outcome = run_trace_stage(
        db, caps=caps, night=night, balance=trace._balance, apply=apply,
        submit_for=lambda folder: trace.recorded_submit(folder, api_key),
        fetch=trace._fetch_queue(api_key) if api_key else (lambda queue_id: []),
        write_ledger=lambda entries: _write_usage_ledger(db, entries),
        write_contacts=lambda contacts: _write_traced_contacts(db, contacts))
    return {"trace_credits_left": outcome.credits_left_before, "lists": outcome.by_list}, False


def _stage_dnc(db, caps: NightlyCaps, night: date, apply: bool) -> tuple[dict, bool]:
    from src.lending.compliance import tracerfy_scrub
    from src.tasks import lending_pr_calling_list_trace as trace

    results = run_dnc_stage(db, caps=caps, night=night, balance=trace._balance, scrubber=tracerfy_scrub, apply=apply)
    return {"dnc_credits_left": remaining(caps.dnc_credits, dnc_credits_sent(night)), "lists": results}, False


def _stage_load(db, now: datetime, apply: bool) -> tuple[dict, bool]:
    records, counts = load_ready_records(db)
    summary: dict[str, Any] = {"to_load": len(records), "lists": counts}
    if not apply:
        db.rollback()
        return summary, False
    if not records:
        return summary, False
    from src.lending.compliance import tracerfy_scrub
    from src.lending.dialer_load import LoadRefused, run_dialer_load
    from src.lending.dialer_port import get_dialer
    from src.lending.dial_order import sort_for_load

    dialer = get_dialer()
    if dialer is None:
        return {**summary, "refused": "BatchDialer is not configured (BATCHDIALER_API_KEY)"}, True
    missing = dialer.missing_for_load() if hasattr(dialer, "missing_for_load") else []
    if missing:
        return {**summary, "refused": f"unconfirmed dialer endpoint(s): {', '.join(missing)}"}, True
    run_id = f"pr-nightly-load-{now:%Y%m%dT%H%M%SZ}"
    try:
        report = run_dialer_load(sort_for_load(records), db, run_id=run_id, dry_run=False, scrubber=tracerfy_scrub,
                                 dialer=dialer, commit=db.commit, now=now)
    except LoadRefused as exc:
        db.rollback()
        return {**summary, "refused": str(exc)}, True
    report_dict = report.as_dict()
    summary["load"] = {k: v for k, v in report_dict.items() if not isinstance(v, list)}  # counts, no phone lists
    return summary, bool(report.failed)


if __name__ == "__main__":
    raise SystemExit(main())
