"""Pull a PropertyRadar calling list (Verified maturity, Cash buyers, Wholesalers) into lending.pr_list_records.

How a pull spends exports: every matching RadarID is listed first (free: a RadarID-only request is
never billed), the ones already bought for the list are dropped, and only the rest are bought, by
RadarID, a page at a time. Each bought page is written to disk exactly as returned (src/lending/
pr_list_landing.py) BEFORE it is parsed or stored; the database is loaded from that page. A record
in a landing file or in the database counts as already bought, so a pull never pays twice: if the
database write fails, ``--load-files`` loads the files again without touching PropertyRadar.

Dry run by default: free listing and preview only, printing what a live run would buy and the free
allowance left. A live run needs ``--confirm-exports N`` equal to the dry run's number, and refuses
to go past the free allowance unless ``--max-paid-exports`` covers the overage. Exports consumed are
read from PropertyRadar's own balance before and after the run.

Usage:
    python -m src.tasks.lending_pr_calling_list_pull --list cash_buyer --from 2026-10-04 --to 2026-11-03
    python -m src.tasks.lending_pr_calling_list_pull --list cash_buyer --from 2026-10-04 --to 2026-11-03 \
        --live --confirm-exports 1582
    python -m src.tasks.lending_pr_calling_list_pull --list verified_maturity            # window from today
    python -m src.tasks.lending_pr_calling_list_pull --list wholesaler --report          # stored results only
    python -m src.tasks.lending_pr_calling_list_pull --list wholesaler --load-files      # files -> database, no API

Needs PROPERTY_RADAR_MODE=live, PROPERTY_RADAR_ENABLED=true and PROPERTY_RADAR_API_KEY.
Exit codes: 0 ok, 1 pull failed part-way (run again to resume), 2 bad arguments, 3 refused.
"""
from __future__ import annotations

import argparse
import json
import logging
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass, field
from datetime import date
from typing import Optional

from config.lending_calling_lists import CALLING_LISTS, DEFAULT_MAX_PAID_EXPORTS, EXPORT_FIELDS, WINDOWED_LISTS
from src.core.database import get_db_context
from src.lending import pr_calling_list_store as store
from src.lending import pr_list_landing as landing
from src.lending.pr_calling_lists import CriteriaSet, PullWindow, build_criteria_sets, pull_window, shape_record
from src.services.property_radar_port import PropertyRadarPort, get_property_radar_port

logger = logging.getLogger(__name__)

EXIT_OK, EXIT_FAILED, EXIT_BAD_ARGS, EXIT_REFUSED = 0, 1, 2, 3
BUY_PAGE = 500  # RadarIDs per purchase: one PropertyRadar page, stored and committed as one batch


def _parse_args(argv: Optional[list[str]]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pull a PropertyRadar calling list into lending.pr_list_records.")
    parser.add_argument("--list", dest="list_key", choices=CALLING_LISTS, required=True)
    parser.add_argument("--from", dest="start", type=date.fromisoformat, help="recording window start (cash_buyer, wholesaler)")
    parser.add_argument("--to", dest="end", type=date.fromisoformat, help="recording window end, inclusive")
    parser.add_argument("--as-of", type=date.fromisoformat, default=None, help="date the maturity window counts from (default today)")
    parser.add_argument("--live", action="store_true", help="buy the records (default: free listing and preview only)")
    parser.add_argument("--confirm-exports", type=int, help="exports you approve buying (the dry run prints it)")
    parser.add_argument("--max-paid-exports", type=int, default=DEFAULT_MAX_PAID_EXPORTS,
                        help="exports allowed beyond the free monthly allowance (default 0)")
    parser.add_argument("--report", action="store_true", help="print what the list holds and exit")
    parser.add_argument("--load-files", action="store_true",
                        help="load every landing file for the list into the database (no PropertyRadar calls)")
    return parser.parse_args(argv)


@dataclass
class PullPlan:
    """What a pull would buy: per search, the matching RadarIDs not yet stored for the list, minus the
    ones another list already bought (``reused``: their stored record is copied, never bought again)."""

    to_buy: dict[str, list[str]]
    matching: dict[str, int]
    already_stored: int
    free_remaining: int
    reused: dict[str, dict] = field(default_factory=dict)

    @property
    def total(self) -> int:
        return sum(len(ids) for ids in self.to_buy.values())

    @property
    def paid(self) -> int:
        return max(0, self.total - self.free_remaining)

    def summary(self) -> dict:
        return {"matching_by_search": self.matching, "already_stored": self.already_stored,
                "reused_from_other_lists": len(self.reused),
                "to_buy_by_search": {k: len(v) for k, v in self.to_buy.items()}, "exports_to_buy": self.total,
                "free_allowance_remaining": self.free_remaining, "paid_exports": self.paid}


def plan_pull(port: PropertyRadarPort, sets: Sequence[CriteriaSet], stored: set[str],
              held_elsewhere: Callable[[list[str]], dict[str, dict]] = lambda ids: {}) -> PullPlan:
    """Free: list every search's RadarIDs, drop the stored ones and any repeated across searches, and
    set aside the ones another list already bought (``held_elsewhere`` returns their stored records)."""
    seen = set(stored)
    to_buy, matching = {}, {}
    already = 0
    for criteria_set in sets:
        ids = port.list_radar_ids(criteria_set.criteria)
        matching[criteria_set.label] = len(ids)
        new = [i for i in dict.fromkeys(ids) if i not in seen]
        already += sum(1 for i in ids if i in stored)
        seen.update(new)
        to_buy[criteria_set.label] = new
    reused = held_elsewhere([i for ids in to_buy.values() for i in ids])
    to_buy = {label: [i for i in ids if i not in reused] for label, ids in to_buy.items()}
    free_remaining = port.cost_preview(sets[0].criteria).quantity_free_remaining
    return PullPlan(to_buy=to_buy, matching=matching, already_stored=already, free_remaining=free_remaining,
                    reused=reused)


class PaidCapReached(RuntimeError):
    """The next page would take paid exports past ``--max-paid-exports``; nothing more is bought."""


def _pages(ids: Sequence[str]) -> Iterator[list[str]]:
    for start in range(0, len(ids), BUY_PAGE):
        yield list(ids[start:start + BUY_PAGE])


@dataclass
class PullProgress:
    bought: int = 0
    stored: int = 0
    pages: int = 0


def buy_and_store(db, port: PropertyRadarPort, pull_id: int, list_key: str, plan: PullPlan,
                  *, as_of: date, progress: PullProgress, max_paid: int, balance_criteria: list[dict]) -> None:
    """Records another list already bought are copied first (no purchase). Then per page: re-check the
    free balance so paid exports never pass ``max_paid``, buy, write the page to disk as-is, then load it
    into the database and commit."""
    if plan.reused:
        progress.pages += 1
        reused = list(plan.reused.values())
        landing.write_page(list_key, pull_id, progress.pages, reused)
        progress.stored += _load(db, pull_id, list_key, reused, as_of=as_of)
        store.update_pull(db, pull_id, exports_consumed=progress.bought, records_stored=progress.stored)
        db.commit()
    paid = 0
    for ids in (page for label_ids in plan.to_buy.values() for page in _pages(label_ids)):
        free_now = port.cost_preview(balance_criteria).quantity_free_remaining
        page_paid = max(0, len(ids) - free_now)
        if paid + page_paid > max_paid:
            raise PaidCapReached(f"next page needs {page_paid} paid exports; {max_paid - paid} left under the cap")
        raw_page = [record.raw for record in port.purchase([{"name": "RadarID", "value": ids}], fields=EXPORT_FIELDS)]
        landing.write_page(list_key, pull_id, progress.pages + 1, raw_page)  # on disk before anything else
        paid += page_paid
        progress.bought += len(raw_page)
        progress.pages += 1
        missing = len(set(ids) - {str(r.get("RadarID")) for r in raw_page})
        if missing:
            logger.warning("[calling-list] page %d: %d requested record(s) not returned (not charged)", progress.pages, missing)
        progress.stored += _load(db, pull_id, list_key, raw_page, as_of=as_of)
        store.update_pull(db, pull_id, exports_consumed=progress.bought, records_stored=progress.stored)
        db.commit()


def _load(db, pull_id: int, list_key: str, raw_page: list[dict], *, as_of: date) -> int:
    return store.store_records(db, pull_id, [shape_record(list_key, raw, as_of=as_of) for raw in raw_page])


def load_files(db, list_key: str, *, as_of: date) -> tuple[int, int]:
    """Every landing file for the list into the database, a page per commit. Returns (pages, records)."""
    pages = records = 0
    for page in landing.iter_pages(list_key):
        records += _load(db, page.pull_id, list_key, page.records, as_of=as_of)
        pages += 1
        db.commit()
    return pages, records


def _exports_spent(port: PropertyRadarPort, plan: PullPlan, sets: Sequence[CriteriaSet], fallback: int) -> int:
    """PropertyRadar's own count: free balance before minus after. Falls back to records received
    when the balance cannot be read or went up (a monthly reset during the run)."""
    try:
        after = port.cost_preview(sets[0].criteria).quantity_free_remaining
    except Exception as exc:  # class only
        logger.warning("[calling-list] could not read the balance after the pull: %s", type(exc).__name__)
        return fallback
    free_used = plan.free_remaining - after
    if free_used < 0:
        return fallback
    # The free balance only shows free exports used; paid ones (bought once it reached 0) show in the
    # records received, so the spend is whichever is larger.
    return max(free_used, fallback)


def _refusal(args: argparse.Namespace, plan: PullPlan) -> Optional[str]:
    if args.confirm_exports != plan.total:
        return f"this pull buys {plan.total} exports; re-run with --confirm-exports {plan.total} once approved"
    if plan.paid > args.max_paid_exports:
        return (f"{plan.paid} exports go past the free allowance; pass --max-paid-exports {plan.paid} "
                f"once that cost (about ${plan.paid * 0.02:,.2f} at $0.02 each) is approved")
    return None


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = _parse_args(argv)
    as_of = args.as_of or date.today()
    try:
        if args.list_key in WINDOWED_LISTS and (args.start is None or args.end is None):
            raise ValueError(f"--from and --to are required for {args.list_key}")
        window: PullWindow = pull_window(args.list_key, as_of=as_of, start=args.start, end=args.end)
        sets = build_criteria_sets(args.list_key, as_of=as_of, window=window)
    except ValueError as exc:
        logger.error("[calling-list] %s", exc)
        return EXIT_BAD_ARGS

    with get_db_context() as db:
        if args.report:
            logger.info("[calling-list] %s report: %s", args.list_key,
                        json.dumps(store.list_report(db, args.list_key), indent=2, default=str))
            return EXIT_OK

        if args.load_files:
            pages, records = load_files(db, args.list_key, as_of=as_of)
            logger.info("[calling-list] %s: loaded %d landing pages (%d records) into the database",
                        args.list_key, pages, records)
            logger.info("[calling-list] %s report: %s", args.list_key,
                        json.dumps(store.list_report(db, args.list_key), indent=2, default=str))
            return EXIT_OK

        port = get_property_radar_port()
        bought = store.stored_radar_ids(db, args.list_key) | landing.landed_radar_ids(args.list_key)
        plan = plan_pull(port, sets, bought,
                         held_elsewhere=lambda ids: store.raw_held_by_other_lists(db, args.list_key, ids))
        logger.info("[calling-list] %s window %s..%s: %s", args.list_key, window.start, window.end,
                    json.dumps(plan.summary()))
        if not args.live:
            return EXIT_OK
        if plan.total == 0 and not plan.reused:
            logger.info("[calling-list] %s: nothing to buy, every matching record is already bought", args.list_key)
            return EXIT_OK
        refusal = _refusal(args, plan)
        if refusal:
            logger.error("[calling-list] refused: %s", refusal)
            return EXIT_REFUSED
        try:
            lock = landing.acquire_pull_lock()
        except landing.PullLocked as exc:
            logger.error("[calling-list] refused: %s", exc)
            return EXIT_REFUSED
        try:
            return _run_live(db, port, args, sets, window, plan, as_of)
        finally:
            landing.release_pull_lock(lock)
    return EXIT_OK


def _run_live(db, port: PropertyRadarPort, args: argparse.Namespace, sets: Sequence[CriteriaSet],
              window: PullWindow, plan: PullPlan, as_of: date) -> int:
    pull_id = store.start_pull(db, args.list_key, window.start, window.end,
                               [{"label": s.label, "criteria": s.criteria} for s in sets], plan.total)
    db.commit()
    progress = PullProgress()
    status, error = "done", None
    try:
        buy_and_store(db, port, pull_id, args.list_key, plan, as_of=as_of, progress=progress,
                      max_paid=args.max_paid_exports, balance_criteria=sets[0].criteria)
    except Exception as exc:  # class only: provider and SQL errors can carry record data
        db.rollback()
        status, error = "failed", type(exc).__name__
        logger.error("[calling-list] pull %s failed part-way (%d bought and saved to disk, %d in the database): "
                     "%s; run --load-files to load what was bought, then run again to buy only what is missing",
                     pull_id, progress.bought, progress.stored, error)
    spent = _exports_spent(port, plan, sets, fallback=progress.bought)
    store.update_pull(db, pull_id, exports_consumed=spent, records_stored=progress.stored,
                      status=status, error=error)
    db.commit()
    if status == "failed":
        return EXIT_FAILED
    logger.info("[calling-list] pull %s done: %d exports spent, %d records stored", pull_id, spent, progress.stored)
    logger.info("[calling-list] %s report: %s", args.list_key,
                json.dumps(store.list_report(db, args.list_key), indent=2, default=str))
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
