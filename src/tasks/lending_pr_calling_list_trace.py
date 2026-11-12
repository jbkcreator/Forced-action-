"""Trace and do-not-call scrub a PropertyRadar calling list's contacts with Tracerfy, within the credits on hand.

One lead per person (``contact_key``) from ``lending.pr_list_records``: confirmed matches first, then
likely; only people with a name (individual owners, or a company with a known principal), since a
company with no name costs two credits per hit. Already-traced addresses are skipped by the shared
skip-trace ledger, so nothing is paid for twice.

Spend safety: tracing has its own credit limit (``--trace-credits``); the rest of the balance is kept
for the do-not-call check. Trace spend is read from the live Tracerfy balance, and each batch is sized
so that even if every lead in it is found (1 credit each) it cannot pass the limit. Each person found
needs 1 do-not-call credit, so found people never outnumber the trace credits spent. Every Tracerfy response is
written to disk exactly as returned (``data/property_radar_lists/<list>/tracerfy/<run>/``) before the
database sees it. The do-not-call check uses each person's first phone only.

Dry run by default (balance and plan only). ``--live`` spends credits.

Usage:
    python -m src.tasks.lending_pr_calling_list_trace --list verified_maturity
    python -m src.tasks.lending_pr_calling_list_trace --list verified_maturity --live --trace-credits 648
"""
from __future__ import annotations

import argparse
import json
import logging
import os
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

from sqlalchemy import text

from config.lending_calling_lists import CALLING_LISTS, LANDING_ROOT, WHOLESALER
from src.core.database import get_db_context
from src.services.phone_utils import normalize as normalize_phone
from src.services.property_radar.live_trace import trace_staged_leads
from src.services.skip_trace_ledger import ADVANCED, NORMAL, RunSpendCap, already_traced, trace_key

logger = logging.getLogger(__name__)

BATCH_SIZE = 250
MAX_BATCH_SIZE = 3000  # records Tracerfy accepts in one POST /trace/ queue
TRACE_CREDITS_PER_HIT = 1  # name + address trace; company-only (2-credit) leads are not selected

# The trace goes to the owner's MAILING address (these owners do not live at the property; the
# property address found ~7% against ~62% for the mailing address), the property when none is on file.
# A person already traced on any of their records is left out.
_LEADS_SQL = text("""
    SELECT DISTINCT ON (r.contact_key)
           r.radar_id,
           coalesce(nullif(r.owner_mailing_address, ''), r.property_address) AS property_address,
           CASE WHEN nullif(r.owner_mailing_address, '') IS NULL THEN r.city ELSE r.owner_mailing_city END AS city,
           CASE WHEN nullif(r.owner_mailing_address, '') IS NULL THEN r.state ELSE r.owner_mailing_state END AS state,
           CASE WHEN nullif(r.owner_mailing_address, '') IS NULL THEN r.zip ELSE r.owner_mailing_zip END AS zip,
           r.owner_name, r.principal_name, r.match_strength
      FROM lending.pr_list_records r
     WHERE r.list_key = :k AND r.qualifies AND r.contact_key IS NOT NULL
       AND (r.ownership_type IS DISTINCT FROM 'Corporate' OR r.principal_name IS NOT NULL)
       AND NOT EXISTS (SELECT 1 FROM lending.pr_list_records r2
                         JOIN property_radar_traced_contacts t ON t.radar_id = r2.radar_id
                        WHERE r2.list_key = r.list_key AND r2.contact_key = r.contact_key)
     ORDER BY r.contact_key, (r.match_strength = 'confirmed') DESC, r.id
""")


@dataclass(frozen=True)
class Lead:
    """A person to trace. ``property_address``/``city``/``state``/``zip`` hold the address the trace is
    sent to (the owner's mailing address); the names match what trace_staged_leads reads."""

    radar_id: str
    property_address: Optional[str]
    city: Optional[str]
    state: Optional[str]
    zip: Optional[str]
    owner_name: Optional[str]
    principal_name: Optional[str]
    match_strength: Optional[str] = None


# Lists whose people come as a name only: traced from lending.pr_list_trace_subjects (who + where, with source).
_SUBJECT_LEADS_SQL = text("""
    SELECT s.radar_id, s.address AS property_address, s.city, s.state, s.zip,
           s.first_name || ' ' || s.last_name AS owner_name, NULL AS principal_name, 'confirmed' AS match_strength
      FROM lending.pr_list_trace_subjects s
      JOIN lending.pr_list_records r ON r.list_key = s.list_key AND r.radar_id = s.radar_id
     WHERE s.list_key = :k AND s.untraceable_reason IS NULL
       AND (CAST(:resold_from AS date) IS NULL OR r.last_transfer_date >= CAST(:resold_from AS date))
       AND (CAST(:resold_before AS date) IS NULL OR r.last_transfer_date < CAST(:resold_before AS date))
       AND NOT EXISTS (SELECT 1 FROM property_radar_traced_contacts t WHERE t.radar_id = s.radar_id)
     ORDER BY r.last_transfer_date DESC NULLS LAST, s.id
""")
SUBJECT_LISTS = frozenset({WHOLESALER})


def load_leads(db, list_key: str, resold_from: Optional[date] = None,
               resold_before: Optional[date] = None) -> list[Lead]:
    """Name-only lists: only people whose qualifying resale was on or after ``resold_from`` and before
    ``resold_before`` (either may be left open), most recent first, so one window is traced at a time."""
    if list_key in SUBJECT_LISTS:
        rows = db.execute(_SUBJECT_LEADS_SQL, {"k": list_key, "resold_from": resold_from,
                                                 "resold_before": resold_before})
    else:
        rows = db.execute(_LEADS_SQL, {"k": list_key})
    leads = [Lead(**row._mapping) for row in rows]
    return sorted(leads, key=lambda lead: lead.match_strength != "confirmed")  # stable: confirmed first


ADVANCED_CREDITS_PER_HIT = 2  # address-only retry of a normal miss


def next_batch_size(trace_limit: int, trace_spent: int, batch_size: int = BATCH_SIZE,
                    credits_per_hit: int = TRACE_CREDITS_PER_HIT) -> int:
    """Largest batch that cannot pass the trace limit even if every lead in it is found."""
    return max(0, min(batch_size, (trace_limit - trace_spent) // credits_per_hit))


def write_landing(folder: Path, name: str, payload: object) -> Path:
    """As-is to disk, atomically, before anything is parsed or stored."""
    folder.mkdir(parents=True, exist_ok=True)
    path, tmp = folder / name, folder / f"{name}.tmp"
    with tmp.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, default=str)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)
    return path


def landed(fn: Callable, folder: Path, prefix: str) -> Callable:
    """Wrap a Tracerfy call so what it sent and received lands on disk before it returns."""
    def call(payload):
        received = fn(payload)
        # Numbered from the files already in the folder, so a new wrapper (one per batch) never overwrites.
        number = len(list(folder.glob(f"{prefix}_*.json"))) + 1
        write_landing(folder, f"{prefix}_{number:04d}.json", {"sent": payload, "received": received})
        return received
    return call


# Tracerfy fills a queue's results gradually; accepting it once the count stops growing for a few polls
# once returned 8 of 65 hits (all 65 charged). Wait longer before accepting, and reconcile afterwards.
STABLE_ROUNDS = 6
SETTLE_SECONDS = 120


def recorded_submit(folder: Path, api_key: str, *, address_only: bool = False) -> Callable[[list[dict]], list[dict]]:
    """Tracerfy batch submit that waits for the queue to settle and writes what was sent and received, with the
    queue id, to disk before returning (so the queue can be fetched again for free and reconciled)."""
    from src.services.tracerfy_batch import _poll_trace_queue, _submit_trace_batch

    def submit(rows: list[dict]) -> list[dict]:
        queue_id, wait = _submit_trace_batch(rows, api_key, address_only=address_only)
        received = _poll_trace_queue(queue_id, api_key, wait, stable_rounds_required=STABLE_ROUNDS,
                                     min_settle_seconds=SETTLE_SECONDS)
        # Advanced results are kept apart: the replay and late-hit steps read trace_* files and know
        # nothing of the name check an address-only hit needs.
        prefix = "advanced" if address_only else "trace"
        number = len(list(folder.glob(f"{prefix}_*.json"))) + 1
        write_landing(folder, f"{prefix}_{number:04d}.json", {"queue_id": queue_id, "sent": rows, "received": received})
        return received

    submit.lands_on_disk = True  # type: ignore[attr-defined]
    return submit


def reconcile_queues(db, folder: Path, *, fetch, write_ledger, write_contacts) -> dict[str, int]:
    """Fetch every queue of this run again (free) and store hits that arrived after the batch was accepted:
    contacts for a lead that has none yet, plus a ledger row for the paid hit. On disk before the database."""
    from src.services.property_radar.live_trace import _contacts_from_row, _core

    found: dict[str, tuple] = {}  # radar_id -> (contacts, ledger entry)
    checked = 0
    for path in sorted(folder.glob("trace_*.json")):
        call = json.loads(path.read_text(encoding="utf-8"))
        if not call.get("queue_id"):
            continue
        checked += 1
        seen = {_core(trace_key(r.get("address"), "")) for r in call["received"]}
        late = [r for r in fetch(call["queue_id"]) if _core(trace_key(r.get("address"), "")) not in seen]
        if not late:
            continue
        write_landing(folder, f"reconcile_{call['queue_id']}.json", {"queue_id": call["queue_id"], "late": late})
        sent = {_core(trace_key(r.get("address"), "")): r for r in call["sent"]}
        for row in late:
            lead = sent.get(_core(trace_key(row.get("address"), "")))
            contacts = _contacts_from_row(row)
            if not lead or not lead.get("label") or contacts.is_empty:
                continue
            found[str(lead["label"])] = (contacts, {
                "vendor": "tracerfy", "purpose": "skip_trace", "success": True, "cost_cents": 2, "property_id": None,
                "target_address": trace_key(lead.get("address"), lead.get("zip")),
                "request_ref": f"reconciled:queue_{call['queue_id']}", "created_at": datetime.now(timezone.utc)})
    if found:
        already = {r[0] for r in db.execute(text(
            "SELECT radar_id FROM property_radar_traced_contacts WHERE radar_id = ANY(:ids)"), {"ids": list(found)})}
        found = {rid: pair for rid, pair in found.items() if rid not in already}
    if found:
        write_contacts({rid: pair[0] for rid, pair in found.items()})
        write_ledger([pair[1] for pair in found.values()])  # commits contacts and ledger together
    return {"queues_checked": checked, "late_hits_stored": len(found)}


def replay_landed(db, list_key: str, *, write_ledger, write_contacts) -> dict[str, int]:
    """Write every landed Tracerfy result for the list to the database from disk, without calling
    Tracerfy. Addresses already in the ledger are skipped, so replaying twice writes nothing new.
    This is what makes an interrupted run safe: a result paid for is never paid for again."""
    replayed = found = 0
    for path in sorted((LANDING_ROOT / list_key / "tracerfy").glob("*/trace_*.json")):
        with path.open(encoding="utf-8") as handle:
            call = json.load(handle)
        leads = [Lead(radar_id=str(row["label"]), property_address=row.get("address"), city=row.get("city"),
                      state=row.get("state"), zip=row.get("zip"),
                      owner_name=" ".join(p for p in (row.get("first_name"), row.get("last_name")) if p) or None,
                      principal_name=None)
                 for row in call["sent"] if row.get("label")]
        keys = {trace_key(lead.property_address, lead.zip) for lead in leads}
        outcome = trace_staged_leads(
            leads, ledger=already_traced(db, "tracerfy", keys), submit=lambda rows, got=call["received"]: got,
            write_ledger=write_ledger, write_contacts=write_contacts, cap=RunSpendCap(10 ** 9),
        )
        replayed += outcome.submitted
        found += len(outcome.contacts)
    return {"replayed_addresses": replayed, "replayed_found": found}


def _balance() -> int:
    from src.services.tracerfy_batch import get_tracerfy_balance

    body = get_tracerfy_balance()
    if "balance" not in body:
        raise RuntimeError("Tracerfy balance unavailable")
    return int(body["balance"])


def _traced_best_phones(db, radar_ids: Sequence[str]) -> dict[str, str]:
    """radar_id -> first traced phone (mobile first, as the trace parser orders them)."""
    rows = db.execute(text("SELECT radar_id, phones FROM property_radar_traced_contacts WHERE radar_id = ANY(:ids)"),
                      {"ids": list(radar_ids)}).all()
    best = {}
    for row in rows:
        phone = next((p for p in (normalize_phone(x) for x in (row.phones or [])) if p), None)
        if phone:
            best[row.radar_id] = phone
    return best


def next_unchecked_phone(phones: Sequence[str], blocked: set[str], clean: set[str]) -> Optional[str]:
    """The person's first phone never checked, only when every checked phone was blocked: a person with a
    clean phone is already loadable, and a phone already checked is never paid for again."""
    if any(p in clean for p in phones):
        return None
    return next((p for p in phones if p not in blocked), None)


def _next_phones(db, radar_ids: Sequence[str]) -> dict[str, str]:
    """radar_id -> the next phone to check for people whose checked phones were all blocked."""
    rows = db.execute(text("SELECT radar_id, phones FROM property_radar_traced_contacts WHERE radar_id = ANY(:ids)"),
                      {"ids": list(radar_ids)}).all()
    people = {row.radar_id: [p for p in dict.fromkeys(normalize_phone(x) for x in row.phones or []) if p]
              for row in rows}
    every_phone = list({p for phones in people.values() for p in phones})
    blocked, clean = set(), set()
    for phone, is_blocked in db.execute(text(
            "SELECT phone, bool_or(national_dnc OR state_dnc OR litigator) FROM lending.dnc_scrubs "
            "WHERE phone = ANY(:p) GROUP BY phone"), {"p": every_phone}):
        (blocked if is_blocked else clean).add(phone)
    chosen = {rid: next_unchecked_phone(phones, blocked, clean) for rid, phones in people.items()}
    return {rid: phone for rid, phone in chosen.items() if phone}


def sent_with_unsplit_name(list_key: str) -> set[str]:
    """RadarIDs once sent to Tracerfy with the county ``LAST,FIRST`` name unsplit (before the name fix): Tracerfy
    could not match those names, so their misses say nothing about the person. Read from the landing files;
    anyone sent again since with a split name is left out (that later miss is a real one)."""
    unsplit: set[str] = set()
    split: set[str] = set()
    for path in (LANDING_ROOT / list_key / "tracerfy").glob("*/trace_*.json"):
        for row in json.loads(path.read_text(encoding="utf-8")).get("sent") or []:
            if row.get("label"):
                name = f"{row.get('first_name') or ''}{row.get('last_name') or ''}"
                (unsplit if "," in name else split).add(row["label"])
    return unsplit - split


def run_trace(db, leads: list[Lead], *, folder: Path, trace_limit: int, balance: Callable[[], int],
              submit, write_ledger, write_contacts, batch_size: int = BATCH_SIZE,
              resend_keys: frozenset[str] = frozenset(), mode: str = NORMAL) -> dict[str, int]:
    """Trace leads in batches until the trace credit limit is reached (spend read from the balance)."""
    start_balance = balance()
    found: set[str] = set()
    submitted = position = 0
    while position < len(leads):
        trace_spent = start_balance - balance()
        size = next_batch_size(trace_limit, trace_spent, batch_size,
                               ADVANCED_CREDITS_PER_HIT if mode == ADVANCED else TRACE_CREDITS_PER_HIT)
        if size == 0:
            logger.warning("[list-trace] trace limit of %d credits reached; %d leads left untraced",
                           trace_limit, len(leads) - position)
            break
        batch = leads[position:position + size]
        position += len(batch)
        keys = {trace_key(lead.property_address, lead.zip) for lead in batch}
        outcome = trace_staged_leads(
            batch, ledger={k: v for k, v in already_traced(db, "tracerfy", keys).items() if k not in resend_keys},
            submit=submit if getattr(submit, "lands_on_disk", False) else landed(submit, folder, "trace"),
            write_ledger=write_ledger, write_contacts=write_contacts, batch_size=batch_size, mode=mode,
            cap=RunSpendCap(len(batch) * 4),  # cents; the credit limit above is the real gate
        )
        submitted += outcome.submitted
        found.update(outcome.contacts)
    return {"leads": len(leads), "submitted": submitted, "found": len(found),
            "trace_credits_spent": start_balance - balance(), "left_untraced": len(leads) - position}


def run_dnc(db, radar_ids: Sequence[str], *, folder: Path, run_id: str, scrubber, max_phones: int,
            next_phone: bool = False) -> dict[str, int]:
    """Do-not-call check on each traced person's first phone, at most ``max_phones`` (the credits left);
    blocks are logged to load_exclusions."""
    from src.lending.compliance import filter_loadable

    best = _next_phones(db, radar_ids) if next_phone else _traced_best_phones(db, radar_ids)
    records = [{"phone": phone, "state": "FL", "source_record_ref": f"pr_list:{rid}"} for rid, phone in best.items()]
    if len(records) > max_phones:
        logger.warning("[list-trace] %d phones to check but %d credits left; checking the first %d",
                       len(records), max_phones, max_phones)
        records = records[:max_phones]
    results = filter_loadable(records, db, scrubber=landed(scrubber, folder, "dnc"), run_id=run_id)
    clean = sum(1 for r in results if r.allowed)
    return {"with_phone": len(records), "clean": clean, "blocked": len(records) - clean}


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Trace and DNC-scrub a calling list within the Tracerfy balance.")
    parser.add_argument("--list", dest="list_key", choices=CALLING_LISTS, required=True)
    parser.add_argument("--live", action="store_true", help="spend Tracerfy credits (default: plan only)")
    parser.add_argument("--trace-credits", type=int, help="most credits tracing may spend; the rest is kept for the scrub")
    parser.add_argument("--advanced-misses", action="store_true",
                        help="address-only trace (2 credits a hit) of people whose normal trace missed; a hit is "
                             "kept only when the returned last name is the owner's")
    parser.add_argument("--retrace-unsplit-names", action="store_true",
                        help="trace again only the people whose earlier miss was sent with an unsplit LAST,FIRST name")
    parser.add_argument("--next-phone", action="store_true",
                        help="with --dnc-only: check the next phone of people whose checked phones were all blocked")
    parser.add_argument("--dnc-only", action="store_true",
                        help="no tracing: do-not-call check the phones already traced (needs --live)")
    parser.add_argument("--resold-from", type=date.fromisoformat,
                        help="wholesalers: only people whose qualifying resale was on or after this date")
    parser.add_argument("--resold-before", type=date.fromisoformat,
                        help="wholesalers: only people whose qualifying resale was before this date")
    parser.add_argument("--reserve-credits", type=int, default=0,
                        help="with --dnc-only: credits left untouched (e.g. for a trace still to run)")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE,
                        help=f"people per Tracerfy request (1-{MAX_BATCH_SIZE}); fewer requests, same credits")
    args = parser.parse_args(argv)
    if not 1 <= args.batch_size <= MAX_BATCH_SIZE:
        parser.error(f"--batch-size must be between 1 and {MAX_BATCH_SIZE}")

    run_id = f"list-trace-{args.list_key}-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"
    folder = LANDING_ROOT / args.list_key / "tracerfy" / run_id
    if not args.live:
        with get_db_context() as db:
            leads = load_leads(db, args.list_key, args.resold_from, args.resold_before)
            logger.info("[list-trace] %s: %d people to trace, balance %d credits", args.list_key, len(leads), _balance())
        return 0
    # One live job at a time: two runs sharing the balance once checked the same phones twice.
    from src.lending import pr_list_landing as landing
    try:
        lock = landing.acquire_pull_lock()
    except landing.PullLocked as exc:
        logger.error("[list-trace] refused: %s", exc)
        return 3
    try:
        return _run_live(args, run_id, folder)
    finally:
        landing.release_pull_lock(lock)


def _fetch_queue(api_key: str):
    from src.services.tracerfy_batch import _poll_trace_queue

    return lambda queue_id: _poll_trace_queue(str(queue_id), api_key, 0, stable_rounds_required=2, min_settle_seconds=0)


def _run_live(args: argparse.Namespace, run_id: str, folder: Path) -> int:
    from config.settings import get_settings
    from src.lending.compliance import tracerfy_scrub
    from src.tasks.property_radar_lead_handoff import _write_traced_contacts, _write_usage_ledger

    api_key = get_settings().tracerfy_api_key.get_secret_value()
    with get_db_context() as db:
        write_ledger = lambda entries: _write_usage_ledger(db, entries)  # noqa: E731
        write_contacts = lambda contacts: _write_traced_contacts(db, contacts)  # noqa: E731
        start_balance = _balance()
        if args.dnc_only:
            radar_ids = [r[0] for r in db.execute(text(
                "SELECT radar_id FROM lending.pr_list_records WHERE list_key = :k AND qualifies"), {"k": args.list_key})]
            dnc = run_dnc(db, radar_ids, folder=folder, run_id=run_id, scrubber=tracerfy_scrub,
                          max_phones=max(0, start_balance - args.reserve_credits), next_phone=args.next_phone)
            db.commit()
            summary = {"run_id": run_id, "dnc": dnc, "credits_spent": start_balance - _balance()}
            write_landing(folder, "summary.json", summary)
            logger.info("[list-trace] dnc-only: %s", json.dumps(summary))
            return 0
        if not args.trace_credits or args.trace_credits * 2 > start_balance:
            logger.error("[list-trace] refused: pass --trace-credits N with 2N <= the balance (%d), so the "
                         "scrub keeps a credit for every person tracing can find", start_balance)
            return 3
        replay = replay_landed(db, args.list_key, write_ledger=write_ledger, write_contacts=write_contacts)
        db.commit()
        logger.info("[list-trace] replayed landed results from disk: %s", json.dumps(replay))
        leads = load_leads(db, args.list_key, args.resold_from, args.resold_before)
        resend_keys: frozenset[str] = frozenset()
        if args.retrace_unsplit_names:
            unsplit = sent_with_unsplit_name(args.list_key)
            leads = [lead for lead in leads if lead.radar_id in unsplit]
            resend_keys = frozenset(k for lead in leads if (k := trace_key(lead.property_address, lead.zip)))
        logger.info("[list-trace] %s: %d people to trace, balance %d credits", args.list_key, len(leads), start_balance)
        trace = run_trace(db, leads, folder=folder, trace_limit=args.trace_credits, balance=_balance,
                          submit=recorded_submit(folder, api_key, address_only=args.advanced_misses), write_ledger=write_ledger,
                          write_contacts=write_contacts, batch_size=args.batch_size, resend_keys=resend_keys,
                          mode=ADVANCED if args.advanced_misses else NORMAL)
        db.commit()
        reconciled = reconcile_queues(db, folder, fetch=_fetch_queue(api_key), write_ledger=write_ledger,
                                      write_contacts=write_contacts)
        db.commit()
        logger.info("[list-trace] reconciled queues: %s", json.dumps(reconciled))
        dnc = run_dnc(db, [lead.radar_id for lead in leads], folder=folder, run_id=run_id, scrubber=tracerfy_scrub,
                      max_phones=_balance())
        db.commit()
        end_balance = _balance()
        summary = {"run_id": run_id, "trace": trace, "reconciled": reconciled, "dnc": dnc,
                   "credits_spent": start_balance - end_balance, "balance_left": end_balance}
        write_landing(folder, "summary.json", summary)
        logger.info("[list-trace] %s", json.dumps(summary))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
