"""Tracerfy lookup of one person per new Florida real estate LLC, for cold email (T3).

Runs after the daily export (``src/tasks/sunbiz_re_llc_export.py``): downloads the same day's state file and the
CATCH_UP_DAYS before it (a file the state posted late is still looked up), keeps the same LLCs, picks a person for each (``src/lending/sunbiz_llc_trace.py``), asks Tracerfy for their phone and email, and stores
the hits in ``lending.sunbiz_llc_contacts``. It never touches the Drive CSV, so a slow lookup cannot delay it.

Every lookup goes through the shared ledger (``skip_trace_ledger``), under address + ZIP + person so it never blocks a
PropertyRadar lookup at the same address: a person is never paid for twice, and SUNBIZ_LLC_TRACE_MAX_COST_CENTS limits a
run (batches are sized to the cap, so a day over it is looked up up to the cap; 0 means no lookups). ``--dry-run`` reads the ledger and
counts what a run would look up and its worst-case cost; it spends nothing and writes nothing. Off until
SUNBIZ_LLC_TRACE_ENABLED=true. run.sh never retries this module (a retry after a lookup began could pay again).

    python -m src.tasks.sunbiz_llc_trace [--day YYYY-MM-DD] [--dry-run]
"""
from __future__ import annotations

import argparse
import logging
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from typing import Optional, Sequence

from sqlalchemy import text

from config.sunbiz_re_llc import CATCH_UP_DAYS, SFTP_HOST_KEY
from src.lending.sunbiz_llc_trace import Plan, TraceLead, ledger_key, plan
from src.scrappers.sunbiz.daily_filings import DailyFileSource, parse_file, real_estate_llcs
from src.services.property_radar.live_trace import CENTS_PER_HIT, trace_staged_leads
from src.services.property_radar.trace_contacts import LeadContacts
from src.services.skip_trace_ledger import RunSpendCap, already_traced, trace_key
from src.tasks.sunbiz_re_llc_export import LOG_FORMAT, export_day

logger = logging.getLogger(__name__)

_UPSERT = text("""
    INSERT INTO lending.sunbiz_llc_contacts (document_number, entity_name, subject_role, subject_name, phones, emails)
    VALUES (:doc, :entity, :role, :name, :phones, :emails)
    ON CONFLICT (document_number) DO UPDATE SET phones = EXCLUDED.phones, emails = EXCLUDED.emails, traced_at = now()
""")


def day_plan(source: DailyFileSource, day: date) -> Optional[Plan]:
    """The day's lookup plan, or None when the state produced no file that day."""
    data = source.fetch(day)
    if data is None:
        return None
    filings, _ = parse_file(data)
    return plan(f for f, _ in real_estate_llcs(filings, day))


def window_plan(source: DailyFileSource, day: date) -> Plan:
    """``day`` and the CATCH_UP_DAYS before it, newest first, so a state file posted after an earlier run is still looked
    up and a lead left over by the cost cap gets another try. Nothing is paid for twice: every submitted lookup is
    ledgered, hit or miss, and a ledgered lead is skipped."""
    merged, seen = Plan(leads=[], skipped=Counter()), set()
    for back in range(CATCH_UP_DAYS + 1):
        found = day_plan(source, day - timedelta(days=back))
        if found is None:
            continue
        merged.skipped.update(found.skipped)
        for lead in found.leads:
            if lead.radar_id not in seen:
                seen.add(lead.radar_id)
                merged.leads.append(lead)
    return merged


def _rounds(leads: Sequence[TraceLead]) -> list[list[TraceLead]]:
    """The shared tracer sends one lead per address in a batch, so two people at one address go in separate rounds."""
    rounds: list[list[TraceLead]] = []
    next_round: dict[str, int] = {}
    for lead in leads:
        key = trace_key(lead.property_address, lead.zip)
        index = next_round.get(key, 0)
        next_round[key] = index + 1
        if index == len(rounds):
            rounds.append([])
        rounds[index].append(lead)
    return rounds


def run(session, day_plan_: Plan, *, submit, cap_cents: int, dry_run: bool) -> dict[str, int]:
    """Look up (or, in a dry run, count) the plan's leads. Returns counts only: no names, addresses or contacts."""
    from src.tasks.property_radar_lead_handoff import _write_usage_ledger

    leads = day_plan_.leads
    counts = {"llcs_with_a_person": len(leads), **{f"skipped_{k}": v for k, v in day_plan_.skipped.items()}}
    ledgered = already_traced(session, "tracerfy", {ledger_key(lead) for lead in leads})
    pending = [lead for lead in leads if not ledgered.get(ledger_key(lead))]
    if dry_run:
        return {**counts, "would_look_up": len(pending), "worst_case_cents": len(pending) * CENTS_PER_HIT}
    if cap_cents <= 0:  # RunSpendCap reads 0 as "no limit": a zero cap must mean no lookups
        return {**counts, "looked_up": 0, "over_cap": len(pending)}
    by_doc = {lead.radar_id: lead for lead in leads}
    cap = RunSpendCap(cap_cents)
    batch_size = max(1, cap_cents // CENTS_PER_HIT)  # a batch is refused whole when its worst case passes the cap
    total = {"looked_up": 0, "with_contacts": 0, "already_traced": 0, "over_cap": 0, "no_result": 0}

    def write_contacts(contacts: dict[str, LeadContacts]) -> None:
        if contacts:  # not committed here: the ledger write that follows commits both together
            session.execute(_UPSERT, [{"doc": doc, "entity": by_doc[doc].entity_name[:200], "role": by_doc[doc].role,
                                       "name": by_doc[doc].subject_name[:80], "phones": list(c.phones),
                                       "emails": list(c.emails)} for doc, c in contacts.items()])

    for round_leads in _rounds(leads):
        qualified = {trace_key(lead.property_address, lead.zip): ledger_key(lead) for lead in round_leads}
        ledgered = already_traced(session, "tracerfy", set(qualified.values()))  # re-read: earlier rounds just wrote

        def write_ledger(entries: list[dict]) -> None:
            _write_usage_ledger(session, [{**e, "target_address": qualified[e["target_address"]]} for e in entries])

        outcome = trace_staged_leads(
            round_leads, ledger={plain: ledgered[q] for plain, q in qualified.items() if ledgered.get(q)},
            submit=submit, write_ledger=write_ledger, write_contacts=write_contacts, cap=cap, batch_size=batch_size)
        total["looked_up"] += outcome.submitted
        total["with_contacts"] += len(outcome.contacts)
        total["already_traced"] += outcome.skipped_already_traced
        total["over_cap"] += outcome.skipped_cap
        total["no_result"] += outcome.skipped_no_result
        if outcome.skipped_cap or outcome.skipped_no_result:
            break  # the cap is spent (or Tracerfy answered nothing): later rounds would only be refused too
    return {**counts, **total}


def main(argv: Optional[Sequence[str]] = None) -> int:
    from config.settings import get_settings
    from src.core.database import get_db_context
    from src.scrappers.sunbiz.daily_filings import SunbizSftp
    from src.tasks.property_radar_lead_handoff import _tracerfy_submit

    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    parser = argparse.ArgumentParser(description="Tracerfy lookup of one person per new Florida real estate LLC")
    parser.add_argument("--day", type=date.fromisoformat, help="state file date (default: yesterday ET)")
    parser.add_argument("--dry-run", action="store_true", help="count the lookups and their worst-case cost; spend nothing")
    args = parser.parse_args(argv)

    s = get_settings()
    if not args.dry_run and not (s.sunbiz_llc_trace_enabled and s.tracerfy_api_key):
        logger.info("[sunbiz-llc-trace] off (needs SUNBIZ_LLC_TRACE_ENABLED=true and TRACERFY_API_KEY)")
        return 0
    if not (s.sunbiz_sftp_username and s.sunbiz_sftp_password):
        logger.error("[sunbiz-llc-trace] SUNBIZ_SFTP_USERNAME / SUNBIZ_SFTP_PASSWORD not set")
        return 0
    day = args.day or export_day(datetime.now(timezone.utc))
    source = SunbizSftp(s.sunbiz_sftp_host, s.sunbiz_sftp_username, s.sunbiz_sftp_password.get_secret_value(),
                        s.sunbiz_sftp_host_key or SFTP_HOST_KEY)
    try:
        window = window_plan(source, day)
        with get_db_context() as session:
            counts = run(session, window, submit=_tracerfy_submit, cap_cents=s.sunbiz_llc_trace_max_cost_cents,
                         dry_run=args.dry_run)
    except Exception as exc:  # class only: SFTP, Tracerfy and DB errors can embed hosts and ids
        logger.error("[sunbiz-llc-trace] %s failed (%s)", day, type(exc).__name__)
        return 1
    logger.info("[sunbiz-llc-trace] %s%s %s", "dry run: " if args.dry_run else "", day, counts)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
