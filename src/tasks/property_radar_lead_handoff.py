"""Hand staged PropertyRadar records to FA Max.

Dry run unless --apply, matching this codebase's sweep-task convention: a dry
run reads everything and prints what would be handed off, suppressed or
skipped, and writes nothing.

Usage:
    PYTHONPATH=. python -m src.tasks.property_radar_lead_handoff
    PYTHONPATH=. python -m src.tasks.property_radar_lead_handoff --thin-path
    PYTHONPATH=. python -m src.tasks.property_radar_lead_handoff \\
        --trace-results path/to/trace_results.csv --apply
    PYTHONPATH=. python -m src.tasks.property_radar_lead_handoff --live-trace --apply  # paid Tracerfy trace

--live-trace spends Tracerfy credits (1 per hit) and needs PROPERTY_RADAR_ENABLED=true.

Contact rules (consent rows) follow PROPERTY_RADAR_CONTACT_RULES_ENABLED and
cannot be switched on from the command line.
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

from sqlalchemy import text

from config.settings import get_settings
from src.core.database import get_db_context
from src.services.property_radar.lead_handoff import (
    HandoffReport,
    SqlHandoffStore,
    iter_staged_leads,
    pretrace_eligible,
    run_handoff,
)
from src.services.property_radar.live_trace import trace_staged_leads
from src.services.property_radar.trace_contacts import LeadContacts, load_trace_contacts
from src.services.skip_trace_ledger import RunSpendCap, already_traced, trace_key

logger = logging.getLogger(__name__)

DEFAULT_CAMPAIGN = "maturity_target_lender"


def _tracerfy_submit(rows: list[dict]) -> list[dict]:
    from src.services.tracerfy_batch import _poll_trace_queue, _submit_trace_batch

    api_key = get_settings().tracerfy_api_key.get_secret_value()
    queue_id, wait = _submit_trace_batch(rows, api_key)
    return _poll_trace_queue(queue_id, api_key, wait)


def _write_usage_ledger(session, entries: list[dict]) -> None:
    session.execute(
        text("INSERT INTO enrichment_usage_logs (vendor, purpose, success, cost_cents, property_id, "
             "target_address, request_ref, created_at) VALUES (:vendor, :purpose, :success, :cost_cents, "
             ":property_id, :target_address, :request_ref, :created_at)"),
        entries,
    )
    # A real Tracerfy charge already happened for this batch (it is billed on submit,
    # not on this write) — committing per batch, not at the end of the whole run,
    # means a later batch's failure can never lose an earlier batch's paid-for ledger
    # row or its persisted contacts (finding #8).
    session.commit()


def _read_trace_contacts(session, keys: list[str]) -> dict[str, LeadContacts]:
    if not keys:
        return {}
    rows = session.execute(
        text("SELECT trace_key, emails, phones FROM property_radar_trace_contacts "
             "WHERE trace_key = ANY(:keys)"),
        {"keys": keys},
    ).fetchall()
    return {r.trace_key: LeadContacts(emails=tuple(r.emails), phones=tuple(r.phones)) for r in rows}


def _write_trace_contacts(session, contacts_by_key: dict[str, LeadContacts]) -> None:
    import json

    session.execute(
        text("INSERT INTO property_radar_trace_contacts (trace_key, emails, phones, traced_at) "
             "VALUES (:trace_key, CAST(:emails AS jsonb), CAST(:phones AS jsonb), now()) "
             "ON CONFLICT (trace_key) DO UPDATE SET emails = EXCLUDED.emails, "
             "phones = EXCLUDED.phones, traced_at = EXCLUDED.traced_at"),
        [
            {"trace_key": key, "emails": json.dumps(list(c.emails)), "phones": json.dumps(list(c.phones))}
            for key, c in contacts_by_key.items()
        ],
    )
    session.commit()


def _live_trace(session, campaign: str, *, thin_path_only: bool) -> dict[str, LeadContacts]:
    settings = get_settings()
    if not settings.property_radar_enabled:
        raise RuntimeError("--live-trace needs PROPERTY_RADAR_ENABLED=true")
    all_leads = [lead for page in iter_staged_leads(session, campaign=campaign) for lead in page]
    facts = SqlHandoffStore(session).screening_facts(all_leads, {})
    if not facts.backflip_feed_fresh:
        logger.warning("[pr-live-trace] Backflip feed is stale; the handoff would skip every "
                       "lead on backflip_feed_stale regardless of contacts, so skipping the "
                       "trace entirely rather than paying Tracerfy for leads that can't be used")
        return {}
    leads = [
        lead for lead in all_leads
        if pretrace_eligible(lead, facts, thin_path_only=thin_path_only)[0]
    ]
    logger.info("PropertyRadar live trace: %d of %d staged leads are eligible to trace "
                "(status/campaign/thin-path/already-handed-off filtered)", len(leads), len(all_leads))
    keys = {trace_key(lead.property_address, lead.zip) for lead in leads}
    outcome = trace_staged_leads(
        leads,
        ledger=already_traced(session, "tracerfy", keys),
        submit=_tracerfy_submit,
        write_ledger=lambda entries: _write_usage_ledger(session, entries),
        cap=RunSpendCap(settings.skip_trace_max_run_cost_cents),
        read_contacts=lambda ks: _read_trace_contacts(session, ks),
        write_contacts=lambda cs: _write_trace_contacts(session, cs),
    )
    logger.info("PropertyRadar live trace: submitted=%d already_traced=%d unkeyable=%d capped=%d",
                outcome.submitted, outcome.skipped_already_traced, outcome.skipped_unkeyable, outcome.skipped_cap)
    return outcome.contacts


def run(
    *,
    campaign: str = DEFAULT_CAMPAIGN,
    trace_results: Path | None = None,
    thin_path_only: bool | None = None,
    apply: bool = False,
    live_trace: bool = False,
) -> HandoffReport:
    if live_trace and not apply:
        # Tracerfy bills on submit; a dry run can roll back its own DB writes but
        # cannot un-charge a real trace, so --live-trace without --apply would spend
        # real money and discard the result on every run.
        raise RuntimeError("--live-trace requires --apply: it is billed immediately and cannot be a dry run")
    settings = get_settings()
    thin_path = settings.property_radar_thin_path_only if thin_path_only is None else thin_path_only
    contacts = load_trace_contacts(trace_results) if trace_results else {}
    with get_db_context() as session:
        if live_trace:
            contacts = {**contacts, **_live_trace(session, campaign, thin_path_only=thin_path)}
        report = run_handoff(
            store=SqlHandoffStore(session),
            pages=iter_staged_leads(session, campaign=campaign),
            contacts_by_radar=contacts,
            thin_path_only=thin_path,
            contact_rules_enabled=settings.property_radar_contact_rules_enabled,
            apply=apply,
            commit=session.commit if apply else None,
        )
        if not apply:
            session.rollback()
    return report


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="Hand staged PropertyRadar records to FA Max.")
    parser.add_argument("--campaign", default=DEFAULT_CAMPAIGN)
    parser.add_argument("--trace-results", type=Path, help="Tracerfy results CSV to attach contacts from")
    parser.add_argument("--thin-path", action="store_true", default=None,
                        help="Limit handoff to the thin-path counties (overrides the setting)")
    parser.add_argument("--live-trace", action="store_true",
                        help="Trace staged addresses with Tracerfy now (spends credits; ledger-gated)")
    parser.add_argument("--apply", action="store_true", help="Write to FA Max (default: dry run)")
    args = parser.parse_args()
    report = run(
        campaign=args.campaign, trace_results=args.trace_results,
        thin_path_only=args.thin_path, apply=args.apply, live_trace=args.live_trace,
    )
    print(report.summary())


if __name__ == "__main__":
    main()
