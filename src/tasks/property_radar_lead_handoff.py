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
    session.commit()


def _live_trace(session, campaign: str) -> dict[str, LeadContacts]:
    settings = get_settings()
    if not settings.property_radar_enabled:
        raise RuntimeError("--live-trace needs PROPERTY_RADAR_ENABLED=true")
    leads = [lead for page in iter_staged_leads(session, campaign=campaign) for lead in page]
    keys = {trace_key(lead.property_address, lead.zip) for lead in leads}
    outcome = trace_staged_leads(
        leads,
        ledger=already_traced(session, "tracerfy", keys),
        submit=_tracerfy_submit,
        write_ledger=lambda entries: _write_usage_ledger(session, entries),
        cap=RunSpendCap(settings.skip_trace_max_run_cost_cents),
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
    settings = get_settings()
    thin_path = settings.property_radar_thin_path_only if thin_path_only is None else thin_path_only
    contacts = load_trace_contacts(trace_results) if trace_results else {}
    with get_db_context() as session:
        if live_trace:
            contacts = {**contacts, **_live_trace(session, campaign)}
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
