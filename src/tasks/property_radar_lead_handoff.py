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

--live-trace spends Tracerfy credits (1 per hit), needs PROPERTY_RADAR_ENABLED=true and
requires --apply: a dry run discards everything, so a rehearsal would pay for contacts and
lose them. Paid contacts are stored (property_radar_traced_contacts, migrations/
apply_property_radar_traced_contacts.py) with the ledger rows, and a later run reuses them.

Contact rules (consent rows) follow PROPERTY_RADAR_CONTACT_RULES_ENABLED and
cannot be switched on from the command line.
"""
from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

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
             "target_address, request_ref, created_at, trace_type) VALUES (:vendor, :purpose, :success, "
             ":cost_cents, :property_id, :target_address, :request_ref, :created_at, :trace_type)"),
        [{"trace_type": None, **entry} for entry in entries],
    )
    session.commit()


def _write_traced_contacts(session, contacts: dict[str, LeadContacts]) -> None:
    """Not committed here: the ledger write that follows commits both together."""
    if not contacts:
        return
    session.execute(
        text("INSERT INTO property_radar_traced_contacts (radar_id, phones, emails) "
             "VALUES (:radar_id, :phones, :emails) ON CONFLICT (radar_id) DO UPDATE "
             "SET phones = EXCLUDED.phones, emails = EXCLUDED.emails, traced_at = now()"),
        [{"radar_id": rid, "phones": list(c.phones), "emails": list(c.emails)} for rid, c in contacts.items()],
    )


def _stored_contacts(session, radar_ids: list[str]) -> dict[str, LeadContacts]:
    rows = session.execute(
        text("SELECT radar_id, phones, emails FROM property_radar_traced_contacts WHERE radar_id = ANY(:ids)"),
        {"ids": radar_ids},
    ).all()
    return {r.radar_id: LeadContacts(emails=tuple(r.emails), phones=tuple(r.phones)) for r in rows}


def _live_trace(session, campaign: str, *, thin_path_only: bool) -> dict[str, LeadContacts]:
    settings = get_settings()
    if not settings.property_radar_enabled:
        raise RuntimeError("--live-trace needs PROPERTY_RADAR_ENABLED=true")
    if session.execute(text("SELECT to_regclass('property_radar_traced_contacts')")).scalar() is None:
        raise RuntimeError("property_radar_traced_contacts is missing: run "
                           "migrations/apply_property_radar_traced_contacts.py before --live-trace")
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
        write_contacts=lambda contacts: _write_traced_contacts(session, contacts),
        cap=RunSpendCap(settings.skip_trace_max_run_cost_cents),
    )
    logger.info("PropertyRadar live trace: submitted=%d already_traced=%d unkeyable=%d capped=%d no_result=%d",
                outcome.submitted, outcome.skipped_already_traced, outcome.skipped_unkeyable,
                outcome.skipped_cap, outcome.skipped_no_result)
    return {**_stored_contacts(session, [lead.radar_id for lead in leads]), **outcome.contacts}


@dataclass(frozen=True)
class DialerLead:
    """A staged dialer-only record to trace. ``property_address``/``city``/``state``/``zip`` hold the address the
    trace is sent to (the owner's mailing address, else the property), the names trace_staged_leads reads;
    ``property_state`` is where the property is, which decides the Georgia rule."""

    radar_id: str
    status: str
    property_state: Optional[str]
    ownership_type: Optional[str]
    owner_name: Optional[str]
    principal_name: Optional[str]
    property_address: Optional[str]
    city: Optional[str]
    state: Optional[str]
    zip: Optional[str]


# These owners mostly do not live at the property (not owner-occupied): the calling-list trace found ~62% at the
# mailing address against ~7% at the property. A record whose contacts are already stored is not traced again.
_DIALER_LEADS_SQL = text("""
    SELECT r.radar_id, r.status, r.state AS property_state, r.ownership_type, r.owner_name, r.principal_name,
           coalesce(nullif(r.mailing_address, ''), r.property_address) AS property_address,
           CASE WHEN nullif(r.mailing_address, '') IS NULL THEN r.city ELSE r.mailing_city END AS city,
           CASE WHEN nullif(r.mailing_address, '') IS NULL THEN r.state ELSE r.mailing_state END AS state,
           CASE WHEN nullif(r.mailing_address, '') IS NULL THEN r.zip ELSE r.mailing_zip END AS zip
      FROM property_radar_records r
     WHERE r.campaign = :campaign
       AND NOT EXISTS (SELECT 1 FROM property_radar_traced_contacts t WHERE t.radar_id = r.radar_id)
     ORDER BY r.id
""")


def dialer_trace_eligible(lead: DialerLead) -> bool:
    """A staged dialer-only lead worth paying Tracerfy for: still active, and not a Georgia owner who may not be
    cold-called (natural persons; the dialer load would block them after the trace was paid for). Classified
    exactly as the calling-pool bridge stages it (PropertyRadar's ownership type and the owner name)."""
    from config.lending_compliance import GEORGIA_ALLOWED_ENTITY_TYPES
    from src.lending.pr_maturity_bridge import map_entity_status

    if lead.status != "active":
        return False
    if (lead.property_state or "").strip().upper() == "GA":
        return map_entity_status(lead.ownership_type, lead.owner_name) in GEORGIA_ALLOWED_ENTITY_TYPES
    return True


def _dialer_trace(session, campaign: str) -> dict[str, LeadContacts]:
    """Trace a dialer-only campaign (config/property_radar_handoff.DIALER_TRACE_CAMPAIGNS): the same live trace,
    ledger and spend cap as ``_live_trace``, but no FA Max handoff follows, so the FA Max screening (handed-off
    leads, Backflip feed) does not apply; the dialer load runs its own gates. Contacts land in
    property_radar_traced_contacts, where the calling-pool bridge picks them up. Returns the contacts traced by
    this run."""
    settings = get_settings()
    if not settings.property_radar_enabled:
        raise RuntimeError("the dialer trace needs PROPERTY_RADAR_ENABLED=true")
    if session.execute(text("SELECT to_regclass('property_radar_traced_contacts')")).scalar() is None:
        raise RuntimeError("property_radar_traced_contacts is missing: run "
                           "migrations/apply_property_radar_traced_contacts.py first")
    all_leads = [DialerLead(**{**row, "radar_id": str(row["radar_id"])})
                 for row in session.execute(_DIALER_LEADS_SQL, {"campaign": campaign}).mappings().all()]
    leads = [lead for lead in all_leads if dialer_trace_eligible(lead)]
    logger.info("PropertyRadar dialer trace %s: %d of %d untraced staged leads are eligible", campaign, len(leads),
                len(all_leads))
    keys = {trace_key(lead.property_address, lead.zip) for lead in leads}
    outcome = trace_staged_leads(
        leads,
        ledger=already_traced(session, "tracerfy", keys),
        submit=_tracerfy_submit,
        write_ledger=lambda entries: _write_usage_ledger(session, entries),
        write_contacts=lambda contacts: _write_traced_contacts(session, contacts),
        cap=RunSpendCap(settings.skip_trace_max_run_cost_cents),
    )
    logger.info("PropertyRadar dialer trace %s: submitted=%d already_traced=%d unkeyable=%d capped=%d no_result=%d",
                campaign, outcome.submitted, outcome.skipped_already_traced, outcome.skipped_unkeyable,
                outcome.skipped_cap, outcome.skipped_no_result)
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
        raise ValueError("--live-trace spends Tracerfy credits and needs --apply; a dry run would discard the contacts")
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
    if args.live_trace and not args.apply:
        parser.error("--live-trace spends Tracerfy credits and requires --apply")
    report = run(
        campaign=args.campaign, trace_results=args.trace_results,
        thin_path_only=args.thin_path, apply=args.apply, live_trace=args.live_trace,
    )
    print(report.summary())


if __name__ == "__main__":
    main()
