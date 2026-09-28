"""Hand staged PropertyRadar records to FA Max.

Dry run unless --apply, matching this codebase's sweep-task convention: a dry
run reads everything and prints what would be handed off, suppressed or
skipped, and writes nothing.

Usage:
    PYTHONPATH=. python -m src.tasks.property_radar_lead_handoff
    PYTHONPATH=. python -m src.tasks.property_radar_lead_handoff --thin-path
    PYTHONPATH=. python -m src.tasks.property_radar_lead_handoff \\
        --trace-results path/to/trace_results.csv --apply

Contact rules (consent rows) follow PROPERTY_RADAR_CONTACT_RULES_ENABLED and
cannot be switched on from the command line.
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

from config.settings import get_settings
from src.core.database import get_db_context
from src.services.property_radar.lead_handoff import (
    HandoffReport,
    SqlHandoffStore,
    iter_staged_leads,
    run_handoff,
)
from src.services.property_radar.trace_contacts import load_trace_contacts

logger = logging.getLogger(__name__)

DEFAULT_CAMPAIGN = "maturity_target_lender"


def run(
    *,
    campaign: str = DEFAULT_CAMPAIGN,
    trace_results: Path | None = None,
    thin_path_only: bool | None = None,
    apply: bool = False,
) -> HandoffReport:
    settings = get_settings()
    thin_path = settings.property_radar_thin_path_only if thin_path_only is None else thin_path_only
    contacts = load_trace_contacts(trace_results) if trace_results else {}
    with get_db_context() as session:
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
    parser.add_argument("--apply", action="store_true", help="Write to FA Max (default: dry run)")
    args = parser.parse_args()
    report = run(
        campaign=args.campaign, trace_results=args.trace_results,
        thin_path_only=args.thin_path, apply=args.apply,
    )
    print(report.summary())


if __name__ == "__main__":
    main()
