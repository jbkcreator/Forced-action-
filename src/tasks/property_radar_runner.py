"""PropertyRadar runner: pull -> stage -> link -> handoff, end to end.

Dry run by default: a free PropertyRadar count (Purchase=0, no exports), then
link + handoff decisions over what is already staged, all inside a transaction
that is rolled back. Prints counts per stage; buys nothing, writes nothing.
--apply buys the new records, stages them, links them and hands them off,
committing each stage.

Usage:
    PYTHONPATH=. python -m src.tasks.property_radar_runner                 # dry run
    PYTHONPATH=. python -m src.tasks.property_radar_runner --apply --mode daily --state FL

Adding a state (config only, no code change):
  1. config/property_radar_fips.py      add the state's FIPS code and every county FIPS
                                        (keep PropertyRadar quirks, e.g. Miami-Dade = 12025).
  2. config/property_radar_campaigns.py add or enable the state's campaign criteria block.
  3. config/property_radar.py           only if FA loads the state's counties: add each
                                        FIPS -> FA county slug so records link to properties.
  4. Dry run with --state <XX> and check the free count per county.
  5. --apply --mode backlog once, then daily runs (cron line below, when enabled).
  A new campaign also needs a CAMPAIGN_PRIORITY entry (config/lead_ownership.py) and
  handoff entries (config/property_radar_handoff.py).

Monitoring stays off: the cron line in scripts/cron/crontab.txt is commented out.
"""
from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Iterator, Optional

from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

DEFAULT_CAMPAIGN = "maturity_target_lender"
STAGE_BATCH_SIZE = 500


@dataclass
class Stages:
    """The four pipeline steps, injectable so the runner is testable without the API."""
    count: Callable[[Session, str, str, str], int]
    pull: Callable[[Session, str, str, str], Iterator[list[dict[str, Any]]]]
    mark_seen: Callable[[Session, str, str, list[str]], None]
    stage: Callable[[Session, list[dict[str, Any]]], tuple[int, int, int]]
    link: Callable[[Session], dict[str, int]]
    handoff: Callable[[Session, str, bool, Optional[Callable[[], None]]], dict[str, int]]


@dataclass
class RunSummary:
    apply: bool
    would_fetch: int = 0
    staged: dict[str, int] = field(default_factory=lambda: {"inserted": 0, "updated": 0, "skipped": 0})
    linked: dict[str, int] = field(default_factory=dict)
    handoff: dict[str, int] = field(default_factory=dict)

    def render(self) -> str:
        mode = "APPLY" if self.apply else "DRY RUN (nothing bought, nothing written)"
        return "\n".join([
            f"PropertyRadar run - {mode}",
            f"  pull     would fetch {self.would_fetch}",
            f"  stage    {self.staged}",
            f"  link     {self.linked}",
            f"  handoff  {self.handoff}",
        ])


def to_contract(n: Any) -> dict[str, Any]:
    """Dev 1's normalized record -> the §3 staging contract field names."""
    return {
        "radar_id": n.radar_id, "state_fips": n.state_fips, "county_fips": n.county_fips,
        "apn": n.apn, "state": n.state, "county_name": n.county_name,
        "property_address": n.address, "city": n.city, "zip": n.zip_code,
        "property_type": n.property_type, "owner_name": n.owner_name,
        "ownership_type": n.ownership_type, "principal_name": n.principal_name,
        "lender_name": n.lender_original, "loan_amount": n.loan_amount,
        "loan_recorded_date": n.loan_date.isoformat() if n.loan_date else None,
        "loan_term_years": str(n.loan_term_years) if n.loan_term_years is not None else None,
        "est_maturity_date": n.est_maturity_date.isoformat() if n.est_maturity_date else None,
        "loan_doc_number": n.loan_doc_number, "campaign": n.campaign, "raw": n.raw,
    }


def _batches(records: Iterable[dict[str, Any]], size: int) -> Iterator[list[dict[str, Any]]]:
    batch: list[dict[str, Any]] = []
    for r in records:
        batch.append(r)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def run(session: Session, stages: Stages, *, mode: str, state: str, campaign: str, apply: bool) -> RunSummary:
    summary = RunSummary(apply=apply)
    summary.would_fetch = stages.count(session, mode, state, campaign)

    if apply:
        for batch in stages.pull(session, mode, state, campaign):
            ins, upd, skip = stages.stage(session, batch)
            session.commit()
            # Seen only after the batch is committed: a failed stage must be re-fetched.
            stages.mark_seen(session, state, campaign, [r["radar_id"] for r in batch])
            session.commit()
            for key, val in zip(("inserted", "updated", "skipped"), (ins, upd, skip)):
                summary.staged[key] += val

    try:
        summary.linked = stages.link(session)
        summary.handoff = stages.handoff(session, campaign, apply, session.commit if apply else None)
        if apply:
            session.commit()
    finally:
        if not apply:
            session.rollback()
    return summary


def default_stages() -> Stages:
    """Real stages. Imported lazily: each belongs to another developer's module."""
    from config.property_radar_campaigns import build_campaign_criteria
    from src.services.property_radar import linking, staging
    from src.services.property_radar.lead_handoff import SqlHandoffStore, iter_staged_leads, run_handoff
    from src.services.property_radar_normalizer import normalize
    from src.services.property_radar_port import get_property_radar_port
    from src.tasks import property_radar_maturity_pull as pull_task

    port = get_property_radar_port()

    def criteria(session: Session, mode: str, state: str, campaign: str) -> list[dict]:
        since = pull_task._last_successful_run_date(session, state, campaign) if mode == "daily" else None
        return build_campaign_criteria(state, campaign, daily_since=since)

    def count(session: Session, mode: str, state: str, campaign: str) -> int:
        return port.count(criteria(session, mode, state, campaign))

    def pull(session: Session, mode: str, state: str, campaign: str) -> Iterator[list[dict[str, Any]]]:
        crit = criteria(session, mode, state, campaign)
        pull_task._check_budget(port, crit, state, campaign)
        seen = pull_task._load_seen_ids(session, state, campaign)
        records = (
            to_contract(n)
            for n in (normalize(r, state=state, campaign=campaign) for r in port.purchase(crit)
                      if r.radar_id not in seen)
            if n is not None
        )
        yield from _batches(records, STAGE_BATCH_SIZE)

    def handoff(session: Session, campaign: str, apply: bool, commit: Optional[Callable[[], None]]) -> dict[str, int]:
        from config.settings import get_settings
        settings = get_settings()
        report = run_handoff(
            store=SqlHandoffStore(session),
            pages=iter_staged_leads(session, campaign=campaign),
            contacts_by_radar={},
            thin_path_only=settings.property_radar_thin_path_only,
            contact_rules_enabled=settings.property_radar_contact_rules_enabled,
            apply=apply,
            commit=commit,
        )
        return {str(k.value if hasattr(k, "value") else k): v for k, v in report.counts().items()}

    return Stages(
        count=count, pull=pull, mark_seen=pull_task._mark_seen,
        stage=staging.upsert_records, link=linking.link_unlinked, handoff=handoff,
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(
        description="PropertyRadar pull -> stage -> link -> handoff.",
        epilog="Adding a state" + __doc__.split("Adding a state", 1)[1],
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--mode", choices=("daily", "backlog"), default="daily")
    parser.add_argument("--state", default="FL")
    parser.add_argument("--campaign", default=DEFAULT_CAMPAIGN)
    parser.add_argument("--apply", action="store_true", help="Buy, stage, link and hand off (default: dry run)")
    args = parser.parse_args()

    from config.settings import get_settings
    if args.apply and not get_settings().property_radar_enabled:
        logger.warning("PROPERTY_RADAR_ENABLED is false — refusing --apply.")
        return

    from src.core.database import get_db_context
    with get_db_context() as session:
        summary = run(session, default_stages(), mode=args.mode, state=args.state,
                      campaign=args.campaign, apply=args.apply)
    logger.info("\n%s", summary.render())


if __name__ == "__main__":
    main()
