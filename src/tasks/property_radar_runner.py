"""PropertyRadar runner: pull -> stage -> link -> handoff, end to end.

Dry run by default: a free PropertyRadar count (Purchase=0, no exports) of
what the pull would buy, then link + handoff decisions over records ALREADY
staged, inside a transaction that is rolled back. New records are not staged
in a dry run — staging them would mean buying them. Buys nothing, writes nothing.
--apply buys the new records, stages them, links them and hands them off,
committing each stage. --apply refuses a state that is not in ENABLED_STATES.

Usage:
    PYTHONPATH=. python -m src.tasks.property_radar_runner                 # dry run
    PYTHONPATH=. python -m src.tasks.property_radar_runner --apply --mode daily --state FL \
        --trace-results path/to/trace_results.csv

Adding a state (config only, no code change):
  1. config/property_radar_fips.py      add the state's FIPS code and every county FIPS
                                        (keep PropertyRadar quirks, e.g. Miami-Dade = 12025).
  2. config/property_radar_campaigns.py add the state's criteria to the campaign's builder
                                        (_CAMPAIGN_BUILDERS entry for (state, campaign)),
                                        then add the state to ENABLED_STATES to switch it on.
  3. config/property_radar.py           only if FA loads the state's counties: add each
                                        FIPS -> FA county slug so records link to properties.
  4. Dry run with --state <XX> (allowed before it is enabled) and check the free count.
  5. --apply --mode backlog once, then daily runs (cron line below, when enabled).
  A new campaign also needs a CAMPAIGN_PRIORITY entry (config/lead_ownership.py) and
  handoff entries (config/property_radar_handoff.py).

Monitoring stays off: the cron line in scripts/cron/crontab.txt is commented out.
"""
from __future__ import annotations

import argparse
import logging
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

DEFAULT_CAMPAIGN = "maturity_target_lender"
STAGE_BATCH_SIZE = 500


@dataclass
class Stages:
    """Pipeline steps (count, pull, mark seen, stage, link, handoff), injectable for tests."""
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


# The normalizer's pre-contract field names (renamed to the contract in PR #315).
_LEGACY_FIELD_NAMES = {
    "address": "property_address", "zip_code": "zip",
    "lender_original": "lender_name", "loan_date": "loan_recorded_date",
}
_CONTRACT_FIELDS = (
    "radar_id", "state_fips", "county_fips", "apn", "state", "county_name",
    "property_address", "city", "zip", "property_type", "owner_name", "ownership_type",
    "mailing_address", "mailing_city", "mailing_state", "mailing_zip", "principal_name",
    "lender_name", "loan_amount", "loan_recorded_date", "loan_term_years", "est_maturity_date",
    "loan_doc_number", "campaign", "raw",
)


def to_contract(record: Any) -> dict[str, Any]:
    """Dev 1's normalized record -> the §3 staging contract (dates/term as strings)."""
    fields = {_LEGACY_FIELD_NAMES.get(k, k): v for k, v in vars(record).items()}
    out = {f: fields.get(f) for f in _CONTRACT_FIELDS}
    for f in ("loan_recorded_date", "est_maturity_date"):
        out[f] = out[f].isoformat() if out[f] else None
    out["loan_term_years"] = str(out["loan_term_years"]) if out["loan_term_years"] is not None else None
    return out


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


_RUN_START_SQL = """
    INSERT INTO property_radar_pull_runs (run_type, state, campaign, started_at, status)
    VALUES (:mode, :state, :campaign, now(), 'running') RETURNING id
"""
_RUN_DONE_SQL = """
    UPDATE property_radar_pull_runs
    SET finished_at = now(), records_fetched = :fetched, exports_consumed = :exports, status = 'done'
    WHERE id = :id
"""
_RUN_FAILED_SQL = "UPDATE property_radar_pull_runs SET finished_at = now(), status = 'failed' WHERE id = :id"


def default_stages(trace_results: Optional[Path] = None) -> Stages:
    """Real stages. Imported lazily: each belongs to another developer's module."""
    from config.property_radar_campaigns import build_campaign_criteria
    from src.services.property_radar import linking, staging
    from src.services.property_radar.lead_handoff import SqlHandoffStore, iter_staged_leads, run_handoff
    from src.services.property_radar.trace_contacts import load_trace_contacts
    from src.services.property_radar_normalizer import normalize
    from src.services.property_radar_port import get_property_radar_port
    from src.tasks import property_radar_maturity_pull as pull_task

    port = get_property_radar_port()

    def criteria(session: Session, mode: str, state: str, campaign: str) -> list[dict]:
        since = pull_task._last_successful_run_date(session, state, campaign) if mode == "daily" else None
        return build_campaign_criteria(state, campaign, daily_since=since)

    def count(session: Session, mode: str, state: str, campaign: str) -> int:
        try:
            return port.count(criteria(session, mode, state, campaign))
        except Exception:
            logger.exception("PropertyRadar free count failed for %s/%s (%s)", state, campaign, mode)
            raise

    def pull(session: Session, mode: str, state: str, campaign: str) -> Iterator[list[dict[str, Any]]]:
        crit = criteria(session, mode, state, campaign)
        try:
            pull_task._check_budget(port, crit, state, campaign)
        except Exception:
            logger.exception("PropertyRadar budget check refused or failed for %s/%s", state, campaign)
            raise
        seen = pull_task._load_seen_ids(session, state, campaign)
        # The pull-run row is the daily watermark: a 'done' run narrows the next
        # daily criteria to newly matured loans instead of re-buying the backlog.
        run_id = session.execute(text(_RUN_START_SQL), {"mode": mode, "state": state, "campaign": campaign}).scalar()
        session.commit()
        exports = 0

        def purchased():
            nonlocal exports
            for r in port.purchase(crit):
                exports += 1
                if r.radar_id not in seen:
                    yield r

        records = (to_contract(n) for n in (normalize(r, state=state, campaign=campaign) for r in purchased())
                   if n is not None)
        staged_batches = fetched = 0
        try:
            for batch in _batches(records, STAGE_BATCH_SIZE):
                yield batch
                staged_batches += 1
                fetched += len(batch)
        except Exception:
            session.rollback()
            session.execute(text(_RUN_FAILED_SQL), {"id": run_id})
            session.commit()
            logger.exception(
                "PropertyRadar pull failed for %s/%s after %d committed batches; "
                "unstaged records stay unseen and are re-fetched next run", state, campaign, staged_batches,
            )
            raise
        session.execute(text(_RUN_DONE_SQL), {"id": run_id, "fetched": fetched, "exports": exports})
        session.commit()

    def handoff(session: Session, campaign: str, apply: bool, commit: Optional[Callable[[], None]]) -> dict[str, int]:
        from config.settings import get_settings
        settings = get_settings()
        contacts = load_trace_contacts(trace_results) if trace_results else {}
        if not contacts:
            logger.warning("No trace contacts supplied: the handoff skips every record as no_contact_data.")
        report = run_handoff(
            store=SqlHandoffStore(session),
            pages=iter_staged_leads(session, campaign=campaign),
            contacts_by_radar=contacts,
            thin_path_only=settings.property_radar_thin_path_only,
            contact_rules_enabled=settings.property_radar_contact_rules_enabled,
            apply=apply,
            commit=commit,
        )
        return dict(Counter(d.outcome.value for d in report.decisions))

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
    parser.add_argument("--trace-results", type=Path,
                        help="Tracerfy results CSV; the handoff needs contacts or it skips every record")
    parser.add_argument("--apply", action="store_true", help="Buy, stage, link and hand off (default: dry run)")
    args = parser.parse_args()

    from config.property_radar_campaigns import ENABLED_STATES
    from config.settings import get_settings
    if args.apply and not get_settings().property_radar_enabled:
        logger.warning("PROPERTY_RADAR_ENABLED is false - refusing --apply.")
        return
    if args.apply and args.state not in ENABLED_STATES:
        logger.warning("State %s is not in ENABLED_STATES - refusing --apply (dry run is allowed).", args.state)
        return

    from src.core.database import get_db_context
    with get_db_context() as session:
        summary = run(session, default_stages(args.trace_results), mode=args.mode, state=args.state,
                      campaign=args.campaign, apply=args.apply)
    logger.info("\n%s", summary.render())


if __name__ == "__main__":
    main()
