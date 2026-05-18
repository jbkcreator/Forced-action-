"""
County launch runner — re-checks gates and executes (v1: manual) the launch
playbook for approved expansion candidates.

Cron: */15 * * * * (every 15 min — cheap; only acts when status='approved')

Usage:
    python -m src.tasks.county_launch_runner [--dry-run]
"""
import json
import logging
import sys
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from config.settings import settings
from src.core.database import get_db_context
from src.core.models import CountyLaunchAudit, ExpansionCandidate
from src.core.redis_client import redis_available, rget
from src.tasks.county_launch_evaluator import _build_gate_snapshot, _all_green

logger = logging.getLogger(__name__)


def _write_audit(db: Session, county_id: str, event_type: str, actor: str,
                 gate_snapshot=None, detail=None) -> None:
    row = CountyLaunchAudit(
        county_id=county_id,
        event_type=event_type,
        actor=actor,
        gate_snapshot=gate_snapshot,
        detail=detail,
    )
    db.add(row)
    db.flush()


def run_county_launch_runner(dry_run: bool = False) -> dict:
    """Process one approved expansion candidate."""
    actor = "launch_job-dryrun" if dry_run else "launch_job"
    source_county = settings.county_launch_source_county
    bot_token = settings.slack_bot_token

    with get_db_context() as db:
        candidate = db.execute(
            select(ExpansionCandidate)
            .where(ExpansionCandidate.status == "approved")
            .with_for_update()
        ).scalar_one_or_none()

        if not candidate:
            return {"no_approved_candidate": True}

        # Re-check gates before launching
        snapshot = _build_gate_snapshot(source_county)
        if not _all_green(snapshot):
            non_green = [f for f, v in snapshot.items() if v["color"] != "green"]
            _write_audit(db, candidate.county_id, "launch_aborted_gate_red", actor,
                         gate_snapshot=snapshot,
                         detail={"candidate_id": candidate.id, "non_green": non_green})

            if bot_token and candidate.last_slack_message_ts:
                _post_thread_reply(
                    bot_token.get_secret_value(),
                    candidate.last_slack_message_ts,
                    f":x: Approval invalidated — gate(s) went non-green: {', '.join(non_green)}. "
                    f"County `{candidate.county_id}` returned to queued status.",
                )
            candidate.status = "queued"
            logger.warning(
                "[CountyLaunchRunner] gates regressed for %s, approval invalidated: %s",
                candidate.county_id, non_green,
            )
            return {"aborted": True, "county_id": candidate.county_id, "non_green": non_green}

        if dry_run:
            logger.info("[CountyLaunchRunner] dry_run — would launch %s", candidate.county_id)
            return {"dry_run": True, "candidate_id": candidate.id, "county_id": candidate.county_id}

        # v1 playbook: flip status, post thread, tag on-call
        candidate.status = "launching"
        _write_audit(db, candidate.county_id, "launch_started", actor,
                     gate_snapshot=snapshot, detail={"candidate_id": candidate.id})

        if bot_token and candidate.last_slack_message_ts:
            _post_thread_reply(
                bot_token.get_secret_value(),
                candidate.last_slack_message_ts,
                f":rocket: All 7 expansion gates re-verified GREEN for `{source_county}`.\n"
                f"*Run the launch playbook for `{candidate.county_id}`.*\n"
                f"Approved by <@{candidate.approved_by_slack_user}>.",
            )

        candidate.status = "launched"
        candidate.launched_at = datetime.now(timezone.utc)
        _write_audit(db, candidate.county_id, "launched", actor,
                     gate_snapshot=snapshot, detail={"candidate_id": candidate.id})

        logger.info("[CountyLaunchRunner] launched %s", candidate.county_id)
        return {"launched": True, "county_id": candidate.county_id}


def _post_thread_reply(token: str, thread_ts: str, text: str) -> None:
    try:
        from slack_sdk import WebClient
        client = WebClient(token=token)
        client.chat_postMessage(
            channel=settings.county_launch_slack_channel,
            thread_ts=thread_ts,
            text=text,
        )
    except Exception as exc:
        logger.error("[CountyLaunchRunner] Slack thread reply failed: %s", exc)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    dry = "--dry-run" in sys.argv
    result = run_county_launch_runner(dry_run=dry)
    print(json.dumps(result, indent=2, default=str))
