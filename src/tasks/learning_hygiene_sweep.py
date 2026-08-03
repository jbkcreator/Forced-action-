"""
Scheduled lesson-hygiene sweep (LEARN-v2.2 Layer 4, Step 12).

Cron driver for src/services/learning_hygiene.py:sweep(). Flags outdated and
disproven `lifecycle_playbook` lessons automatically so nobody prunes the
fleet's memory by hand.

    python -m src.tasks.learning_hygiene_sweep              # dry run (default)
    python -m src.tasks.learning_hygiene_sweep --apply      # actually mutate
    python -m src.tasks.learning_hygiene_sweep --apply --limit 5

Cron: 15 10 * * *  (daily 10:15 UTC — after ab_rollback_check/holdout_check at
09:00 have had their say, so a lesson authored this morning is judged against
today's evidence rather than yesterday's.)

--apply is opt-in, not opt-out. This job mutates learned state and neither
`mark_contradicted` nor `supersede_recommendation` has an inverse, so the
default has to be the safe one. Run it dry for a week, read the digest, then
schedule it with --apply.

The digest leads with the unmeasurable counts on purpose. Entries with no
evidence feed (every non-'lifecycle' agent_domain) and anti_playbook entries
are never touched, which means they accumulate forever — precisely the problem
this job exists to fix. Buried in a log that gap becomes a surprise in six
months; on the first line of a daily digest it becomes a filed ticket.
"""
from __future__ import annotations

import argparse
import logging
import sys
from typing import Optional

from config.learning_hygiene import (
    HYGIENE_EXCLUDED_KINDS,
    HYGIENE_MEASURABLE_DOMAINS,
    RUN_BLAST_RADIUS_EXCEEDED,
    RUN_EVIDENCE_UNAVAILABLE,
    RUN_OK,
    RUN_SCHEMA_NOT_READY,
    VERDICT_CONTRADICT,
    VERDICT_KEEP,
    VERDICT_REPORT_STALE,
    VERDICT_SKIP_EXCLUDED_KIND,
    VERDICT_SKIP_NOT_ACTIONABLE,
    VERDICT_SKIP_ORPHANED_SOURCE,
    VERDICT_SKIP_UNMEASURABLE,
    VERDICT_SKIP_UNTESTED,
    VERDICT_SUPERSEDE,
)
from config.settings import get_settings
from src.core.database import get_db_context
from src.services.learning_hygiene import HygieneReport, sweep

logger = logging.getLogger(__name__)

# Run statuses that mean "the sweep refused to act", not "the sweep found
# nothing to do". These must reach a human — a silent refusal is how a broken
# feed goes unnoticed for a month.
_ALERTING_RUN_STATUSES = frozenset({
    RUN_SCHEMA_NOT_READY, RUN_EVIDENCE_UNAVAILABLE, RUN_BLAST_RADIUS_EXCEEDED,
})

_VERDICT_LABELS: dict[str, str] = {
    VERDICT_CONTRADICT: "contradicted (proven wrong)",
    VERDICT_SUPERSEDE: "superseded (replaced by newer)",
    VERDICT_REPORT_STALE: "stale — reported only, not retired",
    VERDICT_KEEP: "healthy, untouched",
    VERDICT_SKIP_UNTESTED: "untested (too few outcomes to judge)",
    VERDICT_SKIP_UNMEASURABLE: "unmeasurable (no evidence feed)",
    VERDICT_SKIP_ORPHANED_SOURCE: "orphaned source (source_id resolves to nothing)",
    VERDICT_SKIP_EXCLUDED_KIND: "excluded kind (anti_playbook)",
    VERDICT_SKIP_NOT_ACTIONABLE: "already terminal",
}


def format_digest(report: HygieneReport) -> str:
    """Human-readable summary. Leads with what was NOT covered."""
    lines: list[str] = []
    mode = "DRY RUN" if report.dry_run else "APPLIED"
    lines.append(f"*Lesson hygiene sweep* — {mode} — run_status=`{report.run_status}`")

    if report.run_status == RUN_SCHEMA_NOT_READY:
        lines.append(
            f"⛔ Schema not ready — missing columns "
            f"`{', '.join(report.schema.missing_columns) or 'none'}`, missing status "
            f"values `{', '.join(report.schema.missing_status_values) or 'none'}`."
        )
        lines.append(
            "   Run `migrations/apply_lifecycle_playbook_lessons_versioning.py`. "
            "Nothing was touched."
        )
        return "\n".join(lines)

    unmeasurable = report.counts.get(VERDICT_SKIP_UNMEASURABLE, 0)
    excluded = report.counts.get(VERDICT_SKIP_EXCLUDED_KIND, 0)
    orphaned = report.counts.get(VERDICT_SKIP_ORPHANED_SOURCE, 0)

    if unmeasurable or excluded or orphaned:
        lines.append("*Not covered by this sweep* (accumulating — needs a follow-up):")
        if unmeasurable:
            lines.append(
                f"  • {unmeasurable} unmeasurable — agent_domain outside "
                f"`{', '.join(HYGIENE_MEASURABLE_DOMAINS)}` has no evidence feed. "
                f"Building that feed is a precondition for sweeping them."
            )
        if excluded:
            lines.append(
                f"  • {excluded} excluded kind — `{', '.join(HYGIENE_EXCLUDED_KINDS)}` "
                f"needs its own terminal state decided, not an inverted threshold."
            )
        if orphaned:
            lines.append(
                f"  • {orphaned} orphaned source — `source_id` resolves to no source "
                f"row. Either test pollution or a deleted source; needs manual review."
            )

    lines.append(
        f"*Evidence feed*: `{report.feed.state}` "
        f"({report.feed.linked_rows} decision(s) attributed to a lesson)"
    )
    lines.append(
        f"*Measurable population*: {report.measurable_population} "
        f"(mutation cap this run: {report.cap})"
    )

    lines.append("*Verdicts*:")
    for verdict, count in sorted(report.counts.items(), key=lambda kv: -kv[1]):
        lines.append(f"  • {count} × {_VERDICT_LABELS.get(verdict, verdict)}")

    applied = [a for a in report.actions if a.get("applied")]
    if applied:
        lines.append(f"*Mutated* ({len(applied)}):")
        for action in applied:
            lines.append(
                f"  • [{action['verdict']}] lesson {action['lesson_id']} "
                f"`{action['lesson_name']}` — {action['reason']}"
            )
    elif report.actions:
        lines.append(f"*Would mutate* ({len(report.actions)}):")
        for action in report.actions:
            lines.append(
                f"  • [{action['verdict']}] lesson {action['lesson_id']} "
                f"`{action['lesson_name']}` — {action['reason']}"
            )

    for note in report.notes:
        lines.append(f"⚠️  {note}")

    return "\n".join(lines)


def _post_digest(digest: str) -> None:
    """Best-effort Slack post. A failed digest must never fail the sweep."""
    settings = get_settings()
    token = settings.slack_bot_token
    channel = settings.county_launch_slack_channel

    if not token or not channel:
        logger.info(
            "[LessonHygiene] SLACK_BOT_TOKEN or COUNTY_LAUNCH_SLACK_CHANNEL unset "
            "— digest logged only"
        )
        return

    try:
        from slack_sdk import WebClient

        raw_token = token.get_secret_value() if hasattr(token, "get_secret_value") else token
        WebClient(token=raw_token).chat_postMessage(
            channel=channel, text=digest, unfurl_links=False,
        )
        logger.info("[LessonHygiene] digest posted to %s", channel)
    except Exception as exc:
        logger.error("[LessonHygiene] Slack digest failed: %s", exc)


def run(
    *,
    dry_run: bool = True,
    limit: Optional[int] = None,
    post_digest: bool = True,
) -> dict:
    """Evaluate the lesson corpus and report. Returns the report as a dict."""
    with get_db_context() as db:
        report = sweep(db, dry_run=dry_run, limit=limit)

    digest = format_digest(report)
    logger.info("[LessonHygiene] digest:\n%s", digest)

    if post_digest and (report.run_status in _ALERTING_RUN_STATUSES or report.actions):
        _post_digest(digest)

    return report.as_dict()


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Flag outdated and disproven lifecycle_playbook lessons.",
    )
    parser.add_argument(
        "--apply", action="store_true",
        help="actually mutate. Omitted = dry run, which is the default on purpose.",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="cap the number of mutations attempted this run.",
    )
    parser.add_argument(
        "--no-digest", action="store_true", help="skip the Slack post.",
    )
    args = parser.parse_args(argv)

    result = run(
        dry_run=not args.apply,
        limit=args.limit,
        post_digest=not args.no_digest,
    )

    # Non-zero on a refusal so cron surfaces it rather than logging success.
    return 0 if result["run_status"] == RUN_OK else 1


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
    )
    sys.exit(main())
