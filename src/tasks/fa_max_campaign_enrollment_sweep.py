"""FA Max WP-T3-4 — daily campaign enrollment sweep.

Assigns new contacts to a v1 campaign (capital_desk_loop / exit_desk /
rescue_circuit), re-checks every existing enrollment for pause/resume/
cancel, and posts one summary to the RELATIONSHIPS Slack channel (plan
Section 6.13) — never a per-person card; those belong to the drafting
agents' own approval cards.

Run daily, after partner mining (06:00 UTC) and the morning data loads —
see config.fa_max_campaigns.ENROLLMENT_SWEEP_CRON. `--dry-run` runs the
exact same decision logic with no writes (src.services.fa_max_campaigns.
selection.run_enrollment_sweep(dry_run=True)) — useful for the "rough real
leads in FA Max" spot check the client asked for (23/9) once bought data
lands.

    python -m src.tasks.fa_max_campaign_enrollment_sweep [--dry-run]
"""
from __future__ import annotations

import argparse
import logging

from config import fa_max_campaigns as cfg
from src.core.database import get_db_context
from src.services.fa_max_campaigns.selection import SweepSummary, run_enrollment_sweep

logger = logging.getLogger(__name__)


def _summary_text(summary: SweepSummary) -> str:
    lines = ["*FA Max campaign enrollment sweep*"]

    enrolled_parts = [f"{k}: {v}" for k, v in summary.enrolled.items()] or ["none"]
    lines.append(f"New enrollments — {', '.join(enrolled_parts)}")
    lines.append(f"Switched: {summary.switched}  ·  Paused: {summary.paused}  ·  Resumed: {summary.resumed}")

    if summary.cancelled:
        cancelled_parts = [f"{reason}: {count}" for reason, count in summary.cancelled.items()]
        lines.append(f"Cancelled — {', '.join(cancelled_parts)}")

    unresolved_parts = [f"{k}: {v}" for k, v in summary.unresolved.items() if v]
    if unresolved_parts:
        lines.append(f"Matched but unresolved (no linked person/contact yet) — {', '.join(unresolved_parts)}")

    if summary.rules_disabled:
        lines.append(f"Rules switched off — {', '.join(summary.rules_disabled)} (waiting on bought data)")

    return "\n".join(lines)


def _post_slack_summary(text_body: str) -> None:
    from config.settings import get_settings

    settings = get_settings()
    token = getattr(settings, "fa_max_slack_bot_token", None) or settings.slack_bot_token
    channel = getattr(settings, "fa_max_slack_channel_relationships", None)
    if not token or not channel:
        logger.info("fa_max_campaigns: RELATIONSHIPS Slack unconfigured — skipping summary post")
        return
    try:
        from slack_sdk import WebClient

        WebClient(token=token.get_secret_value()).chat_postMessage(channel=channel, text=text_body)
    except Exception:
        logger.warning("fa_max_campaigns: failed to post enrollment sweep summary to Slack", exc_info=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Compute the sweep with no writes")
    args = parser.parse_args()

    cfg.validate_campaign_config()

    with get_db_context() as session:
        summary = run_enrollment_sweep(session, dry_run=args.dry_run)

    text_body = _summary_text(summary)
    print(text_body)
    if not args.dry_run:
        _post_slack_summary(text_body)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
