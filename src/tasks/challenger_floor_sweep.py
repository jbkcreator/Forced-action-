"""
LEARN-v2.2 Layer 3 — challenger protected-floor cron driver (PROPOSE-ONLY).

Walks every eligible active venture, calls evaluate_floor() per venture, and
posts ONE Slack digest PROPOSING a challenger-capacity reserve for any venture
whose challenger cohort is under the 30% floor. It mutates nothing — see
config/challenger_floor.py for why auto-enforcement is deferred (no divisible
production pool + no retained-GP/declared winner pre-launch; spec line 259
"never set").

Cron:
    0 7 * * 1  src.tasks.challenger_floor_sweep   (weekly, Monday)

Usage:
    python -m src.tasks.challenger_floor_sweep [--dry-run]
"""
from __future__ import annotations

import logging
import sys

from sqlalchemy import text

from config.settings import get_settings
from src.core.database import get_db_context
from src.services.challenger_floor import ChallengerFloorReport, evaluate_floor

try:
    from slack_sdk import WebClient
except ImportError:  # pragma: no cover
    WebClient = None  # type: ignore[assignment,misc]

logger = logging.getLogger(__name__)

_ELIGIBLE_VENTURES = """
SELECT venture_key
FROM ventures
WHERE is_active = true
  AND ladder_stage = ANY(:stages)
ORDER BY venture_key
"""


def _eligible_venture_keys(db) -> list[str]:
    from config.challenger_floor import ELIGIBLE_STAGES

    rows = db.execute(
        text(_ELIGIBLE_VENTURES), {"stages": list(ELIGIBLE_STAGES)}
    ).fetchall()
    return [r.venture_key for r in rows]


def run_sweep(dry_run: bool = False) -> dict:
    totals = {
        "ventures_evaluated": 0,
        "under_floor": 0,
        "errors": 0,
    }
    under_floor_reports: list[ChallengerFloorReport] = []

    with get_db_context() as db:
        venture_keys = _eligible_venture_keys(db)

    for venture_key in venture_keys:
        try:
            with get_db_context() as db:
                report = evaluate_floor(db, venture_key)

            totals["ventures_evaluated"] += 1
            if report.under_floor:
                totals["under_floor"] += 1
                under_floor_reports.append(report)

            logger.info(
                "[challenger_floor_sweep] %s: challengers=%d share=%s%% floor=%d%% "
                "under=%s note=%s",
                venture_key,
                len(report.challenger_cells),
                report.challenger_share_pct,
                report.floor_pct,
                report.under_floor,
                report.note,
            )
        except Exception:
            logger.error(
                "[challenger_floor_sweep] error processing venture %s",
                venture_key,
                exc_info=True,
            )
            totals["errors"] += 1

    logger.info(
        "[challenger_floor_sweep] done: ventures=%d under_floor=%d errors=%d dry_run=%s",
        totals["ventures_evaluated"],
        totals["under_floor"],
        totals["errors"],
        dry_run,
    )

    if not dry_run:
        _post_slack_digest(totals, under_floor_reports)

    return totals


def _post_slack_digest(
    totals: dict, under_floor_reports: list[ChallengerFloorReport]
) -> None:
    settings = get_settings()
    token = getattr(settings, "slack_bot_token", None)
    channel = getattr(settings, "relay_slack_channel", None)

    if WebClient is None or not token or not channel:
        logger.info(
            "[challenger_floor_sweep] Slack not configured — digest not posted"
        )
        return

    lines = [
        "*Challenger Floor* — weekly proposal (nothing applied)",
        f"Ventures evaluated: {totals['ventures_evaluated']}",
        f"Under floor: {totals['under_floor']}",
    ]
    for r in under_floor_reports:
        cells = ", ".join(r.challenger_cells) or "(none)"
        lines.append(
            f"• {r.venture_key}: challengers at {r.challenger_share_pct}% vs "
            f"{r.floor_pct}% — propose reserving +{r.shortfall_pct}% production "
            f"capacity for challenger cells: {cells}. (Proposal only — not applied.)"
        )
    if totals["errors"]:
        lines.append(f":warning: Errors: {totals['errors']} — check logs")

    try:
        raw_token = token.get_secret_value() if hasattr(token, "get_secret_value") else token
        WebClient(token=raw_token).chat_postMessage(
            channel=channel,
            text="\n".join(lines),
            blocks=[
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "\n".join(lines)},
                }
            ],
        )
    except Exception:
        logger.error(
            "[challenger_floor_sweep] could not post Slack digest", exc_info=True
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    dry = "--dry-run" in sys.argv
    print(run_sweep(dry_run=dry))
