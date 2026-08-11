"""
LEARN-v2.2 Layer 3 — cell kill / throttle / revive cron driver.

Walks every eligible active venture, calls sweep() per venture, commits after
each, and posts a Slack digest with totals.

Cron:
    0 6 * * *  src.tasks.cell_allocation_sweep

Usage:
    python -m src.tasks.cell_allocation_sweep [--dry-run]
"""
from __future__ import annotations

import logging
import sys

from sqlalchemy import text

from config.settings import get_settings
from src.core.database import get_db_context
from src.services.cell_allocation import AllocationReport, sweep

try:
    from slack_sdk import WebClient
except ImportError:  # pragma: no cover
    WebClient = None  # type: ignore[assignment,misc]

logger = logging.getLogger(__name__)

ACTOR = "cell_allocation_sweep"

_ELIGIBLE_VENTURES = """
SELECT venture_key
FROM ventures
WHERE is_active = true
  AND ladder_stage = ANY(:stages)
ORDER BY venture_key
"""


def _eligible_venture_keys(db) -> list[str]:
    from config.cell_allocation import ELIGIBLE_STAGES

    rows = db.execute(
        text(_ELIGIBLE_VENTURES), {"stages": list(ELIGIBLE_STAGES)}
    ).fetchall()
    return [r.venture_key for r in rows]


def run_sweep(dry_run: bool = False) -> dict:
    totals = {
        "ventures_evaluated": 0,
        "throttled": 0,
        "revived": 0,
        "blast_radius_hit": 0,
        "errors": 0,
    }

    # Fetch the venture list in its own session (read-only) so each per-venture
    # commit below starts fresh.
    with get_db_context() as db:
        venture_keys = _eligible_venture_keys(db)

    for venture_key in venture_keys:
        try:
            with get_db_context() as db:
                report: AllocationReport = sweep(
                    db, venture_key, actor=ACTOR, dry_run=dry_run
                )
                if not dry_run:
                    db.commit()

            totals["ventures_evaluated"] += 1
            totals["throttled"] += len(report.throttled)
            totals["revived"] += len(report.revived)
            if report.blast_radius_hit:
                totals["blast_radius_hit"] += 1

            logger.info(
                "[cell_allocation_sweep] %s: cells=%d throttled=%d revived=%d blast=%s",
                venture_key,
                report.cells_evaluated,
                len(report.throttled),
                len(report.revived),
                report.blast_radius_hit,
            )
        except Exception:
            logger.error(
                "[cell_allocation_sweep] error processing venture %s",
                venture_key,
                exc_info=True,
            )
            totals["errors"] += 1

    logger.info(
        "[cell_allocation_sweep] done: ventures=%d throttled=%d revived=%d "
        "blast_radius_hit=%d errors=%d dry_run=%s",
        totals["ventures_evaluated"],
        totals["throttled"],
        totals["revived"],
        totals["blast_radius_hit"],
        totals["errors"],
        dry_run,
    )

    if not dry_run:
        _post_slack_digest(totals)

    return totals


def _post_slack_digest(totals: dict) -> None:
    settings = get_settings()
    token = getattr(settings, "slack_bot_token", None)
    channel = getattr(settings, "relay_slack_channel", None)

    if WebClient is None or not token or not channel:
        logger.info(
            "[cell_allocation_sweep] Slack not configured — digest not posted"
        )
        return

    lines = [
        f"*Cell Allocation Sweep* — daily run",
        f"Ventures evaluated: {totals['ventures_evaluated']}",
        f"Cells throttled: {totals['throttled']}",
        f"Cells revived: {totals['revived']}",
        f"Blast-radius cap hit: {totals['blast_radius_hit']} venture(s)",
    ]
    if totals["errors"]:
        lines.append(f":warning: Errors: {totals['errors']} — check logs")

    try:
        raw_token = token.get_secret_value() if hasattr(token, "get_secret_value") else token
        WebClient(token=raw_token).chat_postMessage(
            channel=channel,
            text="\n".join(lines),
            blocks=[
                {"type": "header", "text": {"type": "plain_text", "text": "⚙️ Cell Allocation Sweep", "emoji": True}},
                {"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(lines)}},
                {"type": "context", "elements": [{"type": "mrkdwn", "text": "Runs daily 06:00 UTC — blast-radius cap: 3 throttles/run"}]},
            ],
        )
    except Exception:
        logger.error(
            "[cell_allocation_sweep] could not post Slack digest", exc_info=True
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    dry = "--dry-run" in sys.argv
    print(run_sweep(dry_run=dry))
