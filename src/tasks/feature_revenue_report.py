"""
LEARN-v2.2 T-LEARN-06 — weekly feature-to-revenue report.

Runs run_feature_revenue_analysis and reports the top sufficient (n >= MIN_N)
feature/value -> reply-rate correlations, plus a count of insufficient-evidence
features. These are PROPOSALS for NBRA (src/services/nbra_engine.py) targeting
weights — nothing is applied to any ranking.

Usage:
    python -m src.tasks.feature_revenue_report [--dry-run]

Options:
    --dry-run   Read-only; identical output (this task never writes).
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone

try:
    from slack_sdk import WebClient as _SlackWebClient
except ImportError:
    _SlackWebClient = None  # type: ignore[assignment,misc]

from config.settings import get_settings
from src.core.database import get_db_context
from src.services.feature_revenue import FeatureRevenueReport, run_feature_revenue_analysis
from src.utils.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

_TOP_N = 10


def _digest_text(report: FeatureRevenueReport, dry_run: bool) -> str:
    prefix = "[DRY-RUN] " if dry_run else ""
    lines = [
        f"{prefix}:mag: *Feature-to-Revenue Report* (PROPOSAL only — not applied)",
        f"These are proposed NBRA targeting-weight signals, not applied weights.",
        f"• Snapshots scanned: {report.snapshots_scanned}",
    ]
    if report.snapshots_scanned == 0:
        lines.append(
            "• Insufficient evidence: no target_characteristics populated yet "
            "(awaiting buyer-profiling)."
        )
        return "\n".join(lines)

    if report.features:
        lines.append(f"• Top correlations by reply rate:")
        for corr in report.features[:_TOP_N]:
            lines.append(
                f"    {corr.feature_key}={corr.feature_value}: "
                f"{corr.reply_rate_pct}% (N={corr.n}, replies={corr.replies})"
            )
    else:
        lines.append("• No feature value reached the min-N floor (30).")

    lines.append(f"• Insufficient-evidence features (N<30): {len(report.insufficient)}")
    return "\n".join(lines)


def _post_slack_digest(report: FeatureRevenueReport, dry_run: bool) -> None:
    settings = get_settings()
    bot_token = getattr(settings, "slack_bot_token", None)
    channel = getattr(settings, "relay_slack_channel", None)

    if not bot_token or not channel or _SlackWebClient is None:
        logger.info("[FeatureRevenueReport] Slack not configured — skipping digest post.")
        return

    try:
        token = bot_token.get_secret_value() if hasattr(bot_token, "get_secret_value") else bot_token
        client = _SlackWebClient(token=token)
        client.chat_postMessage(channel=channel, text=_digest_text(report, dry_run))
    except Exception as exc:
        logger.warning("[FeatureRevenueReport] Slack digest failed: %s", exc)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="LEARN-v2.2 T-LEARN-06 feature-to-revenue report"
    )
    parser.add_argument("--dry-run", action="store_true", help="Read-only (task never writes)")
    args = parser.parse_args()

    now = datetime.now(tz=timezone.utc)
    logger.info("[FeatureRevenueReport] starting dry_run=%s now=%s", args.dry_run, now.isoformat())

    with get_db_context() as db:
        report = run_feature_revenue_analysis(db)

    logger.info(
        "[FeatureRevenueReport] done snapshots=%d sufficient=%d insufficient=%d",
        report.snapshots_scanned, len(report.features), len(report.insufficient),
    )
    logger.info("[FeatureRevenueReport] %s", _digest_text(report, args.dry_run))

    _post_slack_digest(report, dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
