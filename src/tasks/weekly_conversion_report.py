"""
Weekly CDS tier-by-tier conversion report (E29).

Runs Mondays 07:00 UTC — before the autonomy report (08:45) so both land in
the same Monday morning digest. Produces a tier breakdown for Hillsborough and
Pinellas over a rolling 90-day window, writes one combined learning_cards row
per run, and posts a compact summary to the Lifecycle incident Slack channel.

Usage:
    python -m src.tasks.weekly_conversion_report
    python -m src.tasks.weekly_conversion_report --days 60
    python -m src.tasks.weekly_conversion_report --county hillsborough
"""

import argparse
import json
import logging
from datetime import datetime, timezone

from sqlalchemy import text as sa_text

from src.core.database import get_db_context
from src.tasks.conversion_report import run_conversion_report
from src.utils.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

_DEFAULT_DAYS = 90
_COUNTIES = ["hillsborough", "pinellas"]


def _slack_text(county_id: str, rows: list[dict]) -> str:
    lines = [f"*CDS Tier Conversion — {county_id.title()} (90d)*"]
    for r in rows:
        bar = "#" * int(r["event_rate_pct"] / 0.5)
        lines.append(
            f"  {r['tier']:<18} {r['total']:>6,}  {r['event_rate_pct']:>5.2f}%  {bar}"
        )
    if len(rows) >= 2:
        top, bot = rows[0], rows[-1]
        if bot["event_rate_pct"] > 0:
            lift = round(top["event_rate_pct"] / bot["event_rate_pct"], 2)
            lines.append(f"  _Lift: {top['tier']} = {lift}× {bot['tier']}_")
    return "\n".join(lines)


def _post_slack(text: str) -> None:
    from config.settings import get_settings
    s = get_settings()
    token = s.slack_bot_token
    channel = s.lifecycle_incident_slack_channel
    if not token or not channel:
        return
    try:
        from slack_sdk import WebClient
        WebClient(token=token.get_secret_value()).chat_postMessage(
            channel=channel,
            text=text,
            blocks=[
                {"type": "header", "text": {"type": "plain_text", "text": "📊 Weekly CDS Tier Conversion Report", "emoji": True}},
                {"type": "section", "text": {"type": "mrkdwn", "text": text[:3000]}},
                {"type": "context", "elements": [{"type": "mrkdwn", "text": "Runs every Monday 07:00 UTC — 90-day rolling window"}]},
            ],
        )
    except Exception:
        logger.warning("[weekly_conversion] Slack post failed", exc_info=True)


def _write_learning_card(results: dict[str, list[dict]], run_at: datetime) -> None:
    """Write one combined row for all counties — avoids UNIQUE (card_date, card_type) conflict."""
    summary_parts = []
    for county_id, rows in results.items():
        if not rows:
            continue
        top, bot = rows[0], rows[-1]
        summary_parts.append(
            f"{county_id.title()}: {top.get('tier','?')} {top.get('event_rate_pct',0):.2f}%"
            f" vs {bot.get('tier','?')} {bot.get('event_rate_pct',0):.2f}%"
        )
    summary = " | ".join(summary_parts) or "no data"
    payload = {
        "window_days": _DEFAULT_DAYS,
        "counties": {cid: rows for cid, rows in results.items()},
        "generated_at": run_at.isoformat(),
    }
    with get_db_context() as db:
        db.execute(sa_text("""
            INSERT INTO learning_cards (card_type, card_date, summary_text, data_json, created_at)
            VALUES ('conversion_tier_report', :card_date, :summary, CAST(:payload AS jsonb), :ts)
            ON CONFLICT (card_date, card_type) DO UPDATE
              SET summary_text = EXCLUDED.summary_text,
                  data_json    = EXCLUDED.data_json,
                  created_at   = EXCLUDED.created_at
        """), {
            "payload": json.dumps(payload),
            "ts": run_at,
            "card_date": run_at.date(),
            "summary": summary,
        })
        db.commit()


def run(days: int = _DEFAULT_DAYS, county_id: str | None = None) -> None:
    counties = [county_id] if county_id else _COUNTIES
    run_at = datetime.now(timezone.utc)
    results: dict[str, list[dict]] = {}

    for cid in counties:
        try:
            rows = run_conversion_report(days=days, county_id=cid)
            if not rows:
                logger.info("[weekly_conversion] no rows for county=%s", cid)
                continue
            results[cid] = rows
            _post_slack(_slack_text(cid, rows))
            logger.info("[weekly_conversion] fetched county=%s tiers=%d", cid, len(rows))
        except Exception:
            logger.error("[weekly_conversion] failed for county=%s", cid, exc_info=True)

    if results:
        try:
            _write_learning_card(results, run_at)
            logger.info("[weekly_conversion] learning_card written counties=%s", list(results))
        except Exception:
            logger.error("[weekly_conversion] failed to write learning_card", exc_info=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Weekly CDS tier conversion report")
    parser.add_argument("--days", type=int, default=_DEFAULT_DAYS)
    parser.add_argument("--county", dest="county_id", default=None)
    args = parser.parse_args()
    run(days=args.days, county_id=args.county_id)
