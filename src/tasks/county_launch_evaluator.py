"""
County launch evaluator — checks all 7 expansion gates for the source county,
posts a Slack one-tap approval message when all are green.

Cron: 0 */6 * * * (every 6h; underlying metrics refresh once daily at 06:00 UTC)

Usage:
    python -m src.tasks.county_launch_evaluator [--dry-run]
"""
import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import asc, select

from config.cora_guardrails import EXPANSION_GATES, KILL_SWITCH
from config.settings import settings
from src.core.database import get_db_context
from src.core.models import CountyLaunchAudit, ExpansionCandidate
from src.core.redis_client import redis_available, rget

try:
    from slack_sdk import WebClient
except ImportError:  # pragma: no cover
    WebClient = None  # type: ignore[assignment,misc]

logger = logging.getLogger(__name__)

# Map EXPANSION_GATES keys → Redis metric keys (where they differ)
_GATE_TO_REDIS = {
    "payer_retention_30d": "retention_30d",
}


def _redis_key(feature: str, county_id: str) -> str:
    redis_feature = _GATE_TO_REDIS.get(feature, feature)
    return f"fa:ks_metric:{county_id}:{redis_feature}"


def _gate_color(feature: str, value: Optional[float]) -> str:
    """Return 'green'|'yellow'|'red' for a gate value using KILL_SWITCH thresholds."""
    if value is None:
        return "red"

    # county_profitability is binary (1.0 = green, else red)
    if feature == "county_profitability":
        return "green" if value >= 1.0 else "red"

    ks = KILL_SWITCH.get(feature) or KILL_SWITCH.get(_GATE_TO_REDIS.get(feature, ""))
    if not ks:
        return "red"

    green_threshold = ks.get("green")
    red_threshold = ks.get("red")
    if green_threshold is None or red_threshold is None:
        return "red"

    # free_tier_cost_ratio: lower is better (<=green is green, >=red is red)
    if feature == "free_tier_cost_ratio":
        if value <= green_threshold:
            return "green"
        if value >= red_threshold:
            return "red"
        return "yellow"

    # All other gates: higher is better (>=green is green, <=red is red)
    if value >= green_threshold:
        return "green"
    if value <= red_threshold:
        return "red"
    return "yellow"


def _build_gate_snapshot(county_id: str) -> dict:
    """Read 7 expansion gate values from Redis; return snapshot dict."""
    snapshot = {}
    for feature, gate_cfg in EXPANSION_GATES.items():
        key = _redis_key(feature, county_id)
        raw = rget(key) if redis_available() else None
        value = None
        try:
            if raw is not None:
                value = float(raw)
        except (TypeError, ValueError):
            pass
        color = _gate_color(feature, value)
        snapshot[feature] = {
            "value": value,
            "threshold": gate_cfg.get("threshold_pct") or gate_cfg.get("threshold"),
            "color": color,
        }
    return snapshot


def _all_green(snapshot: dict) -> bool:
    return all(g["color"] == "green" for g in snapshot.values())


def _format_slack_blocks(candidate: "ExpansionCandidate", snapshot: dict, source_county: str) -> list:
    lines = []
    for feature, info in snapshot.items():
        val = info["value"]
        threshold = info["threshold"]
        color = info["color"]
        emoji = ":white_check_mark:" if color == "green" else (":warning:" if color == "yellow" else ":x:")
        if feature == "county_profitability":
            val_str = "net positive" if val and val >= 1.0 else "not profitable"
            threshold_str = "net positive"
        elif val is not None:
            val_str = f"{val:.1f}%"
            threshold_str = f"{threshold}%" if threshold else "?"
        else:
            val_str = "N/A"
            threshold_str = str(threshold) if threshold else "?"
        lines.append(f"• {feature:<28} {val_str} (threshold: {threshold_str}) {emoji}")

    gate_text = "\n".join(lines)
    return [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f":white_check_mark: *All 7 expansion gates GREEN* for `{source_county}`.\n"
                    f"Candidate: *{candidate.county_id}* (priority {candidate.priority})"
                ),
            },
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"```\n{gate_text}\n```"},
        },
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "Approve launch"},
                    "style": "primary",
                    "action_id": "county_launch_decision",
                    "value": json.dumps({"candidate_id": candidate.id, "action": "approve"}),
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "Skip this candidate"},
                    "style": "danger",
                    "action_id": "county_launch_decision",
                    "value": json.dumps({"candidate_id": candidate.id, "action": "skip"}),
                },
            ],
        },
    ]


def _write_audit(db, county_id: str, event_type: str, actor: str,
                 gate_snapshot: Optional[dict] = None, detail: Optional[dict] = None) -> None:
    row = CountyLaunchAudit(
        county_id=county_id,
        event_type=event_type,
        actor=actor,
        gate_snapshot=gate_snapshot,
        detail=detail,
    )
    db.add(row)
    db.flush()


def run_county_launch_evaluator(dry_run: bool = False) -> dict:
    """Evaluate expansion gates and post Slack approval message if all green."""
    source_county = settings.county_launch_source_county
    bot_token = settings.slack_bot_token
    channel = settings.county_launch_slack_channel

    if not bot_token or not channel:
        logger.warning(
            "[CountyLaunchEvaluator] SLACK_BOT_TOKEN or COUNTY_LAUNCH_SLACK_CHANNEL not set — skipping"
        )
        return {"skipped": True, "reason": "slack_not_configured"}

    snapshot = _build_gate_snapshot(source_county)
    actor = "evaluator-dryrun" if dry_run else "evaluator"

    with get_db_context() as db:
        if not _all_green(snapshot):
            _write_audit(db, source_county, "evaluated", actor, gate_snapshot=snapshot)
            non_green = [f for f, v in snapshot.items() if v["color"] != "green"]
            logger.info("[CountyLaunchEvaluator] gates not all green: %s", non_green)
            return {"all_green": False, "non_green": non_green, "snapshot": snapshot}

        # All green — pick top queued candidate
        candidate = db.execute(
            select(ExpansionCandidate)
            .where(ExpansionCandidate.status == "queued")
            .order_by(asc(ExpansionCandidate.priority), asc(ExpansionCandidate.created_at))
        ).scalar_one_or_none()

        if not candidate:
            _write_audit(db, source_county, "evaluated", actor, gate_snapshot=snapshot,
                         detail={"note": "all_green_no_candidate"})
            logger.info("[CountyLaunchEvaluator] all gates green but no queued candidate")
            return {"all_green": True, "candidate": None}

        # Cooldown check
        now = datetime.now(timezone.utc)
        cooldown_hours = settings.county_launch_cooldown_hours
        reminder_days = settings.county_launch_reminder_days

        should_post = False
        is_reminder = False

        if candidate.last_slack_posted_at is None:
            should_post = True
        else:
            last_posted = candidate.last_slack_posted_at
            if last_posted.tzinfo is None:
                last_posted = last_posted.replace(tzinfo=timezone.utc)
            elapsed = now - last_posted
            if elapsed >= timedelta(days=reminder_days):
                should_post = True
                is_reminder = True
            elif elapsed < timedelta(hours=cooldown_hours):
                _write_audit(db, candidate.county_id, "cooldown_skipped", actor,
                             gate_snapshot=snapshot,
                             detail={"candidate_id": candidate.id})
                logger.info("[CountyLaunchEvaluator] cooldown active for %s", candidate.county_id)
                return {"all_green": True, "cooldown_skipped": True}
            else:
                should_post = True

        if not should_post:
            return {"all_green": True, "no_post": True}

        blocks = _format_slack_blocks(candidate, snapshot, source_county)
        slack_payload = {
            "channel": channel,
            "blocks": blocks,
            "text": f"County launch approval needed: {candidate.county_id}",
        }

        if dry_run:
            print(json.dumps(slack_payload, indent=2))
            _write_audit(db, candidate.county_id, "posted", actor,
                         gate_snapshot=snapshot,
                         detail={"candidate_id": candidate.id, "dry_run": True,
                                 "is_reminder": is_reminder})
            return {"all_green": True, "dry_run": True, "payload": slack_payload}

        # Post to Slack
        try:
            client = WebClient(token=bot_token.get_secret_value())
            response = client.chat_postMessage(**slack_payload)
            slack_ts = response["ts"]
        except Exception as exc:
            logger.error("[CountyLaunchEvaluator] Slack post failed: %s", exc)
            return {"all_green": True, "slack_error": str(exc)}

        candidate.last_slack_posted_at = now
        candidate.last_slack_message_ts = slack_ts

        _write_audit(db, candidate.county_id, "posted", actor,
                     gate_snapshot=snapshot,
                     detail={"candidate_id": candidate.id, "slack_ts": slack_ts,
                             "is_reminder": is_reminder})

        logger.info(
            "[CountyLaunchEvaluator] posted Slack approval for %s (ts=%s)",
            candidate.county_id, slack_ts,
        )
        return {"all_green": True, "posted": True, "slack_ts": slack_ts,
                "candidate_id": candidate.id}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    dry = "--dry-run" in sys.argv
    result = run_county_launch_evaluator(dry_run=dry)
    print(json.dumps(result, indent=2, default=str))
