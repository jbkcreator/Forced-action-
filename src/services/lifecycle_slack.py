"""
Slack helper for Lifecycle self-healing incidents (fa034).

Posts incident notifications to the channel configured by
`LIFECYCLE_INCIDENT_SLACK_CHANNEL` using the WebClient pattern already proven
in `src/tasks/county_launch_evaluator.py`.

When the Slack channel isn't configured, **silently falls back** to
`src/services/email.py::send_alert` — the same path heartbeat_monitor,
anomaly_pager, and match_rate_monitor already use. Slack-disabled is
never a silent failure: an alert always goes somewhere.

Usage:
    from src.services.lifecycle_slack import post_incident_alert
    post_incident_alert(incident_dict, kind="new", action_summary="...")

`incident_dict` is the raw-SQL row-like (any object with the column
attributes from lifecycle_incident). `kind` is one of:
    - "new"            — incident just opened, before any action
    - "action_taken"   — Lifecycle took an automatic action within guardrails
    - "human_required" — duration threshold hit, escalating to a person
    - "kill_recommended" — 7-day red trigger; ops review/approve before kill
    - "resolved"       — metric recovered, incident closed
"""

from __future__ import annotations

import logging
from typing import Any, Optional

from config.settings import get_settings

logger = logging.getLogger(__name__)


_KIND_PRELUDE = {
    "new":              ":warning: Lifecycle opened a new incident",
    "action_taken":     ":robot_face: Lifecycle took an automatic action",
    "human_required":   ":rotating_light: Lifecycle needs human review",
    "kill_recommended": ":no_entry: Lifecycle recommends killing a feature",
    "resolved":         ":white_check_mark: Lifecycle incident resolved",
}


def _summary_text(incident: Any, kind: str) -> str:
    """Plain-text summary — used as the Slack fallback text AND the
    email subject when Slack is unavailable. <140 chars."""
    metric = getattr(incident, "metric_name", "?")
    severity = getattr(incident, "severity", "?")
    observed = getattr(incident, "observed_value", None)
    threshold = getattr(incident, "threshold_value", None)
    prelude = {
        "new":              f"[{severity.upper()}] {metric}",
        "action_taken":     f"[ACTION] {metric}",
        "human_required":   f"[HUMAN] {metric}",
        "kill_recommended": f"[KILL?] {metric}",
        "resolved":         f"[OK] {metric}",
    }.get(kind, f"[{kind}] {metric}")
    detail = ""
    if observed is not None and threshold is not None:
        detail = f" — observed {observed} vs threshold {threshold}"
    return (prelude + detail)[:140]


def _incident_blocks(incident: Any, kind: str, action_summary: str) -> list:
    """Slack block-kit payload for the incident. Mirrors the format used in
    county_launch_evaluator so the visual style is consistent."""
    metric = getattr(incident, "metric_name", "?")
    severity = getattr(incident, "severity", "?")
    observed = getattr(incident, "observed_value", None)
    threshold = getattr(incident, "threshold_value", None)
    baseline = getattr(incident, "baseline_value", None)
    county = getattr(incident, "county_id", None) or "all"
    feature = getattr(incident, "feature_name", None) or "n/a"
    action_taken = getattr(incident, "action_taken", "no_op")
    duration_h = getattr(incident, "duration_hours", None)
    breach_started = getattr(incident, "breach_started", None)

    fields = [
        {"type": "mrkdwn", "text": f"*Metric*\n{metric}"},
        {"type": "mrkdwn", "text": f"*Severity*\n{severity}"},
        {"type": "mrkdwn", "text": f"*County*\n{county}"},
        {"type": "mrkdwn", "text": f"*Feature*\n{feature}"},
    ]
    if observed is not None:
        fields.append({"type": "mrkdwn", "text": f"*Observed*\n{observed}"})
    if threshold is not None:
        fields.append({"type": "mrkdwn", "text": f"*Threshold*\n{threshold}"})
    if baseline is not None:
        fields.append({"type": "mrkdwn", "text": f"*7d baseline*\n{baseline}"})
    if duration_h is not None:
        fields.append({"type": "mrkdwn", "text": f"*Duration*\n{duration_h}h"})
    if breach_started is not None:
        fields.append({"type": "mrkdwn", "text": f"*Started*\n{breach_started}"})
    fields.append({"type": "mrkdwn", "text": f"*Action*\n{action_taken}"})

    return [
        {"type": "header", "text": {"type": "plain_text", "text": _KIND_PRELUDE.get(kind, kind)[:150]}},
        {"type": "section", "fields": fields[:10]},   # Slack max 10 per section
        {"type": "context", "elements": [
            {"type": "mrkdwn", "text": action_summary[:300] or "_no action summary_"},
        ]},
    ]


def post_incident_alert(
    incident: Any,
    kind: str,
    action_summary: str = "",
) -> Optional[str]:
    """Post a Lifecycle incident notification.

    Returns the Slack message timestamp (`ts`) on success, or `None` when
    neither Slack nor email could be sent. Slack-unconfigured is NOT a
    failure — falls back to email.send_alert with the same content. Email-
    unconfigured (no SMTP) is a true no-op and logs a warning.
    """
    settings = get_settings()
    token = settings.slack_bot_token
    channel = settings.lifecycle_incident_slack_channel

    summary = _summary_text(incident, kind)

    # Try Slack first.
    if token and channel:
        try:
            # Lazy import keeps the module loadable when slack_sdk isn't
            # installed in environments that don't use Slack.
            from slack_sdk import WebClient
            from slack_sdk.errors import SlackApiError
        except ImportError:
            logger.warning("[lifecycle_slack] slack_sdk not installed — falling back to email")
        else:
            try:
                client = WebClient(token=token.get_secret_value())
                resp = client.chat_postMessage(
                    channel=channel,
                    text=summary,
                    blocks=_incident_blocks(incident, kind, action_summary),
                )
                return resp.get("ts")
            except SlackApiError as exc:
                logger.warning(
                    "[lifecycle_slack] Slack post failed (%s) — falling back to email",
                    exc.response.get("error") if exc.response else exc,
                )
            except Exception:
                # Don't let Slack issues block the incident path — always
                # fall through to email so ops gets notified somewhere.
                logger.warning("[lifecycle_slack] Slack post raised — falling back to email", exc_info=True)

    # Fallback path: email.send_alert (the existing ops alert channel).
    try:
        from src.services.email import send_alert
        body_lines = [
            summary,
            "",
            f"kind:            {kind}",
            f"action_summary:  {action_summary}",
            f"metric:          {getattr(incident, 'metric_name', '?')}",
            f"severity:        {getattr(incident, 'severity', '?')}",
            f"observed:        {getattr(incident, 'observed_value', '?')}",
            f"threshold:       {getattr(incident, 'threshold_value', '?')}",
            f"baseline:        {getattr(incident, 'baseline_value', '?')}",
            f"county:          {getattr(incident, 'county_id', '?')}",
            f"feature:         {getattr(incident, 'feature_name', '?')}",
            f"action_taken:    {getattr(incident, 'action_taken', '?')}",
            f"duration_hours:  {getattr(incident, 'duration_hours', '?')}",
            f"breach_started:  {getattr(incident, 'breach_started', '?')}",
        ]
        ok = send_alert(subject=summary[:80], body="\n".join(body_lines))
        return "email" if ok else None
    except Exception:
        logger.warning("[lifecycle_slack] email fallback raised — no notification sent", exc_info=True)
        return None
