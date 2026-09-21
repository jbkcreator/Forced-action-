"""WP-T2-11 — Slack delivery for GYR routing decisions.

Follows the dial_list/delivery.py convention: WebClient, header block +
threaded entry cards, fa_max_slack_bot_token or slack_bot_token fallback.
No-Slack/dev: log-and-skip.

Internal operator tool only — never borrower-facing.
"""
from __future__ import annotations

import logging
from decimal import Decimal

from config.settings import get_settings
from .models import GyrColor, RouterContext, RoutingDecision

logger = logging.getLogger(__name__)

_COLOR_BADGE = {
    GyrColor.GREEN: "🟢 GREEN",
    GyrColor.YELLOW: "🟡 YELLOW",
    GyrColor.RED: "🔴 RED",
}


def _revenue_label(cents: int) -> str:
    amount = Decimal(cents) / 100
    if amount >= 1_000_000:
        return f"${amount / Decimal('1000000'):.1f}M"
    if amount >= 1_000:
        return f"${amount / Decimal('1000'):.0f}K"
    return f"${amount:,.0f}"


def _money_header_text(ctx: RouterContext, decision: RoutingDecision) -> str:
    badge = _COLOR_BADGE[decision.color]
    rev = _revenue_label(decision.expected_revenue_cents)
    reasons = ", ".join(decision.reason_codes) if decision.reason_codes else ""
    base = f"{badge}  |  {rev}  |  {ctx.opportunity_type}  |  opp `{ctx.opportunity_id[:8]}`"
    if reasons:
        base += f"\n> {reasons}"
    return base


def _exceptions_header_text(ctx: RouterContext, decision: RoutingDecision) -> str:
    reasons = " | ".join(decision.reason_codes) if decision.reason_codes else "no reason"
    rule = decision.disqualifying_rule or ""
    base = (
        f"🔴 EXCEPTIONS  |  {ctx.opportunity_type}  |  opp `{ctx.opportunity_id[:8]}`\n"
        f"> {reasons}"
    )
    if rule:
        base += f"\n> Rule: `{rule}`"
    return base


def post_to_slack(ctx: RouterContext, decision: RoutingDecision) -> None:
    """Post one GYR card to the appropriate Slack channel."""
    settings = get_settings()
    token = settings.fa_max_slack_bot_token or settings.slack_bot_token

    if not token:
        logger.debug(
            "GYR delivery: Slack token not configured — skipping post for %s",
            ctx.opportunity_id,
        )
        return

    if decision.queue == "MONEY":
        channel = settings.fa_max_slack_channel_money
        text_body = _money_header_text(ctx, decision)
    elif decision.queue == "EXCEPTIONS":
        channel = settings.fa_max_slack_channel_exceptions
        text_body = _exceptions_header_text(ctx, decision)
    else:
        # Terminal outcomes — audit only, no Slack post
        return

    if not channel:
        logger.debug(
            "GYR delivery: channel not configured for queue=%s — skipping",
            decision.queue,
        )
        return

    try:
        from slack_sdk import WebClient

        client = WebClient(token=token.get_secret_value())
        client.chat_postMessage(
            channel=channel,
            text=text_body,
            blocks=[
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": text_body},
                }
            ],
        )
    except Exception:
        logger.exception(
            "GYR delivery: Slack post failed for opportunity %s (queue=%s)",
            ctx.opportunity_id,
            decision.queue,
        )


def post_staleness_alert(ctx: RouterContext, decision: RoutingDecision) -> None:
    """Resurface a stale green opportunity with a staleness flag."""
    settings = get_settings()
    token = settings.fa_max_slack_bot_token or settings.slack_bot_token
    channel = settings.fa_max_slack_channel_money

    if not token or not channel:
        logger.debug("GYR staleness: Slack not configured — skipping")
        return

    rev = _revenue_label(decision.expected_revenue_cents)
    text_body = (
        f"⚠️ STALE GREEN  |  {rev}  |  {ctx.opportunity_type}  "
        f"|  opp `{ctx.opportunity_id[:8]}`\n"
        "> Not actioned within one business day — please review."
    )

    try:
        from slack_sdk import WebClient

        client = WebClient(token=token.get_secret_value())
        client.chat_postMessage(
            channel=channel,
            text=text_body,
            blocks=[
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": text_body},
                }
            ],
        )
    except Exception:
        logger.exception(
            "GYR delivery: staleness alert failed for opportunity %s",
            ctx.opportunity_id,
        )
