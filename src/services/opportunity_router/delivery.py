"""WP-T2-11 — Slack delivery for GYR routing decisions.

Follows the dial_list/delivery.py convention: WebClient, header block +
threaded entry cards, fa_max_slack_bot_token or slack_bot_token fallback.
No-Slack/dev: log-and-skip.

Internal operator tool only — never borrower-facing.
"""
from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any, Optional

from config.settings import get_settings
from .models import GyrColor, RouterContext, RoutingDecision

logger = logging.getLogger(__name__)

_HEADER_EMOJI = {
    GyrColor.GREEN: "🟢",
    GyrColor.YELLOW: "🟡",
    GyrColor.RED: "🔴",
}

_HEADER_LABEL = {
    GyrColor.GREEN: "GREEN — Ready to Work",
    GyrColor.YELLOW: "YELLOW — Needs Review",
    GyrColor.RED: "RED — Exceptions Queue",
}


def _revenue_label(cents: int) -> str:
    amount = Decimal(cents) / 100
    if amount >= 1_000_000:
        return f"${amount / Decimal('1000000'):.1f}M"
    if amount >= 1_000:
        return f"${amount / Decimal('1000'):.0f}K"
    return f"${amount:,.0f}"


def _divider() -> dict:
    return {"type": "divider"}


def _header(text: str) -> dict:
    return {"type": "header", "text": {"type": "plain_text", "text": text, "emoji": True}}


def _section(text: str) -> dict:
    return {"type": "section", "text": {"type": "mrkdwn", "text": text}}


def _fields(*pairs: tuple[str, str]) -> dict:
    return {
        "type": "section",
        "fields": [
            {"type": "mrkdwn", "text": f"*{label}*\n{value}"}
            for label, value in pairs
        ],
    }


def _context(*elements: str) -> dict:
    return {
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": el} for el in elements],
    }


def _money_blocks(ctx: RouterContext, decision: RoutingDecision) -> list[dict[str, Any]]:
    emoji = _HEADER_EMOJI[decision.color]
    label = _HEADER_LABEL[decision.color]
    rev = _revenue_label(decision.expected_revenue_cents)

    blocks: list[dict] = [
        _header(f"{emoji}  {label}"),
        _divider(),
        _fields(
            ("Est. Revenue", f"`{rev}`"),
            ("Type", ctx.opportunity_type.replace("_", " ").title()),
            ("Opportunity", f"`{ctx.opportunity_id[:8]}`"),
            ("As of", str(ctx.as_of)),
        ),
    ]

    if decision.reason_codes:
        reasons_text = "  •  ".join(
            r.replace("_", " ").title() for r in decision.reason_codes
        )
        blocks.append(_context(f"⚠️  {reasons_text}"))

    return blocks


def _exceptions_blocks(ctx: RouterContext, decision: RoutingDecision) -> list[dict[str, Any]]:
    reason_codes = [r for r in decision.reason_codes if r not in ("out_of_box", "borrower_suppressed")]
    program_fails = [r for r in reason_codes if r.startswith("[")]
    named_reasons = [r for r in reason_codes if not r.startswith("[")]

    blocks: list[dict] = [
        _header(f"🔴  EXCEPTIONS — {ctx.opportunity_type.replace('_', ' ').title()}"),
        _divider(),
        _fields(
            ("Opportunity", f"`{ctx.opportunity_id[:8]}`"),
            ("Est. Revenue", f"`{_revenue_label(decision.expected_revenue_cents)}`"),
        ),
    ]

    if named_reasons:
        blocks.append(_section(
            "*Disqualification reasons*\n" +
            "\n".join(f"• {r.replace('_', ' ').title()}" for r in named_reasons)
        ))

    if decision.disqualifying_rule:
        blocks.append(_section(f"*Disqualifying rule*\n```{decision.disqualifying_rule}```"))

    if program_fails:
        blocks.append(_context(
            "Program failures:  " + "  |  ".join(program_fails)
        ))

    blocks.append(_context(f"opp `{ctx.opportunity_id}` · as of {ctx.as_of}"))
    return blocks


def _staleness_blocks(ctx: RouterContext, decision: RoutingDecision) -> list[dict[str, Any]]:
    rev = _revenue_label(decision.expected_revenue_cents)
    return [
        _header("⚠️  Stale Green — Not Yet Actioned"),
        _divider(),
        _fields(
            ("Est. Revenue", f"`{rev}`"),
            ("Type", ctx.opportunity_type.replace("_", " ").title()),
            ("Opportunity", f"`{ctx.opportunity_id[:8]}`"),
        ),
        _context("This opportunity has been green for more than one business day with no action. Please review."),
    ]


def _post(token, channel: str, fallback_text: str, blocks: list) -> None:
    from slack_sdk import WebClient
    client = WebClient(token=token.get_secret_value())
    client.chat_postMessage(channel=channel, text=fallback_text, blocks=blocks)


def _slack_token():
    settings = get_settings()
    return settings.fa_max_slack_bot_token or settings.slack_bot_token


def _queue_channel(queue: str) -> Optional[str]:
    settings = get_settings()
    if queue == "MONEY":
        return settings.fa_max_slack_channel_money
    if queue == "EXCEPTIONS":
        return settings.fa_max_slack_channel_exceptions
    return None


def post_queue_header(queue: str, *, count: int, total_revenue_cents: int) -> None:
    """Post the ranked-list header that precedes a sweep's cards, so Josh
    reads the queue top-down from here (Slack shows newest last)."""
    token = _slack_token()
    channel = _queue_channel(queue)
    if not token or not channel:
        logger.debug("GYR delivery: header skipped for queue=%s (token/channel not configured)", queue)
        return
    title = f"{queue} — {count} opportunities, ranked by expected revenue (total {_revenue_label(total_revenue_cents)})"
    try:
        _post(token, channel, title, [_header(title)])
    except Exception:
        logger.exception("GYR delivery: header post failed for queue=%s", queue)


def post_to_slack(
    ctx: RouterContext, decision: RoutingDecision,
    rank: Optional[int] = None, total: Optional[int] = None,
) -> None:
    """Post one GYR card to the appropriate Slack channel. `rank`/`total`
    label the card's position in the sweep's ranked list."""
    settings = get_settings()
    token = settings.fa_max_slack_bot_token or settings.slack_bot_token

    if not token:
        logger.debug("GYR delivery: Slack token not configured — skipping %s", ctx.opportunity_id)
        return

    if decision.queue == "MONEY":
        channel = settings.fa_max_slack_channel_money
        blocks = _money_blocks(ctx, decision)
        fallback = f"{_HEADER_EMOJI[decision.color]} {decision.color.value.upper()} | {_revenue_label(decision.expected_revenue_cents)} | opp {ctx.opportunity_id[:8]}"
    elif decision.queue == "EXCEPTIONS":
        channel = settings.fa_max_slack_channel_exceptions
        blocks = _exceptions_blocks(ctx, decision)
        fallback = f"🔴 EXCEPTIONS | {ctx.opportunity_type} | opp {ctx.opportunity_id[:8]}"
    else:
        return  # terminal — audit only

    if not channel:
        logger.debug("GYR delivery: channel not configured for queue=%s — skipping", decision.queue)
        return

    if rank is not None:
        label = f"#{rank} of {total}"
        blocks = [blocks[0], _context(label), *blocks[1:]]
        fallback = f"{label} · {fallback}"

    try:
        _post(token, channel, fallback, blocks)
    except Exception:
        logger.exception("GYR delivery: Slack post failed for opportunity %s (queue=%s)", ctx.opportunity_id, decision.queue)


def post_reengagement(ctx: RouterContext, decision: RoutingDecision) -> None:
    """EXCEPTIONS card: a deal the Lender Box previously declined now fits."""
    token = _slack_token()
    channel = _queue_channel("EXCEPTIONS")
    if not token or not channel:
        logger.debug("GYR delivery: re-engagement skipped for %s (token/channel not configured)", ctx.opportunity_id)
        return
    rev = _revenue_label(decision.expected_revenue_cents)
    blocks = [
        _header("🔁  Possible Re-engagement — Now Fits the Lender Box"),
        _divider(),
        _fields(
            ("Now", decision.color.value.upper()),
            ("Est. Revenue", f"`{rev}`"),
            ("Opportunity", f"`{ctx.opportunity_id[:8]}`"),
        ),
        _context("Previously out of the box. A program change means this deal may now be fundable."),
    ]
    try:
        _post(token, channel, f"🔁 Re-engagement | {rev} | opp {ctx.opportunity_id[:8]}", blocks)
    except Exception:
        logger.exception("GYR delivery: re-engagement post failed for opportunity %s", ctx.opportunity_id)


def post_staleness_alert(ctx: RouterContext, decision: RoutingDecision) -> None:
    """Resurface a stale green opportunity with a staleness flag."""
    settings = get_settings()
    token = settings.fa_max_slack_bot_token or settings.slack_bot_token
    channel = settings.fa_max_slack_channel_money

    if not token or not channel:
        logger.debug("GYR staleness: Slack not configured — skipping")
        return

    fallback = f"⚠️ STALE GREEN | {_revenue_label(decision.expected_revenue_cents)} | opp {ctx.opportunity_id[:8]}"
    try:
        _post(token, channel, fallback, _staleness_blocks(ctx, decision))
    except Exception:
        logger.exception("GYR delivery: staleness alert failed for opportunity %s", ctx.opportunity_id)
