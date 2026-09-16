"""WP-9 Dial List — delivery adapter (spec Q6).

Formats a ranked ``DialList`` into a Slack digest and posts it as ONE daily
message to the MONEY channel, reusing the same ``slack_bot_token`` + WebClient
convention as ``relay.slack_post`` / ``cora_throughput.batch_slack`` (no
separate Slack app). Delivery only — no ranking, no scoring.

Internal operator calling aid. Never borrower-facing: the digest carries only
the trigger, property, relationship facts, estimated opportunity size, reason,
and talking points — no rate, term, commitment, or borrower-facing message.
"""
from __future__ import annotations

import json
import logging
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from config.settings import get_settings
from src.services.opportunity_outcome import LOSS_REASON_CODES

from .models import DialList, DialListEntry

logger = logging.getLogger(__name__)

_ZERO = Decimal("0")

# Interactive card action IDs (Banks button model, re-labelled for a call list).
ACTION_CALLED = "dial_called"
ACTION_WON = "dial_won"
ACTION_LOST = "dial_lost"
ACTION_SKIP = "dial_skip"
# Prefix for the per-entry actions block_id, so the handler can locate and
# replace exactly the tapped card's action row inside the digest message.
ACTIONS_BLOCK_PREFIX = "dial_act:"


def _name_label(entry: DialListEntry) -> str:
    if entry.contact_name:
        name = entry.contact_name
        return name if entry.borrower_resolved else f"{name} (unverified)"
    return "Unresolved borrower"


def _property_label(entry: DialListEntry) -> str:
    if entry.property_address:
        return entry.property_address
    return f"property `{entry.property_id}`"


def _money_short(amount: Decimal) -> str:
    if amount >= 1_000_000:
        return f"${amount / Decimal('1000000'):.1f}M"
    if amount >= 1_000:
        return f"${amount / Decimal('1000'):.0f}K"
    return f"${amount:,.0f}"


def _size_label(entry: DialListEntry) -> str:
    if entry.expected_loan <= _ZERO:
        return "size n/a"
    return f"~{_money_short(entry.expected_loan)}"


def _trigger_label(trigger: str) -> str:
    _LABELS = {
        "cash_purchase": "💵 Cash Purchase",
        "stalled_flip": "🔨 Stalled Flip",
        "permits_no_financing": "📋 Permits / No Financing",
        "auction_probate": "⚖️ Auction / Probate",
        "out_of_state": "✈️ Out-of-State",
        "financing_intent": "💬 Financing Intent",
        "builder": "🏗️ Builder",
        "maturities": "⏰ Maturity Approaching",
        "exchange_1031": "🔄 1031 Exchange",
        "price_drop": "📉 Price Drop",
        "expired_listing": "❌ Expired Listing",
    }
    return _LABELS.get(trigger, trigger.replace("_", " ").title())


def _entry_blocks(entry: DialListEntry, as_of: object, interactive: bool) -> List[Dict[str, Any]]:
    name = _name_label(entry)
    phone = entry.phone or "—"
    address = _property_label(entry)
    size = _size_label(entry)
    triggers = "  ".join(_trigger_label(t) for t in entry.triggers) if entry.triggers else "—"

    blocks: List[Dict[str, Any]] = [
        {"type": "divider"},
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*#{entry.rank} — {name}*\n📞 {phone}"},
                {"type": "mrkdwn", "text": f"*Property*\n{address}"},
            ],
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Est. Loan*\n{size}"},
                {"type": "mrkdwn", "text": f"*Signals*\n{triggers}"},
            ],
        },
        {
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f"_{entry.reason}_"}],
        },
    ]

    if entry.talking_points:
        points_text = "   •   ".join(entry.talking_points)
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f"💡  {points_text}"}],
        })

    if interactive:
        blocks.append(_actions_block(entry, as_of))

    return blocks


def _entry_value(entry: DialListEntry, as_of: object, **extra: object) -> str:
    """JSON payload carried on each button/select — thread id + context."""
    data: Dict[str, Any] = {
        "thread": entry.opportunity_id,
        "property_id": entry.property_id,
        "as_of": str(as_of),
    }
    data.update(extra)
    return json.dumps(data, separators=(",", ":"))


def _actions_block(entry: DialListEntry, as_of: object) -> Dict[str, Any]:
    base = _entry_value(entry, as_of)
    return {
        "type": "actions",
        "block_id": f"{ACTIONS_BLOCK_PREFIX}{entry.property_id}",
        "elements": [
            {
                "type": "button", "action_id": ACTION_CALLED,
                "text": {"type": "plain_text", "text": ":phone: Called"},
                "value": base,
            },
            {
                "type": "button", "action_id": ACTION_WON, "style": "primary",
                "text": {"type": "plain_text", "text": ":white_check_mark: Won"},
                "value": base,
            },
            {
                "type": "static_select", "action_id": ACTION_LOST,
                "placeholder": {"type": "plain_text", "text": "Lost — reason…"},
                "options": [
                    {
                        "text": {"type": "plain_text", "text": code},
                        "value": _entry_value(entry, as_of, loss_code=code),
                    }
                    for code in LOSS_REASON_CODES
                ],
            },
            {
                "type": "button", "action_id": ACTION_SKIP, "style": "danger",
                "text": {"type": "plain_text", "text": "⏭ Skip"},
                "value": base,
            },
        ],
    }


def _header_blocks(dial_list: DialList) -> Tuple[str, List[Dict[str, Any]]]:
    n = len(dial_list.entries)
    date_str = dial_list.generated_for.strftime("%A, %B ") + str(dial_list.generated_for.day)
    if n == 0:
        fallback = f"📞 Dial List — {date_str} — no opportunities today"
        return fallback, [{"type": "section", "text": {"type": "mrkdwn", "text": fallback}}]

    fallback = f"📞 Dial List — {date_str} — {n} calls"
    low_conf = any(
        e.expected_loan_confidence == "low" and e.expected_loan > _ZERO
        for e in dial_list.entries
    )
    size_note = "\n_⚠️ Some loan sizes are rough estimates (assessed value fallback — ARV comps pending)._" if low_conf else ""
    header_text = (
        f"*📞 Dial List — {date_str}*\n"
        f"{n} calls  ·  {dial_list.candidate_count} candidates scored{size_note}"
    )
    blocks: List[Dict[str, Any]] = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"📞 Today's Call List — {date_str}"},
        },
        {
            "type": "context",
            "elements": [
                {"type": "mrkdwn",
                 "text": f"*{n} calls*  ·  {dial_list.candidate_count} candidates scored  ·  `{dial_list.config_version}`"},
            ],
        },
    ]
    if low_conf:
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn",
                          "text": "⚠️ _Some loan sizes are rough estimates (assessed value fallback — ARV comps pending)._"}],
        })
    if dial_list.from_cache:
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn",
                          "text": "♻️ _Posted from cached state — live generation failed; data may be stale._"}],
        })
    if dial_list.stale_sources:
        srcs = ", ".join(dial_list.stale_sources)
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn",
                          "text": f"⚠️ _Stale sources (data may be behind): {srcs}._"}],
        })
    return fallback, blocks


def format_dial_list_digest(
    dial_list: DialList, *, interactive: bool = False
) -> Tuple[str, List[Dict[str, Any]]]:
    """Render a ``DialList`` into (fallback_text, Slack blocks). Pure, no I/O.

    When ``interactive`` is set, each entry card carries Called/Won/Lost/Skip buttons.
    """
    fallback, blocks = _header_blocks(dial_list)
    if not dial_list.entries:
        return fallback, blocks

    for entry in dial_list.entries:
        blocks.extend(_entry_blocks(entry, dial_list.generated_for, interactive))

    blocks.append({"type": "divider"})
    return fallback, blocks


def _resolve_channel(explicit: Optional[str]) -> str:
    if explicit:
        return explicit
    return get_settings().dial_list_slack_channel


def deliver_dial_list(
    dial_list: DialList,
    *,
    channel: Optional[str] = None,
    interactive: bool = False,
) -> Optional[str]:
    """Post the daily dial-list digest to the MONEY Slack channel.

    Returns the posted message's ``ts`` on success, else ``None``. No-ops
    (logs and returns None) when Slack isn't configured — mirrors
    ``relay.slack_post.post_for_approval`` so local/dev without Slack still
    runs the pipeline end to end. Never raises: a failed post must not fail
    the daily job. When ``interactive`` is set, cards carry action buttons.
    """
    settings = get_settings()
    token = settings.slack_bot_token
    target = _resolve_channel(channel)
    if not token or not target:
        logger.warning(
            "[DialList] Slack not configured (dial_list_slack_channel/slack_bot_token "
            "unset) — digest for %s not posted (%d entries)",
            dial_list.generated_for, len(dial_list.entries),
        )
        return None

    fallback, header_blocks = _header_blocks(dial_list)
    posted_timestamps: List[str] = []
    try:
        from slack_sdk import WebClient

        client = WebClient(token=token.get_secret_value())
        # Post header as the channel message (single clean notification)
        resp = client.chat_postMessage(
            channel=target, text=fallback, blocks=header_blocks
        )
        thread_ts = resp["ts"]
        posted_timestamps.append(thread_ts)
        # Post each entry as a threaded reply — keeps the channel clean and
        # stays well under Slack's 50-block-per-message limit (each card ≤ 6 blocks).
        for entry in dial_list.entries:
            entry_blocks = _entry_blocks(entry, dial_list.generated_for, interactive)
            entry_response = client.chat_postMessage(
                channel=target,
                text=f"#{entry.rank} — {_name_label(entry)}",
                blocks=entry_blocks,
                thread_ts=thread_ts,
            )
            posted_timestamps.append(entry_response["ts"])
        logger.info(
            "[DialList] digest posted for %s (%d entries, thread_ts=%s)",
            dial_list.generated_for, len(dial_list.entries), thread_ts,
        )
        return thread_ts
    except Exception as exc:
        # Avoid leaving a partial queue that a retry would duplicate. Delete
        # children first, then the header; cleanup is best effort because the
        # original Slack failure may also affect deletion.
        for posted_ts in reversed(posted_timestamps):
            try:
                client.chat_delete(channel=target, ts=posted_ts)
            except Exception:
                logger.warning("[DialList] partial-post cleanup failed for %s", posted_ts,
                               exc_info=True)
        logger.error(
            "[DialList] Slack post failed for %s: %s",
            dial_list.generated_for, exc, exc_info=True,
        )
        return None


def generate_and_deliver(
    session: Any,
    *,
    as_of,
    county_id: Optional[str] = None,
    config: Optional[Any] = None,
    channel: Optional[str] = None,
    interactive: bool = False,
) -> Tuple[DialList, Optional[str]]:
    """Chain retrieval → rank → deliver. Returns (dial_list, posted_ts).

    The convenience the daily cron calls: assemble candidates from the DB,
    rank them, and post the digest. Delegates ranking to the pure core via
    ``repository.generate_dial_list`` — this function adds only delivery.

    Failure behavior (amendment): if live generation fails, fall back to the
    last cached snapshot and post that (flagged from-cache) so the morning list
    still goes out. On success the fresh list is snapshotted for a future
    fallback. If generation fails AND no snapshot exists, the error propagates.
    """
    from sqlalchemy.exc import SQLAlchemyError

    from .repository import (
        generate_dial_list,
        load_latest_dial_list_snapshot,
        stale_dial_list_sources,
        write_dial_list_snapshot,
    )

    try:
        dial_list = generate_dial_list(
            session, as_of=as_of, county_id=county_id, config=config
        )
    except SQLAlchemyError:
        logger.error(
            "[DialList] live generation failed for %s — attempting cached fallback",
            as_of, exc_info=True,
        )
        session.rollback()
        dial_list = load_latest_dial_list_snapshot(session, county_id=county_id)
        if dial_list is None:
            logger.error("[DialList] no cached snapshot to fall back to")
            raise
        # Snapshot data describes the last good list, but source health must
        # describe the current failed run so Slack can name stale feeds.
        dial_list.stale_sources = stale_dial_list_sources(
            session,
            as_of=as_of,
            sla_days=get_settings().dial_list_source_sla_days,
            county_id=county_id,
        )
    else:
        # Snapshot the fresh list best-effort — a snapshot-write failure must
        # never discard a good live list or trigger the cached fallback.
        try:
            write_dial_list_snapshot(session, dial_list, county_id=county_id)
        except SQLAlchemyError:
            logger.warning(
                "[DialList] snapshot write failed for %s (live list still posts)",
                as_of, exc_info=True,
            )

    ts = deliver_dial_list(dial_list, channel=channel, interactive=interactive)
    return dial_list, ts
