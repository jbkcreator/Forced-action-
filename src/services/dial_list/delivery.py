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

import logging
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from config.settings import get_settings

from .models import DialList, DialListEntry

logger = logging.getLogger(__name__)

_ZERO = Decimal("0")


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


def _entry_line(entry: DialListEntry) -> str:
    triggers = ", ".join(entry.triggers) if entry.triggers else "—"
    phone = f" · :phone: {entry.phone}" if entry.phone else ""
    line = (
        f"*#{entry.rank}* — *{_name_label(entry)}*{phone}\n"
        f"  {_property_label(entry)}\n"
        f"  _{triggers}_ · Est. {_size_label(entry)}\n"
        f"  {entry.reason}"
    )
    if entry.talking_points:
        points = "\n".join(f"    • {p}" for p in entry.talking_points)
        line = f"{line}\n{points}"
    return line


def _header_text(dial_list: DialList) -> str:
    n = len(dial_list.entries)
    if n == 0:
        return f"*Dial List — {dial_list.generated_for}* — no opportunities today"
    header = (
        f"*Dial List — {dial_list.generated_for}* — top {n} calls "
        f"({dial_list.candidate_count} candidates · {dial_list.config_version})"
    )
    if any(
        e.expected_loan_confidence == "low" and e.expected_loan > _ZERO
        for e in dial_list.entries
    ):
        header += (
            "\n_Sizes are rough estimates from assessed value "
            "(ARV comps pending)._"
        )
    return header


def format_dial_list_digest(dial_list: DialList) -> Tuple[str, List[Dict[str, Any]]]:
    """Render a ``DialList`` into (header_text, Slack blocks). Pure, no I/O.

    ``header_text`` is the message fallback/notification text; ``blocks`` is the
    rich digest. Deterministic — same list in, same payload out.
    """
    header = _header_text(dial_list)
    blocks: List[Dict[str, Any]] = [
        {"type": "section", "text": {"type": "mrkdwn", "text": header}},
    ]
    if not dial_list.entries:
        return header, blocks

    blocks.append({"type": "divider"})
    for entry in dial_list.entries:
        blocks.append(
            {"type": "section", "text": {"type": "mrkdwn", "text": _entry_line(entry)}}
        )
    return header, blocks


def _resolve_channel(explicit: Optional[str]) -> str:
    if explicit:
        return explicit
    return get_settings().dial_list_slack_channel


def deliver_dial_list(
    dial_list: DialList,
    *,
    channel: Optional[str] = None,
) -> Optional[str]:
    """Post the daily dial-list digest to the MONEY Slack channel.

    Returns the posted message's ``ts`` on success, else ``None``. No-ops
    (logs and returns None) when Slack isn't configured — mirrors
    ``relay.slack_post.post_for_approval`` so local/dev without Slack still
    runs the pipeline end to end. Never raises: a failed post must not fail
    the daily job.
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

    header, blocks = format_dial_list_digest(dial_list)
    try:
        from slack_sdk import WebClient

        client = WebClient(token=token.get_secret_value())
        response = client.chat_postMessage(channel=target, text=header, blocks=blocks)
        logger.info(
            "[DialList] digest posted for %s (%d entries)",
            dial_list.generated_for, len(dial_list.entries),
        )
        return response["ts"]
    except Exception as exc:
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
) -> Tuple[DialList, Optional[str]]:
    """Chain retrieval → rank → deliver. Returns (dial_list, posted_ts).

    The convenience the daily cron calls: assemble candidates from the DB,
    rank them, and post the digest. Delegates ranking to the pure core via
    ``repository.generate_dial_list`` — this function adds only delivery.
    """
    from .repository import generate_dial_list

    dial_list = generate_dial_list(
        session, as_of=as_of, county_id=county_id, config=config
    )
    ts = deliver_dial_list(dial_list, channel=channel)
    return dial_list, ts
