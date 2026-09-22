"""
Push a pending relay_approval_queue row to Slack for Josh's decision
(RELAY-v2.2 sub-task R1, build spec §1.1.13 — "Decision Packets render as
interactive approve/reject buttons").

Mirrors the existing county-launch approval pattern
(src/api/admin_router.py:slack_county_launch_interact / _update_slack_message)
— same WebClient usage, same "JSON string in the button value" convention.
"""
from __future__ import annotations

import json
import hashlib
import logging
from typing import TYPE_CHECKING

from config.settings import get_settings
from config.venture_template import DEFAULT_VENTURE_KEY
from src.services.relay import queue
from src.services.relay.queue import QueueItem
from src.utils.venture_config import get_venture_config

if TYPE_CHECKING:
    from src.services.relay.engine import BatchResult

logger = logging.getLogger(__name__)


def _summary_text(item: QueueItem) -> str:
    subject = item.payload.get("subject") if isinstance(item.payload, dict) else None
    preview = subject or str(item.payload)[:120]
    return (
        f"*Relay approval needed* (#{item.id})  Ref: `{_card_ref(item)}`\n"
        f"Channel: `{item.channel}`  ·  To: `{item.recipient}`\n"
        f"{preview}\n"
        "Reply in this thread with `approve` or `reject`, or use the buttons below."
    )


def _card_ref(item: QueueItem) -> str:
    """Opaque queue-row identity for cross-environment Slack reconciliation."""
    material = f"{item.idempotency_key}:{item.created_at.isoformat()}".encode()
    return hashlib.sha256(material).hexdigest()[:20]


_FA_MAX_VENTURE = "fa_max_lending"

_LANE_CHANNEL_ATTR = {
    "MONEY": "fa_max_slack_channel_money",
    "EXCEPTIONS": "fa_max_slack_channel_exceptions",
    "RELATIONSHIPS": "fa_max_slack_channel_relationships",
}


def _resolve_channel(item: QueueItem, settings) -> str:
    """Return the Slack channel for this item.

    FA Max items with a lane route to their lane-specific channel from
    settings. All other items (and FA Max items with no lane) fall back to
    the venture's relay_slack_channel.
    """
    if item.venture_key == _FA_MAX_VENTURE and item.lane:
        attr = _LANE_CHANNEL_ATTR.get(item.lane)
        if attr:
            lane_channel = getattr(settings, attr, "")
            if lane_channel:
                return lane_channel
    return get_venture_config(item.venture_key).relay_slack_channel


def _resolve_bot_token(item: QueueItem, settings):
    # FA Max uses its dedicated Slack app; other ventures use the shared app.
    if item.venture_key == _FA_MAX_VENTURE:
        return getattr(settings, 'fa_max_slack_bot_token', None)
    return settings.slack_bot_token


def _resolve_bot_token_for_venture(venture_key: str, settings):
    """Bot token for a venture without needing a QueueItem (WP-T2-12 channel path)."""
    if venture_key == _FA_MAX_VENTURE:
        return getattr(settings, "fa_max_slack_bot_token", None)
    return settings.slack_bot_token


def post_note(*, venture_key: str, channel: str, thread_ts: str, text: str) -> None:
    """Post a plain-text reply into `channel`, threaded under `thread_ts`.

    The general in-channel poster used by the WP-T2-12 FA Max Slack responder.
    Works with or without an originating card — the caller supplies the channel
    and the ts to thread under. Best-effort: logs and returns on any error.
    """
    settings = get_settings()
    token = _resolve_bot_token_for_venture(venture_key, settings)
    if not token or not channel or not thread_ts:
        logger.warning(
            "[slack_post.post_note] missing token/channel/ts (venture=%s channel=%s)",
            venture_key, channel,
        )
        return
    try:
        from slack_sdk import WebClient
        WebClient(token=token.get_secret_value()).chat_postMessage(
            channel=channel, thread_ts=thread_ts, text=text,
        )
    except Exception as exc:
        logger.error("[slack_post.post_note] post failed (channel=%s): %s", channel, exc)


def fetch_thread_history(
    *,
    venture_key: str,
    channel: str,
    thread_ts: str,
    exclude_ts: str = "",
    limit: int = 10,
) -> list[dict]:
    """Return prior turns of a Slack thread as classify message history.

    Maps our bot's messages to role 'assistant' and human messages to 'user',
    oldest first, excluding `exclude_ts` (the message being classified now) and
    Slack join/system subtypes. Capped to the last `limit` turns to bound tokens.

    Best-effort: returns [] on any error or when Slack isn't configured, so a
    history fetch failure degrades to single-shot classification, never an error.
    """
    settings = get_settings()
    token = _resolve_bot_token_for_venture(venture_key, settings)
    if not token or not channel or not thread_ts:
        return []
    try:
        from slack_sdk import WebClient
        resp = WebClient(token=token.get_secret_value()).conversations_replies(
            channel=channel, ts=thread_ts, limit=limit + 10,
        )
        turns: list[dict] = []
        for msg in resp.get("messages", []):
            if msg.get("ts") == exclude_ts or msg.get("subtype"):
                continue
            content = str(msg.get("text") or "").strip()
            if not content:
                continue
            role = "assistant" if msg.get("bot_id") else "user"
            turns.append({"role": role, "content": content})
        return turns[-limit:]
    except Exception as exc:
        logger.warning(
            "[slack_post.fetch_thread_history] failed (channel=%s): %s", channel, exc
        )
        return []


def post_thread_note(item: QueueItem, text: str) -> None:
    """Post a plain-text reply inside the item's existing card thread.

    Thin wrapper over post_note for the card-thread path — resolves the channel
    and venture from the QueueItem. Best-effort — logs and returns on any error.
    """
    settings = get_settings()
    channel = _resolve_channel(item, settings)
    if not channel or not item.slack_message_ts:
        logger.warning(
            "[slack_post.post_thread_note] missing channel/ts for item %s", item.id
        )
        return
    post_note(
        venture_key=item.venture_key,
        channel=channel,
        thread_ts=item.slack_message_ts,
        text=text,
    )


def post_for_approval(item: QueueItem) -> None:
    """Post an interactive Approve/Reject Slack message for a pending item.

    For FA Max items, routes to the lane-specific channel (MONEY /
    EXCEPTIONS / RELATIONSHIPS) from settings. Other ventures use the
    venture's relay_slack_channel as before (CLONE-v2.2 / CL3).

    No-ops (logs and returns) if Slack isn't configured — this keeps --seed
    usable in local/dev environments without a live Slack app.
    """
    settings = get_settings()
    if item.status != "pending" or item.slack_message_ts:
        return
    token = _resolve_bot_token(item, settings)
    channel = _resolve_channel(item, settings)
    if not token or not channel:
        logger.info(
            "[Relay] Slack not configured for venture %s lane %s (no slack channel or "
            "bot token) — item %d stays pending without a posted message",
            item.venture_key, item.lane, item.id,
        )
        return

    lease_until = None
    if item.venture_key == _FA_MAX_VENTURE:
        lease_until = queue.claim_slack_post(item.id)
        if lease_until is None:
            return

    try:
        from slack_sdk import WebClient
        client = WebClient(token=token.get_secret_value())
        if lease_until is not None:
            # Reconcile the uncertain window where Slack accepted the card but
            # the process died before the database saved its timestamp.
            cursor = None
            while True:
                page = client.conversations_history(
                    channel=channel, oldest=str(item.created_at.timestamp() - 1),
                    limit=200, **({"cursor": cursor} if cursor else {}),
                )
                for message in page.get("messages", []):
                    if ("Relay approval needed" in message.get("text", "")
                            and f"(#{item.id})" in message.get("text", "")
                            and f"Ref: `{_card_ref(item)}`" in message.get("text", "")):
                        queue.set_slack_message_ts(
                            item.id, message["ts"], lease_until=lease_until,
                        )
                        logger.info("[Relay] reconciled existing Slack card for item %d", item.id)
                        return
                cursor = page.get("response_metadata", {}).get("next_cursor")
                if not cursor:
                    break
        response = client.chat_postMessage(
            channel=channel,
            text=_summary_text(item),
            blocks=_build_approval_blocks(item),
        )
        queue.set_slack_message_ts(item.id, response["ts"], lease_until=lease_until)
    except Exception as exc:
        logger.error("[Relay] Slack post failed for item %d: %s", item.id, exc, exc_info=True)
    finally:
        if lease_until is not None:
            queue.release_slack_post(item.id, lease_until)


def _build_approval_blocks(item: QueueItem) -> list:
    """Shared block layout for the interactive approval card — used both by
    post_for_approval() (first post) and refresh_card_after_revision()
    (WP-T2-2 review fix, below). approve_value/reject_value always carry
    THIS item's CURRENT revision_count, so a card built by this function
    always matches the stale-card guard in
    src.api.admin_router._handle_relay_decision as of the moment it's built.
    """
    approve_value = json.dumps({
        "item_id": item.id, "action": "approve", "revision_count_at_post": item.revision_count,
    })
    reject_value = json.dumps({"item_id": item.id, "action": "reject"})
    skip_value = json.dumps({"item_id": item.id, "action": "skip"})
    snooze_value = json.dumps({"item_id": item.id, "action": "snooze"})
    revise_value = json.dumps({"item_id": item.id, "action": "revise"})
    return [
        {"type": "section", "text": {"type": "mrkdwn", "text": _summary_text(item)}},
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "Approve"},
                    "style": "primary",
                    "action_id": "approve",
                    "value": approve_value,
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "Reject"},
                    "style": "danger",
                    "action_id": "reject",
                    "value": reject_value,
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "Skip"},
                    "action_id": "fa_max_skip",
                    "value": skip_value,
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "Snooze"},
                    "action_id": "fa_max_snooze",
                    "value": snooze_value,
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "Revise"},
                    "action_id": "fa_max_revise",
                    "value": revise_value,
                },
            ],
        },
    ]


def refresh_card_after_revision(item: QueueItem) -> bool:
    """Rebuild the posted card's blocks after a Revise submission (WP-T2-2
    review fix) so the Approve/Reject/Skip/Snooze/Revise buttons carry the
    item's NEW revision_count and the visible text reflects the revised
    content -- not the pre-revision card.

    Before this existed, a Revise only posted a thread reply under the
    original card; the card's OWN Approve button kept the revision_count
    baked in at first-post time (always 0), so any revision at all made the
    stale-card guard in admin_router._handle_relay_decision refuse the
    original button FOREVER -- there was no way to approve a revised item
    through the card again. Updating the card in place (chat.update on the
    same message ts) is the fix: it is the SAME mechanism
    _update_relay_slack_message already uses for terminal-state edits, just
    reused here for a non-terminal (still-pending) content refresh.

    Returns True if the update was sent (or Slack isn't configured, which
    is a legitimate no-op, not a failure), False only on an actual Slack
    API error. Never raises -- called from the Revise submission handler,
    which must not fail the revision itself over a Slack hiccup; the
    revision is already durably saved by the time this runs.
    """
    if not item.slack_message_ts:
        return True

    settings = get_settings()
    token = _resolve_bot_token(item, settings)
    channel = _resolve_channel(item, settings)
    if not token or not channel:
        return True

    try:
        from slack_sdk import WebClient
        client = WebClient(token=token.get_secret_value())
        client.chat_update(
            channel=channel,
            ts=item.slack_message_ts,
            text=_summary_text(item),
            blocks=_build_approval_blocks(item),
        )
        return True
    except Exception as exc:
        logger.error(
            "[Relay] refresh_card_after_revision chat.update failed for item %d: %s",
            item.id, exc, exc_info=True,
        )
        return False


def _build_revise_modal(item: QueueItem) -> dict:
    """Modal for the Slack Revise button (WP-T2-2 item 9).

    No existing free-text-capture Slack primitive was found in this repo
    (batch_slack.open_draft_modal is read-only — it has no input block and
    no view_submission handler). This is a new, minimal one: a single
    multiline text input pre-filled with the current content
    (final_content if this item has already been revised once, else
    original_draft), submitted back as a view_submission carrying item_id
    in private_metadata.
    """
    prefill = (item.final_content or item.original_draft or "")[:3000]
    return {
        "type": "modal",
        "callback_id": "fa_max_revise_submit",
        "private_metadata": json.dumps({"item_id": item.id}),
        "title": {"type": "plain_text", "text": f"Revise #{item.id}"[:24]},
        "submit": {"type": "plain_text", "text": "Save revision"},
        "close": {"type": "plain_text", "text": "Cancel"},
        "blocks": [
            {
                "type": "input",
                "block_id": "revised_content_block",
                "label": {"type": "plain_text", "text": "Revised content"},
                "element": {
                    "type": "plain_text_input",
                    "action_id": "revised_content",
                    "multiline": True,
                    "initial_value": prefill,
                },
            },
        ],
    }


def open_revise_modal(trigger_id: str, item: QueueItem) -> bool:
    """Open the Revise modal for a pending item. Returns True on success."""
    settings = get_settings()
    token = _resolve_bot_token(item, settings)
    if not token or not trigger_id:
        logger.info("[Relay] cannot open revise modal for item %d — no token or trigger_id", item.id)
        return False
    try:
        from slack_sdk import WebClient
        WebClient(token=token.get_secret_value()).views_open(
            trigger_id=trigger_id, view=_build_revise_modal(item),
        )
        return True
    except Exception as exc:
        logger.error("[Relay] views.open failed for revise item %d: %s", item.id, exc, exc_info=True)
        return False


def post_unposted_fa_max_cards(*, limit: int = 50) -> int:
    """Retry committed, pending FA Max cards after Slack or process failure."""
    items = queue.unposted_fa_max_items(limit=limit)
    for item in items:
        post_for_approval(item)
    return len(items)


def post_completion_receipt(
    batch_id: str,
    result: "BatchResult",
    venture_key: str = DEFAULT_VENTURE_KEY,
) -> None:
    """Post a send-completion receipt to the venture's relay Slack channel.

    Called by the cron sweep after execute_batch() returns so Josh can see
    in #agent-daily that an approved batch actually went out (build spec §1.1
    Phase 1 DoD: "first receipted sends").

    No-ops if Slack isn't configured — mirrors post_for_approval's behaviour.
    Never raises: a failed receipt must not mark the batch as failed.
    """
    settings = get_settings()
    token = (settings.fa_max_slack_bot_token if venture_key == _FA_MAX_VENTURE
             else settings.slack_bot_token)
    channel = get_venture_config(venture_key).relay_slack_channel
    if not token or not channel:
        logger.info(
            "[Relay] Slack not configured for venture %s — skipping completion receipt for %s",
            venture_key, batch_id,
        )
        return

    if result.halted:
        icon = "🛑"
        suffix = " — halted by kill switch, remaining items queued for next sweep"
    elif result.failed:
        icon = "⚠️"
        suffix = " — check logs"
    else:
        icon = "✅"
        suffix = ""

    text = (
        f"{icon} Relay `{batch_id}` · "
        f"{result.sent} sent · {result.failed} failed · {result.deferred} deferred"
        f"{suffix}"
    )

    try:
        from slack_sdk import WebClient
        client = WebClient(token=token.get_secret_value())
        client.chat_postMessage(channel=channel, text=text)
        logger.info("[Relay] completion receipt posted for %s", batch_id)
    except Exception as exc:
        logger.error(
            "[Relay] completion receipt post failed for %s: %s",
            batch_id, exc, exc_info=True,
        )


def post_blocked_action(item: QueueItem, reason: str) -> None:
    """Surface a send-layer refusal in the owning FA Max Slack lane.

    This is deliberately called after the durable queue row has been marked
    skipped.  A Slack outage therefore cannot turn a prohibited send back
    into an executable one.
    """
    settings = get_settings()
    token = _resolve_bot_token(item, settings)
    channel = _resolve_channel(item, settings)
    if not token or not channel:
        logger.warning("[Relay] blocked item %d (%s); Slack not configured", item.id, reason)
        return
    message = f"🛑 *Relay send blocked* (#{item.id})\nTo: `{item.recipient}`\nReason: `{reason}`"
    try:
        from slack_sdk import WebClient
        WebClient(token=token.get_secret_value()).chat_postMessage(channel=channel, text=message)
    except Exception as exc:
        logger.error("[Relay] blocked-action post failed for item %d: %s", item.id, exc, exc_info=True)


def post_exceptions_alert(*, venture_key: str, rule: str, message: str) -> bool:
    """Post an operational (non-item) alert to the FA Max EXCEPTIONS lane.

    Used for send-infrastructure health (WP-T2-1: "Alert in EXCEPTIONS queue
    when reputation falls below threshold") and for a failed suppression
    sync — these are not `RelayApprovalQueueItem` decisions, so they don't
    go through post_for_approval/post_blocked_action. Returns True if the
    alert was actually posted (Slack configured and the call succeeded),
    False otherwise, so a caller with durable dedup (e.g. ScraperAlertLog)
    can decide whether to record the alert as delivered.
    """
    settings = get_settings()
    token = settings.slack_bot_token
    channel = getattr(settings, "fa_max_slack_channel_exceptions", "") or get_venture_config(venture_key).relay_slack_channel
    if not token or not channel:
        logger.warning("[Relay][EXCEPTIONS] %s (venture=%s): Slack not configured — %s", rule, venture_key, message)
        return False
    try:
        from slack_sdk import WebClient
        WebClient(token=token.get_secret_value()).chat_postMessage(
            channel=channel, text=f":rotating_light: *{rule}* (venture={venture_key})\n{message}",
        )
        return True
    except Exception:
        logger.exception("[Relay][EXCEPTIONS] alert post failed for rule %s (venture=%s)", rule, venture_key)
        return False


def post_uncertain_action(item: QueueItem) -> None:
    """Ask an operator to reconcile an ambiguous provider result; never retry it."""
    settings = get_settings()
    token = _resolve_bot_token(item, settings)
    channel = _resolve_channel(item, settings)
    if not token or not channel:
        logger.error("[Relay] uncertain provider result for item %d; Slack unavailable", item.id)
        return
    try:
        from slack_sdk import WebClient
        WebClient(token=token.get_secret_value()).chat_postMessage(
            channel=channel,
            text=(f":warning: *Relay send outcome uncertain* (#{item.id}). "
                  "The provider may have accepted it. Check the provider before any retry."),
        )
    except Exception:
        logger.exception("[Relay] uncertain-result alert failed for item %d", item.id)
