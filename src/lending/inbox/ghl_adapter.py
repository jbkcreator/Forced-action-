"""R1 1.5: the GHL adapter. Every inbound GHL text and email becomes one ``lending.inbox_events`` row, and every
outbound reply-agent / person message in GHL marks the open texts of that contact as answered.

Called from the GHL reply webhook (``reply_webhook.handle_ghl_reply`` and its non-journal branch) in the caller's
transaction, so the item, the opt-out of a stop request and the handoff post stand or fall (and are retried) together.
Both steps are idempotent, keyed on the GHL message id.

A keyword STOP text (the whole message is a stop word) is left to GHL's own do-not-disturb and the opt-out webhook.
Any other stop wording, in a text or an email, is opted out here through ``compliance.propagate_opt_out`` (GHL sets no
do-not-disturb for "remove me from your list"): the texting phone for a text, the address for an email. The reply
agent's own "we won't text you again" message opts that phone out too. Logs carry ids only, never message text.
"""
from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any, Optional

from sqlalchemy import text

from config.lending_compliance import OptOutChannel
from config.lending_unified_inbox import (
    CHANNEL_GHL_EMAIL,
    CHANNEL_GHL_OTHER,
    CHANNEL_GHL_SMS,
    GHL_EMAIL_TYPES,
    GHL_MESSAGE_TYPE_PATHS,
    GHL_SMS_TYPES,
    REPLY_AGENT_HANDOFF_MESSAGES,
    REPLY_AGENT_OPT_OUT_MESSAGES,
    SOURCE_GHL,
)
from config.settings import get_settings
from src.lending.compliance import propagate_opt_out
from src.lending.inbox.unified_triage import InboundItem, record_inbound, stop_opt_out
from src.lending.instantly_replies import is_stop_line, is_stop_request
from src.lending.reply_guard import ReplyEvent

logger = logging.getLogger(__name__)

_MARK_ANSWERED = text("""
    UPDATE lending.inbox_events
       SET answered_by = CASE WHEN :person OR answered_by = 'person' THEN 'person' ELSE 'reply_agent' END,
           ai_handoff = ai_handoff OR :handoff,
           answered_at = COALESCE(answered_at, CURRENT_TIMESTAMP)
     WHERE status = 'new' AND channel IN ('ghl_sms', 'ghl_other') AND ghl_contact_id = :contact_id
""")


def _enabled() -> bool:
    return bool(get_settings().lending_unified_inbox_enabled)


def _dig(raw: Mapping[str, Any], path: tuple[str, ...]) -> Optional[str]:
    node: Any = raw
    for key in path:
        node = node.get(key) if isinstance(node, Mapping) else None
    return str(node).strip() if isinstance(node, (str, int)) and str(node).strip() else None


def message_channel(raw: Mapping[str, Any]) -> str:
    """text / email / other, from the first message-type value GHL sent; no type at all counts as a text."""
    kind = next((value for value in (_dig(raw, path) for path in GHL_MESSAGE_TYPE_PATHS) if value), "").lower()
    if kind in GHL_EMAIL_TYPES:
        return CHANNEL_GHL_EMAIL
    return CHANNEL_GHL_SMS if kind in GHL_SMS_TYPES else CHANNEL_GHL_OTHER


def ghl_message_item(event: ReplyEvent, raw: Mapping[str, Any]) -> Optional[InboundItem]:
    """The inbox item for an inbound GHL message, or None when there is nothing to record: an outbound message, a
    keyword STOP text (GHL owns it), or a message with no phone and no contact id."""
    if event.direction != "inbound" or not (event.phone or event.contact_id):
        return None
    channel = message_channel(raw)
    email = _dig(raw, ("email",)) or _dig(raw, ("contact", "email"))
    stop = is_stop_request(event.body)
    if stop and channel == CHANNEL_GHL_SMS and is_stop_line(event.body.strip()):
        return None
    if stop and not (email or event.phone):
        stop = False  # nobody to opt out: let the sweep triage it and ask a person
    return InboundItem(
        source=SOURCE_GHL, channel=channel, dedup_key=f"ghl_msg:{event.message_id}", ghl_contact_id=event.contact_id,
        phone=event.phone, email=email, first_name=event.first_name,
        subject=_dig(raw, ("subject",)) or _dig(raw, ("message", "subject")), body=event.body, stop=stop)


def enqueue_ghl_message(db, event: ReplyEvent, raw: Mapping[str, Any]) -> Optional[int]:
    """Record the inbound message once (and opt a stop request out, once). Does not commit; raises on failure so the
    caller's transaction rolls back and GHL / the journal redelivers."""
    if not _enabled():
        return None
    item = ghl_message_item(event, raw)
    if item is None:
        return None
    with db.begin_nested():
        item_id = record_inbound(db, item)
        if item_id is not None and item.stop:
            event_id = propagate_opt_out(db, source_ref=f"inbox:{item.dedup_key}", actor="ghl_reply",
                                         **stop_opt_out(item.channel, item.phone, item.email))
            logger.info("[unified-inbox] stop message %s opted out (opt-out event %s)", event.message_id, event_id)
    return item_id


def _normalized(body: str) -> str:
    return " ".join(body.lower().replace("’", "'").split())


def is_reply_agent_handoff(body: str) -> bool:
    return _normalized(body) in REPLY_AGENT_HANDOFF_MESSAGES


def is_reply_agent_opt_out(body: str) -> bool:
    return _normalized(body) in REPLY_AGENT_OPT_OUT_MESSAGES


def mark_answered(db, event: ReplyEvent) -> int:
    """An outbound GHL message (the reply agent's, or a person's when ``userId`` is set) answers the contact's open
    texts: a person wins, a reply-agent handoff message is remembered, a plain agent answer never clears either.
    Returns the rows touched; does not commit."""
    if event.direction != "outbound" or not event.contact_id or not _enabled():
        return 0
    if not event.from_user and event.phone and is_reply_agent_opt_out(event.body):
        # The agent told the borrower "we won't text you again": keep that promise (idempotent per message id).
        propagate_opt_out(db, phone=event.phone, source_ref=f"inbox:ghl_msg:{event.message_id}",
                          actor="reply_agent", channel=OptOutChannel.SMS)
        logger.info("[unified-inbox] reply agent opt-out message %s: phone opted out", event.message_id)
    return db.execute(_MARK_ANSWERED, {
        "contact_id": event.contact_id, "person": event.from_user,
        "handoff": (not event.from_user) and is_reply_agent_handoff(event.body)}).rowcount


def record_ghl_event(db, event: ReplyEvent, raw: Mapping[str, Any]) -> None:
    """The one call the reply webhook makes for every GHL message, whichever way it points."""
    if event.direction == "outbound":
        mark_answered(db, event)
    else:
        enqueue_ghl_message(db, event, raw)
