"""Cora, the Next Deal Lending Slack operator: process entry point (systemd ``fa-lending-cora``).

Builds the ``packages.agent_core`` pieces from this app's settings and runs the Socket Mode
session. Each message is answered by the native Messages API loop (``agent_core.agent_loop``)
with the thread so far as history, the standing rules from ``lending.agent_memory`` in the
system prompt, and Cora's lending tools. Draft tools only queue approval cards. A sweep expires
stale drafts and sends approved actions that were not sent at click time; every send first
passes the lending send-time check (suppressed / do-not-contact / text consent).

Usage:
    PYTHONPATH=. python -m src.lending.cora_agent
"""
from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from datetime import timedelta
from typing import Any, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from config.settings import AppSettings, get_settings
from packages.agent_core.agent_loop import AgentLoop, MessagesClient
from packages.agent_core.chatport import ChatPort, SlackChatPort
from packages.agent_core.config import AgentCoreConfig, parse_user_ids
from packages.agent_core.halt import AgentHalted, HaltSwitch
from packages.agent_core.memory import AgentMemory
from packages.agent_core.pending_actions import PendingAction, PendingActionQueue
from packages.agent_core.relay import EgressExecutor, Relay, SendCheck
from packages.agent_core.revision import DraftReviser, RevisionFailed
from packages.agent_core.send_gate import SendGate
from packages.agent_core.slack_session import InboundMessage, SlackSession, run_socket_mode
from packages.agent_core.store import AgentStore
from packages.agent_core.thread_history import build_history
from packages.agent_core.tools import ToolContext, ToolRegistry
from src.lending.consent import has_text_consent
from src.lending.cora_prompt import CORA_BASE_PROMPT, REVISION_PROMPT
from src.lending.cora_tools import CoraToolkit
from src.lending.models import LENDING_SCHEMA
from src.services.phone_utils import normalize

logger = logging.getLogger(__name__)

AGENT_NAME = "Cora"
SWEEP_SECONDS = 60
HISTORY_FETCH_LIMIT = 40  # Slack messages fetched; build_history keeps the latest 20 turns of them
PLACEHOLDER_TEXT = "Working on it…"

# Egress channels Cora may send through once approved. Empty: drafts are approvable, but an approved
# draft is marked failed instead of sent until the GHL senders are registered after copy/consent review.
EGRESS_EXECUTORS: dict[str, EgressExecutor] = {}
# Channels that deliver a text message and therefore need live text consent.
SMS_CHANNELS = frozenset({"ghl_sms"})
# What a revision may change, per draft channel. Recipient fields are never editable.
EDITABLE_FIELDS: dict[str, tuple[str, ...]] = {"ghl_sms": ("body",), "ghl_email": ("subject", "body"),
                                               "ghl_stage": ("stage",)}

# Same suppression rule the booking reminder worker applies before every send.
_SUPPRESSED = text("""
    SELECT EXISTS (SELECT 1 FROM lending.suppression_list WHERE (phone = :p AND :p IS NOT NULL)
                                                              OR (email = :e AND :e IS NOT NULL))
        OR EXISTS (SELECT 1 FROM lending.contacts WHERE phone = :p AND :p IS NOT NULL AND do_not_contact)
""")

_MODEL_UNAVAILABLE = "I couldn't reach my language model just now. Nothing was sent; please try again in a minute."


def _secret(value) -> Optional[str]:
    return value.get_secret_value() if value else None


def build_config(settings: AppSettings) -> AgentCoreConfig:
    return AgentCoreConfig(
        agent_name=AGENT_NAME,
        slack_bot_token=_secret(settings.lending_cora_slack_bot_token),
        slack_app_token=_secret(settings.lending_cora_slack_app_token),
        slack_channel_id=settings.lending_cora_channel or None,
        approver_user_ids=parse_user_ids(settings.lending_cora_approver_user_ids),
        operator_user_ids=parse_user_ids(settings.lending_cora_operator_user_ids),
        db_schema=LENDING_SCHEMA,
        model=settings.lending_cora_model,
        anthropic_api_key=_secret(getattr(settings, "anthropic_api_key", None)),
        calendar_service_account_key_path=settings.lending_cora_calendar_service_account_key_path or None,
        calendar_id=settings.lending_cora_calendar_id or None,
        pending_action_ttl_hours=settings.lending_cora_pending_action_ttl_hours,
    )


def make_send_check(engine: Engine) -> SendCheck:
    """Lending rule run by the relay right before a send. Returns a reason to block, or None."""

    def check(action: PendingAction) -> Optional[str]:
        phone = normalize(action.recipient_phone) if action.recipient_phone else None
        email = (action.recipient_email or "").strip().lower() or None
        if not phone and not email:
            return "no recipient recorded, so opt-out status cannot be verified"
        with engine.connect() as conn:
            if conn.execute(_SUPPRESSED, {"p": phone, "e": email}).scalar():
                return "recipient is suppressed or marked do-not-contact"
            if action.channel in SMS_CHANNELS and not (phone and has_text_consent(conn, phone)):
                return "no text consent on record for this number"
        return None

    return check


def _is_model_error(error: Exception) -> bool:
    """Anthropic SDK errors (rate limit, overload, network). Imported lazily so tests need no SDK client."""
    try:
        import anthropic
    except ImportError:
        return False
    return isinstance(error, anthropic.APIError)


class CoraConversation:
    """Turns one Slack message (or revise reply) into an answer in the same thread."""

    def __init__(self, *, config: AgentCoreConfig, chat: ChatPort, loop: AgentLoop, reviser: DraftReviser,
                 bot_user_id: str) -> None:
        self._config = config
        self._chat = chat
        self._loop = loop
        self._reviser = reviser
        self._bot_user_id = bot_user_id

    def _context(self, message: InboundMessage, tool_use_id: str) -> ToolContext:
        return ToolContext(user_id=message.user_id, is_approver=message.user_id in self._config.approver_user_ids,
                           channel=message.channel, thread_ts=message.conversation_ts, tool_use_id=tool_use_id)

    def _conversation_so_far(self, message: InboundMessage) -> list[dict[str, Any]]:
        """Inside a thread: that thread. Top level: the channel's recent messages, since replies are inline."""
        if message.thread_ts:
            recent = self._chat.replies(channel=message.channel, thread_ts=message.thread_ts, limit=HISTORY_FETCH_LIMIT)
        else:
            recent = self._chat.history(channel=message.channel, limit=HISTORY_FETCH_LIMIT)
        return build_history(recent, bot_user_id=self._bot_user_id, before_ts=message.ts,
                             skip_texts=frozenset({PLACEHOLDER_TEXT}))

    def _finish(self, message: InboundMessage, placeholder_ts: Optional[str], answer: str) -> None:
        if placeholder_ts:
            updated = self._chat.update(channel=message.channel, ts=placeholder_ts, text=answer)
            if updated.ok:
                return
        self._chat.post(text=answer, channel=message.channel, thread_ts=message.reply_thread_ts)

    def on_message(self, message: InboundMessage) -> None:
        if not message.text.strip():
            self._chat.post(text="How can I help?", channel=message.channel, thread_ts=message.reply_thread_ts)
            return
        placeholder = self._chat.post(text=PLACEHOLDER_TEXT, channel=message.channel,
                                      thread_ts=message.reply_thread_ts)
        history = self._conversation_so_far(message)
        try:
            reply = self._loop.run(history=history, user_text=f"<@{message.user_id}>: {message.text}",
                                   context_for=lambda tool_use_id: self._context(message, tool_use_id))
            answer = reply.text
            logger.info("[cora] answered ts=%s rounds=%s tools=%s queued=%s stop=%s", message.ts, reply.rounds,
                        list(reply.tool_calls), list(reply.queued_action_ids), reply.stop_reason)
        except Exception as error:
            if not _is_model_error(error):
                raise
            logger.error("[cora] model call failed for ts=%s (%s)", message.ts, type(error).__name__)
            answer = _MODEL_UNAVAILABLE
        self._finish(message, placeholder.ts if placeholder.ok else None, answer)

    def on_revision(self, action: PendingAction, message: InboundMessage) -> None:
        try:
            self._reviser.revise(action, message.text, message.user_id)
            answer = f"Revised action #{action.action_id}. A fresh approval card is posted; nothing has been sent."
        except RevisionFailed as error:
            answer = (f"I couldn't revise action #{action.action_id} ({error}). Reply with another instruction, "
                      "or say `cancel` to keep the draft as it is.")
        except Exception as error:
            if not _is_model_error(error):
                raise
            logger.error("[cora] revision model call failed for action %s (%s)", action.action_id,
                         type(error).__name__)
            answer = _MODEL_UNAVAILABLE
        self._chat.post(text=answer, channel=message.channel, thread_ts=message.reply_thread_ts)


def build_session(config: AgentCoreConfig, store: AgentStore, chat: ChatPort, bot_user_id: str, *,
                  messages_client: MessagesClient, send_check: Optional[SendCheck] = None,
                  toolkit_factory: Callable[[Engine, AgentMemory], Any] = CoraToolkit) -> tuple[SlackSession, Relay]:
    """Wire Cora. ``toolkit_factory(engine, memory)`` builds the tool set; tests swap the lead lookups."""
    queue = PendingActionQueue(store, default_ttl=timedelta(hours=config.pending_action_ttl_hours))
    halt = HaltSwitch(store, config.agent_name)
    relay = Relay(queue, halt, EGRESS_EXECUTORS, send_check=send_check)
    memory = AgentMemory(store)
    gate = SendGate(queue, chat, config.slack_channel_id)
    toolkit = toolkit_factory(store.engine, memory)

    def rules() -> list[str]:
        return [rule.rule_text for rule in memory.active_rules()]

    loop = AgentLoop(client=messages_client, model=config.model, registry=ToolRegistry(toolkit.tools(), gate),
                     base_prompt=CORA_BASE_PROMPT, rules_provider=rules)
    reviser = DraftReviser(client=messages_client, model=config.model, queue=queue, gate=gate,
                           base_prompt=REVISION_PROMPT, rules_provider=rules, editable_fields=EDITABLE_FIELDS)
    conversation = CoraConversation(config=config, chat=chat, loop=loop, reviser=reviser, bot_user_id=bot_user_id)
    session = SlackSession(config=config, chat=chat, queue=queue, halt=halt, relay=relay,
                           on_message=conversation.on_message, on_revision=conversation.on_revision,
                           bot_user_id=bot_user_id)
    return session, relay


def run_sweep(session: SlackSession, relay: Relay) -> None:
    """One pass: expire stale drafts (always, even while halted), then send approved actions."""
    expired = session.expire_stale_actions()
    if expired:
        logger.info("[cora] expired %s stale draft(s)", expired)
    try:
        result = relay.run()
    except AgentHalted:
        return
    if result.sent or result.failed or result.blocked:
        logger.info("[cora] relay sweep sent=%s failed=%s blocked=%s", result.sent, result.failed, result.blocked)


def _sweep_forever(session: SlackSession, relay: Relay, stop: threading.Event) -> None:
    while not stop.wait(SWEEP_SECONDS):
        try:
            run_sweep(session, relay)
        except Exception as exc:
            logger.error("[cora] sweep failed (%s); retrying next interval", type(exc).__name__)


def main() -> int:
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = get_settings()
    config = build_config(settings)
    missing = config.missing_for_live_session()
    if not config.anthropic_api_key:
        missing.append("anthropic_api_key")
    if missing:
        # Fail closed: without approvers no click could ever be trusted to release a send.
        logger.error("[cora] not starting; missing settings: %s", ", ".join(missing))
        return 1

    import anthropic
    from slack_sdk.web import WebClient

    web = WebClient(token=config.slack_bot_token)
    try:
        bot_user_id = web.auth_test()["user_id"]
    except Exception as exc:
        logger.error("[cora] Slack auth.test failed (%s); check LENDING_CORA_SLACK_BOT_TOKEN", type(exc).__name__)
        return 1

    engine = create_engine(settings.database_url, pool_pre_ping=True)
    store = AgentStore(engine, config.db_schema)
    messages_client = anthropic.Anthropic(api_key=config.anthropic_api_key, timeout=90.0, max_retries=2).messages
    session, relay = build_session(config, store, SlackChatPort(web, config.slack_channel_id), bot_user_id,
                                   messages_client=messages_client, send_check=make_send_check(engine))
    stop = threading.Event()
    threading.Thread(target=_sweep_forever, args=(session, relay, stop), name="cora-sweep", daemon=True).start()
    logger.info("[cora] starting Socket Mode session as %s (model %s)", bot_user_id, config.model)
    run_socket_mode(session, app_token=config.slack_app_token, web_client=web)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
