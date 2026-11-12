"""Cora, the Next Deal Lending Slack operator: process entry point (systemd ``fa-lending-cora``).

Cora has her own Slack app and connects over Socket Mode (an outbound WebSocket, so no public
endpoint). Messages and Approve / Revise / Reject clicks arrive on that connection. Each message
is answered by the native Messages API loop (``agent_core.agent_loop``) with the conversation so
far as history, the standing rules from ``lending.agent_memory`` in the system prompt, and Cora's
lending tools. Draft tools only queue approval cards.

Alongside the session, a sweep runs once a minute: it expires stale drafts, triages new Instantly
replies (P1.3: classify, draft an answer card, post the triage), re-posts approval cards Slack
refused, and sends approved actions that were not sent at click time. Every send first
passes the lending send-time check (suppressed / do-not-contact / text consent). Run exactly one
instance.

Usage:
    PYTHONPATH=. python -m src.lending.cora_agent
"""
from __future__ import annotations

import logging
import signal
import threading
from collections.abc import Callable, Mapping
from datetime import datetime, timedelta, timezone
from typing import Any, NamedTuple, Optional
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from config.lending_agent_jobs import COLD_EMAIL_CHANNEL
from config.lending_compliance import CALL_WINDOW_END, CALL_WINDOW_START, DEFAULT_TZ
from config.settings import AppSettings, get_settings
from packages.agent_core.agent_loop import AgentLoop, MessagesClient
from packages.agent_core.chatport import ChatPort, SlackChatPort
from packages.agent_core.config import AgentCoreConfig, parse_user_ids
from packages.agent_core.halt import AgentHalted, HaltSwitch
from packages.agent_core.memory import AgentMemory
from packages.agent_core.pending_actions import PendingAction, PendingActionQueue
from packages.agent_core.relay import EgressExecutor, Relay, SendCheck, SendDeferred
from packages.agent_core.revision import DraftReviser, RevisionFailed
from packages.agent_core.send_gate import SendGate
from packages.agent_core.slack_session import InboundMessage, SlackSession, run_socket_mode
from packages.agent_core.store import AgentStore
from packages.agent_core.thread_history import build_history
from packages.agent_core.tools import ToolContext, ToolRegistry
from src.lending.agent_jobs.drive import google_job_folders
from src.lending.agent_jobs.worker import JobQueueWorker, make_check
from src.lending.attempt_log import lending_ghl_readers
from src.lending.compliance import text_window_open
from src.lending.consent import has_text_consent
from src.lending.cora_channels import ChannelDirectory
from src.lending.cora_inbox import format_inbox_context, items_on_post, surface_new_items
from src.lending.cora_prompt import CORA_BASE_PROMPT, REVISION_PROMPT
from src.lending.cora_tools import CoraToolkit, draft_content_check
from src.lending.global_halt import EmergencyHalt
from src.lending.ghl_crm import GhlCrmWriter, get_crm_writer
from src.lending.ghl_email import GhlEmailSender, get_email_sender
from src.lending.ghl_sms import GhlSmsSender, get_sender
from src.lending.instantly_client import InstantlyAccount, block_address, lending_instantly_account, send_reply
from src.lending.instantly_email import NdlInstantlySender, get_ndl_instantly_sender
from src.lending.instantly_replies import CHANNEL as INSTANTLY_CHANNEL, ReplyProcessor
from src.lending.standing_sends import (
    StandingSendsStep,
    auto_approval_lapsed,
    make_edit_recorder,
    make_is_graduated,
)
from src.lending.ops_alerts import post_ops_notice, post_replies_notice
from src.lending.cora_morning_digest import post_morning_digest
from src.lending.models import LENDING_SCHEMA
from src.lending.routing.slack_extension import RoutingSlack
from src.services.phone_utils import normalize

logger = logging.getLogger(__name__)

AGENT_NAME = "Cora"
SWEEP_SECONDS = 60
HISTORY_FETCH_LIMIT = 40  # Slack messages fetched; build_history keeps the latest 20 turns of them
# A top-level message sees only the channel's recent exchange, not every earlier topic in it.
TOP_LEVEL_HISTORY_SECONDS = 60 * 60
TOP_LEVEL_HISTORY_TURNS = 6
PLACEHOLDER_TEXT = "Working on it…"

# Channels that deliver a text message and therefore need live text consent.
SMS_CHANNELS = frozenset({"ghl_sms"})
# P1.3's thread replies (INSTANTLY_CHANNEL) and P1.1's cold emails (COLD_EMAIL_CHANNEL) both go through NDL's Instantly.
EMAIL_CHANNELS = frozenset({"ghl_email", INSTANTLY_CHANNEL, COLD_EMAIL_CHANNEL})
INSTANTLY_CHANNELS = frozenset({INSTANTLY_CHANNEL, COLD_EMAIL_CHANNEL})
# What a revision may change, per draft channel. Recipient fields are never editable.
EDITABLE_FIELDS: dict[str, tuple[str, ...]] = {"ghl_sms": ("body",), "ghl_email": ("subject", "body"),
                                               "ghl_stage": ("stage",), "ghl_tag": ("tag",), "ghl_note": ("body",),
                                               "ghl_task": ("title",), INSTANTLY_CHANNEL: ("subject", "body"),
                                               COLD_EMAIL_CHANNEL: ("subject", "body")}

# Same suppression rule the booking reminder worker applies before every send.
_SUPPRESSED = text("""
    SELECT EXISTS (SELECT 1 FROM lending.suppression_list WHERE (phone = :p AND :p IS NOT NULL)
                                                              OR (email = :e AND :e IS NOT NULL))
        OR EXISTS (SELECT 1 FROM lending.contacts WHERE phone = :p AND :p IS NOT NULL AND do_not_contact)
""")
# An Instantly answer also honours FA's email opt-outs (an unsubscribe Instantly reported to FA's webhook reaches the
# lending list only on the opt-out poller's next pass).
_EMAIL_OPTED_OUT = text("SELECT EXISTS (SELECT 1 FROM email_opt_outs WHERE lower(email) = :e)")

_MODEL_UNAVAILABLE = "I couldn't reach my language model just now. Nothing was sent; please try again in a minute."


class CoraServices(NamedTuple):
    session: SlackSession
    relay: Relay
    gate: SendGate
    web_client: Any = None  # the Slack WebClient Socket Mode replies through; None in tests
    inbox_digest: Optional[Callable[[], int]] = None  # posts new inbox items to Cora's channel; returns how many
    morning_digest: Optional[Callable[[], bool]] = None  # posts the 7:30 AM ET digest once a day; True when posted
    instantly_replies: Optional[Callable[[], Any]] = None  # P1.3: triages new Instantly replies
    queue: Optional[PendingActionQueue] = None  # the drafts table the session, gate and relay share
    agent_jobs: Optional[Callable[[], Any]] = None  # P1.1: one pass of the Drive job queue worker
    standing_sends: Optional[Callable[[], Any]] = None  # P1.4: graduates templates that meet the rule


def lending_channel_ids(settings: AppSettings) -> dict[str, str]:
    """Inbox channel keys -> Slack channel ids, for naming where an item was already posted."""
    channels = {"replies": settings.lending_replies_channel, "ops": settings.lending_ops_channel,
                "ops_notice": settings.lending_ops_notice_channel, "dnc": settings.lending_dnc_alert_channel,
                "dial_tasks": settings.lending_dial_tasks_channel, "daily": settings.lending_daily_channel,
                "deals": settings.lending_deals_channel}
    return {key: channel_id for key, channel_id in channels.items() if channel_id}


def make_morning_digest(engine: Engine, store: AgentStore, chat: ChatPort, config: AgentCoreConfig) -> Callable[[], bool]:
    """The 7:30 AM digest, posted to Cora's channel; reads the kill switch each time so the digest shows it."""
    halt = HaltSwitch(store, config.agent_name)

    def digest() -> bool:
        return post_morning_digest(engine, chat, config.slack_channel_id, halted=halt.is_halted(),
                                   halt_reason=halt.reason(),
                                   approvals_channel=config.approval_channel_id)

    return digest


def make_inbox_digest(engine: Engine, chat: ChatPort, channel: str,
                      channel_ids: dict[str, str]) -> Callable[[], int]:
    """One sweep's digest in its own transaction: the claimed items stay locked until they are marked surfaced."""

    def digest() -> int:
        with Session(engine) as db, db.begin():
            return surface_new_items(db, chat, channel=channel, channel_ids=channel_ids)

    return digest


def make_inbox_context(engine: Engine) -> Callable[[str, str], str]:
    """The ``<inbox_items>`` block for a reply in a digest thread; empty for any other thread."""

    def lookup(channel: str, thread_ts: str) -> str:
        with Session(engine) as db:
            items = items_on_post(db, channel, thread_ts)
        return format_inbox_context(items) if items else ""

    return lookup


def _secret(value) -> Optional[str]:
    return value.get_secret_value() if value else None


def build_config(settings: AppSettings) -> AgentCoreConfig:
    return AgentCoreConfig(
        agent_name=AGENT_NAME,
        slack_bot_token=_secret(settings.lending_cora_slack_bot_token),
        slack_app_token=_secret(settings.lending_cora_slack_app_token),
        slack_channel_id=settings.lending_cora_channel or None,
        approval_channel_id=settings.lending_cora_approvals_channel or None,
        approver_user_ids=parse_user_ids(settings.lending_cora_approver_user_ids),
        operator_user_ids=parse_user_ids(settings.lending_cora_operator_user_ids),
        db_schema=LENDING_SCHEMA,
        model=settings.lending_cora_model,
        anthropic_api_key=_secret(getattr(settings, "anthropic_api_key", None)),
        calendar_service_account_key_path=settings.lending_cora_calendar_service_account_key_path or None,
        calendar_id=settings.lending_cora_calendar_id or None,
        pending_action_ttl_hours=settings.lending_cora_pending_action_ttl_hours,
    )


def email_window_open(phone: Optional[str], *, now: Optional[datetime] = None) -> bool:
    """8am-8pm for an email's recipient (brief section 7, "Hours", applies to every send): the borrower's own
    window when the phone is known, else 8am-8pm Eastern."""
    if phone:
        return text_window_open(phone, now=now)
    local = (now or datetime.now(timezone.utc)).astimezone(ZoneInfo(DEFAULT_TZ)).time()
    return CALL_WINDOW_START <= local < CALL_WINDOW_END


def make_send_check(engine: Engine, *, carrier_approved: bool = False,
                    text_window: Callable[[str], bool] = text_window_open,
                    email_window: Callable[[Optional[str]], bool] = email_window_open) -> SendCheck:
    """Lending rule run by the relay right before a send. Returns a reason to block, or None.

    A text also waits for the carrier (10DLC) approval (LENDING_SMS_CARRIER_APPROVED) and for the borrower's
    8am-8pm window, and an email (GHL or Instantly) for the recipient's 8am-8pm window: these are deferrals, so an
    approved send stays approved and goes on a later sweep once they hold, or is blocked when the draft expires. An
    action approved automatically (P1.4 graduation) whose template has since been revoked is held too, and the next
    sweep sends it back for a person's approval."""

    def check(action: PendingAction) -> Optional[str]:
        phone = normalize(action.recipient_phone) if action.recipient_phone else None
        email = (action.recipient_email or "").strip().lower() or None
        if not phone and not email:
            return "no recipient recorded, so opt-out status cannot be verified"
        with engine.connect() as conn:
            if conn.execute(_SUPPRESSED, {"p": phone, "e": email}).scalar():
                return "recipient is suppressed or marked do-not-contact"
            if action.channel in INSTANTLY_CHANNELS and conn.execute(_EMAIL_OPTED_OUT, {"e": email}).scalar():
                return "recipient is suppressed or marked do-not-contact"
            if action.channel in SMS_CHANNELS and not (phone and has_text_consent(conn, phone)):
                return "no text consent on record for this number"
            if auto_approval_lapsed(conn, action.decided_by, action.payload.get("template_version")):
                # P1.4: its template was revoked after the automatic approval. Held, never sent; the next sweep
                # sends it back for a person's approval (standing_sends.return_ungraduated_auto_approvals).
                return SendDeferred("its message type no longer auto-sends; going back for approval")
        if action.channel in SMS_CHANNELS:
            if not carrier_approved:
                return SendDeferred("texting is not approved by the carrier yet (10DLC)")
            if not text_window(phone):
                return SendDeferred("outside the borrower's 8am-8pm text window")
        if action.channel in EMAIL_CHANNELS and not email_window(phone):
            return SendDeferred("outside the recipient's 8am-8pm window")
        return None

    return check


def email_executor(sender: GhlEmailSender) -> EgressExecutor:
    """Sends an approved ``draft_email`` payload exactly as approved; returns the GHL message id."""

    def send(payload: Mapping[str, Any]) -> str:
        # The phone keeps GHL on the lead's existing contact; drafts made before it was added carry none.
        return sender(payload["to_email"], payload["subject"], payload["body"], phone=payload.get("to_phone"))

    return send


def sms_executor(sender: GhlSmsSender) -> EgressExecutor:
    """Sends an approved ``draft_sms`` payload exactly as approved; returns the GHL message id. The relay's send
    check runs first (opt-out, text consent, LENDING_SMS_CARRIER_APPROVED, the borrower's 8am-8pm window), and
    the relay never retries, so an ambiguous GHL failure is never sent twice."""

    def send(payload: Mapping[str, Any]) -> str:
        return sender(payload["to_phone"], payload["body"])

    return send


def instantly_executor(account: InstantlyAccount) -> EgressExecutor:
    """Sends an approved Instantly answer exactly as approved, inside the lead's thread from the inbox the reply
    reached; returns Instantly's email id. Never retried (the relay records a failure instead of sending twice)."""

    def send(payload: Mapping[str, Any]) -> Optional[str]:
        return send_reply(account, eaccount=payload["from_inbox"], reply_to_uuid=payload["reply_to_uuid"],
                          subject=payload["subject"], body=payload["body"])

    return send


def crm_executors(crm: GhlCrmWriter) -> dict[str, EgressExecutor]:
    """Approved CRM drafts, applied exactly as approved to the GHL contact the draft carries."""
    return {
        "ghl_stage": lambda payload: crm.move_stage(payload["ghl_contact_id"], payload["stage"]),
        "ghl_tag": lambda payload: crm.add_tag(payload["ghl_contact_id"], payload["tag"]),
        "ghl_note": lambda payload: crm.add_note(payload["ghl_contact_id"], payload["body"]),
        "ghl_task": lambda payload: crm.create_task(payload["ghl_contact_id"], payload["title"],
                                                    datetime.fromisoformat(payload["due_at"]), payload.get("assignee")),
    }


def cold_email_executor(sender: NdlInstantlySender) -> EgressExecutor:
    """Hands an approved cold email to NDL's Instantly campaign exactly as approved (the sender adds the CAN-SPAM
    footer); returns the Instantly lead id."""
    return lambda payload: sender.send(payload["to_email"], payload["subject"], payload["body"])


def build_egress_executors() -> dict[str, EgressExecutor]:
    """Channels Cora may send through once approved: ``instantly_email`` (needs ``LENDING_INSTANTLY_API_KEY``),
    ``ghl_email`` (needs the GHL account and
    ``LENDING_GHL_EMAIL_FROM``) and ``ghl_sms`` (needs the GHL account and ``LENDING_GHL_SMS_FROM_NUMBER``; texts
    also wait for LENDING_SMS_CARRIER_APPROVED in the send check), and the CRM writes ``ghl_stage``, ``ghl_tag``
    and ``ghl_note`` (need the GHL account; a stage move also needs LENDING_GHL_PIPELINE_ID)."""
    executors: dict[str, EgressExecutor] = {}
    email_sender = get_email_sender()
    if email_sender is None:
        logger.warning("[cora] GHL email sender not configured; approved emails will not be sent")
    else:
        executors["ghl_email"] = email_executor(email_sender)
    sms_sender = get_sender()
    if sms_sender is None:
        logger.warning("[cora] GHL SMS sender not configured; approved texts will not be sent")
    else:
        executors["ghl_sms"] = sms_executor(sms_sender)
    instantly = lending_instantly_account()
    if instantly is None:
        logger.warning("[cora] NDL Instantly account not configured; approved Instantly answers will not be sent")
    else:
        executors[INSTANTLY_CHANNEL] = instantly_executor(instantly)
    crm = get_crm_writer()
    if crm is None:
        logger.warning("[cora] GHL account not configured; approved stage moves, tags, notes and tasks will not apply")
    else:
        executors.update(crm_executors(crm))
    cold = get_ndl_instantly_sender()
    if cold is None:
        logger.warning("[cora] NDL Instantly cold-email campaign not configured; approved cold emails will not be sent")
    else:
        executors[COLD_EMAIL_CHANNEL] = cold_email_executor(cold)
    return executors


def build_agent_jobs(settings: AppSettings, *, engine: Engine, gate: SendGate, queue: PendingActionQueue,
                     approval_channel: str,
                     is_graduated: Callable[[str], bool] = lambda _: False) -> Optional[JobQueueWorker]:
    """The P1.1 Drive job queue worker, or None (logged) while it is switched off or not configured."""
    if not settings.lending_agent_jobs_enabled:
        logger.info("[cora] agent job queue off (LENDING_AGENT_JOBS_ENABLED=false)")
        return None
    folders = google_job_folders()
    crm = get_crm_writer()
    if folders is None or crm is None:
        logger.error("[cora] agent job queue enabled but not configured (Drive folders or the GHL account missing)")
        return None
    instantly_ready = get_ndl_instantly_sender() is not None
    return JobQueueWorker(engine, folders, gate, queue, crm.read_contact, approval_channel=approval_channel,
                          check=make_check(engine, instantly_ready=instantly_ready), is_graduated=is_graduated)


def _ts_seconds(ts: Optional[str]) -> float:
    """A Slack ts ("1792145692.511199") as epoch seconds; 0 for a missing or malformed one."""
    try:
        return float(ts or 0)
    except ValueError:
        return 0.0


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
                 bot_user_id: str, inbox_context: Optional[Callable[[str, str], str]] = None,
                 on_edit: Optional[Callable[[int], None]] = None) -> None:
        self._config = config
        self._on_edit = on_edit  # P1.4: records the card's before and after; never raises
        self._chat = chat
        self._loop = loop
        self._reviser = reviser
        self._bot_user_id = bot_user_id
        self._inbox_context = inbox_context

    def _thread_inbox_items(self, message: InboundMessage) -> str:
        """The inbox items a digest thread lists, so a reply like "dismiss 41" has its items in view. A failed
        lookup only costs that context: Cora still answers and can call list_inbox_items herself."""
        if not (message.thread_ts and self._inbox_context):
            return ""
        try:
            return self._inbox_context(message.channel, message.thread_ts)
        except Exception as error:  # class only: SQL errors embed bound parameters
            logger.error("[cora] inbox lookup for thread %s failed (%s)", message.thread_ts, type(error).__name__)
            return ""

    def _context(self, message: InboundMessage, tool_use_id: str) -> ToolContext:
        return ToolContext(user_id=message.user_id, is_approver=message.user_id in self._config.approver_user_ids,
                           channel=message.channel, thread_ts=message.conversation_ts, tool_use_id=tool_use_id)

    def _conversation_so_far(self, message: InboundMessage) -> list[dict[str, Any]]:
        """Inside a thread: that thread. Top level: only the channel's last few messages from the past hour.
        The channel is one long running conversation; older top-level topics in the history read to the
        model as open business, and it revisits them unasked."""
        if message.thread_ts:
            recent = self._chat.replies(channel=message.channel, thread_ts=message.thread_ts, limit=HISTORY_FETCH_LIMIT)
            return build_history(recent, bot_user_id=self._bot_user_id, before_ts=message.ts,
                                 skip_texts=frozenset({PLACEHOLDER_TEXT}))
        since = _ts_seconds(message.ts) - TOP_LEVEL_HISTORY_SECONDS
        recent = [item for item in self._chat.history(channel=message.channel, limit=HISTORY_FETCH_LIMIT)
                  if _ts_seconds(item.get("ts")) >= since]
        return build_history(recent, bot_user_id=self._bot_user_id, before_ts=message.ts,
                             skip_texts=frozenset({PLACEHOLDER_TEXT}), limit=TOP_LEVEL_HISTORY_TURNS)

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
        user_text = f"<@{message.user_id}>: {message.text}"
        inbox_items = self._thread_inbox_items(message)
        if inbox_items:
            user_text = f"{inbox_items}\n{user_text}"
        try:
            reply = self._loop.run(history=history, user_text=user_text,
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
            queued = self._reviser.revise(action, message.text, message.user_id)
            if self._on_edit is not None:
                self._on_edit(action.action_id)
            answer = (f"Revised action #{action.action_id}. A fresh approval card is posted; nothing has been sent."
                      if queued.card_posted else
                      f"Revised action #{action.action_id}, but Slack refused the new approval card; it will be "
                      "retried automatically. Nothing has been sent.")
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
                  toolkit_factory: Callable[[Engine, AgentMemory], Any] = CoraToolkit,
                  inbox_context: Optional[Callable[[str, str], str]] = None,
                  executors: Optional[Mapping[str, EgressExecutor]] = None,
                  extension: Any = None, record_edits: bool = False) -> CoraServices:
    """Wire Cora. ``toolkit_factory(engine, memory)`` builds the tool set; tests swap the lead lookups.
    ``inbox_context(channel, thread_ts)`` returns the inbox items a digest thread lists (empty when none).
    ``executors`` defaults to none, so nothing sends unless the caller registers a channel.
    ``extension`` is the session's hook for host-owned Slack traffic (``SlackSession``).
    The "@Cora emergency halt" alert goes to Cora's own channel (client answer 12).
    ``record_edits`` stores each card Revise in ``lending.card_edits`` (P1.4)."""
    queue = PendingActionQueue(store, default_ttl=timedelta(hours=config.pending_action_ttl_hours))
    halt = HaltSwitch(store, config.agent_name)
    relay = Relay(queue, halt, executors or {}, send_check=send_check)
    memory = AgentMemory(store)
    gate = SendGate(queue, chat, config.approval_channel_id)
    toolkit = toolkit_factory(store.engine, memory)

    def rules() -> list[str]:
        return [rule.rule_text for rule in memory.active_rules()]

    loop = AgentLoop(client=messages_client, model=config.model, registry=ToolRegistry(toolkit.tools(), gate),
                     base_prompt=CORA_BASE_PROMPT, rules_provider=rules)
    reviser = DraftReviser(client=messages_client, model=config.model, queue=queue, gate=gate,
                           base_prompt=REVISION_PROMPT, rules_provider=rules, editable_fields=EDITABLE_FIELDS,
                           content_check=draft_content_check)
    conversation = CoraConversation(config=config, chat=chat, loop=loop, reviser=reviser, bot_user_id=bot_user_id,
                                    inbox_context=inbox_context,
                                    on_edit=make_edit_recorder(store.engine, queue.get) if record_edits else None)
    session = SlackSession(config=config, chat=chat, queue=queue, halt=halt, relay=relay,
                           on_message=conversation.on_message, on_revision=conversation.on_revision,
                           bot_user_id=bot_user_id,
                           command_hook=EmergencyHalt(chat=chat, cora_halt=halt, approver_ids=config.approver_user_ids,
                                                      alert_channel=config.slack_channel_id),
                           extension=extension)
    return CoraServices(session, relay, gate, queue=queue)


def run_sweep(services: CoraServices) -> None:
    """One pass: expire stale drafts, triage new Instantly replies, retry missing approval cards, run the agent job
    queue, graduate templates (P1.4), post the inbox digest and the 7:30 AM
    morning digest (always, even while halted: the halt stops sends to borrowers, not Cora's own channel), then
    send approved actions."""
    expired = services.session.expire_stale_actions()
    if expired:
        logger.info("[cora] expired %s stale draft(s)", expired)
    if services.instantly_replies is not None:
        try:
            replies = services.instantly_replies()
        except Exception as exc:  # class only; the replies stay new and are retried next sweep
            logger.error("[cora] Instantly reply triage failed (%s); retrying next sweep", type(exc).__name__)
        else:
            if replies and (replies.triaged or replies.suppressed or replies.failed):
                logger.info("[cora] Instantly replies triaged=%s drafted=%s suppressed=%s failed=%s", replies.triaged,
                            replies.drafted, replies.suppressed, replies.failed)
    reposted = services.gate.retry_missing_cards()
    if reposted:
        logger.info("[cora] posted %s delayed approval card(s)", reposted)
    if services.agent_jobs is not None:
        try:
            stats = services.agent_jobs()
        except Exception as exc:  # class only: Drive or GHL trouble never skips the sends below
            logger.error("[cora] agent job queue failed (%s); retrying next sweep", type(exc).__name__)
        else:
            if stats and any(stats):
                logger.info("[cora] agent jobs %s", stats)
    if services.standing_sends is not None:
        try:
            graduated = services.standing_sends()
        except Exception as exc:  # class only; retried next sweep, nothing graduates meanwhile
            logger.error("[cora] standing sends check failed (%s); retrying next sweep", type(exc).__name__)
        else:
            if graduated:
                logger.info("[cora] graduated to standing auto-send: %s",
                            ", ".join(t.template_version for t in graduated))
    if services.inbox_digest is not None:
        try:
            surfaced = services.inbox_digest()
        except Exception as exc:  # class only: SQL errors embed bound parameters; never skip the sends below
            logger.error("[cora] inbox digest failed (%s); retrying next sweep", type(exc).__name__)
        else:
            if surfaced:
                logger.info("[cora] surfaced %s inbox item(s)", surfaced)
    if services.morning_digest is not None:
        try:
            services.morning_digest()
        except Exception as exc:  # class only, like the inbox digest; the claim is rolled back and retried
            logger.error("[cora] morning digest failed (%s); retrying next sweep", type(exc).__name__)
    relay = services.relay
    try:
        result = relay.run()
    except AgentHalted:
        return
    if result.sent or result.failed or result.blocked:
        logger.info("[cora] relay sweep sent=%s failed=%s blocked=%s", result.sent, result.failed, result.blocked)


def missing_settings(config: AgentCoreConfig) -> list[str]:
    """Settings Cora cannot run without."""
    missing = config.missing_for_live_session()
    if not config.anthropic_api_key:
        missing.append("anthropic_api_key")
    return missing


def build_live_services(settings: AppSettings) -> Optional[CoraServices]:
    """Cora wired to Slack, PostgreSQL and Claude; None (logged) when it cannot be built.

    None means either missing settings (see :func:`missing_settings`, a configuration problem) or a
    Slack ``auth.test`` failure (usually transient). Fails closed: without approvers no click could
    ever be trusted to release a send.
    """
    config = build_config(settings)
    missing = missing_settings(config)
    if missing:
        logger.error("[cora] not configured; missing settings: %s", ", ".join(missing))
        return None

    import anthropic
    from slack_sdk.web import WebClient

    web = WebClient(token=config.slack_bot_token)
    try:
        bot_user_id = web.auth_test()["user_id"]
    except Exception as exc:
        logger.error("[cora] Slack auth.test failed (%s); check LENDING_CORA_SLACK_BOT_TOKEN", type(exc).__name__)
        return None
    engine = create_engine(settings.database_url, pool_pre_ping=True)
    messages_client: MessagesClient = anthropic.Anthropic(
        api_key=config.anthropic_api_key, timeout=90.0, max_retries=2).messages
    chat = SlackChatPort(web, config.slack_channel_id)
    channel_ids = lending_channel_ids(settings)
    # Cora's own bot lists, reads and routes to the channels it has been invited to.
    channels = ChannelDirectory(web, own_channel_id=config.slack_channel_id, key_ids=channel_ids)

    attempt_messages, attempt_recordings = lending_ghl_readers()

    ledger_manifest = None
    from src.lending.drive_upload import key_path

    if settings.lending_ledger_export_folder_id and key_path():
        from src.lending.drive_upload import latest_manifest

        folder_id = settings.lending_ledger_export_folder_id

        def ledger_manifest():
            return latest_manifest(folder_id)

    def toolkit_factory(toolkit_engine: Engine, memory: AgentMemory) -> CoraToolkit:
        return CoraToolkit(toolkit_engine, memory, channels=channels, attempt_messages=attempt_messages,
                           attempt_recordings=attempt_recordings, ledger_manifest=ledger_manifest)

    store = AgentStore(engine, config.db_schema)
    services = build_session(config, store, chat,
                             bot_user_id, messages_client=messages_client, send_check=make_send_check(
                                 engine, carrier_approved=settings.lending_sms_carrier_approved),
                             toolkit_factory=toolkit_factory, inbox_context=make_inbox_context(engine),
                             executors=build_egress_executors(), extension=RoutingSlack(web),
                             record_edits=True)
    digest = make_inbox_digest(engine, chat, config.slack_channel_id, channel_ids)
    worker = build_agent_jobs(settings, engine=engine, gate=services.gate, queue=services.queue,
                              approval_channel=config.approval_channel_id or "",
                              is_graduated=make_is_graduated(engine))
    return services._replace(web_client=web, inbox_digest=digest,
                             morning_digest=make_morning_digest(engine, store, chat, config),
                             instantly_replies=make_instantly_replies(engine, store, config, messages_client,
                                                                      services.gate),
                             agent_jobs=worker.run_once if worker is not None else None,
                             standing_sends=make_standing_sends_step(engine))


def make_standing_sends_step(engine: Engine) -> Callable[[], Any]:
    """P1.4 sweep step: record any card edit the revise handler missed, then graduate templates that meet the rule.
    Incremental (only drafts decided or revised since the last run), with a full pass at start and once a day."""
    return StandingSendsStep(engine)


def make_instantly_replies(engine: Engine, store: AgentStore, config: AgentCoreConfig, messages_client: MessagesClient,
                           gate: SendGate) -> Callable[[], Any]:
    """P1.3: the sweep step that classifies new Instantly replies, queues the drafted answer as an approval card
    (posted at once; a refused card is re-posted by ``retry_missing_cards``) and posts the triage."""
    queue = PendingActionQueue(store, default_ttl=timedelta(hours=config.pending_action_ttl_hours))
    memory = AgentMemory(store)
    instantly = lending_instantly_account()
    processor = ReplyProcessor(
        session_factory=lambda: Session(engine), client=messages_client, model=config.model, enqueue=queue.enqueue,
        post_card=gate.post_card, post_triage=post_replies_notice, post_notice=post_ops_notice,
        rules=lambda: [rule.rule_text for rule in memory.active_rules()],
        block_address=(lambda email: block_address(instantly, email)) if instantly else (lambda email: False))
    return processor.run


def _adopt_open_cards(services: CoraServices) -> None:
    """Replace approval cards Cora's app cannot act on (those the lending app posted before Cora had
    her own app) so every open draft stays approvable. Best effort: a failure never blocks startup."""
    try:
        reposted = services.gate.adopt_cards()
    except Exception as exc:
        logger.error("[cora] checking open approval cards failed (%s); continuing", type(exc).__name__)
        return
    if reposted:
        logger.info("[cora] posted %s replacement approval card(s) under Cora's app", reposted)


def _sweep_until_stopped(services: CoraServices, stop: threading.Event) -> None:
    while not stop.wait(SWEEP_SECONDS):
        try:
            run_sweep(services)
        except Exception as exc:
            logger.error("[cora] sweep failed (%s); retrying next interval", type(exc).__name__)


def main(stop: Optional[threading.Event] = None) -> int:
    """Run Cora's Socket Mode session and the minute sweep until SIGTERM or Ctrl+C.

    Exits 1 only for missing settings (a configuration problem a restart cannot fix). A failed
    Slack ``auth.test`` or Socket Mode connect is retried in-process every sweep interval, so a
    Slack outage at boot never exhausts systemd's restart limit and leaves the worker dead.
    """
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    # slack_sdk logs request bodies at DEBUG, and a /partners answer carries a partner link (a credential).
    logging.getLogger("slack_sdk").setLevel(logging.INFO)
    stop = stop or threading.Event()
    # systemctl stop sends SIGTERM; Ctrl+C sends SIGINT. Both close the socket and end cleanly.
    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, lambda *_: stop.set())
    settings = get_settings()
    config = build_config(settings)
    missing = missing_settings(config)
    if missing:
        logger.error("[cora] not starting; missing settings: %s", ", ".join(missing))
        return 1
    services = build_live_services(settings)
    while services is None:
        logger.warning("[cora] could not reach Slack; retrying in %ss", SWEEP_SECONDS)
        if stop.wait(SWEEP_SECONDS):
            return 0
        services = build_live_services(settings)
    _adopt_open_cards(services)
    threading.Thread(target=_sweep_until_stopped, args=(services, stop), name="cora-sweep", daemon=True).start()
    logger.info("[cora] starting Socket Mode session (model %s, sweep every %ss)", config.model, SWEEP_SECONDS)
    while not stop.is_set():
        try:
            run_socket_mode(services.session, app_token=config.slack_app_token, web_client=services.web_client,
                            stop=stop)
        except Exception as exc:
            logger.error("[cora] Socket Mode session failed (%s); reconnecting in %ss", type(exc).__name__,
                         SWEEP_SECONDS)
            stop.wait(SWEEP_SECONDS)
    logger.info("[cora] stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
