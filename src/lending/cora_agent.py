"""Cora, the Next Deal Lending Slack operator: process entry point (systemd ``fa-lending-cora``).

Builds the ``packages.agent_core`` pieces from this app's settings and runs the Socket Mode
session, plus a sweep that expires stale drafts and sends approved actions that were not sent at
click time (a restart, or a halt lifted later). Every send first passes the lending send-time
check: the recipient must not be suppressed or do-not-contact, and a text needs live consent.

Conversation itself is not wired yet: until the agent loop lands, a message gets a short
"connected" reply, so the daemon and Slack app can be verified end to end.

Usage:
    PYTHONPATH=. python -m src.lending.cora_agent
"""
from __future__ import annotations

import logging
import threading
from datetime import timedelta
from typing import Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from config.settings import AppSettings, get_settings
from packages.agent_core.chatport import ChatPort, SlackChatPort
from packages.agent_core.config import AgentCoreConfig, parse_user_ids
from packages.agent_core.halt import AgentHalted, HaltSwitch
from packages.agent_core.pending_actions import PendingAction, PendingActionQueue
from packages.agent_core.relay import EgressExecutor, Relay, SendCheck
from packages.agent_core.slack_session import InboundMessage, SlackSession, run_socket_mode
from packages.agent_core.store import AgentStore
from src.lending.consent import has_text_consent
from src.lending.models import LENDING_SCHEMA
from src.services.phone_utils import normalize

logger = logging.getLogger(__name__)

AGENT_NAME = "Cora"
SWEEP_SECONDS = 60

# Egress channels Cora may send through once approved. Empty until the agent loop registers its
# send tools; an approved action on an unregistered channel is marked failed, never sent.
EGRESS_EXECUTORS: dict[str, EgressExecutor] = {}
# Channels that deliver a text message and therefore need live text consent.
SMS_CHANNELS = frozenset({"ghl_sms"})

# Same suppression rule the booking reminder worker applies before every send.
_SUPPRESSED = text("""
    SELECT EXISTS (SELECT 1 FROM lending.suppression_list WHERE (phone = :p AND :p IS NOT NULL)
                                                              OR (email = :e AND :e IS NOT NULL))
        OR EXISTS (SELECT 1 FROM lending.contacts WHERE phone = :p AND :p IS NOT NULL AND do_not_contact)
""")

_NOT_ENABLED_REPLY = "I'm connected, but conversation isn't switched on yet. Nothing was sent."


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


def build_session(config: AgentCoreConfig, store: AgentStore, chat: ChatPort, bot_user_id: str,
                  send_check: Optional[SendCheck] = None) -> tuple[SlackSession, Relay]:
    queue = PendingActionQueue(store, default_ttl=timedelta(hours=config.pending_action_ttl_hours))
    halt = HaltSwitch(store, config.agent_name)
    relay = Relay(queue, halt, EGRESS_EXECUTORS, send_check=send_check)

    def on_message(message: InboundMessage) -> None:
        chat.post(text=_NOT_ENABLED_REPLY, channel=message.channel, thread_ts=message.reply_thread_ts)

    def on_revision(action: PendingAction, message: InboundMessage) -> None:
        chat.post(text=_NOT_ENABLED_REPLY, channel=message.channel, thread_ts=message.reply_thread_ts)

    session = SlackSession(config=config, chat=chat, queue=queue, halt=halt, relay=relay,
                           on_message=on_message, on_revision=on_revision, bot_user_id=bot_user_id)
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
    if missing:
        # Fail closed: without approvers no click could ever be trusted to release a send.
        logger.error("[cora] not starting; missing settings: %s", ", ".join(missing))
        return 1

    from slack_sdk.web import WebClient

    web = WebClient(token=config.slack_bot_token)
    try:
        bot_user_id = web.auth_test()["user_id"]
    except Exception as exc:
        logger.error("[cora] Slack auth.test failed (%s); check LENDING_CORA_SLACK_BOT_TOKEN", type(exc).__name__)
        return 1

    engine = create_engine(settings.database_url, pool_pre_ping=True)
    store = AgentStore(engine, config.db_schema)
    session, relay = build_session(config, store, SlackChatPort(web, config.slack_channel_id), bot_user_id,
                                   send_check=make_send_check(engine))
    stop = threading.Event()
    threading.Thread(target=_sweep_forever, args=(session, relay, stop), name="cora-sweep", daemon=True).start()
    logger.info("[cora] starting Socket Mode session as %s", bot_user_id)
    run_socket_mode(session, app_token=config.slack_app_token, web_client=web)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
