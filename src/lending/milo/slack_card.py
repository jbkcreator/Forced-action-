"""The Slack Confirm / Cancel card for one Central Lending Milo application (P3.3, team decision Q44).

Nothing goes to Milo without a person: the card shows the full mapped application in #approvals
(``LENDING_CORA_APPROVALS_CHANNEL``, Cora's bot) and only Cora's approvers (``LENDING_CORA_APPROVER_USER_IDS``) may
press Confirm or Cancel; anyone else is told so privately and nothing changes. What the card showed is stored on
``lending.milo_cards``, so Confirm sends exactly that.

Confirm then runs the duplicate guard (``submissions.claim``: one package, one application) and the Milo filler
passed in as ``submitter``. Until the filler exists (it needs Central's Milo login) there is none: Confirm says that
nothing was sent and the card stays open. Clicks arrive through Cora's Socket Mode listener
(``routing.slack_extension.RoutingSlack``).

    PYTHONPATH=. python -m src.lending.milo.slack_card --package config/lending_milo_test_package.json
"""
from __future__ import annotations

import argparse
import json
import logging
from contextlib import AbstractContextManager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.lending_milo import ACTION_CANCEL, ACTION_CONFIRM, CARD_FLAG_TEXT
from packages.agent_core.redaction import slack_escape
from src.lending.milo import submissions
from src.lending.milo.mapping import MiloApplication

logger = logging.getLogger(__name__)

# The filler: takes the application, returns Milo's loan id or URL, raises when Milo refused it.
Submitter = Callable[[MiloApplication], Optional[str]]
SessionFactory = Callable[[], AbstractContextManager[Session]]

_INSERT_CARD = text("""
    INSERT INTO lending.milo_cards (package_ref, application, is_test)
    VALUES (:ref, CAST(:application AS jsonb), :is_test) RETURNING id
""")
_SET_POSTED = text("UPDATE lending.milo_cards SET channel = :channel, message_ts = :ts WHERE id = :id")
_CARD = text("SELECT id, package_ref, application, is_test, status FROM lending.milo_cards WHERE id = :id FOR UPDATE")
_DECIDE = text("""
    UPDATE lending.milo_cards SET status = :status, decided_by = :user, decided_at = :now
     WHERE id = :id AND status = 'open'
""")


def application_from_json(data: dict) -> MiloApplication:
    return MiloApplication(package_ref=data["package_ref"], step1=dict(data["step1"]),
                           step2_property=dict(data["step2_property"]), step2_borrower=dict(data["step2_borrower"]),
                           checks=dict(data["checks"]), flags=tuple(data.get("flags") or ()))


def _fields(title: str, values: dict[str, str]) -> dict:
    lines = "\n".join(f"{slack_escape(name)}: *{slack_escape(value or '-')}*" for name, value in values.items())
    return {"type": "section", "text": {"type": "mrkdwn", "text": f"*{title}*\n{lines}"}}


def card_blocks(card_id: int, application: MiloApplication, *, is_test: bool) -> list[dict]:
    """The whole application as Milo will receive it, its flags, and the two buttons."""
    heading = f"{'TEST - ' if is_test else ''}Central Lending Milo application, package {application.package_ref}"
    blocks: list[dict] = [
        {"type": "header", "text": {"type": "plain_text", "text": heading[:150]}},
        _fields("Step 1: what are you financing?", application.step1),
        _fields("Step 2: property", application.step2_property),
        _fields("Step 2: borrower", application.step2_borrower),
        _fields("Milo should calculate", application.checks),
    ]
    if application.flags:
        flags = "\n".join(f"- {slack_escape(CARD_FLAG_TEXT.get(flag, flag))}" for flag in application.flags)
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": f"*Check before confirming*\n{flags}"}})
    blocks.append({"type": "actions", "elements": [
        {"type": "button", "action_id": ACTION_CONFIRM, "value": str(card_id), "style": "primary",
         "text": {"type": "plain_text", "text": "Confirm and submit"},
         "confirm": {"title": {"type": "plain_text", "text": "Submit to Central Lending?"},
                     "text": {"type": "mrkdwn", "text": "This creates the application in Milo. It is done once."},
                     "confirm": {"type": "plain_text", "text": "Submit"},
                     "deny": {"type": "plain_text", "text": "Not yet"}}},
        {"type": "button", "action_id": ACTION_CANCEL, "value": str(card_id), "style": "danger",
         "text": {"type": "plain_text", "text": "Cancel"}},
    ]})
    return blocks


def post_card(db: Session, client: Any, channel: str, application: MiloApplication, *, is_test: bool) -> int:
    """Store the application, post its card and record where. Commits. Raises when Slack refused the post (the
    stored card stays open with no message, so a re-post makes a new one)."""
    card_id = db.execute(_INSERT_CARD, {"ref": application.package_ref, "is_test": is_test,
                                        "application": json.dumps(application.to_json())}).scalar_one()
    db.commit()
    response = client.chat_postMessage(channel=channel, text=f"Milo application {application.package_ref}: confirm?",
                                       blocks=card_blocks(card_id, application, is_test=is_test))
    db.execute(_SET_POSTED, {"id": card_id, "channel": response.get("channel") or channel, "ts": response.get("ts")})
    db.commit()
    logger.info("[milo-card] card %s posted for package %s", card_id, application.package_ref)
    return card_id


def is_milo_action(payload: dict) -> bool:
    actions = payload.get("actions") or []
    return payload.get("type") == "block_actions" and bool(actions) and \
        actions[0].get("action_id") in (ACTION_CONFIRM, ACTION_CANCEL)


def _tell(client: Any, payload: dict, message: str) -> None:
    channel = (payload.get("channel") or {}).get("id")
    user = (payload.get("user") or {}).get("id")
    try:
        client.chat_postEphemeral(channel=channel, user=user, text=message)
    except Exception as exc:  # class only: Slack errors can echo request detail
        logger.warning("[milo-card] could not tell the user (%s)", type(exc).__name__)


def _close_card(client: Any, payload: dict, note: str) -> None:
    """Replace the buttons with what happened, so the card cannot be pressed again."""
    message = payload.get("message") or {}
    blocks = [block for block in message.get("blocks") or [] if block.get("type") != "actions"]
    blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": note}]})
    try:
        client.chat_update(channel=(payload.get("channel") or {}).get("id"), ts=message.get("ts"),
                           text=message.get("text") or note, blocks=blocks)
    except Exception as exc:  # class only
        logger.warning("[milo-card] card not updated (%s)", type(exc).__name__)


def handle_action(payload: dict, client: Any, *, approvers: frozenset[str], submitter: Optional[Submitter],
                  session_factory: SessionFactory, now: Optional[datetime] = None) -> str:
    """One Confirm or Cancel click. Returns the outcome (for logs and tests)."""
    action = payload["actions"][0]
    user = (payload.get("user") or {}).get("id") or ""
    if user not in approvers:
        _tell(client, payload, "Only Josh's approvers can confirm or cancel a Milo application. Nothing changed.")
        return "not_allowed"
    try:
        card_id = int(action.get("value") or "")
    except ValueError:
        return "unknown_card"
    now = now or datetime.now(timezone.utc)
    with session_factory() as db:
        card = db.execute(_CARD, {"id": card_id}).mappings().first()
        if card is None or card["status"] != "open":
            db.rollback()
            _tell(client, payload, "This card was already decided. Nothing changed.")
            return "already_decided"
        if action["action_id"] == ACTION_CANCEL:
            db.execute(_DECIDE, {"id": card_id, "status": "cancelled", "user": user, "now": now})
            db.commit()
            _close_card(client, payload, f"Cancelled by <@{user}>. Nothing was sent to Milo.")
            return "cancelled"
        db.rollback()
        if submitter is None:
            _tell(client, payload, "Nothing was sent: the Milo filler is not connected yet (it needs Central's Milo "
                                   "login). The card stays open.")
            return "no_submitter"
        application = application_from_json(card["application"])
        claim = submissions.claim(db, application, is_test=card["is_test"], requested_by=f"slack:{user}")
        if not claim.claimed:
            _tell(client, payload, f"Not sent: this package is already {claim.existing_status} in Milo"
                                   f"{f' ({claim.milo_ref})' if claim.milo_ref else ''}. One package makes one "
                                   "application.")
            return "duplicate"
        try:
            milo_ref = submitter(application)
        except Exception as exc:  # class only: the filler's errors can carry form values
            submissions.mark_failed(db, application.package_ref, type(exc).__name__)
            _tell(client, payload, "Milo did not take the application; nothing was created. The card stays open.")
            logger.error("[milo-card] package %s failed (%s)", application.package_ref, type(exc).__name__)
            return "failed"
        submissions.mark_submitted(db, application.package_ref, milo_ref)
        db.execute(_DECIDE, {"id": card_id, "status": "confirmed", "user": user, "now": now})
        db.commit()
    _close_card(client, payload, f"Confirmed by <@{user}> and submitted to Milo{f': {milo_ref}' if milo_ref else ''}.")
    logger.info("[milo-card] package %s submitted", application.package_ref)
    return "submitted"


def milo_submitter() -> Optional[Submitter]:
    """The Milo filler. None until it is built (needs Central's Milo login)."""
    return None


def main(argv: Optional[list[str]] = None) -> int:
    from config.settings import get_settings
    from src.lending.db import lending_session
    from src.lending.milo.mapping import map_package
    from src.lending.milo.package import load_package
    from src.lending.routing.notify import _client

    parser = argparse.ArgumentParser(description="Post a Milo application's Confirm / Cancel card to #approvals")
    parser.add_argument("--package", type=Path, required=True, help="loan package JSON file")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    channel, client = get_settings().lending_cora_approvals_channel, _client()
    if not (channel and client):
        logger.error("[milo-card] needs LENDING_CORA_APPROVALS_CHANNEL and LENDING_CORA_SLACK_BOT_TOKEN")
        return 1
    package = load_package(args.package)
    with lending_session() as db:
        post_card(db, client, channel, map_package(package), is_test=package.is_test)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
