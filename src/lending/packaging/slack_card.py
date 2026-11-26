"""The secondary lender package card in the ops channel, and its buttons.

Posted by Cora's bot so its clicks reach Cora's Socket Mode listener (``routing.slack_extension``). Mark submitted is
only on the card while the package can be submitted; otherwise the card says why. Approvers only.
"""
from __future__ import annotations

import json
import logging
from datetime import date
from typing import Any, Callable, ContextManager, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.lending_closing import DOCUMENT_TYPES
from config.lending_packaging import (
    ACTION_DISMISS,
    ACTION_IDS,
    ACTION_OVERRIDE,
    ACTION_SUBMIT,
    ACTION_TAG,
    OVERRIDE_CALLBACK_ID,
    TAG_CALLBACK_ID,
)
from src.lending.audit import deal_ledger
from src.lending.packaging import actions
from src.lending.packaging.package import LOAN_FILE, CompileResult, compile_package, lender_name
from src.lending.packaging.packet_validator import PARTNER_FILE_UPLOADS, Outcome

logger = logging.getLogger(__name__)

SessionFactory = Callable[[], ContextManager[Session]]
Scheduler = Callable[..., None]
_OUTCOME_MARK = {Outcome.PASS: ":white_check_mark:", Outcome.BLOCK: ":warning:", Outcome.QUARANTINE: ":no_entry:"}


def _section(body: str) -> dict:
    return {"type": "section", "text": {"type": "mrkdwn", "text": body[:2900]}}


def _button(label: str, action_id: str, value: str, style: Optional[str] = None) -> dict:
    button = {"type": "button", "text": {"type": "plain_text", "text": label[:75]}, "action_id": action_id,
              "value": value}
    if style:
        button["style"] = style
    return button


def file_ref(result: CompileResult) -> str:
    return f"Loan file #{result.source_id}" if result.source_type == LOAN_FILE else f"LendingFlow lead #{result.source_id}"


def blocked_reasons(result: CompileResult) -> list[str]:
    """Why Mark submitted is not offered; empty when it is."""
    reasons = []
    if not result.box_confirmed:
        reasons.append("the lender's box is not confirmed")
    if result.verdict.quarantined:
        reasons.append(f"{len(result.verdict.quarantined)} document(s) quarantined")
    if result.verdict.blocked:
        reasons.append(f"{len(result.verdict.blocked)} document(s) blocked")
    return reasons


def _fit_text(fit: dict) -> str:
    lines = []
    for miss in fit.get("misses") or []:
        lines.append(f"• {miss['lender_name']}: missed ({'; '.join(miss['reasons']) or 'no reason given'})")
    for maybe in fit.get("possible") or []:
        lines.append(f"• {maybe['lender_name']}: undetermined ({maybe['why']})")
    return "\n".join(lines) or "_Every secondary lender fits this file._"


def _docs_text(result: CompileResult) -> str:
    lines = []
    for doc in result.verdict.per_doc:
        label = DOCUMENT_TYPES.get(doc.item_code or "", "untyped")
        note = " (override)" if doc.overridden else ""
        why = f": {'; '.join(doc.reasons)}" if doc.reasons else ""
        lines.append(f"{_OUTCOME_MARK[doc.outcome]} #{doc.upload_id} {label}{note}{why}")
    return "\n".join(lines) or "_No documents on file yet._"


def card_blocks(result: CompileResult, *, note: str = "") -> list[dict]:
    head = f"*{lender_name(result.lender_key)}* package PKG-{result.package_id} · {file_ref(result)} · {result.status}"
    blocks = [_section(head)]
    if not result.box_confirmed:
        blocks.append(_section(":warning: *Box not confirmed — do not submit.* This lender's rules are placeholders."))
    blocks.append(_section("*Other secondary lenders*\n" + _fit_text(result.fit_result)))
    checklist = "\n".join(f"{':white_check_mark:' if i['received'] else ':x:'} {i['label']}" for i in result.checklist)
    blocks.append(_section("*Checklist*\n" + (checklist or "_No required documents configured._")))
    blocks.append(_section("*Documents*\n" + _docs_text(result)))
    if result.deal_facts_path:
        blocks.append(_section("Deal facts PDF attached in the thread."))
    if note:
        blocks.append(_section(note))
    if result.status in ("submitted", "dismissed"):
        return blocks
    why_not = blocked_reasons(result)
    if why_not:
        blocks.append(_section("Mark submitted is not available: " + "; ".join(why_not) + "."))
    buttons = []
    if not why_not:
        buttons.append(_button("Mark submitted", ACTION_SUBMIT, str(result.package_id), "primary"))
    for doc in result.verdict.blocked:
        value = json.dumps({"p": result.package_id, "t": doc.upload_table, "u": doc.upload_id})
        if doc.item_code is None and doc.upload_table == PARTNER_FILE_UPLOADS:
            buttons.append(_button(f"Tag type #{doc.upload_id}", f"{ACTION_TAG}:{doc.upload_id}", value))
        else:
            buttons.append(_button(f"Override #{doc.upload_id}", f"{ACTION_OVERRIDE}:{doc.key}", value))
    buttons.append(_button("Dismiss", ACTION_DISMISS, str(result.package_id), "danger"))
    for start in range(0, len(buttons), 25):
        blocks.append({"type": "actions", "elements": buttons[start:start + 25]})
    return blocks


def cora_client() -> Any:
    from config.settings import get_settings

    token = get_settings().lending_cora_slack_bot_token
    if not token:
        return None
    from slack_sdk import WebClient

    return WebClient(token=token.get_secret_value())


def post_card(db: Session, client: Any, channel: str, result: CompileResult) -> Optional[str]:
    """Post the card, remember where, and attach the deal facts PDF in its thread. Returns the message ts."""
    response = client.chat_postMessage(channel=channel, blocks=card_blocks(result),
                                       text=f"{lender_name(result.lender_key)} package PKG-{result.package_id}")
    ts = str(response.get("ts") or "")
    db.execute(text("UPDATE lending.lender_packages SET card_channel = :c, card_ts = :ts WHERE id = :id"),
               {"c": channel, "ts": ts, "id": result.package_id})
    if result.deal_facts_path:
        try:
            client.files_upload_v2(channel=channel, thread_ts=ts, file=result.deal_facts_path,
                                   filename=f"PKG-{result.package_id}-deal-facts.pdf",
                                   title=f"Deal facts PKG-{result.package_id}")
        except Exception as exc:  # class only
            logger.error("[packaging] deal facts upload failed for package %s (%s)", result.package_id,
                         type(exc).__name__)
    return ts


def _refresh(client: Any, db: Session, package_id: int, note: str, today: date) -> None:
    row = db.execute(text("SELECT card_channel, card_ts, status FROM lending.lender_packages WHERE id = :id"),
                     {"id": package_id}).mappings().first()
    if not row or not row["card_channel"] or not row["card_ts"]:
        return
    result = compile_package(db, package_id, today=today, renderer=None)
    if result is None:  # closed: keep the card's content, drop its buttons
        blocks = [_section(f"Package PKG-{package_id} is {row['status']}."), _section(note)]
    else:
        blocks = card_blocks(result, note=note)
    try:
        client.chat_update(channel=row["card_channel"], ts=row["card_ts"], blocks=blocks, text=note)
    except Exception as exc:  # class only
        logger.error("[packaging] card update failed for package %s (%s)", package_id, type(exc).__name__)


def _tell(client: Any, channel: str, user: str, message: str) -> None:
    try:
        client.chat_postEphemeral(channel=channel, user=user, text=message)
    except Exception as exc:  # class only
        logger.error("[packaging] ephemeral notice failed (%s)", type(exc).__name__)


def is_package_action(payload: dict) -> bool:
    found = payload.get("actions") or []
    return (payload.get("type") == "block_actions" and bool(found)
            and str(found[0].get("action_id") or "").split(":")[0] in ACTION_IDS)


def opens_form(payload: dict) -> bool:
    action_id = str(((payload.get("actions") or [{}])[0]).get("action_id") or "")
    return action_id.split(":")[0] in (ACTION_OVERRIDE, ACTION_TAG)


def is_package_submission(payload: dict) -> bool:
    return (payload.get("type") == "view_submission"
            and (payload.get("view") or {}).get("callback_id") in (OVERRIDE_CALLBACK_ID, TAG_CALLBACK_ID))


def _user(payload: dict) -> str:
    return str((payload.get("user") or {}).get("id") or "")


def _modal(callback_id: str, title: str, meta: dict, field: dict) -> dict:
    return {"type": "modal", "callback_id": callback_id, "title": {"type": "plain_text", "text": title},
            "submit": {"type": "plain_text", "text": "Save"}, "close": {"type": "plain_text", "text": "Cancel"},
            "private_metadata": json.dumps(meta), "blocks": [field]}


def override_view(meta: dict) -> dict:
    return _modal(OVERRIDE_CALLBACK_ID, "Override block", meta, {
        "type": "input", "block_id": "reason_b", "label": {"type": "plain_text", "text": "Why is this document fine?"},
        "element": {"type": "plain_text_input", "action_id": "v", "multiline": True}})


def tag_view(meta: dict) -> dict:
    options = [{"text": {"type": "plain_text", "text": label[:75]}, "value": code}
               for code, label in DOCUMENT_TYPES.items()]
    return _modal(TAG_CALLBACK_ID, "Document type", meta, {
        "type": "input", "block_id": "type_b", "label": {"type": "plain_text", "text": "What is this document?"},
        "element": {"type": "static_select", "action_id": "v", "options": options}})


def handle_action(payload: dict, client: Any, *, approvers: frozenset[str], session_factory: SessionFactory,
                  today: date) -> None:
    """A click on the card. Override / Tag type open a form; Mark submitted / Dismiss act now."""
    user = _user(payload)
    channel = str((payload.get("channel") or {}).get("id") or "")
    if user not in approvers:
        _tell(client, channel, user, "Only approved users can act on lender packages.")
        return
    action = payload["actions"][0]
    kind = str(action.get("action_id") or "").split(":")[0]
    actor = deal_ledger.human(user)
    try:
        if kind in (ACTION_OVERRIDE, ACTION_TAG):
            meta = json.loads(action.get("value") or "{}")
            view = override_view(meta) if kind == ACTION_OVERRIDE else tag_view(meta)
            client.views_open(trigger_id=payload.get("trigger_id", ""), view=view)
            return
        package_id = int(action.get("value") or 0)
        with session_factory() as db:
            if kind == ACTION_SUBMIT:
                actions.mark_submitted(db, package_id, actor=actor, today=today)
                note = f":outbox_tray: Marked submitted by <@{user}>."
            else:
                actions.dismiss(db, package_id, actor=actor)
                note = f":wastebasket: Dismissed by <@{user}>."
            _refresh(client, db, package_id, note, today)
    except actions.ActionRefused as refused:
        _tell(client, channel, user, str(refused))
    except Exception as exc:  # class only: SQL errors can embed bound values
        logger.error("[packaging] card click failed (%s)", type(exc).__name__)
        _tell(client, channel, user, "Something went wrong; nothing was changed.")


def _field(values: dict, block: str) -> str:
    cell = (values.get(block) or {}).get("v") or {}
    if cell.get("type") == "static_select":
        return str((cell.get("selected_option") or {}).get("value") or "")
    return str(cell.get("value") or "").strip()


def handle_submission(payload: dict, client: Any, run_later: Scheduler, *, approvers: frozenset[str],
                      session_factory: SessionFactory, today: date) -> dict:
    """An Override or Tag type form. Checked here (errors stay in the form); applied after the ack."""
    user = _user(payload)
    view = payload.get("view") or {}
    is_override = view.get("callback_id") == OVERRIDE_CALLBACK_ID
    block = "reason_b" if is_override else "type_b"
    if user not in approvers:
        return {"response_action": "errors", "errors": {block: "Only approved users can do this."}}
    value = _field((view.get("state") or {}).get("values") or {}, block)
    if not value:
        return {"response_action": "errors", "errors": {block: "Required."}}
    if not is_override and value not in DOCUMENT_TYPES:
        return {"response_action": "errors", "errors": {block: "Pick a document type from the list."}}
    meta = json.loads(view.get("private_metadata") or "{}")
    run_later(_apply_form, client, is_override, meta, value, user, session_factory, today)
    return {}


def _apply_form(client: Any, is_override: bool, meta: dict, value: str, user: str,
                session_factory: SessionFactory, today: date) -> None:
    actor = deal_ledger.human(user)
    package_id = int(meta.get("p") or 0)
    try:
        with session_factory() as db:
            if is_override:
                actions.override_block(db, package_id, str(meta.get("t")), int(meta.get("u") or 0), reason=value,
                                       actor=actor, today=today)
                note = f":unlock: Block on #{meta.get('u')} overridden by <@{user}>: {value[:200]}"
            else:
                actions.tag_document_type(db, int(meta.get("u") or 0), value, actor=actor)
                note = f":label: #{meta.get('u')} tagged {DOCUMENT_TYPES[value]} by <@{user}>."
            _refresh(client, db, package_id, note, today)
    except actions.ActionRefused as refused:
        logger.info("[packaging] form refused: %s", refused)
        _tell_user(client, user, str(refused))
    except Exception as exc:  # class only
        logger.error("[packaging] form failed (%s)", type(exc).__name__)
        _tell_user(client, user, "Something went wrong; nothing was changed.")


def _tell_user(client: Any, user: str, message: str) -> None:
    try:
        client.chat_postMessage(channel=user, text=message)
    except Exception as exc:  # class only
        logger.error("[packaging] direct notice failed (%s)", type(exc).__name__)
