"""R-1 lender rules edited from Slack (Josh's R1.4: a Slack form, not a web admin screen).

``/lenders`` (a slash command on Cora's Slack app, which runs over Socket Mode) opens one modal: pick an action,
pick a lender, type a value. Cora's Socket Mode listener hands the command and the submission to
``slack_extension.RoutingSlack``, which calls :func:`open_form` and :func:`handle_submission` here. Every change goes through the same functions the admin API uses, so the same
validation, locking and conflict rules apply; nothing here writes to the rule tables itself.

Only Cora's approvers (``LENDING_CORA_APPROVER_USER_IDS``) may open or submit the form; an empty list means
nobody (fail closed). Each applied change is posted to Cora's channel (``LENDING_CORA_CHANNEL``) with the person who
made it: that post is the audit trail.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Optional

from fastapi import HTTPException
from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.orm import Session

from config.lending_routing import AGREEMENT_STATUS_CHOICES
from config.settings import get_settings
from src.lending.contracts import LoanType
from src.lending.db import lending_session
from src.lending.routing import admin_api, notify

logger = logging.getLogger(__name__)

CALLBACK_ID = "routing_lender_form"

ACTIONS: tuple[tuple[str, str], ...] = (
    ("add_lender", "Add a lender (name in Value, box fields below)"),
    ("add_box", "Add a product box to the chosen lender"),
    ("pause", "Pause the chosen lender"),
    ("resume", "Resume the chosen lender"),
    ("agreement", "Set agreement status (Value: signed, pending, none or terminated)"),
    ("seat_cap", "Set monthly seat cap (Value: a number, or none)"),
    ("move_up", "Move the chosen lender up one place"),
    ("move_down", "Move the chosen lender down one place"),
    ("make_primary", "Make the chosen lender primary"),
)
_NEEDS_LENDER = {a for a, _ in ACTIONS} - {"add_lender"}
_BOX_FIELDS = ("types_b", "states_b", "min_b", "max_b", "credit_b")


def allowed_user_ids() -> frozenset[str]:
    settings = get_settings()
    raw = getattr(settings, "lending_cora_approver_user_ids", "") or ""
    return frozenset(item.strip() for item in raw.split(",") if item.strip())


def _user_id(payload: dict) -> str:
    return str((payload.get("user") or {}).get("id") or "")


def _lenders(db: Session) -> list[dict]:
    rows = db.execute(text(
        "SELECT id, name, active, is_primary, agreement_status, priority, monthly_cap"
        "  FROM lending.lender_routing_rules ORDER BY priority ASC, id ASC")).mappings().all()
    return [dict(r) for r in rows]


def _label(lender: dict) -> str:
    flags = [lender["agreement_status"], "primary" if lender["is_primary"] else None, None if lender["active"] else "paused"]
    return f"{lender['priority']}. {lender['name']} ({', '.join(f for f in flags if f)})"[:75]


def _input(block_id: str, label: str, element: dict, *, optional: bool = True, hint: Optional[str] = None) -> dict:
    block = {"type": "input", "block_id": block_id, "optional": optional,
             "label": {"type": "plain_text", "text": label}, "element": element}
    if hint:
        block["hint"] = {"type": "plain_text", "text": hint}
    return block


def _text(placeholder: str = "") -> dict:
    return {"type": "plain_text_input", "action_id": "v", "placeholder": {"type": "plain_text", "text": placeholder or " "}}


def build_form_view(lenders: list[dict]) -> dict:
    listing = "\n".join(f"{_label(l)}" + (f" · cap {l['monthly_cap']}" if l["monthly_cap"] is not None else "")
                        for l in lenders) or "No lenders yet."
    option = lambda value, label: {"text": {"type": "plain_text", "text": label[:75]}, "value": value}  # noqa: E731
    blocks: list[dict] = [
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*Waterfall order*\n```{listing}```"}},
        _input("action_b", "Action", {"type": "static_select", "action_id": "v", "options": [option(a, t) for a, t in ACTIONS]},
               optional=False),
    ]
    if lenders:
        blocks.append(_input("lender_b", "Lender (not needed to add one)",
                             {"type": "static_select", "action_id": "v", "options": [option(str(l["id"]), _label(l)) for l in lenders[:100]]}))
    blocks += [
        _input("value_b", "Value", _text("name, signed/pending, or seat cap")),
        _input("types_b", "Box: loan types",
               {"type": "multi_static_select", "action_id": "v",
                "options": [option(m.value, m.value.replace("_", " ").title()) for m in LoanType]},
               hint="Used by 'Add a lender' and 'Add a product box'. A lender with no box receives nothing."),
        _input("states_b", "Box: states", _text("FL, GA (blank = all states)")),
        _input("min_b", "Box: min loan ($)", _text()),
        _input("max_b", "Box: max loan ($)", _text()),
        _input("credit_b", "Box: min credit score", _text()),
    ]
    return {"type": "modal", "callback_id": CALLBACK_ID, "title": {"type": "plain_text", "text": "Lender rules"},
            "submit": {"type": "plain_text", "text": "Apply"}, "close": {"type": "plain_text", "text": "Cancel"},
            "blocks": blocks}


def _value(values: dict, block: str) -> str:
    cell = (values.get(block) or {}).get("v") or {}
    if cell.get("type") == "static_select":
        return str((cell.get("selected_option") or {}).get("value") or "")
    if cell.get("type") == "multi_static_select":
        return ",".join(o.get("value", "") for o in cell.get("selected_options") or [])
    return str(cell.get("value") or "").strip()


def _int_or_none(raw: str, label: str) -> Optional[int]:
    cleaned = raw.replace(",", "").replace("$", "").strip()
    if not cleaned:
        return None
    if not cleaned.isdigit():
        raise ValueError(f"{label} must be a whole number")
    return int(cleaned)


def _box_from(values: dict) -> admin_api.ProductBoxIn:
    types = [t for t in _value(values, "types_b").split(",") if t]
    states = [s for s in _value(values, "states_b").replace(" ", "").split(",") if s]
    return admin_api.ProductBoxIn(
        loan_types=types, approved_states=states or None,
        min_loan_amount=_int_or_none(_value(values, "min_b"), "Min loan"),
        max_loan_amount=_int_or_none(_value(values, "max_b"), "Max loan"),
        credit_floor=_int_or_none(_value(values, "credit_b"), "Min credit"))


def _reason(exc: Exception) -> str:
    if isinstance(exc, HTTPException):
        return str(exc.detail)
    if isinstance(exc, ValidationError):
        return "; ".join(str(e["msg"]).removeprefix("Value error, ") for e in exc.errors())[:250]
    return str(exc)[:250]


def apply_change(values: dict, db: Session) -> str:
    """Apply one submitted form. Returns the audit sentence; raises ``ValueError``/``HTTPException`` with a clean message."""
    action = _value(values, "action_b")
    value = _value(values, "value_b")
    lender_raw = _value(values, "lender_b")
    if action not in {a for a, _ in ACTIONS}:
        raise ValueError("Choose an action.")
    lender: Optional[dict] = None
    if action in _NEEDS_LENDER:
        if not lender_raw.isdigit():
            raise ValueError("Choose a lender for this action.")
        lender = next((l for l in _lenders(db) if l["id"] == int(lender_raw)), None)
        if lender is None:
            raise ValueError("That lender no longer exists. Open the form again.")
    name = lender["name"] if lender else value

    if action == "add_lender":
        if not value:
            raise ValueError("Type the lender's name in Value.")
        box = _box_from(values) if _value(values, "types_b") else None
        taken = max([l["priority"] for l in _lenders(db)], default=0) + 1
        created = admin_api.create_lender(
            body=admin_api.LenderIn(name=value, priority=taken, agreement_status="pending",
                                    takes_ground_up=bool(box and LoanType.GROUND_UP_CONSTRUCTION.value in box.loan_types)),
            _=None, db=db)
        if box is not None:
            admin_api.add_box(lender_id=created["id"], body=box, _=None, db=db)
        note = " with a box" if box else " (no box yet: it receives nothing until one is added)"
        return f"added lender *{value}* at position {taken}, agreement pending{note}"
    if action == "add_box":
        if not _value(values, "types_b"):
            raise ValueError("Pick at least one loan type for the box.")
        admin_api.add_box(lender_id=lender["id"], body=_box_from(values), _=None, db=db)
        return f"added a product box to *{name}*"
    if action in ("pause", "resume"):
        admin_api.update_lender(lender_id=lender["id"], body=admin_api.LenderPatch(active=action == "resume"), _=None, db=db)
        return f"{'resumed' if action == 'resume' else 'paused'} *{name}*"
    if action == "agreement":
        if value not in AGREEMENT_STATUS_CHOICES:
            raise ValueError(f"Value must be one of: {', '.join(AGREEMENT_STATUS_CHOICES)}.")
        admin_api.update_lender(lender_id=lender["id"], body=admin_api.LenderPatch(agreement_status=value), _=None, db=db)
        return f"set *{name}* agreement status to {value}"
    if action == "seat_cap":
        cap = None if value.lower() in ("none", "no cap") else _int_or_none(value, "Seat cap")
        if cap is None and value.lower() not in ("none", "no cap"):
            raise ValueError("Value must be a number, or none.")
        admin_api.update_lender(lender_id=lender["id"], body=admin_api.LenderPatch(monthly_cap=cap), _=None, db=db)
        return f"set *{name}* seat cap to {'none' if cap is None else cap}"
    if action in ("move_up", "move_down"):
        order = [l["id"] for l in _lenders(db)]
        index = order.index(lender["id"])
        target = index - 1 if action == "move_up" else index + 1
        if not 0 <= target < len(order):
            raise ValueError(f"{name} is already {'first' if action == 'move_up' else 'last'}.")
        order[index], order[target] = order[target], order[index]
        admin_api.reorder_lenders(body=admin_api.ReorderIn(ordered_ids=order), _=None, db=db)
        return f"moved *{name}* {'up' if action == 'move_up' else 'down'} to position {target + 1}"
    admin_api.make_primary(lender_id=lender["id"], _=None, db=db)
    return f"made *{name}* the primary lender"


def _post_audit(user_id: str, summary: str) -> None:
    client, channel = notify._client(), notify._channel()
    if client is None or not channel:
        logger.warning("[routing-slack] change applied but LENDING_CORA_CHANNEL or LENDING_CORA_SLACK_BOT_TOKEN is not set")
        return
    try:
        client.chat_postMessage(channel=channel, text=f":gear: Lender rules changed by <@{user_id}>: {summary}")
    except Exception as exc:  # class only
        logger.error("[routing-slack] audit post failed: %s", type(exc).__name__)


def handle_submission(payload: dict) -> dict[str, Any]:
    """Slack ``view_submission`` for the lender modal. Returns the Slack response (errors keep the modal open)."""
    user = _user_id(payload)
    if user not in allowed_user_ids():
        logger.warning("[routing-slack] user %s is not allowed to edit lender rules", user or "unknown")
        return {"response_action": "errors", "errors": {"action_b": "You are not allowed to edit lender rules."}}
    values = (payload.get("view") or {}).get("state", {}).get("values") or {}
    try:
        with lending_session() as db:
            summary = apply_change(values, db)
    except (ValueError, HTTPException, ValidationError) as exc:
        return {"response_action": "errors", "errors": {"value_b": _reason(exc)}}
    except Exception as exc:  # class only: SQL errors can embed bound values
        logger.error("[routing-slack] applying a lender change failed: %s", type(exc).__name__)
        return {"response_action": "errors", "errors": {"value_b": "Something went wrong; nothing was changed."}}
    logger.info("[routing-slack] lender change by %s: %s", user, summary.replace("*", ""))
    _post_audit(user, summary)
    return {}


def is_routing_submission(payload: dict) -> bool:
    return payload.get("type") == "view_submission" and (payload.get("view") or {}).get("callback_id") == CALLBACK_ID


def open_form(client: Any, payload: dict) -> None:
    """The ``/lenders`` slash command: open the form for an allowed user, otherwise say no (privately)."""
    user = str(payload.get("user_id") or "")
    if user not in allowed_user_ids():
        logger.warning("[routing-slack] user %s is not allowed to open the lender form", user or "unknown")
        try:
            client.chat_postEphemeral(channel=payload.get("channel_id", ""), user=user,
                                      text="Only approved users can edit lender rules.")
        except Exception as exc:  # class only
            logger.error("[routing-slack] denial notice failed: %s", type(exc).__name__)
        return
    try:
        with lending_session() as db:
            view = build_form_view(_lenders(db))
        client.views_open(trigger_id=payload.get("trigger_id", ""), view=view)
    except Exception as exc:  # class only
        logger.error("[routing-slack] opening the form failed: %s", type(exc).__name__)
