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
import re
from dataclasses import dataclass
from typing import Any, Optional

from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.orm import Session

from config.lending_routing import AGREEMENT_STATUS_CHOICES
from config.settings import get_settings
from src.lending.contracts import LoanType
from src.lending.db import lending_session
from src.lending.routing import lender_service, notify
from src.lending.routing.lender_service import RoutingError

logger = logging.getLogger(__name__)

CALLBACK_ID = "routing_lender_form"

ACTIONS: tuple[tuple[str, str], ...] = (
    ("add_lender", "Add a lender (name in Value, box fields below)"),
    ("add_box", "Add a product box to the chosen lender"),
    ("replace_boxes", "Replace the chosen lender's boxes with the box below"),
    ("clear_boxes", "Remove all product boxes from the chosen lender"),
    ("pause", "Pause the chosen lender"),
    ("resume", "Resume the chosen lender"),
    ("agreement", "Set agreement status (Value: signed, pending, none or terminated)"),
    ("seat_cap", "Set monthly seat cap (Value: a number, or none)"),
    ("ground_up_min", "Set ground-up minimum (Value: dollars, or none)"),
    ("takes_ground_up", "Set takes ground-up (Value: yes or no)"),
    ("move_up", "Move the chosen lender up one place"),
    ("move_down", "Move the chosen lender down one place"),
    ("make_primary", "Make the chosen lender primary"),
    ("remove", "Remove the chosen lender for good (Value: type its name)"),
    ("reopen_held", "Re-route every file held with no lender match"),
    ("add_reason", "Add a decline reason (Value: its code, Label: the wording)"),
)
_NEEDS_LENDER = {a for a, _ in ACTIONS} - {"add_lender", "reopen_held", "add_reason"}
_BOX_FIELDS = ("types_b", "states_b", "min_b", "max_b", "credit_b")


def allowed_user_ids() -> frozenset[str]:
    settings = get_settings()
    raw = getattr(settings, "lending_cora_approver_user_ids", "") or ""
    return frozenset(item.strip() for item in raw.split(",") if item.strip())


def _user_id(payload: dict) -> str:
    return str((payload.get("user") or {}).get("id") or "")


def _lenders(db: Session) -> list[dict]:
    rows = db.execute(text(
        "SELECT id, name, active, is_primary, agreement_status, priority, monthly_cap,"
        "       construction_min_amount, takes_ground_up"
        "  FROM lending.lender_routing_rules ORDER BY priority ASC, id ASC")).mappings().all()
    return [dict(r) for r in rows]


def _box_ids(db: Session, lender_id: int) -> list[int]:
    return list(db.execute(text("SELECT id FROM lending.lender_product_boxes WHERE lender_id = :l ORDER BY id"),
                           {"l": lender_id}).scalars())


def _box_line(row: Any) -> str:
    types = ", ".join(t.replace("_", " ").title() for t in row.loan_types)
    states = ", ".join(row.approved_states) if row.approved_states else "all states"
    low, high = row.min_loan_amount, row.max_loan_amount
    amount = (f"${low:,}-${high:,}" if low is not None and high is not None else f"from ${low:,}" if low is not None
              else f"up to ${high:,}" if high is not None else "any amount")
    credit = f" · min credit {row.credit_floor}" if row.credit_floor else ""
    return f"{types} · {states} · {amount}{credit}"


def _boxes_by_lender(db: Session) -> dict[int, list[str]]:
    rows = db.execute(text(
        "SELECT lender_id, loan_types, approved_states, min_loan_amount, max_loan_amount, credit_floor"
        "  FROM lending.lender_product_boxes ORDER BY lender_id, id")).all()
    boxes: dict[int, list[str]] = {}
    for row in rows:
        boxes.setdefault(row.lender_id, []).append(_box_line(row))
    return boxes


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


def _listing(lenders: list[dict], boxes: dict[int, list[str]]) -> str:
    lines: list[str] = []
    for l in lenders:
        extras = [f"cap {l['monthly_cap']}" if l["monthly_cap"] is not None else "",
                  f"ground-up min ${l['construction_min_amount']:,}" if l.get("construction_min_amount") else "",
                  "takes ground-up" if l.get("takes_ground_up") else ""]
        lines.append(_label(l) + "".join(f" · {e}" for e in extras if e))
        lines += [f"      - {box}" for box in boxes.get(l["id"], [])] or ["      - no box: receives nothing"]
    return ("\n".join(lines) or "No lenders yet.")[:2800]


@dataclass(frozen=True)
class _Fields:
    """What an action needs on the form: the lender, a Value (label, example, hint), a Label, the box fields."""

    lender: bool = False
    value: Optional[tuple[str, str, str]] = None
    label: bool = False
    box: bool = False


_BOX_NOTE = "A lender with no box receives nothing."
FIELDS: dict[str, _Fields] = {
    "add_lender": _Fields(value=("Lender name", "Acme Lending", "The new lender starts as pending. Sign it afterwards."), box=True),
    "add_box": _Fields(lender=True, box=True),
    "replace_boxes": _Fields(lender=True, box=True),
    "clear_boxes": _Fields(lender=True),
    "pause": _Fields(lender=True),
    "resume": _Fields(lender=True),
    "agreement": _Fields(lender=True, value=("Agreement status", "signed", "signed, pending, none or terminated.")),
    "seat_cap": _Fields(lender=True, value=("Monthly seat cap", "15", "A whole number, or none for no cap.")),
    "ground_up_min": _Fields(lender=True, value=("Ground-up minimum ($)", "500000", "Dollars, or none.")),
    "takes_ground_up": _Fields(lender=True, value=("Takes ground-up", "yes", "yes or no.")),
    "move_up": _Fields(lender=True),
    "move_down": _Fields(lender=True),
    "make_primary": _Fields(lender=True),
    "remove": _Fields(lender=True, value=("Type the lender's name to confirm", "Spare",
                                           "Removing is for good. A lender that has routed a file cannot be removed.")),
    "reopen_held": _Fields(),
    "add_reason": _Fields(value=("Reason code", "appraisal_low", "Lowercase letters, numbers and underscores."), label=True),
}


def shown_blocks(action: str) -> set[str]:
    """The input blocks the form shows for an action (errors are always shown under one of these)."""
    spec = FIELDS.get(action)
    shown = {"action_b"}
    if spec is None:
        return shown
    if spec.lender:
        shown.add("lender_b")
    if spec.value:
        shown.add("value_b")
    if spec.label:
        shown.add("label_b")
    if spec.box:
        shown |= set(_BOX_FIELDS)
    return shown


def _text_input(placeholder: str, previous: str = "") -> dict:
    element = _text(placeholder)
    if previous:
        element["initial_value"] = previous
    return element


def build_form_view(lenders: list[dict], boxes: Optional[dict[int, list[str]]] = None,
                    action: Optional[str] = None, values: Optional[dict] = None) -> dict:
    """The lender form. Before an action is chosen only the list and the Action field show; choosing an action
    rebuilds the form (:func:`refresh_form`) with just the fields that action needs, keeping what was typed."""
    values = values or {}
    option = lambda value, label: {"text": {"type": "plain_text", "text": label[:75]}, "value": value}  # noqa: E731
    action_options = [option(a, t) for a, t in ACTIONS]
    action_element: dict = {"type": "static_select", "action_id": "v", "options": action_options}
    spec = FIELDS.get(action or "")
    if spec is not None:
        action_element["initial_option"] = next(o for o in action_options if o["value"] == action)
    action_block = _input("action_b", "Action", action_element, optional=False)
    action_block["dispatch_action"] = True  # Slack tells us when it changes, so the fields below can follow
    blocks: list[dict] = [
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*Waterfall order*\n```{_listing(lenders, boxes or {})}```"}},
        action_block,
    ]
    if spec is None:
        blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": "Pick an action: the fields it needs appear here."}]})
    else:
        if spec.lender:
            if lenders:
                lender_options = [option(str(l["id"]), _label(l)) for l in lenders[:100]]
                element = {"type": "static_select", "action_id": "v", "options": lender_options}
                chosen = next((o for o in lender_options if o["value"] == _value(values, "lender_b")), None)
                if chosen:
                    element["initial_option"] = chosen
                blocks.append(_input("lender_b", "Lender", element, optional=False))
            else:
                blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": "No lenders yet: add one first."}]})
        if spec.value:
            label, example, hint = spec.value
            blocks.append(_input("value_b", label, _text_input(example, _value(values, "value_b")), optional=False, hint=hint))
        if spec.label:
            blocks.append(_input("label_b", "Label", _text_input("Appraisal came in low", _value(values, "label_b")),
                                 optional=False, hint="The wording people will see in the Decline form."))
        if spec.box:
            type_options = [option(m.value, m.value.replace("_", " ").title()) for m in LoanType]
            chosen_types = [o for o in type_options if o["value"] in _value(values, "types_b").split(",")]
            types_element: dict = {"type": "multi_static_select", "action_id": "v", "options": type_options}
            if chosen_types:
                types_element["initial_options"] = chosen_types
            blocks += [
                _input("types_b", "Box: loan types", types_element, optional=action == "add_lender", hint=_BOX_NOTE),
                _input("states_b", "Box: states", _text_input("FL, GA (blank = all states)", _value(values, "states_b"))),
                _input("min_b", "Box: min loan ($)", _text_input(" ", _value(values, "min_b"))),
                _input("max_b", "Box: max loan ($)", _text_input(" ", _value(values, "max_b"))),
                _input("credit_b", "Box: min credit score", _text_input(" ", _value(values, "credit_b"))),
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


class FormError(ValueError):
    """A problem with one field of the form: Slack shows ``message`` under the input block ``block``."""

    def __init__(self, message: str, block: str = "value_b") -> None:
        super().__init__(message)
        self.block = block


def _int_or_none(raw: str, label: str, block: str = "value_b") -> Optional[int]:
    cleaned = raw.replace(",", "").replace("$", "").strip()
    if not cleaned:
        return None
    if not cleaned.isdigit():
        raise FormError(f"{label} must be a whole number.", block)
    return int(cleaned)


def _box_from(values: dict) -> lender_service.ProductBoxIn:
    types = [t for t in _value(values, "types_b").split(",") if t]
    states = [s for s in _value(values, "states_b").replace(" ", "").split(",") if s]
    return lender_service.ProductBoxIn(
        loan_types=types, approved_states=states or None,
        min_loan_amount=_int_or_none(_value(values, "min_b"), "Min loan", "min_b"),
        max_loan_amount=_int_or_none(_value(values, "max_b"), "Max loan", "max_b"),
        credit_floor=_int_or_none(_value(values, "credit_b"), "Min credit", "credit_b"))


# Which input block a rejected value belongs to, and what to tell the person (never the internal field name).
_FIELD_ERRORS = {
    "loan_types": ("types_b", "Pick at least one loan type."),
    "approved_states": ("states_b", "States must be two-letter codes, like FL, GA."),
    "min_loan_amount": ("min_b", "Min loan must be 0 or more."),
    "max_loan_amount": ("max_b", "Max loan must be 0 or more."),
    "credit_floor": ("credit_b", "Min credit must be between 0 and 900."),
    "name": ("value_b", "The name must be 1 to 100 characters."),
    "monthly_cap": ("value_b", "The seat cap must be a number from 0 to 100000."),
    "agreement_status": ("value_b", f"Value must be one of: {', '.join(AGREEMENT_STATUS_CHOICES)}."),
}


class FormErrors(ValueError):
    """Several problems found in one pass: ``errors`` maps each input block to its message."""

    def __init__(self, errors: dict[str, str]) -> None:
        super().__init__("; ".join(errors.values()))
        self.errors = errors


def _box_errors(values: dict) -> tuple[Optional[lender_service.ProductBoxIn], dict[str, str]]:
    """The product box the form describes (None when it has no loan type), and every problem with its fields."""
    errors: dict[str, str] = {}
    numbers: dict[str, Optional[int]] = {}
    for block, label, key in (("min_b", "Min loan", "min_loan_amount"), ("max_b", "Max loan", "max_loan_amount"),
                              ("credit_b", "Min credit", "credit_floor")):
        try:
            numbers[key] = _int_or_none(_value(values, block), label, block)
        except FormError as exc:
            errors[exc.block] = str(exc)
    types = [t for t in _value(values, "types_b").split(",") if t]
    states = [x for x in _value(values, "states_b").replace(" ", "").split(",") if x]
    if not types:
        return None, errors
    try:
        box = lender_service.ProductBoxIn(loan_types=types, approved_states=states or None, **numbers)
    except ValidationError as exc:
        for block, message in _form_errors(exc, values).items():
            errors.setdefault(block, message)
        return None, errors
    return (box if not errors else None), errors


def collect_errors(values: dict, db: Session) -> dict[str, str]:
    """Every problem with the submitted form, found in one pass so the person fixes them all at once."""
    errors: dict[str, str] = {}
    action = _value(values, "action_b")
    value = _value(values, "value_b")
    if action not in {a for a, _ in ACTIONS}:
        return {"action_b": "Choose an action."}
    lender: Optional[dict] = None
    lenders = _lenders(db)
    if action in _NEEDS_LENDER:
        raw = _value(values, "lender_b")
        if not raw.isdigit():
            errors["lender_b"] = "Choose a lender for this action."
        else:
            lender = next((l for l in lenders if l["id"] == int(raw)), None)
            if lender is None:
                errors["lender_b"] = "That lender no longer exists. Close this and open the form again."
    if action in ("add_lender", "add_box", "replace_boxes"):
        _, box_errors = _box_errors(values)
        errors.update(box_errors)
        has_types = bool(_value(values, "types_b"))
        filled = any(_value(values, b) for b in ("states_b", "min_b", "max_b", "credit_b"))
        if action in ("add_box", "replace_boxes") and not has_types:
            errors["types_b"] = "Pick at least one loan type for the box."
        elif action == "add_lender" and filled and not has_types:
            errors["types_b"] = "Pick a loan type for the box fields you filled in."
        if action == "add_lender" and not value:
            errors["value_b"] = "Type the lender's name in Value."
    elif action == "add_reason":
        code, label = value.strip(), _value(values, "label_b")
        if not code or not re.fullmatch(r"[a-z0-9_]{1,50}", code):
            errors["value_b"] = "The code is lowercase letters, numbers and underscores only, like appraisal_low (50 characters at most)."
        if not label or len(label) > 200:
            errors["label_b"] = "Type the wording people will see, up to 200 characters."
    elif action == "remove" and lender is not None and value.strip().lower() != lender["name"].lower():
        errors["value_b"] = f"Type the lender's name ({lender['name']}) in Value to confirm removing it."
    elif action == "ground_up_min" and value.lower() not in ("none", "no minimum"):
        digits = value.replace(",", "").replace("$", "").strip()
        if not digits.isdigit() or int(digits) > 1_000_000_000_000:
            errors["value_b"] = "Value must be a dollar amount, like 500000, or none."
    elif action == "takes_ground_up" and value.lower() not in ("yes", "no", "y", "n", "true", "false"):
        errors["value_b"] = "Value must be yes or no."
    elif action == "agreement" and value not in AGREEMENT_STATUS_CHOICES:
        errors["value_b"] = f"Value must be one of: {', '.join(AGREEMENT_STATUS_CHOICES)}."
    elif action == "seat_cap" and value.lower() not in ("none", "no cap"):
        digits = value.replace(",", "").strip()
        if not digits.isdigit() or int(digits) > 100_000:
            errors["value_b"] = "Value must be a whole number up to 100000, or none."
    elif action in ("move_up", "move_down") and lender is not None:
        order = [l["id"] for l in lenders]
        index = order.index(lender["id"])
        if (action == "move_up" and index == 0) or (action == "move_down" and index == len(order) - 1):
            errors["lender_b"] = f"{lender['name']} is already {'first' if action == 'move_up' else 'last'}."
    return errors


def _form_errors(exc: Exception, values: dict) -> dict[str, str]:
    """Slack's ``errors`` map for a rejected submission: each message under the field it is about."""
    if isinstance(exc, FormErrors):
        return exc.errors
    if isinstance(exc, FormError):
        return {exc.block: str(exc)}
    if isinstance(exc, ValidationError):
        errors: dict[str, str] = {}
        for item in exc.errors():
            field = str(item["loc"][0]) if item["loc"] else ""
            if field in _FIELD_ERRORS:
                block, message = _FIELD_ERRORS[field]
            elif "min_loan_amount" in str(item["msg"]):
                block, message = "max_b", "Max loan must be at least the min loan."
            else:
                block, message = "value_b", str(item["msg"]).removeprefix("Value error, ")[:200]
            errors.setdefault(block, message)
        return errors
    block = "lender_b" if _value(values, "lender_b") else "value_b"
    return {block: str(exc)[:250]}


# Changes that can let a lender take a file it could not before (Q31, Oct 23: "auto re-check held files").
_WIDENING_ACTIONS = frozenset({"add_box", "replace_boxes", "resume", "seat_cap", "ground_up_min", "takes_ground_up",
                               "make_primary"})
REOPEN_LIMIT = 200


def apply_change(values: dict, db: Session) -> str:
    """Apply one submitted form, then send no-match holds back to the router when the change can widen eligibility.

    Returns the audit sentence; raises ``ValueError``/``RoutingError`` with a clean message.
    """
    summary = _apply_change(values, db)
    action = _value(values, "action_b")
    signed = action == "agreement" and _value(values, "value_b").strip().lower() == "signed"
    if action not in _WIDENING_ACTIONS and not signed:
        return summary
    db.commit()  # the lender change stands on its own: a failed re-route below must never undo it
    from src.lending.routing.router import NO_LENDER_HOLD_REASONS, reopen_held

    try:
        count = sum(reopen_held(db, reason, REOPEN_LIMIT) for reason in NO_LENDER_HOLD_REASONS)
    except Exception as exc:  # class only: SQL errors can embed bound values
        db.rollback()
        logger.error("[routing-slack] re-routing held files after a lender change failed: %s", type(exc).__name__)
        return summary + "; held files were not re-routed (use 'Re-route every file held with no lender match')"
    if count:
        summary += f"; {count} held file(s) sent back to the router (routed within 5 minutes)"
    return summary


def _apply_change(values: dict, db: Session) -> str:
    problems = collect_errors(values, db)
    if problems:
        raise FormErrors(problems)
    action = _value(values, "action_b")
    value = _value(values, "value_b")
    lender_raw = _value(values, "lender_b")
    if action not in {a for a, _ in ACTIONS}:
        raise FormError("Choose an action.", "action_b")
    lender: Optional[dict] = None
    if action in _NEEDS_LENDER:
        if not lender_raw.isdigit():
            raise FormError("Choose a lender for this action.", "lender_b")
        lender = next((l for l in _lenders(db) if l["id"] == int(lender_raw)), None)
        if lender is None:
            raise FormError("That lender no longer exists. Close this and open the form again.", "lender_b")
    name = lender["name"] if lender else value

    if action == "add_lender":
        if not value:
            raise FormError("Type the lender's name in Value.", "value_b")
        box = _box_from(values) if _value(values, "types_b") else None
        taken = max([l["priority"] for l in _lenders(db)], default=0) + 1
        created = lender_service.create_lender(
            body=lender_service.LenderIn(name=value, priority=taken, agreement_status="pending",
                                    takes_ground_up=bool(box and LoanType.GROUND_UP_CONSTRUCTION.value in box.loan_types)),
            db=db)
        if box is not None:
            lender_service.add_box(lender_id=created["id"], body=box, db=db)
        note = " with a box" if box else " (no box yet: it receives nothing until one is added)"
        return f"added lender *{value}* at position {taken}, agreement pending{note}"
    if action == "add_box":
        if not _value(values, "types_b"):
            raise FormError("Pick at least one loan type for the box.", "types_b")
        lender_service.add_box(lender_id=lender["id"], body=_box_from(values), db=db)
        return f"added a product box to *{name}*"
    if action == "add_reason":
        reason = lender_service.add_decline_reason(
            lender_service.DeclineReasonIn(code=value.strip(), label=_value(values, "label_b")), db)
        return f"{'added' if reason['created'] else 'updated'} decline reason *{reason['code']}* ({_value(values, 'label_b')})"
    if action == "reopen_held":
        from src.lending.routing.router import NO_LENDER_HOLD_REASONS, reopen_held

        count = sum(reopen_held(db, reason, 200) for reason in NO_LENDER_HOLD_REASONS)
        return f"sent {count} held file(s) back to the router; the routing sweep routes them within 5 minutes"
    if action == "replace_boxes":
        old_boxes = _box_ids(db, lender["id"])
        lender_service.add_box(lender_id=lender["id"], body=_box_from(values), db=db)  # new box first: a refusal keeps the old ones
        for box_id in old_boxes:
            lender_service.delete_box(lender_id=lender["id"], box_id=box_id, db=db)
        return f"replaced the product boxes of *{name}* with one new box"
    if action == "clear_boxes":
        for box_id in _box_ids(db, lender["id"]):
            lender_service.delete_box(lender_id=lender["id"], box_id=box_id, db=db)
        return f"removed all product boxes from *{name}* (it receives nothing until a box is added)"
    if action == "ground_up_min":
        amount = None if value.lower() in ("none", "no minimum") else _int_or_none(value.replace("$", ""), "Ground-up minimum")
        lender_service.update_lender(lender_id=lender["id"], body=lender_service.LenderPatch(construction_min_amount=amount), db=db)
        return f"set *{name}* ground-up minimum to {'none' if amount is None else f'${amount:,}'}"
    if action == "takes_ground_up":
        flag = value.lower() in ("yes", "y", "true")
        lender_service.update_lender(lender_id=lender["id"], body=lender_service.LenderPatch(takes_ground_up=flag), db=db)
        return f"set *{name}* takes ground-up to {'yes' if flag else 'no'}"
    if action == "remove":
        lender_service.delete_lender(lender_id=lender["id"], db=db)
        return f"removed lender *{name}* for good"
    if action in ("pause", "resume"):
        lender_service.update_lender(lender_id=lender["id"], body=lender_service.LenderPatch(active=action == "resume"), db=db)
        return f"{'resumed' if action == 'resume' else 'paused'} *{name}*"
    if action == "agreement":
        if value not in AGREEMENT_STATUS_CHOICES:
            raise FormError(f"Value must be one of: {', '.join(AGREEMENT_STATUS_CHOICES)}.", "value_b")
        lender_service.update_lender(lender_id=lender["id"], body=lender_service.LenderPatch(agreement_status=value), db=db)
        return f"set *{name}* agreement status to {value}"
    if action == "seat_cap":
        cap = None if value.lower() in ("none", "no cap") else _int_or_none(value, "Seat cap")
        if cap is None and value.lower() not in ("none", "no cap"):
            raise FormError("Value must be a number, or none.", "value_b")
        lender_service.update_lender(lender_id=lender["id"], body=lender_service.LenderPatch(monthly_cap=cap), db=db)
        return f"set *{name}* seat cap to {'none' if cap is None else cap}"
    if action in ("move_up", "move_down"):
        order = [l["id"] for l in _lenders(db)]
        index = order.index(lender["id"])
        target = index - 1 if action == "move_up" else index + 1
        if not 0 <= target < len(order):
            raise FormError(f"{name} is already {'first' if action == 'move_up' else 'last'}.", "lender_b")
        order[index], order[target] = order[target], order[index]
        lender_service.reorder_lenders(body=lender_service.ReorderIn(ordered_ids=order), db=db)
        return f"moved *{name}* {'up' if action == 'move_up' else 'down'} to position {target + 1}"
    lender_service.make_primary(lender_id=lender["id"], db=db)
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


def _on_shown_fields(errors: dict[str, str], values: dict) -> dict[str, str]:
    """Slack refuses an error under a field that is not on the form, so move it to the Action field."""
    shown = shown_blocks(_value(values, "action_b"))
    moved: dict[str, str] = {}
    for block, message in errors.items():
        moved.setdefault(block if block in shown else "action_b", message)
    return moved


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
    except (ValueError, RoutingError, ValidationError) as exc:
        return {"response_action": "errors", "errors": _on_shown_fields(_form_errors(exc, values), values)}
    except Exception as exc:  # class only: SQL errors can embed bound values
        logger.error("[routing-slack] applying a lender change failed: %s", type(exc).__name__)
        return {"response_action": "errors", "errors": _on_shown_fields({"value_b": "Something went wrong; nothing was changed."}, values)}
    logger.info("[routing-slack] lender change by %s: %s", user, summary.replace("*", ""))
    _post_audit(user, summary)
    return {}


def is_routing_submission(payload: dict) -> bool:
    return payload.get("type") == "view_submission" and (payload.get("view") or {}).get("callback_id") == CALLBACK_ID


def is_form_action_change(payload: dict) -> bool:
    actions = payload.get("actions") or []
    return (payload.get("type") == "block_actions" and (payload.get("view") or {}).get("callback_id") == CALLBACK_ID
            and bool(actions) and actions[0].get("block_id") == "action_b")


def refresh_form(client: Any, payload: dict) -> None:
    """The Action field changed: show only the fields that action needs, keeping what was already typed."""
    if _user_id(payload) not in allowed_user_ids():
        return
    view = payload["view"]
    values = (view.get("state") or {}).get("values") or {}
    action = str(((payload["actions"][0]).get("selected_option") or {}).get("value") or "")
    try:
        with lending_session() as db:
            updated = build_form_view(_lenders(db), _boxes_by_lender(db), action or None, values)
        client.views_update(view_id=view["id"], view=updated)
    except Exception as exc:  # class only
        logger.error("[routing-slack] refreshing the form failed: %s", type(exc).__name__)


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
            view = build_form_view(_lenders(db), _boxes_by_lender(db))
        client.views_open(trigger_id=payload.get("trigger_id", ""), view=view)
    except Exception as exc:  # class only
        logger.error("[routing-slack] opening the form failed: %s", type(exc).__name__)
