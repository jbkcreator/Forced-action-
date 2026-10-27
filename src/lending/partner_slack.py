"""P-1 partners managed from Slack: ``/partners`` on Cora's Slack app (Socket Mode), like R-1's ``/lenders``.

One modal lists the partners and offers three actions: add a partner, issue a new link, switch a link off. Cora's
listener hands the command and the form to ``routing.slack_extension.RoutingSlack``, which calls :func:`open_form`,
:func:`refresh_form` and :func:`handle_submission` here. The checks and writes are ``partner_service``'s.

The link is a credential. After an add or a new link the form is replaced by a one-time view showing the full link
to the person who asked, and nowhere else: it is not posted, logged or stored. Only Cora's approvers
(``LENDING_CORA_APPROVER_USER_IDS``) may use the form; an empty list means nobody. Each change is posted to Cora's
channel (``LENDING_CORA_CHANNEL``) with the person who made it, without the link: that post is the audit trail.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from config.lending_partners import AGREEMENT_STATUSES, PARTNER_TYPES
from config.settings import get_settings
from packages.agent_core.redaction import slack_escape
from src.lending import partner_service
from src.lending.db import lending_session
from src.lending.partner_service import IssuedLink, PartnerError, PartnerIn
from src.lending.routing import notify
from src.lending.routing.slack_admin import allowed_user_ids

logger = logging.getLogger(__name__)

COMMAND = "/partners"
CALLBACK_ID = "partner_admin_form"

ACTIONS: tuple[tuple[str, str], ...] = (
    ("add", "Add a partner"),
    ("new_link", "Issue a new link for a partner"),
    ("revoke", "Switch off a partner's link"),
)
_ACTION_KEYS = frozenset(a for a, _ in ACTIONS)
_ADD_BLOCKS = ("name_b", "type_b", "company_b", "email_b", "phone_b", "states_b", "agreement_b", "terms_b", "sms_b")
_LINK_BLOCKS = ("partner_b", "confirm_b")
# partner_service field -> the input block its error is shown under
_FIELD_BLOCKS = {
    "name": "name_b", "partner_type": "type_b", "company": "company_b", "email": "email_b", "phone": "phone_b",
    "states": "states_b", "agreement_status": "agreement_b", "referral_terms": "terms_b", "sms_consent": "sms_b",
    "partner_id": "partner_b",
}
_CONFIRM_TEXT = {
    "new_link": "The old link stops working at once. If this partner's link was switched off, this switches it back on.",
    "revoke": "The link stops working at once. The partner record and their files stay.",
}
_VIEW_CHARS = 2700


def _slack_error(exc: Exception) -> str:
    """The exception class plus Slack's error code and messages (field paths, never values), for the log."""
    response = getattr(exc, "response", None)
    if response is None:
        return type(exc).__name__
    try:
        code = response.get("error")
        details = (response.get("response_metadata") or {}).get("messages") or []
    except Exception:  # not a Slack response
        return type(exc).__name__
    return f"{type(exc).__name__} {code} {details}" if details else f"{type(exc).__name__} {code}"


def _user_id(payload: dict) -> str:
    return str((payload.get("user") or {}).get("id") or "")


def _value(values: dict, block: str) -> str:
    cell = (values.get(block) or {}).get("v") or {}
    if cell.get("type") == "static_select":
        return str((cell.get("selected_option") or {}).get("value") or "")
    if cell.get("type") == "checkboxes":
        return ",".join(o.get("value", "") for o in cell.get("selected_options") or [])
    return str(cell.get("value") or "").strip()


def _option(value: str, label: str) -> dict:
    return {"text": {"type": "plain_text", "text": label[:75]}, "value": value}


def _input(block_id: str, label: str, element: dict, *, optional: bool = True, hint: Optional[str] = None) -> dict:
    block = {"type": "input", "block_id": block_id, "optional": optional,
             "label": {"type": "plain_text", "text": label}, "element": element}
    if hint:
        block["hint"] = {"type": "plain_text", "text": hint}
    return block


def _text_input(previous: str = "", *, multiline: bool = False) -> dict:
    element: dict = {"type": "plain_text_input", "action_id": "v", "multiline": multiline}
    if previous:
        element["initial_value"] = previous
    return element


def _select(options: list[dict], previous: str = "") -> dict:
    element: dict = {"type": "static_select", "action_id": "v", "options": options}
    chosen = next((o for o in options if o["value"] == previous), None)
    if chosen:
        element["initial_option"] = chosen
    return element


def _checkbox(label: str, previous: str = "") -> dict:
    option = _option("yes", label)
    element: dict = {"type": "checkboxes", "action_id": "v", "options": [option]}
    if previous == "yes":
        element["initial_options"] = [option]
    return element


def _listing(partners: list[dict]) -> str:
    lines = [f"• *#{p['id']}* {slack_escape(p['name'])}{' _(test)_' if p['is_test'] else ''} · {p['partner_type']}"
             f" · {p['agreement_status']} · link {p['link_state']}" for p in partners]
    while len("\n".join(lines)) > _VIEW_CHARS:  # Slack caps a section's text: drop whole partners, never half a line
        lines.pop()
    return "\n".join(lines)


def shown_blocks(action: str) -> set[str]:
    if action == "add":
        return {"action_b", *_ADD_BLOCKS}
    if action in ("new_link", "revoke"):
        return {"action_b", *_LINK_BLOCKS}
    return {"action_b"}


def _listing_text(partners: Optional[list[dict]]) -> str:
    if partners is None:
        return "*Partners*\n_Loading…_"
    listing = _listing(partners)
    return f"*Partners* (newest first)\n{listing}" if listing else "*No partners yet.*"


def build_form_view(partners: Optional[list[dict]], action: Optional[str] = None,
                    values: Optional[dict] = None) -> dict:
    """The partner form. Choosing an action rebuilds it (:func:`refresh_form`) with only that action's fields.
    ``partners=None`` is the first view, opened before the list is read (:func:`open_form`)."""
    values = values or {}
    previous = lambda block: _value(values, block)  # noqa: E731
    action = action if action in _ACTION_KEYS else None
    action_block = _input("action_b", "Action", _select([_option(a, t) for a, t in ACTIONS], action or ""),
                          optional=False)
    action_block["dispatch_action"] = True  # Slack tells us when it changes, so the fields below can follow
    blocks: list[dict] = [
        {"type": "section", "text": {"type": "mrkdwn", "text": _listing_text(partners)}},
        action_block,
    ]
    if action is None:
        blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": "Pick an action: the fields it needs appear here."}]})
    elif action == "add":
        blocks += [
            _input("name_b", "Name", _text_input(previous("name_b")), optional=False),
            _input("type_b", "Type", _select([_option(t, t) for t in PARTNER_TYPES], previous("type_b")), optional=False),
            _input("company_b", "Company", _text_input(previous("company_b"))),
            _input("email_b", "Email", _text_input(previous("email_b")),
                   hint="Needed: the partner's file updates are emailed here. Only leave it blank if you add a "
                        "phone and tick 'Partner agreed to status texts'."),
            _input("phone_b", "Phone", _text_input(previous("phone_b"))),
            _input("states_b", "States", _text_input(previous("states_b")), hint="Comma-separated, e.g. FL,GA."),
            _input("agreement_b", "Agreement status",
                   _select([_option(s, s) for s in AGREEMENT_STATUSES], previous("agreement_b") or "pending"),
                   optional=False),
            _input("terms_b", "Referral terms", _text_input(previous("terms_b"), multiline=True)),
            _input("sms_b", "Status texts", _checkbox("Partner agreed to status texts", previous("sms_b"))),
        ]
    elif partners:
        blocks += [
            _input("partner_b", "Partner", _select([_option(str(p["id"]), f"#{p['id']} {p['name']} ({p['link_state']})")
                                                    for p in partners[:100]], previous("partner_b")), optional=False),
            _input("confirm_b", "Confirm", _checkbox("I understand"), optional=False, hint=_CONFIRM_TEXT[action]),
        ]
    else:
        blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": "No partners yet: add one first."}]})
    return {"type": "modal", "callback_id": CALLBACK_ID, "title": {"type": "plain_text", "text": "Partners"},
            "submit": {"type": "plain_text", "text": "Apply"}, "close": {"type": "plain_text", "text": "Cancel"},
            "blocks": blocks}


def link_result_view(issued: IssuedLink, *, created: bool) -> dict:
    """The one place the link is shown: to the person who asked for it, once."""
    name = slack_escape(issued.name)
    heading = f"*Partner created: #{issued.partner_id} {name}*" if created else f"*New link for #{issued.partner_id} {name}*"
    lines = [heading,
             "Copy this link now and send it to the partner. It is shown only once; if it is lost, issue a new link.",
             f"```{partner_service.partner_link(issued.token)}```"]
    if issued.reused:
        lines.append("This partner was created a moment ago, so this is a fresh link; the earlier one no longer works.")
    if not get_settings().lending_partner_consent_text.strip():
        lines.append(":warning: The partner form stays closed until the consent wording "
                     "(LENDING_PARTNER_CONSENT_TEXT) is set.")
    return {"type": "modal", "title": {"type": "plain_text", "text": "Partner link"},
            "close": {"type": "plain_text", "text": "Close"},
            "blocks": [{"type": "section", "text": {"type": "mrkdwn", "text": "\n\n".join(lines)}}]}


def _partner_in(values: dict) -> PartnerIn:
    return PartnerIn(name=_value(values, "name_b"), partner_type=_value(values, "type_b"),
                     company=_value(values, "company_b"), email=_value(values, "email_b"),
                     phone=_value(values, "phone_b"), states=_value(values, "states_b"),
                     agreement_status=_value(values, "agreement_b") or "pending",
                     referral_terms=_value(values, "terms_b"), sms_consent=_value(values, "sms_b") == "yes")


def _errors(errors: dict[str, str], action: str) -> dict[str, Any]:
    """Slack refuses an error under a field that is not on the form, so such an error goes under Action."""
    shown = shown_blocks(action)
    placed: dict[str, str] = {}
    for key, message in errors.items():
        block = _FIELD_BLOCKS.get(key, key)
        placed.setdefault(block if block in shown else "action_b", message)
    return {"response_action": "errors", "errors": placed}


def _post_audit(text: str) -> None:
    client, channel = notify._client(), notify._channel()
    if client is None or not channel:
        logger.warning("[partner-slack] change applied but LENDING_CORA_CHANNEL or LENDING_CORA_SLACK_BOT_TOKEN is not set")
        return
    try:
        client.chat_postMessage(channel=channel, text=text)
    except Exception as exc:  # class only
        logger.error("[partner-slack] audit post failed: %s", _slack_error(exc))


def _created_summary(user: str, issued: IssuedLink, body: PartnerIn) -> str:
    """The audit post for a new partner: who added whom, with the details the team needs. Never the link."""
    none = "_none_"
    contact = [f"email {slack_escape(body.email)}" if body.email else "no email",
               "agreed to status texts" if body.sms_consent else "no status texts"]
    return "\n".join([
        f":handshake: *New partner added* by <@{user}>",
        f"*Partner:* #{issued.partner_id} {slack_escape(body.name)}",
        f"*Company:* {slack_escape(body.company) if body.company else none}",
        f"*Type:* {body.partner_type} · *Agreement:* {body.agreement_status}",
        f"*States:* {body.states.replace(',', ', ') if body.states else none}",
        f"*Contact:* {' · '.join(contact)}",
        f"*Referral terms:* {slack_escape(body.referral_terms) if body.referral_terms else none}",
    ])


def _check(action: str, values: dict) -> dict[str, str]:
    if action not in _ACTION_KEYS:
        return {"action_b": "Choose an action."}
    errors: dict[str, str] = {}
    if action in ("new_link", "revoke"):
        if not _value(values, "partner_b").isdigit():
            errors["partner_b"] = "Choose a partner."
        if _value(values, "confirm_b") != "yes":
            errors["confirm_b"] = "Tick the box to confirm."
    if action in ("add", "new_link") and partner_service.link_base_url() is None:
        errors["action_b"] = "Partner links are not set up yet (LENDING_PARTNER_LINK_BASE_URL). Nothing was changed."
    return errors


def handle_submission(payload: dict) -> dict[str, Any]:
    """Slack ``view_submission`` for the partner form; the return value is the ack (errors keep the form open)."""
    user = _user_id(payload)
    if user not in allowed_user_ids():
        logger.warning("[partner-slack] user %s is not allowed to manage partners", user or "unknown")
        return {"response_action": "errors", "errors": {"action_b": "You are not allowed to manage partners."}}
    values = (payload.get("view") or {}).get("state", {}).get("values") or {}
    action = _value(values, "action_b")
    problems = _check(action, values)
    if problems:
        return _errors(problems, action)
    partner_id = int(_value(values, "partner_b")) if action != "add" else 0
    issued: Optional[IssuedLink] = None
    name = ""
    try:
        with lending_session() as db:
            if action == "add":
                issued = partner_service.create(db, _partner_in(values), created_by=f"slack:{user}")
            elif action == "new_link":
                issued = partner_service.new_link(db, partner_id)
            else:
                name = partner_service.revoke(db, partner_id)
    except PartnerError as exc:
        return _errors(exc.errors, action)
    except Exception as exc:  # class only: SQL errors can embed bound values
        logger.error("[partner-slack] %s failed: %s", action, type(exc).__name__)
        return {"response_action": "errors", "errors": {"action_b": "Something went wrong; nothing was changed."}}

    # Committed by now: only from here on may the link be shown.
    if issued is None:
        logger.info("[partner-slack] revoke by %s: partner %s", user, partner_id)
        _post_audit(f":no_entry: Link switched off by <@{user}> for #{partner_id} {slack_escape(name)}. "
                    "Their record and files are unchanged.")
        return {}
    logger.info("[partner-slack] %s by %s: partner %s%s", action, user, issued.partner_id,
                " (retried create, link re-issued)" if issued.reused else "")
    label = f"#{issued.partner_id} {slack_escape(issued.name)}"
    if action == "add" and not issued.reused:
        _post_audit(_created_summary(user, issued, partner_service.clean(_partner_in(values))))
    else:
        _post_audit(f":link: New link issued by <@{user}> for {label}. The old link no longer works.")
    return {"response_action": "update", "view": link_result_view(issued, created=action == "add")}


def is_partner_submission(payload: dict) -> bool:
    return payload.get("type") == "view_submission" and (payload.get("view") or {}).get("callback_id") == CALLBACK_ID


def is_form_action_change(payload: dict) -> bool:
    actions = payload.get("actions") or []
    return (payload.get("type") == "block_actions" and (payload.get("view") or {}).get("callback_id") == CALLBACK_ID
            and bool(actions) and actions[0].get("block_id") == "action_b")


def _partners() -> list[dict]:
    with lending_session() as db:
        return partner_service.list_partners(db)


def refresh_form(client: Any, payload: dict) -> None:
    """The Action field changed: show only the fields that action needs, keeping what was already typed."""
    if _user_id(payload) not in allowed_user_ids():
        return
    view = payload["view"]
    values = (view.get("state") or {}).get("values") or {}
    action = str((payload["actions"][0].get("selected_option") or {}).get("value") or "")
    try:
        client.views_update(view_id=view["id"], view=build_form_view(_partners(), action or None, values))
    except Exception as exc:  # class only
        logger.error("[partner-slack] refreshing the form failed: %s", _slack_error(exc))


def open_form(client: Any, payload: dict) -> None:
    """The ``/partners`` slash command: open the form for an approver, otherwise say no (privately)."""
    user = str(payload.get("user_id") or "")
    logger.info("[partner-slack] /partners from %s", user or "unknown")
    if user not in allowed_user_ids():
        logger.warning("[partner-slack] user %s is not allowed to open the partner form", user or "unknown")
        try:
            client.chat_postEphemeral(channel=payload.get("channel_id", ""), user=user,
                                      text="Only approved users can manage partners.")
        except Exception as exc:  # class only
            logger.error("[partner-slack] denial notice failed: %s", _slack_error(exc))
        return
    # Slack's trigger expires 3 s after the command: open the form first, read the partners after.
    try:
        opened = client.views_open(trigger_id=payload.get("trigger_id", ""), view=build_form_view(None))
    except Exception as exc:  # class only
        logger.error("[partner-slack] opening the form failed: %s", _slack_error(exc))
        return
    try:
        # The hash makes Slack refuse this update if an action was picked meanwhile (refresh_form then shows the list).
        client.views_update(view_id=opened["view"]["id"], hash=opened["view"].get("hash"),
                            view=build_form_view(_partners()))
    except Exception as exc:  # class only: the form is open, the list just stays "Loading"
        logger.error("[partner-slack] filling in the partner list failed: %s", _slack_error(exc))
