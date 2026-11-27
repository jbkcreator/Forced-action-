"""R-1's Slack surface, plugged into Cora's Socket Mode listener (``SlackSession(extension=...)``).

Cora's Slack app owns the channel, the bot and the socket, so everything R-1 does in Slack goes through it: the
routing posts are posted by her bot (``notify``), and the clicks, forms and the ``/lenders`` slash command that come
back arrive here. Nothing is served over HTTP.

  * ``respond`` runs before Slack is acked, so a form's validation errors can ride in the ack.
  * ``handle`` runs after the ack: the ``/lenders`` command, and a click on Decline / Approved / Funded.

P-1's ``/partners`` command and form (``src/lending/partner_slack.py``) ride on the same hook, and so do P3.3's Milo
Confirm / Cancel clicks (``src/lending/milo/slack_card.py``) and the secondary lender package card
(``src/lending/packaging/slack_card.py``).

Anything that is not R-1's or P-1's is left alone (returns None / False), so Cora's own approval cards and chat keep
working.
"""
from __future__ import annotations

import logging
import threading
from typing import Any, Callable, Optional

from src.lending import partner_slack
from src.lending.milo import slack_card as milo_card
from src.lending.packaging import slack_card as package_card
from src.lending.routing import slack_actions, slack_admin

logger = logging.getLogger(__name__)

COMMAND = "/lenders"


def _in_thread(fn: Callable[..., None], *args: Any) -> None:
    threading.Thread(target=fn, args=args, daemon=True, name="routing-slack").start()


def _milo_click(payload: dict[str, Any], web_client: Any) -> None:
    from src.lending.db import lending_session

    try:
        milo_card.handle_action(payload, web_client, approvers=slack_admin.allowed_user_ids(),
                                submitter=milo_card.milo_submitter(), session_factory=lending_session)
    except Exception as exc:  # class only: SQL errors embed bound parameters
        logger.error("[routing-slack] Milo card click failed (%s)", type(exc).__name__)


def _today_et():
    from datetime import datetime

    from config.lending_routing import ROUTING_TZ

    return datetime.now(ROUTING_TZ).date()


def _package_click(payload: dict[str, Any], web_client: Any) -> None:
    from src.lending.db import lending_session

    package_card.handle_action(payload, web_client, approvers=slack_admin.allowed_user_ids(),
                               session_factory=lending_session, today=_today_et())


class RoutingSlack:
    def __init__(self, web_client: Any, run_later: Callable[..., None] = _in_thread) -> None:
        self._web = web_client
        self._later = run_later

    def respond(self, request_type: str, payload: dict[str, Any]) -> Optional[dict[str, Any]]:
        if request_type != "interactive":
            return None
        if slack_admin.is_routing_submission(payload):
            return slack_admin.handle_submission(payload) or None
        if slack_actions.is_decline_submission(payload):
            return slack_actions.handle_decline_submission(payload, self._web, self._later) or None
        if partner_slack.is_partner_submission(payload):
            return partner_slack.handle_submission(payload) or None
        if package_card.is_package_submission(payload):
            from src.lending.db import lending_session

            return package_card.handle_submission(payload, self._web, self._later,
                                                  approvers=slack_admin.allowed_user_ids(),
                                                  session_factory=lending_session, today=_today_et()) or None
        return None

    def handle(self, request_type: str, payload: dict[str, Any]) -> bool:
        if request_type == "slash_commands":
            if payload.get("command") == COMMAND:
                slack_admin.open_form(self._web, payload)
                return True
            if payload.get("command") == partner_slack.COMMAND:
                partner_slack.open_form(self._web, payload)
                return True
            return False
        if request_type != "interactive":
            return False
        if slack_admin.is_form_action_change(payload):
            slack_admin.refresh_form(self._web, payload)
            return True
        if partner_slack.is_form_action_change(payload):
            partner_slack.refresh_form(self._web, payload)
            return True
        if slack_actions.is_routing_action(payload):
            slack_actions.handle_action(payload, self._web)
            return True
        if milo_card.is_milo_action(payload):
            self._later(_milo_click, payload, self._web)  # the Milo fill can take a while: never inside the ack
            return True
        if package_card.is_package_action(payload):
            if package_card.opens_form(payload):
                _package_click(payload, self._web)  # a form must open within Slack's trigger window
            else:
                self._later(_package_click, payload, self._web)  # re-validation reads the documents
            return True
        return (slack_admin.is_routing_submission(payload) or slack_actions.is_decline_submission(payload)
                or partner_slack.is_partner_submission(payload)
                or package_card.is_package_submission(payload))  # answered in respond
