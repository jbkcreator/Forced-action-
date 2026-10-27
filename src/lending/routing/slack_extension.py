"""R-1's Slack surface, plugged into Cora's Socket Mode listener (``SlackSession(extension=...)``).

Cora's Slack app owns the channel, the bot and the socket, so everything R-1 does in Slack goes through it: the
routing posts are posted by her bot (``notify``), and the clicks, forms and the ``/lenders`` slash command that come
back arrive here. Nothing is served over HTTP.

  * ``respond`` runs before Slack is acked, so a form's validation errors can ride in the ack.
  * ``handle`` runs after the ack: the ``/lenders`` command, and a click on Decline / Approved / Funded.

P-1's ``/partners`` command and form (``src/lending/partner_slack.py``) ride on the same hook.

Anything that is not R-1's or P-1's is left alone (returns None / False), so Cora's own approval cards and chat keep
working.
"""
from __future__ import annotations

import logging
import threading
from typing import Any, Callable, Optional

from src.lending import partner_slack
from src.lending.routing import slack_actions, slack_admin

logger = logging.getLogger(__name__)

COMMAND = "/lenders"


def _in_thread(fn: Callable[..., None], *args: Any) -> None:
    threading.Thread(target=fn, args=args, daemon=True, name="routing-slack").start()


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
        return (slack_admin.is_routing_submission(payload) or slack_actions.is_decline_submission(payload)
                or partner_slack.is_partner_submission(payload))  # answered in respond
