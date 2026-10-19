"""T-05 (Runbook § 1, Principle 2): alert Josh's phone the moment a LendingFlow lead arrives.

LendingFlow leads go to Josh, not the caller floor. ``alert_josh`` handles ``LendingFlowLeadCreated`` (emitted once
per deduplicated, non-suppressed lead after its GHL contact exists) and posts to the lending ops channel with an
@-mention of each approver (``LENDING_CORA_APPROVER_USER_IDS``, Josh). The mention is what makes the Slack mobile
app push it to his phone. This is the "high-priority push" until Josh asks for a dedicated push app (Q-36).

Off until ``LENDING_LEAD_ARRIVAL_ALERT_ENABLED=true``. Internal only: it never contacts the borrower, so it needs no
consent or 10DLC approval and runs during the lending emergency halt. The phone is shown as its last 4 digits only.
A failed post is logged by lead id and never affects the lead; the 2- and 5-minute alarms still follow.
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Optional

from config.settings import get_settings
from packages.agent_core.config import parse_user_ids
from src.lending.contracts import LendingFlowLeadCreated

logger = logging.getLogger(__name__)


def _slack_client() -> Any:
    from src.lending.disposition_delivery import _slack_client as lending_slack_client

    return lending_slack_client()


# Read at call time so tests swap in a fake client and never post to Slack.
_client_factory: Callable[[], Any] = _slack_client


def alert_text(event: LendingFlowLeadCreated, mention_ids: frozenset[str]) -> str:
    mentions = " ".join(f"<@{user_id}>" for user_id in sorted(mention_ids))
    details = ", ".join(part for part in (
        event.loan_type.value if event.loan_type else None,
        f"${event.loan_amount:,.0f}" if event.loan_amount is not None else None,
        event.state,
    ) if part)
    return (f":rotating_light: {mentions} *New LendingFlow lead: call now.*"
            + (f" {details}." if details else "")
            + f" Phone ending {event.phone[-4:]}. Lead `{event.lead_id}` (open it in GHL).")


def alert_josh(event: LendingFlowLeadCreated) -> Optional[bool]:
    """Post the arrival alert. True when posted, False when Slack refused it, None when switched off."""
    settings = get_settings()
    channel = settings.lending_ops_channel
    mention_ids = parse_user_ids(settings.lending_cora_approver_user_ids)
    if not (settings.lending_lead_arrival_alert_enabled and settings.lending_slack_bot_token and channel
            and mention_ids):
        return None
    try:
        _client_factory().chat_postMessage(channel=channel, text=alert_text(event, mention_ids))
    except Exception as exc:  # class only: Slack errors can echo the message text
        logger.error("[lead-arrival-alert] lead=%s alert not posted (%s)", event.lead_id, type(exc).__name__)
        return False
    logger.info("[lead-arrival-alert] lead=%s alert posted", event.lead_id)
    return True
