"""T-05 (Runbook § 1, Principle 2): alert Josh's phone the moment a LendingFlow lead arrives.

LendingFlow leads go to Josh, not the caller floor. ``alert_josh`` handles ``LendingFlowLeadCreated`` (emitted once
per deduplicated, non-suppressed lead after its GHL contact exists) and posts to the lending ops channel with an
@-mention of each approver (``LENDING_CORA_APPROVER_USER_IDS``, Josh) and a link that opens the GHL contact
(``LENDING_GHL_APP_URL`` + ``LENDING_GHL_LOCATION_ID`` + the contact id), where he can call the lead in one tap.
The mention is what makes the Slack mobile app push it to his phone. This is the "high-priority push" until Josh asks for a dedicated push app (Q-36).

Off until ``LENDING_LEAD_ARRIVAL_ALERT_ENABLED=true``. Internal only: it never contacts the borrower, so it needs no
consent or 10DLC approval and runs during the lending emergency halt. The phone is shown as its last 4 digits only.
A post Slack provably refused (e.g. rate limited) is retried twice after a short wait; a post that may have gone out
(timeout, 5xx) is never retried, so Josh is not pinged twice. A post that still fails is logged by lead id and
queued in Cora's inbox for the ops channel, so it is surfaced instead of lost; it never affects the lead, and the
2- and 5-minute alarms still follow.
"""
from __future__ import annotations

import logging
import time
from typing import Any, Callable, Optional

from slack_sdk.errors import SlackApiError
from sqlalchemy import text

from config.settings import get_settings
from packages.agent_core.config import parse_user_ids
from src.lending.contracts import LendingFlowLeadCreated
from src.lending.cora_inbox import add_to_inbox_in_own_session
from src.lending.db import lending_session
from src.lending.slack_names import name_and_ref
from src.lending.slack_test_filter import is_hidden_lendingflow_uuid, log_skipped

logger = logging.getLogger(__name__)

# Waits before the 2nd and 3rd attempt. Short: the handler runs inline with the lead's processing.
RETRY_DELAYS_SECONDS = (1, 2)


def _slack_client() -> Any:
    from src.lending.disposition_delivery import _slack_client as lending_slack_client

    return lending_slack_client()


# Read at call time so tests swap in a fake client and never post to Slack.
_client_factory: Callable[[], Any] = _slack_client
_sleep: Callable[[float], None] = time.sleep


def _provably_not_posted(exc: Exception) -> bool:
    from src.lending.uncalled_alarm_worker import _slack_rejected

    return isinstance(exc, SlackApiError) and _slack_rejected(exc)


def ghl_contact_url(app_url: str, location_id: Optional[str], contact_id: Optional[str]) -> Optional[str]:
    """The contact's page in the GHL web/mobile app, or None when either id is missing."""
    if not (app_url and location_id and contact_id):
        return None
    return f"{app_url.rstrip('/')}/v2/location/{location_id}/contacts/detail/{contact_id}"


_LEAD_NAME = text("""
    SELECT NULLIF(btrim(concat_ws(' ', first_name, last_name)), '') FROM lending.lendingflow_leads
     WHERE lead_uuid::text = :lead_uuid
""")


def _lead_name(event: LendingFlowLeadCreated) -> Optional[str]:
    """The lead's full name for the post; a failed lookup only drops the name, never the alert."""
    try:
        with lending_session() as db:
            return db.execute(_LEAD_NAME, {"lead_uuid": str(event.lead_id)}).scalar()
    except Exception as exc:  # class only
        logger.warning("[lead-arrival-alert] lead=%s name lookup failed (%s)", event.lead_id, type(exc).__name__)
        return None


def _is_test_lead(event: LendingFlowLeadCreated) -> bool:
    try:
        with lending_session() as db:
            return is_hidden_lendingflow_uuid(db, event.lead_id)
    except Exception as exc:  # class only: a failed check posts as usual
        logger.warning("[lead-arrival-alert] lead=%s test check failed (%s)", event.lead_id, type(exc).__name__)
        return False


def alert_text(event: LendingFlowLeadCreated, mention_ids: frozenset[str], contact_url: Optional[str] = None,
               name: Optional[str] = None) -> str:
    mentions = " ".join(f"<@{user_id}>" for user_id in sorted(mention_ids))
    details = ", ".join(part for part in (
        event.loan_type.value if event.loan_type else None,
        f"${event.loan_amount:,.0f}" if event.loan_amount is not None else None,
        event.state,
    ) if part)
    return (f":rotating_light: {mentions} *New LendingFlow lead: call now.*"
            + f" {name_and_ref(name, 'lead ' + str(event.lead_id)[:8])}."
            + (f" {details}." if details else "")
            + f" Phone ending {event.phone[-4:]}. "
            + (f"<{contact_url}|Open in GHL to call>" if contact_url else "Open it in GHL to call."))


def alert_josh(event: LendingFlowLeadCreated) -> Optional[bool]:
    """Post the arrival alert. True when posted, False when Slack refused it, None when switched off or the lead is a
    test record while LENDING_SLACK_TEST_ENABLED is off."""
    settings = get_settings()
    channel = settings.lending_ops_channel
    mention_ids = parse_user_ids(settings.lending_cora_approver_user_ids)
    if not (settings.lending_lead_arrival_alert_enabled and settings.lending_slack_bot_token and channel
            and mention_ids):
        return None
    if _is_test_lead(event):
        log_skipped("lead arrival alert", event.lead_id)
        return None
    contact_url = ghl_contact_url(settings.lending_ghl_app_url, settings.lending_ghl_location_id, event.ghl_contact_id)
    name = _lead_name(event)
    body = alert_text(event, mention_ids, contact_url, name)
    for attempt in range(len(RETRY_DELAYS_SECONDS) + 1):
        try:
            _client_factory().chat_postMessage(channel=channel, text=body)
        except Exception as exc:  # class only: Slack errors can echo the message text
            if attempt < len(RETRY_DELAYS_SECONDS) and _provably_not_posted(exc):
                _sleep(RETRY_DELAYS_SECONDS[attempt])
                continue
            logger.error("[lead-arrival-alert] lead=%s alert not posted (%s)", event.lead_id, type(exc).__name__)
            _queue_unposted(event, name)
            return False
        logger.info("[lead-arrival-alert] lead=%s alert posted", event.lead_id)
        return True
    return False


def _queue_unposted(event: LendingFlowLeadCreated, name: Optional[str] = None) -> None:
    """Cora surfaces it in her inbox digest: the lead's name and id, and the last 4 digits only (never the full
    phone). Cora's inbox escapes the summary when it posts it, so the name goes in unescaped here."""
    who = name_and_ref(name, f"lead {str(event.lead_id)[:8]}", escape=False)
    add_to_inbox_in_own_session(
        event_type="lead_arrival_alert_failed", dedup_key=str(event.lead_id), target_channel="ops",
        summary=f"New LendingFlow lead alert for {who} did not post to Slack: call the lead now",
        payload={"name": name, "lead_id": str(event.lead_id), "ghl_contact_id": event.ghl_contact_id,
                 "phone_last4": event.phone[-4:]},
    )
