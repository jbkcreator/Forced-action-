"""
Relay unsubscribe write-back (RELAY-v2.2 sub-task R3, client Q1).

R3's execution-time DNC recheck (src.services.relay.guards) only READS
email_opt_outs. Nothing wrote a Relay recipient's unsubscribe back into it
until this module: when a prospect clicks the link this passthrough
campaign's emails now carry (src.services.relay.channels_email), that event
lands inside Instantly and stops there unless something polls for it.

Mirrors the proven pattern in src.tasks.email_campaign_sync (map_lead_status
+ suppress_contact), but that task is bound to EmailCampaign rows joined to
dbpr_contacts — the Relay passthrough campaign is neither, so it's never
seen by that sync. suppress_contact() cascades email_opt_outs -> sms_opt_outs
(ADR 0028), which is what satisfies the client's Q1 cross-contamination
requirement: a Relay opt-out also stops the Lifecycle runtime.

ponytail: re-reads every lead in the campaign each sweep. At the 20/day
ceiling the campaign holds ~600 leads/month = ~6 pages -- fine. Switch to an
Instantly unsubscribe webhook, or track a last-synced cursor, if this ever
gets slow.
"""
from __future__ import annotations

import logging

from config.settings import get_settings
from src.core.database import get_db_context
from src.services import instantly_service as instantly
from src.services.instantly_service import map_lead_status
from src.services.email_suppression import suppress_contact

logger = logging.getLogger(__name__)

_SUPPRESS_ON_STATUS = {"unsubscribed", "bounced"}


def sync_unsubscribes() -> int:
    """Pull unsubscribed/bounced leads from the Relay passthrough campaign
    into email_opt_outs (cascading to sms_opt_outs). Returns the number of
    leads suppressed this run. No-op (returns 0) if the email channel isn't
    configured yet."""
    settings = get_settings()
    campaign_id = settings.relay_instantly_campaign_id
    if not campaign_id:
        return 0

    synced = 0
    cursor = None
    with get_db_context() as db:
        while True:
            page = instantly.list_leads(campaign_id, cursor=cursor)
            if not page:
                break
            leads = page.get("leads", [])
            if not leads:
                break

            for lead in leads:
                email = (lead.get("email") or "").strip().lower()
                if not email:
                    continue
                raw_status = lead.get("interest_status") or lead.get("status") or "active"
                if map_lead_status(raw_status) in _SUPPRESS_ON_STATUS:
                    suppress_contact(db, email=email, source="instantly_sync")
                    synced += 1

            cursor = page.get("next_starting_after")
            if not cursor:
                break
        # get_db_context() commits on clean exit from this block (session_scope);
        # no manual commit needed or safe to add here.

    return synced
