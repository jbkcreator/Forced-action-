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

CLONE-v2.2 / CL3: each venture sends through its own Instantly campaign, so
the unsubscribe pull is scoped to the calling venture's campaign id
(src.utils.venture_config), not the fleet-wide settings value. Reading only
settings.relay_instantly_campaign_id here would mean venture B's
unsubscribes/bounces never reach email_opt_outs, and the execution guard has
nothing local to suppress them with before venture B's next sweep sends.

ponytail: re-reads every lead in the campaign each sweep. At the 20/day
ceiling the campaign holds ~600 leads/month = ~6 pages -- fine. Switch to an
Instantly unsubscribe webhook, or track a last-synced cursor, if this ever
gets slow.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Literal

from config.venture_template import DEFAULT_VENTURE_KEY
from src.core.database import get_db_context
from src.services import instantly_service as instantly
from src.services.instantly_service import map_lead_status
from src.services.email_suppression import suppress_contact
from src.utils.venture_config import get_venture_config

logger = logging.getLogger(__name__)

_SUPPRESS_ON_STATUS = {"unsubscribed", "bounced"}

# NOTE (production-execution review, finding 4; investigated WP-T2-1
# go-live review, 2026-09): Instantly's lead status vocabulary (see
# instantly_service._LEAD_STATUS_MAP) has "bounced" and "unsubscribed" but
# no distinct spam-complaint status — this poll cannot ingest complaints
# today. Do not treat this module as complaint-complete; it is a live-lane
# blocker (WP-T2-1 Done When: "complaint event ingestion: spam complaints
# auto-suppress immediately").
#
# Investigation findings (public API v2 docs, 2026-09 — re-verify against
# this account's actual plan/webhook config before building anything, since
# public docs alone can't confirm account-level availability):
#   - Instantly's documented webhook event catalog
#     (https://developer.instantly.ai/guides/webhook-events) has
#     `email_bounced` and `lead_unsubscribed` events but NO distinct
#     spam-complaint/FBL event type. Confirmed precisely, not overstated
#     (code-review finding, 2026-09 -- an earlier version of this comment
#     said "permanent... unless Instantly adds one," which is internally
#     contradictory and too narrow a claim anyway): this establishes only
#     that INSTANTLY'S OWN API doesn't expose a complaint signal today, not
#     that complaint data is unobtainable by any means. Complaint feedback
#     loops (FBL) commonly exist independent of the ESP entirely -- e.g.
#     Microsoft's JMRP delivers a per-complaint ARF report directly to a
#     registered address for Outlook/Hotmail/Live/MSN complaints, and
#     Gmail's Postmaster Tools API exposes an aggregate daily spam-rate
#     metric -- neither investigated here, both a genuinely separate path
#     from anything Instantly does or doesn't add to its API. Until one of
#     these is investigated and either wired up or ruled out, treat
#     complaint ingestion as unimplemented and undecided, not as a settled
#     dead end -- and do not infer complaint data from bounce rate, warmup
#     score, or Relay's own failure rate (those measure different things
#     and would silently misclassify).
#   - The SAME webhook catalog DOES expose `email_bounced`, which would let
#     hard-bounce suppression become immediate (webhook-driven) instead of
#     this module's current ~30-min poll -- a real, separate improvement
#     opportunity from the complaint gap above. NOT implemented here:
#     Instantly's docs state webhooks require the Hypergrowth plan ($97/mo)
#     or higher, and this codebase has no way to confirm this account's
#     actual plan/webhook configuration without live account access. Building
#     a webhook endpoint (mirroring src/api/main.py's existing Telnyx
#     message.finalized pattern at line ~5587) is a well-specified follow-up
#     once that one fact is confirmed -- not blocked on API capability
#     research anymore, only on an account-level confirmation.


@dataclass(frozen=True)
class SyncResult:
    status: Literal["synced", "not_configured"]
    count: int = 0


class SuppressionSyncFailed(RuntimeError):
    """Raised when the Instantly poll itself fails (network/API error) —
    distinct from `not_configured`, which is an expected state for a venture
    whose email channel isn't wired up yet, not a fault."""


def sync_unsubscribes(venture_key: str = DEFAULT_VENTURE_KEY) -> SyncResult:
    """Pull unsubscribed/bounced leads from `venture_key`'s Relay passthrough
    campaign into email_opt_outs (cascading to sms_opt_outs).

    Returns SyncResult(status="not_configured") if that venture's email
    channel isn't set up yet — an expected state, not an error, so the
    caller must not treat it as a failed sync (production-execution review,
    finding 3: the old `return 0` for this case was indistinguishable from
    "polled and found nothing to suppress").

    Raises SuppressionSyncFailed if the Instantly poll itself fails. The
    caller (sweep.run_sweep) must treat that as reason to defer the whole
    batch rather than send against a suppression list that may now be
    stale — the entire point of syncing before the guard recheck.
    """
    campaign_id = get_venture_config(venture_key).relay_instantly_campaign_id
    if not campaign_id:
        return SyncResult(status="not_configured")

    synced = 0
    cursor = None
    try:
        with get_db_context() as db:
            while True:
                page = instantly.list_leads(campaign_id, cursor=cursor)
                if page is None:
                    # instantly_service.list_leads() returns None for TWO
                    # different reasons it does not distinguish: the global
                    # Instantly integration being disabled/unconfigured, or a
                    # genuine mid-poll request failure (network/API error) --
                    # see its own docstring. Both look identical here, and
                    # both mean this run cannot prove what changed since the
                    # last sync. Treating either as "reached the last page"
                    # (the pre-review-fix behaviour) silently truncated the
                    # poll and reported status="synced" even when a failure
                    # happened on page 2 of 6 -- exactly the stale-suppression
                    # risk this module exists to prevent. Fail closed instead:
                    # raise so the caller (sweep.run_sweep) defers the whole
                    # batch rather than send against a partial suppression
                    # view.
                    raise SuppressionSyncFailed(
                        f"Instantly list_leads returned None for venture {venture_key} "
                        f"(campaign {campaign_id}, cursor={cursor!r}) -- integration "
                        f"disabled or request failed; cannot confirm this page synced"
                    )
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
    except Exception as exc:
        raise SuppressionSyncFailed(f"Instantly suppression poll failed for venture {venture_key}: {exc}") from exc

    return SyncResult(status="synced", count=synced)
