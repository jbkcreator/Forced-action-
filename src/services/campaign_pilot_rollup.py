"""Pilot rollup: reach / reply / booked / wrong person / paid off, per campaign.

Read-only, one query over existing tables. Each event is credited to the
campaign that owned the person (lead_campaign_assignments) when the event
happened, so a preempted campaign keeps the results it earned. Measures are
distinct people:

  reach         relay touch sent, or dial-list call made on a linked property
  reply         any classified inbound reply (fa_max_concierge_log)
  booked        pending or confirmed booking
  wrong person  concierge `wrong_person`, or a lost outcome coded wrong_contact
  paid off      a lost outcome coded loan_paid_off

Dial touches and outcomes are keyed by property / opportunity thread, so they
reach a person through fa_max_property_associations and relay thread ids.
"""
from __future__ import annotations

from dataclasses import dataclass

import logging

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from config.lead_ownership import CAMPAIGN_PRIORITY, FA_MAX_ENGINE_CAMPAIGNS

logger = logging.getLogger(__name__)

MEASURES = ("reach", "reply", "booked", "wrong_person", "paid_off")

_ROLLUP_SQL = """
WITH owners AS (
    SELECT person_id, campaign, created_at AS from_ts,
           COALESCE(ended_at, 'infinity'::timestamptz) AS to_ts
    FROM lead_campaign_assignments
    WHERE status IN ('active', 'preempted') AND campaign = ANY(:campaigns)
),
props AS (
    SELECT DISTINCT a.person_id, a.property_id
    FROM fa_max_property_associations a JOIN owners o ON o.person_id = a.person_id
),
threads AS (
    SELECT q.person_id, q.thread_id
    FROM relay_approval_queue q JOIN owners o ON o.person_id = q.person_id
    WHERE q.thread_id IS NOT NULL
    UNION
    SELECT p.person_id, t.opportunity_thread_id
    FROM dial_list_touch t JOIN props p ON p.property_id = t.property_id
    WHERE t.opportunity_thread_id IS NOT NULL
),
events AS (
    SELECT q.person_id, 'reach' AS kind, COALESCE(q.decided_at, q.created_at) AS ts
    FROM relay_approval_queue q JOIN owners o ON o.person_id = q.person_id
    WHERE q.status = 'sent'
    UNION ALL
    SELECT p.person_id, 'reach', t.touched_at
    FROM dial_list_touch t JOIN props p ON p.property_id = t.property_id
    WHERE t.action = 'dial_called'
    UNION ALL
    SELECT c.person_id, 'reply', c.created_at
    FROM fa_max_concierge_log c JOIN owners o ON o.person_id = c.person_id
    WHERE c.classification IS NOT NULL
    UNION ALL
    SELECT c.person_id, 'wrong_person', c.created_at
    FROM fa_max_concierge_log c JOIN owners o ON o.person_id = c.person_id
    WHERE c.classification = 'wrong_person'
    UNION ALL
    SELECT b.person_id, 'booked', b.created_at
    FROM fa_max_bookings b JOIN owners o ON o.person_id = b.person_id
    WHERE b.status IN ('pending', 'confirmed')
    UNION ALL
    SELECT th.person_id,
           CASE oc.reason_code WHEN 'wrong_contact' THEN 'wrong_person' ELSE 'paid_off' END,
           oc.created_at
    FROM agent_lane_opportunity_outcomes oc JOIN threads th ON th.thread_id = oc.opportunity_thread_id
    WHERE oc.outcome = 'lost' AND oc.reason_code IN ('wrong_contact', 'loan_paid_off')
)
SELECT o.campaign,
       COUNT(DISTINCT e.person_id) FILTER (WHERE e.kind = 'reach')        AS reach,
       COUNT(DISTINCT e.person_id) FILTER (WHERE e.kind = 'reply')        AS reply,
       COUNT(DISTINCT e.person_id) FILTER (WHERE e.kind = 'booked')       AS booked,
       COUNT(DISTINCT e.person_id) FILTER (WHERE e.kind = 'wrong_person') AS wrong_person,
       COUNT(DISTINCT e.person_id) FILTER (WHERE e.kind = 'paid_off')     AS paid_off
FROM owners o
JOIN events e ON e.person_id = o.person_id AND e.ts >= o.from_ts AND e.ts < o.to_ts
GROUP BY o.campaign
"""


@dataclass(frozen=True)
class CampaignRollup:
    campaign: str
    reach: int = 0
    reply: int = 0
    booked: int = 0
    wrong_person: int = 0
    paid_off: int = 0


def lead_campaigns() -> tuple[str, ...]:
    """Campaigns owned through lead_campaign_assignments (not the FA Max engine)."""
    return tuple(c for c in CAMPAIGN_PRIORITY if c not in FA_MAX_ENGINE_CAMPAIGNS)


def pilot_rollup(session: Session) -> list[CampaignRollup]:
    """One row per lead campaign, in priority order; zeros before any sends."""
    campaigns = lead_campaigns()
    try:
        rows = session.execute(text(_ROLLUP_SQL), {"campaigns": list(campaigns)}).mappings().all()
    except SQLAlchemyError:
        logger.exception("Pilot rollup query failed for campaigns %s", campaigns)
        raise
    by_campaign = {r["campaign"]: CampaignRollup(**dict(r)) for r in rows}
    return [by_campaign.get(c, CampaignRollup(c)) for c in campaigns]


def format_rollup(rows: list[CampaignRollup]) -> str:
    header = f"{'campaign':<26}" + "".join(f"{m:>14}" for m in MEASURES)
    lines = [header] + [
        f"{r.campaign:<26}" + "".join(f"{getattr(r, m):>14}" for m in MEASURES) for r in rows
    ]
    return "\n".join(lines)
