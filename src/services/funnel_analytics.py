"""Funnel analytics — one count per traffic-capture stage (Part 3).

Reads the existing business-event audit trail (`webhook_events`, written by
`src.services.business_events.log_business_event`) and buckets it into the
five funnel stages: visits, sample views, checkout started, paid, rebilled.

This is deliberately separate from src/services/revenue_metrics.py and
revenue_telemetry.py, which report dollars/margin per account — this module
answers "how many people reached each stage", not "how much did we make".

Stage → event_type mapping (v1, see docs/plans funnel-analytics plan):
  visits           -> LANDING_PAGE_VIEWED
  sample_views     -> SAMPLE_LEADS_VIEWED
  checkout_started -> PAYMENT_STARTED      (bundle checkout excluded — separate flow)
  paid             -> PAYMENT_SUCCEEDED
  rebilled         -> SUBSCRIPTION_RENEWED (wallet-subscription renewals excluded — separate flow)

checkout_started's PAYMENT_STARTED emitter has fired from the frontend since
May (FirstSessionWall.jsx, DashboardPage.jsx) via POST /api/business-event —
the "no emitter yet" note once here was stale, not an open gap.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import text
from sqlalchemy.orm import Session

_STAGE_EVENT_TYPES = {
    "visits": "LANDING_PAGE_VIEWED",
    "sample_views": "SAMPLE_LEADS_VIEWED",
    "checkout_started": "PAYMENT_STARTED",
    "paid": "PAYMENT_SUCCEEDED",
    "rebilled": "SUBSCRIPTION_RENEWED",
}


def compute_funnel_counts(db: Session, frm: datetime, to: datetime) -> dict:
    """Return one count per funnel stage for the window [frm, to)."""
    rows = db.execute(
        text(
            "SELECT event_type, count(*) AS n FROM webhook_events "
            "WHERE event_type = ANY(:event_types) "
            "AND source IN ('business', 'frontend') "
            "AND processed_at >= :frm AND processed_at < :to "
            "GROUP BY event_type"
        ),
        {"event_types": list(_STAGE_EVENT_TYPES.values()), "frm": frm, "to": to},
    ).fetchall()
    counts_by_event_type = {r.event_type: r.n for r in rows}
    return {
        stage: counts_by_event_type.get(event_type, 0)
        for stage, event_type in _STAGE_EVENT_TYPES.items()
    }
