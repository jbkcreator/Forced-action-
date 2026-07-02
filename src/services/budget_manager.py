"""Task 6.2 — Algorithmic Variance Control Layer: the budget gate.

Decides whether a paid enrichment provider call is allowed right now, based
on the rolling paid-enrichment-spend / captured-subscription-revenue ratio
computed by src/services/revenue_telemetry.py:compute_platform_enrichment_
spend_ratio() (Task 6.1's own module — this is a real dependency on that
ledger, not a parallel query against a table this module doesn't own).

Pure decision logic: this module never calls a provider and never decides
what "free fallback" means — that's src/services/enrichment_router.py's job.
Keeping the two separate means the ratio math is independently testable from
the routing/logging side effects.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session

from config.settings import get_settings
from src.services.revenue_telemetry import compute_platform_enrichment_spend_ratio

logger = logging.getLogger(__name__)


def get_current_enrichment_spend_ratio(
    db: Session, window_days: Optional[int] = None, as_of: Optional[datetime] = None,
) -> dict:
    """Thin pass-through to revenue_telemetry's ratio computation, using the
    configured window by default. Kept as its own function (matching the
    task's requested method name) so callers depend on "the budget manager"
    rather than reaching into revenue_telemetry directly.

    as_of is test-only plumbing (see compute_platform_enrichment_spend_ratio)
    — production callers never pass it, always using real current time.
    """
    settings = get_settings()
    return compute_platform_enrichment_spend_ratio(
        db,
        window_days=window_days if window_days is not None else settings.enrichment_spend_ratio_window_days,
        as_of=as_of,
    )


def is_paid_enrichment_allowed(
    db: Session, override: bool = False, as_of: Optional[datetime] = None,
) -> tuple[bool, dict]:
    """Routing decision for paid enrichment. Returns (allowed, detail).

    detail keys: spend_cents, revenue_cents, ratio (float|None), threshold,
    window_days, routing_reason, selected_path, override_applied.

    routing_reason values: spend_ratio_safe, spend_ratio_exceeded,
    zero_revenue_guard, manual_override. missing_telemetry_guard is NOT
    raised here — a clean query returning ratio=None (revenue_cents == 0)
    is genuinely different from this function raising an exception (DB
    outage, etc.), and only the caller (EnrichmentRouter) can distinguish
    "the query succeeded and revenue is zero" from "the query itself
    failed," since no table records which case occurred.

    Edge cases (per spec): ratio exactly at the threshold is blocked
    (>=, not >). override=True always allows, regardless of ratio.
    """
    settings = get_settings()
    threshold = settings.enrichment_spend_ratio_threshold

    if override:
        metrics = get_current_enrichment_spend_ratio(db, as_of=as_of)
        return True, {
            **metrics,
            "threshold": threshold,
            "routing_reason": "manual_override",
            "selected_path": "override_paid",
            "override_applied": True,
        }

    metrics = get_current_enrichment_spend_ratio(db, as_of=as_of)
    ratio = metrics["ratio"]

    if ratio is None:
        return False, {
            **metrics,
            "threshold": threshold,
            "routing_reason": "zero_revenue_guard",
            "selected_path": "blocked",
            "override_applied": False,
        }

    if ratio >= threshold:
        return False, {
            **metrics,
            "threshold": threshold,
            "routing_reason": "spend_ratio_exceeded",
            "selected_path": "blocked",
            "override_applied": False,
        }

    return True, {
        **metrics,
        "threshold": threshold,
        "routing_reason": "spend_ratio_safe",
        "selected_path": "paid_trace",
        "override_applied": False,
    }
