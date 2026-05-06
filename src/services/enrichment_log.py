"""
Enrichment cost logging — fa005 (2026-05-04).

One-call utility for every code path that hits a paid enrichment vendor
(BatchData skip-trace today; Twilio Lookup, IDI, etc. tomorrow). Fire-and-
forget — failures here must never break the underlying enrichment flow.

Default cost estimates live here; override per call site when the vendor
provides actual per-record pricing in its response.
"""

from __future__ import annotations

import logging
from typing import Optional

from sqlalchemy.orm import Session

from src.core.models import EnrichmentUsageLog

logger = logging.getLogger(__name__)


# Conservative wholesale estimates in US cents per successful lookup.
# Adjust when vendor pricing is renegotiated. Source these from settings/
# config if they need to change at runtime.
DEFAULT_COST_CENTS = {
    ("batchdata", "premium_transfer"):   12,   # ~$0.12/lookup retail
    ("batchdata", "premium_byol"):       12,
    ("batchdata", "lead_unlock"):        12,
    ("batchdata", "batch_skip_trace"):   12,
    ("twilio_lookup", "phone_verify"):   1,
}


def log_usage(
    db: Session,
    vendor: str,
    purpose: str,
    success: bool,
    cost_cents: Optional[int] = None,
    subscriber_id: Optional[int] = None,
    property_id: Optional[int] = None,
    target_address: Optional[str] = None,
    error: Optional[str] = None,
    request_ref: Optional[str] = None,
) -> Optional[EnrichmentUsageLog]:
    """Insert one EnrichmentUsageLog row. Best-effort — swallows errors."""
    try:
        if cost_cents is None:
            cost_cents = DEFAULT_COST_CENTS.get((vendor, purpose), 0)
        row = EnrichmentUsageLog(
            vendor=vendor,
            purpose=purpose,
            subscriber_id=subscriber_id,
            property_id=property_id,
            target_address=target_address,
            cost_cents=cost_cents if success else 0,   # don't bill on failure
            success=success,
            error=(error or "")[:255] if error else None,
            request_ref=request_ref,
        )
        db.add(row)
        db.flush()
        return row
    except Exception as exc:
        logger.warning("[EnrichmentUsage] log failed: %s", exc)
        return None
