"""
Price escalation task — PERMANENTLY DISABLED (2026-08-12).

Founding members keep their locked rate indefinitely. This file is retained
as a tombstone so the scheduler entry and any callers continue to resolve
without error.
"""

import logging

logger = logging.getLogger(__name__)


def run_price_escalation(dry_run: bool = False) -> dict:
    """No-op. Founding member rate escalation is permanently off."""
    logger.info("[PriceEscalation] DISABLED — no action taken.")
    return {"checked": 0, "eligible": 0, "escalated": 0, "failed": 0, "dry_run": dry_run, "disabled": True}
