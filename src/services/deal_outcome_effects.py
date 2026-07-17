"""CDE-11 — subscriber-only side-effects for a DealOutcome.

The single seam every DealOutcome writer routes through for subscriber-facing
side-effects (suppression, win graphic + win story, annual-at-win push,
conversion attribution). Guarded on subscriber presence: an ownerless outcome
(founder import / public-record inferred, subscriber_id IS NULL) fires none of
them. Learning-loop autopsies (snapshot, loss autopsy) are subscriber-agnostic
and stay on the caller — they are NOT run here.

See CONTEXT.md § Outcome Confidence (CDE-11), ADR 0025.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)


def record_outcome_side_effects(outcome, sub, db) -> dict:
    """Run subscriber-only side-effects for a DealOutcome; return response bits.

    No-op for ownerless outcomes (outcome.subscriber_id IS NULL). Each effect is
    fail-soft — a failure logs and is skipped, never aborts the others.
    Returns {"graphic_url": str|None, "annual_offered": bool}.
    """
    result: dict = {"graphic_url": None, "annual_offered": False}
    if outcome.subscriber_id is None:
        return result

    is_skip = outcome.deal_size_bucket == "skip"

    try:
        from src.services.cora_suppression import create_suppression
        create_suppression(
            db,
            subscriber_id=outcome.subscriber_id,
            reason="deal_lost" if is_skip else "deal_won",
            source="deal_capture",
            source_id=outcome.id,
            notes="Auto-pause triggered by deal outcome",
            cancel_reason="deal_outcome_auto_pause",
        )
    except Exception as exc:
        logger.warning("[DealOutcomeEffects] cora suppression failed: %s", exc)

    if not is_skip:
        try:
            from src.services.win_graphic import generate as gen_graphic
            path = gen_graphic(outcome.id, db)
            if path:
                result["graphic_url"] = f"/api/win-graphic/{outcome.id}"
        except Exception as exc:
            logger.warning("[DealOutcomeEffects] win graphic gen failed: %s", exc)

        try:
            from src.services.win_autopsy import record_win_autopsy
            record_win_autopsy(outcome.id, db)
        except Exception as exc:
            logger.warning("[DealOutcomeEffects] win autopsy failed: %s", exc)

        try:
            with db.begin_nested():
                from src.services.referral_prompt_service import maybe_send_referral_prompt
                maybe_send_referral_prompt(
                    sub, db,
                    trigger_type="deal_win",
                    trigger_source_table="deal_outcomes",
                    trigger_source_id=outcome.id,
                )
        except Exception as exc:
            logger.warning("[DealOutcomeEffects] referral prompt failed: %s", exc)

    is_big = (outcome.deal_amount and outcome.deal_amount >= 10000) \
        or outcome.deal_size_bucket in ("10_25k", "25k_plus")
    if is_big and sub is not None:
        try:
            from src.tasks.annual_push import _push_annual_offer
            if _push_annual_offer(sub, "deal_win_10k", db):
                result["annual_offered"] = True
        except Exception as exc:
            logger.warning("[DealOutcomeEffects] annual push failed: %s", exc)

    try:
        from src.services.attribution_service import record_conversion_attribution
        record_conversion_attribution(
            conversion_type="deal_win_reported",
            source_table="deal_outcomes",
            source_event_id=str(outcome.id),
            subscriber_id=outcome.subscriber_id,
            occurred_at=datetime.now(timezone.utc),
            property_id=outcome.property_id,
            deal_size_bucket=outcome.deal_size_bucket,
            db=db,
        )
    except Exception:
        logger.warning("[DealOutcomeEffects] attribution failed sub=%s", outcome.subscriber_id, exc_info=True)

    return result
