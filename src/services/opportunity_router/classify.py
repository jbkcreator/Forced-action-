"""WP-T2-11 — pure color classification.

classify(context, config) -> RoutingDecision

No I/O, no DB, no network. Deterministic and fully unit-testable.
Order: terminal outcome → suppressed → out_of_box → yellow gaps → green/yellow.
"""
from __future__ import annotations

from decimal import Decimal

from ..dial_list.config import DEFAULT_CONFIG
from ..dial_list.models import DialCandidate
from ..dial_list.rank import score_candidate
from .models import (
    GyrColor,
    GyrQueue,
    RedReason,
    RouterConfig,
    RouterContext,
    RoutingDecision,
    YellowReason,
)

_TERMINAL_OUTCOME_MAP = {
    "funded": RedReason.ALREADY_FUNDED.value,
    "dead": RedReason.DEAD_SIGNAL.value,
    "recycled": RedReason.RECYCLED.value,
    "referred": RedReason.REFERRED.value,
}


def _build_dial_candidate(ctx: RouterContext) -> DialCandidate:
    from ..dial_list.models import DialCandidate as DC, TriggerType
    triggers = [t for t in ctx.dial_candidate_triggers if t]
    if not triggers:
        triggers = ["financing_intent"]
    return DC(
        property_id=0,
        opportunity_id=ctx.opportunity_id,
        triggers=triggers,  # type: ignore[arg-type]
        intent_tier=ctx.dial_candidate_intent_tier,  # type: ignore[arg-type]
        expected_loan_override=ctx.dial_candidate_loan_override,
        expected_loan_override_confidence=ctx.dial_candidate_loan_override_confidence,  # type: ignore[arg-type]
        arv=ctx.dial_candidate_arv,
        max_ltc=ctx.dial_candidate_max_ltc,
        assessed_value_mkt=ctx.dial_candidate_assessed_value,
        last_sale_price=ctx.dial_candidate_last_sale_price,
        is_builder=ctx.dial_candidate_is_builder,
        urgency_date=ctx.dial_candidate_urgency_date,
    )


def _expected_revenue_cents(ctx: RouterContext) -> int:
    candidate = _build_dial_candidate(ctx)
    revenue = score_candidate(candidate, ctx.as_of, DEFAULT_CONFIG)
    return int(revenue * 100)


def classify(ctx: RouterContext, config: RouterConfig) -> RoutingDecision:
    """Classify one opportunity. First match wins for red; else green/yellow."""
    rev_cents = _expected_revenue_cents(ctx)

    # 1. Red — terminal outcome
    if ctx.outcome != "open":
        reason = _TERMINAL_OUTCOME_MAP.get(ctx.outcome, ctx.outcome)
        return RoutingDecision(
            color=GyrColor.RED,
            expected_revenue_cents=rev_cents,
            reason_codes=[reason],
            disqualifying_rule=None,
            queue=None,  # audit-only, not posted to EXCEPTIONS
        )

    # 2. Red — borrower suppressed (no deliverable channel)
    if ctx.suppressed:
        return RoutingDecision(
            color=GyrColor.RED,
            expected_revenue_cents=rev_cents,
            reason_codes=[RedReason.BORROWER_SUPPRESSED.value],
            disqualifying_rule=None,
            queue="EXCEPTIONS",
        )

    # 3. Red — out of box with no exception path
    if ctx.lender_box_status == "out_of_box":
        return RoutingDecision(
            color=GyrColor.RED,
            expected_revenue_cents=rev_cents,
            reason_codes=[RedReason.OUT_OF_BOX.value] + ctx.lender_box_fail_reasons,
            disqualifying_rule=ctx.lender_box_fail_reasons[0] if ctx.lender_box_fail_reasons else None,
            queue="EXCEPTIONS",
        )

    # 4. Yellow — near/uncertain: collect all applicable reason codes
    yellow_reasons: list[str] = []

    if ctx.lender_box_status == "uncertain":
        if "program_mismatch" in " ".join(ctx.lender_box_uncertain_flags).lower():
            yellow_reasons.append(YellowReason.PROGRAM_MISMATCH_EXCEPTION_POSSIBLE.value)

    if ctx.arv is None or ctx.arv <= Decimal("0"):
        yellow_reasons.append(YellowReason.MISSING_ARV.value)
    elif ctx.arv_confidence == "low":
        yellow_reasons.append(YellowReason.LOW_ARV_CONFIDENCE.value)

    if not ctx.has_interaction:
        yellow_reasons.append(YellowReason.BORROWER_NOT_CONTACTED.value)

    if ctx.lender_box_status == "uncertain":
        return RoutingDecision(
            color=GyrColor.YELLOW,
            expected_revenue_cents=rev_cents,
            reason_codes=yellow_reasons,
            disqualifying_rule=None,
            queue="MONEY",
        )

    # 5. Green (in_box) — check revenue floor
    if ctx.lender_box_status == "in_box":
        if rev_cents >= config.green_min_expected_revenue_cents and not yellow_reasons:
            return RoutingDecision(
                color=GyrColor.GREEN,
                expected_revenue_cents=rev_cents,
                reason_codes=[],
                disqualifying_rule=None,
                queue="MONEY",
            )
        # In-box but below floor or has data gaps → yellow (near-box, worth exception look)
        reasons = yellow_reasons or [YellowReason.BELOW_REVENUE_FLOOR.value]
        return RoutingDecision(
            color=GyrColor.YELLOW,
            expected_revenue_cents=rev_cents,
            reason_codes=reasons,
            disqualifying_rule=None,
            queue="MONEY",
        )

    # Fallback: uncertain with no specific gap found → yellow
    return RoutingDecision(
        color=GyrColor.YELLOW,
        expected_revenue_cents=rev_cents,
        reason_codes=yellow_reasons or [YellowReason.PROGRAM_MISMATCH_EXCEPTION_POSSIBLE.value],
        disqualifying_rule=None,
        queue="MONEY",
    )
