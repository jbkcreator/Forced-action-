"""Loss Autopsy Engine (Phase 3 A1).

Executes a structured retrospective whenever a lead fails to convert.
Gathers multi-source context, prompts Claude to classify the failure,
and persists the result to loss_autopsies for downstream learning loops.
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Optional
from uuid import UUID

from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

from src.core.models import LossAutopsy
from src.services.claude_router import call_claude_with_usage

logger = logging.getLogger(__name__)


def _jsonify(obj: Any) -> Any:
    """Recursively coerce DB row values to JSON-safe Python types."""
    if isinstance(obj, dict):
        return {k: _jsonify(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_jsonify(v) for v in obj]
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    return obj


# Primary rejection reason taxonomy — kept in sync with the tool schema below.
_REJECTION_REASONS = (
    "PRICING_TOO_HIGH",
    "COMPETITOR_WON",
    "UNDERWRITING_REJECTED",
    "CONTACT_EXHAUSTED",
    "TIMELINE_MISMATCH",
    "PROPERTY_CONDITION",
    "OWNER_UNRESPONSIVE",
    "BROKER_CONFLICT",
    "UNKNOWN",
)

_SYSTEM_PROMPT = """\
You are Lifecycle's Loss Autopsy Engine — an analytical subsystem that learns from every failed \
lead conversion to prevent scoring decay.

Given the context of a distressed-property lead that did not convert, you must:
1. Identify the single most likely primary rejection reason from the taxonomy.
2. Note any competitor rate advantage (as a decimal fraction, e.g. 0.015 = 1.5%) \
   if pricing data suggests a competitor offered a materially better rate.
3. Record any underwriting blocker verbatim from transcript or notes (LTV, structural damage, etc.).
4. Prescribe exactly ONE actionable Lifecycle behaviour adjustment for similar future leads.

Always call the record_loss_autopsy tool with your findings. Be specific and evidence-based \
— reference transcript snippets or signal values when available.

Rejection reason taxonomy:
PRICING_TOO_HIGH | COMPETITOR_WON | UNDERWRITING_REJECTED | CONTACT_EXHAUSTED | \
TIMELINE_MISMATCH | PROPERTY_CONDITION | OWNER_UNRESPONSIVE | BROKER_CONFLICT | UNKNOWN
"""

_AUTOPSY_TOOL: dict = {
    "name": "record_loss_autopsy",
    "description": "Record the structured loss autopsy classification.",
    "input_schema": {
        "type": "object",
        "properties": {
            "primary_rejection_reason": {
                "type": "string",
                "enum": list(_REJECTION_REASONS),
                "description": "The dominant reason this lead did not convert.",
            },
            "competitor_rate_delta": {
                "type": ["number", "null"],
                "description": (
                    "Fractional rate advantage held by a competitor (e.g. 0.015 = 1.5% cheaper). "
                    "Null if no competitor pricing evidence exists."
                ),
            },
            "underwriting_blocker": {
                "type": ["string", "null"],
                "description": "Verbatim underwriting decline reason from transcript/notes, if any.",
            },
            "lifecycle_behavior_adjustment": {
                "type": "string",
                "description": (
                    "One concrete action Lifecycle should take differently for similar future leads. "
                    "Start with a verb: e.g. 'Reduce urgency cadence for OWNER_UNRESPONSIVE leads "
                    "with <3 contact attempts by spacing SMS 72h apart.'"
                ),
            },
        },
        "required": ["primary_rejection_reason", "lifecycle_behavior_adjustment"],
    },
}


# ---------------------------------------------------------------------------
# Context gatherers (all pure SQL per project standards)
# ---------------------------------------------------------------------------

def _gather_property_context(property_id: int, db: Session) -> dict:
    row = db.execute(
        sa_text("""
            SELECT
                p.id,
                p.parcel_id,
                p.address,
                p.zip,
                p.county_id,
                p.property_type,
                o.owner_type,
                o.absentee_status,
                o.contact_info_confidence,
                o.contact_info_confidence_score,
                ds.final_cds_score,
                ds.lead_tier,
                ds.urgency_level,
                ds.vertical_scores,
                ds.distress_types
            FROM properties p
            LEFT JOIN owners o           ON o.property_id = p.id
            LEFT JOIN distress_scores ds ON ds.property_id = p.id
            WHERE p.id = :pid
            ORDER BY ds.score_date DESC NULLS LAST
            LIMIT 1
        """),
        {"pid": property_id},
    ).mappings().first()
    if not row:
        return {}
    return dict(row)


def _gather_deal_context(deal_outcome_id: int, db: Session) -> dict:
    row = db.execute(
        sa_text("""
            SELECT
                d.subscriber_id,
                d.deal_size_bucket,
                d.pipeline_stage,
                d.county_id,
                d.trade_vertical,
                d.days_to_close,
                d.lead_source,
                cc.objection_type,
                cc.pitch_variant,
                cc.sentiment,
                cc.topics,
                cc.call_outcome,
                cc.follow_ups,
                LEFT(cc.transcript_text, 3000) AS transcript_excerpt
            FROM deal_outcomes d
            LEFT JOIN closer_calls cc ON cc.subscriber_id = d.subscriber_id
                AND cc.started_at = (
                    SELECT MAX(cc2.started_at)
                    FROM closer_calls cc2
                    WHERE cc2.subscriber_id = d.subscriber_id
                )
            WHERE d.id = :did
        """),
        {"did": deal_outcome_id},
    ).mappings().first()
    if not row:
        return {}
    return dict(row)


def _gather_pricing_context(county_id: Optional[str], trade_vertical: Optional[str], db: Session) -> dict:
    if not county_id or not trade_vertical:
        return {}
    row = db.execute(
        sa_text("""
            SELECT county_id, trade_vertical, price_type,
                   base_price_cents, adjusted_price_cents, adjustment_pct, status
            FROM pricing_cohorts
            WHERE county_id = :county AND trade_vertical = :vertical
              AND status = 'active'
            LIMIT 1
        """),
        {"county": county_id, "vertical": trade_vertical},
    ).mappings().first()
    return dict(row) if row else {}


def _gather_lifecycle_decisions(subscriber_id: Optional[int], db: Session) -> list[dict]:
    if not subscriber_id:
        return []
    rows = db.execute(
        sa_text("""
            SELECT graph_name, terminal_status, override_reason, started_at
            FROM agent_decisions
            WHERE subscriber_id = :sid
            ORDER BY started_at DESC
            LIMIT 5
        """),
        {"sid": subscriber_id},
    ).mappings().all()
    return [dict(r) for r in rows]


def _build_context(
    property_id: Optional[int],
    deal_outcome_id: Optional[int],
    db: Session,
) -> dict:
    prop = _gather_property_context(property_id, db) if property_id else {}
    deal = _gather_deal_context(deal_outcome_id, db) if deal_outcome_id else {}
    pricing = _gather_pricing_context(
        deal.get("county_id") or prop.get("county_id"),
        deal.get("trade_vertical"),
        db,
    )
    decisions = _gather_lifecycle_decisions(deal.get("subscriber_id"), db)
    return _jsonify({
        "property": prop,
        "deal": deal,
        "active_pricing_cohort": pricing,
        "recent_lifecycle_decisions": decisions,
    })


def _format_prompt(trigger_reason: str, context: dict) -> str:
    prop = context.get("property") or {}
    deal = context.get("deal") or {}
    pricing = context.get("active_pricing_cohort") or {}
    decisions = context.get("recent_lifecycle_decisions") or []

    sections = [f"## Loss Autopsy Request\nTrigger: {trigger_reason}\n"]

    if prop:
        sections.append(
            f"### Property\n"
            f"Address: {prop.get('address')} | ZIP: {prop.get('zip')} | "
            f"County: {prop.get('county_id')} | Property Type: {prop.get('property_type')}\n"
            f"CDS Score: {prop.get('final_cds_score')} | Tier: {prop.get('lead_tier')} | "
            f"Urgency: {prop.get('urgency_level')}\n"
            f"Distress Signals: {prop.get('distress_types')}\n"
            f"Owner Type: {prop.get('owner_type')} | Absentee: {prop.get('absentee_status')} | "
            f"Contact Confidence: {prop.get('contact_info_confidence')}"
        )

    if deal:
        sections.append(
            f"### Deal Outcome\n"
            f"Bucket: {deal.get('deal_size_bucket')} | Stage: {deal.get('pipeline_stage')} | "
            f"Vertical: {deal.get('trade_vertical')} | Days to Close: {deal.get('days_to_close')}\n"
            f"Lead Source: {deal.get('lead_source')}\n"
            f"Closer Call — Objection: {deal.get('objection_type')} | "
            f"Pitch: {deal.get('pitch_variant')} | Sentiment: {deal.get('sentiment')} | "
            f"Outcome: {deal.get('call_outcome')}\n"
            f"Topics: {deal.get('topics')} | Follow-ups: {deal.get('follow_ups')}"
        )
        if deal.get("transcript_excerpt"):
            sections.append(f"### Transcript Excerpt\n{deal['transcript_excerpt']}")

    if pricing:
        sections.append(
            f"### Active Pricing Cohort\n"
            f"Price Type: {pricing.get('price_type')} | "
            f"Base: {pricing.get('base_price_cents')}¢ | "
            f"Adjusted: {pricing.get('adjusted_price_cents')}¢ | "
            f"Adj %: {pricing.get('adjustment_pct')}"
        )

    if decisions:
        lines = [
            f"  - {d.get('graph_name')} → {d.get('terminal_status')} "
            f"(override: {d.get('override_reason') or 'none'}, at {d.get('started_at')})"
            for d in decisions
        ]
        sections.append("### Recent Lifecycle Decisions\n" + "\n".join(lines))

    return "\n\n".join(sections)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_loss_autopsy(
    property_id: Optional[int],
    trigger_reason: str,
    db: Session,
    deal_outcome_id: Optional[int] = None,
    prospect_id: Optional[UUID] = None,
) -> Optional[LossAutopsy]:
    """Execute a loss autopsy and write the result to loss_autopsies.

    Returns the new LossAutopsy instance (caller must commit), or None if an
    autopsy has already been run for this deal_outcome_id (idempotency guard).
    """
    if trigger_reason not in ("CLOSED_LOST", "DECLINED", "GHOSTED_SLA"):
        logger.error("[loss_autopsy] invalid trigger_reason=%s", trigger_reason)
        return None

    if deal_outcome_id is not None:
        existing = db.execute(
            sa_text("SELECT id FROM loss_autopsies WHERE deal_outcome_id = :d"),
            {"d": deal_outcome_id},
        ).scalar_one_or_none()
        if existing is not None:
            logger.info("[loss_autopsy] already run for deal_outcome_id=%d, skipping", deal_outcome_id)
            return None

    context = _build_context(property_id, deal_outcome_id, db)
    user_message = _format_prompt(trigger_reason, context)

    try:
        result = call_claude_with_usage(
            task_type="loss_autopsy",
            messages=[{"role": "user", "content": user_message}],
            system=_SYSTEM_PROMPT,
            cache_system=True,
            max_tokens=512,
            db=db,
            tools=[_AUTOPSY_TOOL],
            tool_choice={"type": "tool", "name": "record_loss_autopsy"},
        )
    except Exception:
        logger.error(
            "[loss_autopsy] Claude call failed property_id=%s deal_outcome_id=%s",
            property_id, deal_outcome_id,
            exc_info=True,
        )
        return None

    tool_input: Optional[dict] = result.get("tool_input") or {}
    if not tool_input:
        logger.warning(
            "[loss_autopsy] no tool_input returned property_id=%s deal_outcome_id=%s text=%s",
            property_id, deal_outcome_id, result.get("text", "")[:200],
        )
        return None

    autopsy = LossAutopsy(
        property_id=property_id,
        prospect_id=prospect_id,
        deal_outcome_id=deal_outcome_id,
        trigger_reason=trigger_reason,
        primary_rejection_reason=tool_input.get("primary_rejection_reason"),
        competitor_rate_delta=tool_input.get("competitor_rate_delta"),
        underwriting_blocker=tool_input.get("underwriting_blocker"),
        lifecycle_behavior_adjustment=tool_input.get("lifecycle_behavior_adjustment"),
        raw_context=context,
        model_response={
            "tool_input": tool_input,
            "model": result.get("model"),
            "input_tokens": result.get("input_tokens"),
            "output_tokens": result.get("output_tokens"),
        },
        claude_cost_usd=result.get("cost_usd"),
    )
    db.add(autopsy)
    db.flush()

    logger.info(
        "[loss_autopsy] recorded id=%s trigger=%s reason=%s property_id=%s deal_outcome_id=%s cost=$%.6f",
        autopsy.id,
        trigger_reason,
        autopsy.primary_rejection_reason,
        property_id,
        deal_outcome_id,
        result.get("cost_usd") or 0,
    )
    return autopsy
