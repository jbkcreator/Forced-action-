"""
DFY-Lite pitch builder — compiles property distress context and generates
personalized outbound copy via Claude.

All DB reads use raw SQL via session.execute(sa.text(...)). The Claude call
reuses call_claude_with_usage from claude_router so routing + cost tracking
are handled centrally.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Optional

import sqlalchemy as sa
from sqlalchemy.orm import Session

from src.services.claude_router import call_claude_with_usage

logger = logging.getLogger(__name__)

# ── Validation constants (imported by dfy_lite_router for Pydantic validators) ──

VALID_TARGET_VERTICALS: frozenset[str] = frozenset({
    "hard_money_lender", "wholesaler", "fix_and_flip", "roofer",
    "restoration_contractor", "public_adjuster", "attorney",
    "property_manager", "other",
})

VALID_PITCH_TYPES: frozenset[str] = frozenset({
    "loan_offer", "cash_buyout", "contractor_repair_help",
    "insurance_claim_help", "legal_help", "tax_or_lien_resolution", "custom",
})

VALID_OFFER_ANGLES: frozenset[str] = frozenset({
    "unlock_equity", "sell_as_is_fast", "avoid_further_costs",
    "repair_property_damage", "resolve_public_record_pressure",
    "explore_private_lending", "avoid_code_or_tax_escalation", "custom",
})

VALID_OUTPUT_FORMATS: frozenset[str] = frozenset({
    "email_subject", "email_pitch", "sms_pitch",
    "call_script", "linkedin_message", "evidence_summary",
})

DEFAULT_OUTPUT_FORMATS: list[str] = ["email_subject", "email_pitch", "sms_pitch"]
MAX_GENERATIONS_PER_PAIR: int = 3

# Routes to Sonnet via claude_router._TASK_ROUTING — no changes to claude_router needed.
PITCH_TASK_TYPE: str = "lead_analysis"
PITCH_MAX_TOKENS: int = 2048

_BANNED_PHRASES: tuple[str, ...] = (
    "I know you are in foreclosure",
    "you are delinquent",
    "you are in legal trouble",
)


# ── DB context builder ────────────────────────────────────────────────────────

def build_property_pitch_context(session: Session, property_id: int) -> dict:
    """
    Run 8 scoped SQL queries to assemble full distress context for a property.
    Raises ValueError if the property does not exist.
    """
    # 1. Core property + owner + financials (single join)
    prop_row = session.execute(
        sa.text("""
            SELECT
                p.id,
                p.address,
                p.city,
                p.state,
                p.zip,
                p.county_id,
                p.parcel_id,
                p.property_type,
                p.year_built,
                p.sq_ft,
                p.lat,
                p.lon,
                o.owner_name,
                o.mailing_address,
                o.phone_1,
                o.phone_2,
                o.email_1,
                o.email_2,
                f.assessed_value_mkt,
                f.assessed_value_tax,
                f.est_equity,
                f.est_mortgage_bal,
                f.last_sale_price,
                f.last_sale_date,
                f.total_lien_amount,
                f.annual_tax_amount
            FROM properties p
            LEFT JOIN owners     o ON o.property_id = p.id
            LEFT JOIN financials f ON f.property_id = p.id
            WHERE p.id = :property_id
        """),
        {"property_id": property_id},
    ).mappings().first()

    if prop_row is None:
        raise ValueError(f"Property {property_id} not found")

    # 2. Latest distress score
    score_row = session.execute(
        sa.text("""
            SELECT
                final_cds_score,
                lead_tier,
                distress_types,
                vertical_scores,
                score_date
            FROM distress_scores
            WHERE property_id = :property_id
            ORDER BY score_date DESC
            LIMIT 1
        """),
        {"property_id": property_id},
    ).mappings().first()

    # 3. Open / active code violations (most recent 5)
    violations = session.execute(
        sa.text("""
            SELECT
                violation_type,
                description,
                status,
                opened_date,
                record_number
            FROM code_violations
            WHERE property_id = :property_id
              AND status ILIKE 'open%'
            ORDER BY opened_date DESC NULLS LAST
            LIMIT 5
        """),
        {"property_id": property_id},
    ).mappings().all()

    # 4. Non-closed legal proceedings (most recent 5)
    proceedings = session.execute(
        sa.text("""
            SELECT
                record_type,
                case_number,
                case_status,
                filing_date,
                associated_party,
                secondary_party
            FROM legal_proceedings
            WHERE property_id = :property_id
              AND case_status NOT ILIKE 'closed%'
            ORDER BY filing_date DESC NULLS LAST
            LIMIT 5
        """),
        {"property_id": property_id},
    ).mappings().all()

    # 5. Liens sorted by amount descending (top 5)
    liens = session.execute(
        sa.text("""
            SELECT
                record_type,
                amount,
                filing_date,
                creditor
            FROM legal_and_liens
            WHERE property_id = :property_id
            ORDER BY amount DESC NULLS LAST
            LIMIT 5
        """),
        {"property_id": property_id},
    ).mappings().all()

    # 6. Tax delinquencies most recent 3 years
    tax_delinquencies = session.execute(
        sa.text("""
            SELECT
                tax_year,
                total_amount_due,
                years_delinquent,
                source_account_number
            FROM tax_delinquencies
            WHERE property_id = :property_id
            ORDER BY tax_year DESC
            LIMIT 3
        """),
        {"property_id": property_id},
    ).mappings().all()

    # 7. Most recent foreclosure
    foreclosure = session.execute(
        sa.text("""
            SELECT
                case_number,
                case_status,
                filing_date,
                plaintiff,
                judgment_amount
            FROM foreclosures
            WHERE property_id = :property_id
            ORDER BY filing_date DESC NULLS LAST
            LIMIT 1
        """),
        {"property_id": property_id},
    ).mappings().first()

    # 8. Enforcement building permits only (is_enforcement_permit = TRUE)
    enforcement_permits = session.execute(
        sa.text("""
            SELECT
                permit_type,
                description,
                status,
                issue_date,
                permit_number
            FROM building_permits
            WHERE property_id = :property_id
              AND is_enforcement_permit = TRUE
            ORDER BY issue_date DESC NULLS LAST
            LIMIT 3
        """),
        {"property_id": property_id},
    ).mappings().all()

    return {
        "property": dict(prop_row),
        "distress_score": dict(score_row) if score_row else None,
        "code_violations": [dict(r) for r in violations],
        "legal_proceedings": [dict(r) for r in proceedings],
        "liens": [dict(r) for r in liens],
        "tax_delinquencies": [dict(r) for r in tax_delinquencies],
        "foreclosure": dict(foreclosure) if foreclosure else None,
        "enforcement_permits": [dict(r) for r in enforcement_permits],
    }


# ── Distress stack builder ────────────────────────────────────────────────────

def build_distress_stack_array(context: dict) -> list[str]:
    """
    Pure transform — no DB calls.
    Converts context dict to soft-worded bullet strings for the Claude prompt.
    Missing or None signals are skipped gracefully.
    """
    bullets: list[str] = []

    score = context.get("distress_score") or {}
    if score.get("final_cds_score"):
        bullets.append(
            f"distress_score: CDS score of {score['final_cds_score']:.0f}/100 "
            f"({score.get('lead_tier', 'unknown tier')})"
        )

    for v in context.get("code_violations") or []:
        vtype = v.get("violation_type") or "code issue"
        desc = v.get("description") or ""
        detail = f" — {desc[:80]}" if desc else ""
        bullets.append(f"code_violation: Public records indicate a {vtype}{detail}")

    for p in context.get("legal_proceedings") or []:
        ctype = p.get("record_type") or "legal matter"
        bullets.append(
            f"legal_proceeding: Public records show an open {ctype} proceeding "
            f"(case {p.get('case_number', 'on file')})"
        )

    for lien in context.get("liens") or []:
        ltype = lien.get("record_type") or "lien"
        amount = lien.get("amount")
        amt_str = f" of ${float(amount):,.0f}" if amount else ""
        bullets.append(f"lien: Public records reflect a {ltype}{amt_str} recorded against the property")

    for td in context.get("tax_delinquencies") or []:
        year = td.get("tax_year") or "recent"
        amt = td.get("total_amount_due")
        amt_str = f" (${float(amt):,.0f} due)" if amt else ""
        bullets.append(f"tax_delinquency: Property tax records indicate a delinquency for {year}{amt_str}")

    fc = context.get("foreclosure")
    if fc:
        bullets.append(
            f"foreclosure: Public records indicate a foreclosure proceeding "
            f"(status: {fc.get('case_status', 'on file')})"
        )

    for ep in context.get("enforcement_permits") or []:
        ptype = ep.get("permit_type") or "enforcement permit"
        bullets.append(f"enforcement_permit: An active {ptype} has been issued on this property")

    return bullets


# ── Claude generation ─────────────────────────────────────────────────────────

def generate_pitch_with_claude(
    context: dict,
    request_options: dict,
    subscriber_id: int,
    db: Session,
) -> dict:
    """
    Build Claude messages from context + request options, call the model,
    parse and return the structured output dict.

    Raises ValueError on JSON parse failure or missing required output keys.
    """
    prop = context.get("property") or {}
    address = prop.get("address", "the property")
    owner_name = prop.get("owner_name") or "the property owner"
    vertical = request_options.get("target_vertical", "")
    pitch_type = request_options.get("pitch_type", "")
    offer_angle = request_options.get("offer_angle") or ""
    formats: list[str] = request_options.get("selected_output_formats") or DEFAULT_OUTPUT_FORMATS
    custom_instructions = request_options.get("custom_instructions") or ""

    distress_bullets = build_distress_stack_array(context)
    bullets_text = "\n".join(f"  - {b}" for b in distress_bullets) if distress_bullets else "  - No specific distress signals identified in public records"

    format_instructions = "\n".join(
        f'  "{fmt}": "<your {fmt.replace("_", " ")} here>"'
        for fmt in formats
    )
    null_formats = "\n".join(
        f'  "{fmt}": null'
        for fmt in VALID_OUTPUT_FORMATS - set(formats)
    )

    system_prompt = f"""You are a compliance-aware outbound pitch copywriter for real estate service providers.

RULES — follow strictly:
1. Use ONLY soft, public-record language. Never state facts as certain — use "may", "appears to", "public-record indicators suggest".
2. Required phrases to include where appropriate:
   - "public-record indicators"
   - "the property may benefit from"
   - "there may be an opportunity to explore options"
   - Any SMS output MUST end with "Reply STOP to opt out."
3. NEVER use these phrases:
   - "I know you are in foreclosure"
   - "you are delinquent"
   - "you are in legal trouble"
4. Do NOT mention the buyer's company name, address, license number, or any specific financial offer amounts.
5. Keep tone professional, empathetic, and helpful — not aggressive or threatening.
6. Respond with ONLY a valid JSON object. No markdown fences, no explanation text.

RESPONSE SHAPE:
{{
{format_instructions}
{null_formats}
  "metadata": {{
    "model": "claude",
    "generated_at": "<ISO timestamp>",
    "soft_wording_applied": true
  }}
}}"""

    user_message = f"""Generate outbound pitch copy for the following property and context.

PROPERTY: {address}
OWNER CONTEXT: Addressed to {owner_name}
SERVICE VERTICAL: {vertical}
PITCH TYPE: {pitch_type}
OFFER ANGLE: {offer_angle or "not specified"}

PUBLIC-RECORD DISTRESS SIGNALS:
{bullets_text}

REQUESTED OUTPUT FORMATS: {", ".join(formats)}
{f"CUSTOM INSTRUCTIONS: {custom_instructions}" if custom_instructions else ""}

Generate the JSON response now."""

    result = call_claude_with_usage(
        task_type=PITCH_TASK_TYPE,
        messages=[{"role": "user", "content": user_message}],
        system=system_prompt,
        max_tokens=PITCH_MAX_TOKENS,
        subscriber_id=subscriber_id,
        db=db,
    )

    raw_text: str = result.get("text", "")
    # Strip ```json ... ``` fencing if Claude wraps the output
    cleaned = raw_text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.split("```", 2)[-1] if cleaned.count("```") >= 2 else cleaned
        if cleaned.startswith("json"):
            cleaned = cleaned[4:]
        cleaned = cleaned.rstrip("`").strip()

    try:
        parsed: dict = json.loads(cleaned)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Claude returned non-JSON output: {exc}") from exc

    if not isinstance(parsed, dict):
        raise ValueError("Claude output was not a JSON object")

    # Inject real model metadata over the placeholder
    parsed["metadata"] = {
        "model": result.get("model", "claude"),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "soft_wording_applied": True,
        "input_tokens": result.get("input_tokens"),
        "output_tokens": result.get("output_tokens"),
        "cost_usd": result.get("cost_usd"),
    }

    return parsed
