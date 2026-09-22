"""WP-T2-11 — context assembly (I/O layer).

assemble(opportunity_id, db) -> RouterContext
assemble_batch(ids, db) -> list[RouterContext]

Gathers all inputs needed by classify(): opportunity row, ARV/confidence,
lender box eligibility, suppression state, contact state. No classification
logic here — pure I/O.

Suppression proxy: a person is considered suppressed when they have no
fa_max_person_consent row with consented=true. Full send-layer suppression_reason()
check (which needs an actual email/phone identifier) requires contact identifiers
to be stored on the person record; that link is a WP-1 spine dependency.
"""
from __future__ import annotations

import logging
from datetime import date
from decimal import Decimal
from typing import List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from .models import RouterContext

logger = logging.getLogger(__name__)


def assemble(opportunity_id: str, db: Session) -> Optional[RouterContext]:
    """Assemble context for one opportunity. Returns None if not found."""
    rows = assemble_batch([opportunity_id], db)
    return rows[0] if rows else None


def assemble_batch(ids: List[str], db: Session) -> List[RouterContext]:
    """Batch-friendly assembly — one SQL round-trip per logical query set."""
    if not ids:
        return []

    id_list = list(dict.fromkeys(ids))  # deduplicate, preserve order

    # 1. Opportunity + person rows (single query)
    opp_rows = db.execute(
        text("""
            SELECT
                o.opportunity_id::text,
                o.person_id::text,
                o.outcome,
                o.opportunity_type,
                o.loan_amount_cents,
                o.maturity_months,
                o.expected_need_date
            FROM fa_max_opportunities o
            WHERE o.opportunity_id = ANY(CAST(:ids AS uuid[]))
        """),
        {"ids": id_list},
    ).mappings().all()

    if not opp_rows:
        return []

    person_ids = [r["person_id"] for r in opp_rows]
    opp_by_id = {r["opportunity_id"]: r for r in opp_rows}

    # 2. Suppression: any consented channel per person
    consent_rows = db.execute(
        text("""
            SELECT person_id::text, bool_or(consented) AS any_consented
            FROM fa_max_person_consent
            WHERE person_id = ANY(CAST(:pids AS uuid[]))
            GROUP BY person_id
        """),
        {"pids": person_ids},
    ).mappings().all()
    consented_by_person = {r["person_id"]: r["any_consented"] for r in consent_rows}

    # 3. Contact state: any interaction per person
    interaction_rows = db.execute(
        text("""
            SELECT DISTINCT person_id::text
            FROM fa_max_interactions
            WHERE person_id = ANY(CAST(:pids AS uuid[]))
        """),
        {"pids": person_ids},
    ).mappings().all()
    has_interaction_set = {r["person_id"] for r in interaction_rows}

    # 4. ARV results: latest computed ARV per opportunity property
    arv_rows = db.execute(
        text("""
            SELECT
                op.opportunity_id::text,
                a.low,
                a.high,
                a.point,
                a.confidence
            FROM fa_max_opportunity_properties op
            LEFT JOIN fa_max_arv_results a ON a.property_id = op.property_id
                AND a.status = 'computed'
            WHERE op.opportunity_id = ANY(CAST(:ids AS uuid[]))
        """),
        {"ids": id_list},
    ).mappings().all()
    arv_by_opp: dict[str, dict] = {}
    for r in arv_rows:
        oid = r["opportunity_id"]
        if oid not in arv_by_opp:
            arv_by_opp[oid] = dict(r)

    # 5. Property assessed values + last sale for dial candidate
    prop_rows = db.execute(
        text("""
            SELECT
                op.opportunity_id::text,
                f.assessed_value_mkt,
                f.last_sale_price,
                bp.opportunity_type AS opp_type_hint
            FROM fa_max_opportunity_properties op
            LEFT JOIN financials f ON f.property_id = op.property_id
            LEFT JOIN fa_max_opportunities bp ON bp.opportunity_id = op.opportunity_id
            WHERE op.opportunity_id = ANY(CAST(:ids AS uuid[]))
        """),
        {"ids": id_list},
    ).mappings().all()
    prop_by_opp: dict[str, dict] = {}
    for r in prop_rows:
        oid = r["opportunity_id"]
        if oid not in prop_by_opp:
            prop_by_opp[oid] = dict(r)

    # 6. Lender box eligibility (WP-T2-10) — lazy import to avoid circular deps
    eligibility_by_opp = _evaluate_lender_box_batch(id_list, opp_by_id, prop_by_opp, db)

    today = date.today()
    results: List[RouterContext] = []

    for oid in id_list:
        row = opp_by_id.get(oid)
        if not row:
            continue

        person_id = row["person_id"]
        arv_data = arv_by_opp.get(oid, {})
        prop_data = prop_by_opp.get(oid, {})
        elig = eligibility_by_opp.get(oid)

        arv_point = arv_data.get("point")
        arv_val = Decimal(str(arv_point)) if arv_point is not None else None
        arv_conf = arv_data.get("confidence")  # "high" | "low" | None

        suppressed = not consented_by_person.get(person_id, False)

        triggers = _infer_triggers(row)
        is_builder = row["opportunity_type"] == "construction"

        results.append(RouterContext(
            opportunity_id=oid,
            person_id=person_id,
            outcome=row["outcome"],
            opportunity_type=row["opportunity_type"],
            loan_amount_cents=row["loan_amount_cents"],
            maturity_months=row["maturity_months"],
            expected_need_date=row["expected_need_date"],
            lender_box_status=elig.status if elig else "uncertain",
            lender_box_fail_reasons=elig.fail_reasons if elig else [],
            lender_box_uncertain_flags=elig.uncertain_flags if elig else [],
            arv=arv_val,
            arv_confidence=arv_conf if arv_conf in ("high", "low") else None,
            dial_candidate_assessed_value=(
                Decimal(str(prop_data["assessed_value_mkt"]))
                if prop_data.get("assessed_value_mkt") else None
            ),
            dial_candidate_last_sale_price=(
                Decimal(str(prop_data["last_sale_price"]))
                if prop_data.get("last_sale_price") else None
            ),
            dial_candidate_arv=arv_val,
            dial_candidate_max_ltc=Decimal("0.85") if is_builder else None,
            dial_candidate_loan_override=(
                Decimal(str(row["loan_amount_cents"])) / 100
                if row["loan_amount_cents"] else None
            ),
            dial_candidate_loan_override_confidence="low" if row["loan_amount_cents"] else None,
            dial_candidate_triggers=triggers,
            dial_candidate_intent_tier=None,
            dial_candidate_is_builder=is_builder,
            dial_candidate_urgency_date=(
                row["expected_need_date"].date()
                if row["expected_need_date"] else None
            ),
            suppressed=suppressed,
            has_interaction=person_id in has_interaction_set,
            as_of=today,
        ))

    return results


def _infer_triggers(opp_row) -> list[str]:
    """Map opportunity_type to a dial-list trigger for scoring."""
    _MAP = {
        "construction": "builder",
        "rehab": "permits_no_financing",
        "acquisition": "financing_intent",
        "extension": "maturities",
        "refinance": "financing_intent",
        "dscr_takeout": "financing_intent",
        "repeat": "financing_intent",
    }
    trigger = _MAP.get(opp_row["opportunity_type"], "financing_intent")
    return [trigger]


def _evaluate_lender_box_batch(
    ids: List[str],
    opp_by_id: dict,
    prop_by_opp: dict,
    db: Session,
) -> dict:
    """Call Lender Box evaluate() for each opportunity. Returns {opp_id: EligibilityResult}."""
    try:
        from src.services.lender_box import DealInput, evaluate
    except ImportError:
        logger.warning("lender_box module not available — all opps will be uncertain")
        return {}

    results = {}
    for oid in ids:
        row = opp_by_id.get(oid)
        prop = prop_by_opp.get(oid, {})
        if not row:
            continue
        try:
            deal = DealInput(
                property_type=row.get("opportunity_type", "acquisition"),
                state="FL",
                proposed_loan_amount=Decimal(str(row["loan_amount_cents"])) / 100
                if row["loan_amount_cents"] else Decimal("0"),
                purchase_price=None,
                rehab_estimate=None,
                arv=Decimal(str(prop["assessed_value_mkt"])) if prop.get("assessed_value_mkt") else None,
                borrower_prior_loans=None,
            )
            results[oid] = evaluate(deal, db)
        except Exception:
            logger.exception("lender_box.evaluate failed for opportunity %s", oid)
    return results
