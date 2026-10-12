"""Turn the caller's property facts into a stored soft approval PDF.

Flow: lead profile (LendingFlow, by phone) -> first lender fit -> per lender in ranked order, compute the
figures and re-run the fit with the computed loan amount -> render with the shared PDF helper -> store.
Nothing is sent to the borrower here; the GHL email is T-07's flow. Fails closed: no lead, no fit,
unconfirmed lender inputs or a render failure all store a status and no PDF.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Callable, Mapping, Optional

from config.lender_matrix import LENDER_MATRIX, LenderRules
from config.lending_soft_approval import (
    NON_BINDING_SOFT_APPROVAL_WATERMARK,
    SOFT_APPROVAL_LOAN_TYPES,
    SOFT_APPROVAL_PARAMS,
    SOFT_APPROVAL_TEMPLATE,
    SoftApprovalParams,
)
from src.lending.contracts import LenderFitEvaluator, LoanRequest
from src.lending.lender_fit import evaluate_lender_fit
from src.lending.pdf.render import render_pdf
from src.lending.soft_approval.calc import CalculationUnavailable, LenderTerms, SoftApprovalFigures, calculate_figures
from src.lending.soft_approval.facts import SoftApprovalFacts
from src.lending.soft_approval.lead_source import LeadProfileSource, default_lead_source
from src.lending.soft_approval.store import save_soft_approval

logger = logging.getLogger(__name__)

TEMPLATE_VERSION = "draft-1"  # bump when Josh's reviewed wording lands

Renderer = Callable[[dict], bytes]
RulesLookup = Callable[[str], Optional[LenderRules]]


class MatrixLenderFitEvaluator:
    """The real evaluator (T-05) behind the LenderFitEvaluator protocol."""

    def __init__(self, matrix: tuple[LenderRules, ...] = LENDER_MATRIX) -> None:
        self._matrix = matrix

    def evaluate(self, borrower_profile, loan_request):
        return evaluate_lender_fit(borrower_profile, loan_request, matrix=self._matrix)


def _matrix_rules_lookup(key: str) -> Optional[LenderRules]:
    return next((rules for rules in LENDER_MATRIX if rules.key == key), None)


def _default_renderer(context: dict) -> bytes:
    return render_pdf(SOFT_APPROVAL_TEMPLATE, context, watermark=NON_BINDING_SOFT_APPROVAL_WATERMARK)


@dataclass(frozen=True)
class SoftApprovalOutcome:
    status: str
    reason: Optional[str]
    approval_id: Optional[int]
    lender_key: Optional[str] = None


def _money(value: Decimal) -> str:
    return f"${value:,.0f}"


def _terms(rules: LenderRules, params: SoftApprovalParams) -> LenderTerms:
    return LenderTerms(
        rehab_funding_pct=params.rehab_funding_pct,
        purchase_advance_pct=params.purchase_advance_pct,
        arv_cap=rules.max_ltv,
        ltc_cap=rules.max_ltc,
        max_loan=rules.max_loan_amount,
    )


def _pick_lender(first_fit, borrower, request, facts, *, evaluator, rules_lookup, params):
    """The first ranked lender that still fits at its computed loan amount, else (None, reason)."""
    skipped_unconfirmed = False
    for fit in first_fit.fitting:
        rules = rules_lookup(fit.lender_key)
        lender_params = params.get(fit.lender_key)
        if rules is None or lender_params is None or not lender_params.confirmed():
            skipped_unconfirmed = True
            continue
        try:
            figures = calculate_figures(facts.purchase_price, facts.rehab_budget, facts.arv, _terms(rules, lender_params))
        except CalculationUnavailable as exc:
            logger.info("[soft-approval] lender %s skipped: %s", fit.lender_key, exc.reason)
            continue
        if figures.net_loan <= 0:
            continue
        refit = evaluator.evaluate(borrower, request.model_copy(update={"loan_amount": figures.net_loan}))
        if any(f.lender_key == fit.lender_key for f in refit.fitting):
            return (fit.lender_key, figures), "ok"
    return None, "terms_unconfirmed" if skipped_unconfirmed else "no_lender_fits"


def generate_soft_approval(
    db,
    *,
    phone: str,
    dialer_call_id: str,
    facts: SoftApprovalFacts,
    submitted_by: Optional[str],
    lead_source: Optional[LeadProfileSource] = None,
    evaluator: Optional[LenderFitEvaluator] = None,
    rules_lookup: RulesLookup = _matrix_rules_lookup,
    params: Mapping[str, SoftApprovalParams] = SOFT_APPROVAL_PARAMS,
    renderer: Renderer = _default_renderer,
    now: Optional[datetime] = None,
) -> SoftApprovalOutcome:
    """Evaluate, render and store. Commits nothing; the caller owns the transaction."""
    lead_source = lead_source or default_lead_source()
    evaluator = evaluator or MatrixLenderFitEvaluator()

    def finish(status, reason, *, figures=None, lender_key=None, pdf=None):
        approval_id = save_soft_approval(
            db, phone=phone, dialer_call_id=dialer_call_id, status=status, reason=reason, facts=facts,
            figures=figures, lender_key=lender_key, pdf=pdf,
            template_version=TEMPLATE_VERSION if pdf else None, submitted_by=submitted_by,
        )
        logger.info("[soft-approval] call=%s status=%s reason=%s", dialer_call_id, status, reason)
        return SoftApprovalOutcome(status, reason, approval_id, lender_key)

    lead = lead_source.for_phone(db, phone)
    if lead is None:
        return finish("no_lead", "no_lendingflow_lead")
    if not lead.has_core_fields():
        return finish("no_lead", "lead_missing_core_fields")
    if lead.loan_type not in SOFT_APPROVAL_LOAN_TYPES:
        return finish("out_of_scope", "loan_type_not_supported")

    close_date = (
        datetime.combine(facts.target_close_date, datetime.min.time(), tzinfo=timezone.utc)
        if facts.target_close_date else None
    )
    request = LoanRequest(
        loan_type=lead.loan_type, loan_amount=lead.loan_amount, state=lead.state,
        property_type=facts.property_type, purchase_price=facts.purchase_price,
        rehab_budget=facts.rehab_budget, arv=facts.arv, property_address=facts.property_address,
        target_close_date=close_date,
    )
    first_fit = evaluator.evaluate(lead.borrower, request)
    picked, reason = _pick_lender(first_fit, lead.borrower, request, facts,
                                  evaluator=evaluator, rules_lookup=rules_lookup, params=params)
    if picked is None:
        return finish("terms_unconfirmed" if reason == "terms_unconfirmed" else "no_fit", reason)

    lender_key, figures = picked
    figures_json = {
        "net_loan": str(figures.net_loan),
        "rehab_funding": str(figures.rehab_funding),
        "max_purchase_price": str(figures.max_purchase_price) if figures.max_purchase_price is not None else None,
        "full_advance_unavailable": figures.full_advance_unavailable,
        "cash_needed": str(figures.cash_needed),
    }
    context = {
        "generated_date": (now or datetime.now(timezone.utc)).strftime("%B %d, %Y"),
        "property_address": facts.property_address,
        "net_loan": _money(figures.net_loan),
        "rehab_funding": _money(figures.rehab_funding),
        "max_purchase_price": _money(figures.max_purchase_price) if figures.max_purchase_price is not None else "",
        "purchase_price": _money(facts.purchase_price),
        "rehab_budget": _money(facts.rehab_budget),
        "arv": _money(facts.arv),
    }
    try:
        pdf = renderer(context)
    except Exception as exc:  # class only: the context carries the borrower's property address
        logger.error("[soft-approval] call=%s render failed: %s", dialer_call_id, type(exc).__name__)
        return finish("render_failed", type(exc).__name__)
    return finish("generated", None, figures=figures_json, lender_key=lender_key, pdf=pdf)
