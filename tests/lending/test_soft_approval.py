"""T-08 Call-One soft approval PDF: calculation, service, storage, Slack card/form, route."""
from __future__ import annotations

import hashlib
import hmac
import json
import re
import time
from contextlib import contextmanager
from decimal import Decimal as D
from types import SimpleNamespace
from urllib.parse import urlencode

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import ValidationError
from sqlalchemy import text

from config.lender_matrix import LENDER_MATRIX, LenderRules
from config.lending_soft_approval import SoftApprovalParams
from migrations.apply_lending_lendingflow import apply_to as apply_lendingflow
from migrations.apply_lending_soft_approvals import apply_to as apply_soft_approvals
from src.lending import soft_approval_webhook as webhook
from src.lending.call_pipeline import follow_up
from src.lending.contracts import (
    BindingCap, BorrowerProfile, DeclineReason, LenderFit, LenderFitResult, LenderMiss, LoanRequest, LoanType,
    SizingFlag,
)
from src.lending.dispositions import RecordedCall
from src.lending.fakes import FakeLenderFitEvaluator
from src.lending.models import LendingContact
from src.lending.pdf.render import render_html
from src.lending.soft_approval import slack_card
from src.lending.soft_approval.calc import CalculationUnavailable, LenderTerms, calculate_figures
from src.lending.soft_approval.facts import SoftApprovalFacts
from src.lending.soft_approval.lead_source import (
    FakeLeadProfileSource,
    LeadProfile,
    LendingFlowLeadProfileSource,
    default_lead_source,
)
from src.lending.soft_approval import service as sa_service
from src.lending.soft_approval.service import generate_soft_approval
from src.tasks import lending_soft_approval_card_retry as card_retry

PHONE = "+17275550101"
LENDER = "test_flip"
RULES = LenderRules(key=LENDER, name="Test Lender", verified=True, loan_types=frozenset({LoanType.FIX_AND_FLIP}),
                    max_ltv=D("0.75"))
PARAMS = {LENDER: SoftApprovalParams(rehab_funding_pct=D("1"), purchase_advance_pct=D("1"))}
FITTING = LenderFitResult(fitting=[LenderFit(lender_key=LENDER, lender_name="Test Lender")])
FACTS = SoftApprovalFacts(property_address="12 Palm Ave, Tampa, FL", purchase_price=D("250000"),
                          rehab_budget=D("40000"), arv=D("400000"))
LEAD = LeadProfile(lead_ref="lf-1", borrower=BorrowerProfile(credit_band_min_fico=640),
                   loan_type=LoanType.FIX_AND_FLIP, loan_amount=D("290000"), state="FL")


@pytest.fixture
def soft_db(lending_db):
    conn = lending_db.get_bind()
    LendingContact.__table__.create(bind=conn, checkfirst=True)  # lendingflow_leads.contact_id references it
    apply_lendingflow(conn)
    apply_soft_approvals(conn)
    return lending_db


def _flow_lead(db, *, vendor="v1", phone=PHONE, fico=640, amount=290000, loan_type="FIX_AND_FLIP", state="FL",
               suppressed=False, received="2026-10-01 10:00:00+00"):
    db.execute(
        text("INSERT INTO lending.lendingflow_leads (vendor_lead_id, dedupe_hash, phone, credit_band_min_fico, loan_amount, "
             "loan_type, property_state, suppressed, received_at, raw_payload) "
             "VALUES (:v, :v, :p, :fico, :amt, :lt, :st, :sup, :rec, '{}'::jsonb)"),
        {"v": vendor, "p": phone, "fico": fico, "amt": amount, "lt": loan_type, "st": state, "sup": suppressed, "rec": received},
    )


def _generate(db, *, facts=FACTS, lead=LEAD, evaluator=None, params=PARAMS, renderer=None, call_id="call-1"):
    return generate_soft_approval(
        db, phone=PHONE, dialer_call_id=call_id, facts=facts, submitted_by="U123",
        lead_source=FakeLeadProfileSource({PHONE: lead} if lead else {}),
        evaluator=evaluator or FakeLenderFitEvaluator(FITTING),
        rules_lookup=lambda key: RULES if key == LENDER else None,
        params=params, renderer=renderer or (lambda ctx: b"%PDF-test"),
    )


def _row(db):
    return db.execute(text("SELECT * FROM lending.soft_approvals WHERE phone = :p"), {"p": PHONE}).mappings().first()


# ---- calculation (Josh, B1(c)) -----------------------------------------------------------------

def _terms(a=1, f=1, arv=None, ltc=None, cap=None, minimum=None):
    def opt(value):
        return None if value is None else D(value)

    return LenderTerms(rehab_funding_pct=D(f), purchase_advance_pct=D(a), arv_cap=opt(arv), ltc_cap=opt(ltc),
                       max_loan=opt(cap), min_loan=opt(minimum))


class TestCalc:
    def test_requested_loan_when_no_cap_cuts_it(self):
        f = calculate_figures(D(250000), D(40000), D(400000), _terms(arv="0.75"))
        assert (f.net_loan, f.rehab_funding, f.purchase_advance, f.cash_at_closing) == (D(290000), D(40000), D(250000), D(0))
        assert f.binding_cap is BindingCap.REQUESTED and f.max_purchase_price == D(260000) and f.flags == ()

    def test_arv_cap_binds_and_the_purchase_advance_is_cut_first(self):
        f = calculate_figures(D(280000), D(40000), D(400000), _terms(arv="0.75"))
        assert f.net_loan == D(300000) and f.binding_cap is BindingCap.ARV
        assert f.rehab_funding == D(40000)  # rehab stays whole
        assert (f.purchase_advance, f.cash_at_closing, f.max_purchase_price) == (D(260000), D(20000), D(260000))

    def test_ltc_cap_binds(self):
        f = calculate_figures(D(200000), D(100000), D(1_000_000), _terms(ltc="0.85"))
        assert f.net_loan == D(255000) and f.binding_cap is BindingCap.LTC
        assert (f.rehab_funding, f.purchase_advance, f.cash_at_closing) == (D(100000), D(155000), D(45000))
        assert f.full_advance_unavailable and f.max_purchase_price is None  # no price avoids a cut

    def test_max_loan_binds_and_rehab_funding_never_exceeds_the_loan(self):
        f = calculate_figures(D(100000), D(50000), D(400000), _terms(cap=30000))
        assert f.net_loan == D(30000) and f.binding_cap is BindingCap.MAX_LOAN
        assert (f.rehab_funding, f.purchase_advance, f.cash_at_closing) == (D(30000), D(0), D(100000))

    def test_max_price_is_stepped_so_it_holds_whichever_cap_binds(self):
        terms = _terms(a="0.9", f="0.5", ltc="0.8", cap=10_000_000)
        assert calculate_figures(D(200000), D(100000), D(1_000_000), terms).max_purchase_price == D(300000)
        # the same price is not cut by the LTC cap, and one step higher is
        at = calculate_figures(D(300000), D(100000), D(1_000_000), terms)
        above = calculate_figures(D(301000), D(100000), D(1_000_000), terms)
        assert at.binding_cap is BindingCap.REQUESTED and above.binding_cap is BindingCap.LTC

    def test_the_loan_is_rounded_down_to_the_nearest_thousand(self):
        f = calculate_figures(D(100500), D(10250), D(150000), _terms())
        assert f.net_loan == D(110000) and f.binding_cap is BindingCap.REQUESTED  # 110,750 requested
        assert calculate_figures(D(299999), D(0), D(500000), _terms()).net_loan == D(299000)

    def test_no_cap_at_all_is_not_an_error(self):
        f = calculate_figures(D(250000), D(40000), D(400000), _terms())
        assert f.net_loan == D(290000) and f.max_purchase_price is None and not f.full_advance_unavailable

    @pytest.mark.parametrize("rehab,ok", [(D(39999), False), (D(40000), True)])
    def test_loan_below_the_lender_minimum_is_no_fit(self, rehab, ok):
        terms = _terms(minimum=100000)  # purchase 60,000 + rehab: 99,999 rounds down to 99,000
        if ok:
            assert calculate_figures(D(60000), rehab, D(300000), terms).net_loan == D(100000)
        else:
            with pytest.raises(CalculationUnavailable) as exc:
                calculate_figures(D(60000), rehab, D(300000), terms)
            assert exc.value.reason == "loan_too_small"

    def test_a_loan_that_rounds_to_zero_is_too_small(self):
        with pytest.raises(CalculationUnavailable) as exc:
            calculate_figures(D(500), D(0), D(1000), _terms())
        assert exc.value.reason == "loan_too_small"

    def test_upside_down_flag_is_not_a_rejection(self):
        f = calculate_figures(D(150000), D(60000), D(200000), _terms())
        assert SizingFlag.UPSIDE_DOWN in f.flags and f.net_loan > 0
        assert SizingFlag.UPSIDE_DOWN not in calculate_figures(D(150000), D(60000), D(210000), _terms()).flags  # equal

    def test_arv_over_three_times_the_price_flags_a_possible_typo(self):
        assert SizingFlag.ARV_OVER_3X in calculate_figures(D(100000), D(0), D(300001), _terms()).flags
        assert SizingFlag.ARV_OVER_3X not in calculate_figures(D(100000), D(0), D(300000), _terms()).flags  # exactly 3x

    @pytest.mark.parametrize("a,f,reason", [(D(0), D(1), "invalid_lender_terms"), (D("1.1"), D(1), "invalid_lender_terms"),
                                            (D(1), D("-0.1"), "invalid_lender_terms")])
    def test_bad_lender_shares_are_refused(self, a, f, reason):
        with pytest.raises(CalculationUnavailable) as exc:
            calculate_figures(D(1000), D(100), D(2000), LenderTerms(rehab_funding_pct=f, purchase_advance_pct=a, arv_cap=D("0.7")))
        assert exc.value.reason == reason

    def test_a_non_positive_cap_is_refused(self):
        with pytest.raises(CalculationUnavailable) as exc:
            calculate_figures(D(1000), D(100), D(2000), _terms(arv="0"))
        assert exc.value.reason == "invalid_lender_terms"

    @pytest.mark.parametrize("p,r,v", [(D(0), D(1), D(1)), (D(1), D(-1), D(1)), (D(1), D(1), D(0))])
    def test_bad_inputs_are_refused(self, p, r, v):
        with pytest.raises(CalculationUnavailable) as exc:
            calculate_figures(p, r, v, _terms(arv="0.75"))
        assert exc.value.reason == "invalid_inputs"

    @pytest.mark.parametrize("purchase", [D(50000), D(120000), D(333333)])
    @pytest.mark.parametrize("rehab", [D(0), D(25000), D(90000)])
    @pytest.mark.parametrize("terms", [_terms(arv="0.7"), _terms(ltc="0.85"), _terms(cap=80000), _terms(a="0.85", ltc="0.8")])
    def test_outputs_are_never_negative_and_the_loan_is_in_whole_thousands(self, purchase, rehab, terms):
        try:
            f = calculate_figures(purchase, rehab, D(400000), terms)
        except CalculationUnavailable:
            return
        assert f.net_loan % 1000 == 0 and f.net_loan > 0
        assert min(f.rehab_funding, f.purchase_advance, f.cash_at_closing) >= 0
        assert f.rehab_funding + f.purchase_advance == f.net_loan
        assert all(isinstance(v, D) for v in (f.net_loan, f.rehab_funding, f.purchase_advance, f.cash_at_closing))


# ---- facts / form parsing ----------------------------------------------------------------------

def _state(**over):
    base = {"address": "12 Palm Ave", "purchase": "250000", "rehab": "40000", "arv": "400000", "ptype": "", "close": ""}
    base.update(over)
    out = {}
    for block, value in base.items():
        key = "selected_date" if block == "close" else "value"
        out[block] = {"value": {key: value or None}}
    return out


class TestFacts:
    @pytest.mark.parametrize("field", ["property_address", "purchase_price", "rehab_budget", "arv"])
    def test_every_core_fact_is_required(self, field):
        data = FACTS.model_dump()
        del data[field]
        with pytest.raises(ValidationError):
            SoftApprovalFacts(**data)

    def test_form_with_all_facts_parses(self):
        facts, errors = slack_card.parse_submission(_state())
        assert errors == {} and facts.purchase_price == D(250000) and facts.property_type is None

    @pytest.mark.parametrize("block", ["address", "purchase", "rehab", "arv"])
    def test_missing_form_value_is_an_inline_error(self, block):
        facts, errors = slack_card.parse_submission(_state(**{block: ""}))
        assert facts is None and list(errors) == [block]

    def test_zero_price_is_invalid_but_zero_rehab_is_allowed(self):
        assert slack_card.parse_submission(_state(purchase="0"))[0] is None
        assert slack_card.parse_submission(_state(rehab="0"))[0] is not None


# ---- service -----------------------------------------------------------------------------------

class TestService:
    def test_generated_pdf_is_stored_with_figures_and_no_lender_in_the_context(self, soft_db):
        seen = {}
        out = _generate(soft_db, renderer=lambda ctx: seen.update(ctx) or b"%PDF-ok")
        row = _row(soft_db)
        assert out.status == "generated" and bytes(row["pdf"]) == b"%PDF-ok" and row["lender_key"] == LENDER
        assert row["figures"]["net_loan"] == "290000" and row["submitted_by"] == "U123"
        assert row["figures"]["binding_cap"] == "requested" and row["figures"]["flags"] == []
        assert row["figures"]["purchase_advance"] == "250000" and row["figures"]["cash_at_closing"] == "0"
        assert row["figures"]["subject_to_confirmation"] is False and out.subject_to_confirmation is False
        assert seen["net_loan"] == "$290,000" and seen["property_address"] == FACTS.property_address
        assert seen["subject_to_confirmation"] is False
        assert "Test Lender" not in json.dumps(seen)

    def test_no_lead_stores_no_pdf(self, soft_db):
        out = _generate(soft_db, lead=None)
        assert (out.status, out.reason) == ("no_lead", "no_lendingflow_lead") and _row(soft_db)["pdf"] is None

    def test_lead_without_core_fields_stores_no_pdf(self, soft_db):
        incomplete = LeadProfile(lead_ref="lf-2", borrower=BorrowerProfile(), loan_type=None)
        assert _generate(soft_db, lead=incomplete).reason == "lead_missing_core_fields"

    def test_unsupported_loan_type_is_out_of_scope(self, soft_db):
        dscr = LeadProfile(lead_ref="lf-3", borrower=BorrowerProfile(), loan_type=LoanType.DSCR_RENTAL,
                           loan_amount=D(100000), state="FL")
        assert _generate(soft_db, lead=dscr).status == "out_of_scope"

    def test_no_fitting_lender_sends_nothing(self, soft_db):
        out = _generate(soft_db, evaluator=FakeLenderFitEvaluator(LenderFitResult()))
        assert out.status == "no_fit" and _row(soft_db)["pdf"] is None

    def test_unconfirmed_shares_take_the_placeholder_and_the_result_is_flagged(self, soft_db):
        """A1: a share Josh has not confirmed is never skipped; it takes 85% purchase / 100% rehab."""
        unconfirmed = {LENDER: SoftApprovalParams(rehab_funding_pct=D(1), purchase_advance_pct=None)}
        seen = {}
        out = _generate(soft_db, params=unconfirmed, renderer=lambda ctx: seen.update(ctx) or b"%PDF-ok")
        row = _row(soft_db)
        assert seen["subject_to_confirmation"] is True  # the PDF itself is told to print the mark
        assert out.status == "generated" and out.subject_to_confirmation is True
        assert row["figures"]["subject_to_confirmation"] is True
        assert row["figures"]["net_loan"] == "252000"  # 250,000 x 0.85 + 40,000 = 252,500, rounded down

    def test_a_lender_with_no_share_entry_at_all_uses_the_placeholder_box(self, soft_db):
        out = _generate(soft_db, params={})
        assert out.status == "generated" and _row(soft_db)["figures"]["subject_to_confirmation"] is True

    def test_a_loan_below_the_lender_minimum_is_no_fit_loan_too_small(self, soft_db):
        big_minimum = LenderRules(key=LENDER, name="Test Lender", verified=True, min_loan_amount=D("1000000"))
        out = generate_soft_approval(
            soft_db, phone=PHONE, dialer_call_id="c", facts=FACTS, submitted_by=None,
            lead_source=FakeLeadProfileSource({PHONE: LEAD}), evaluator=FakeLenderFitEvaluator(FITTING),
            rules_lookup=lambda key: big_minimum, params=PARAMS, renderer=lambda ctx: b"%PDF")
        assert (out.status, out.reason) == ("no_fit", "loan_too_small") and _row(soft_db)["pdf"] is None

    def test_a_lender_declined_only_on_the_ltv_ltc_caps_is_sized_not_dropped(self, soft_db):
        """B1(c): the formulas cut the loan to the caps; the re-fit at the computed loan decides."""
        capped = LenderFitResult(non_fitting=[LenderMiss(lender_key=LENDER, lender_name="L", reasons=["LTC over"],
                                                          reason_codes=[DeclineReason.LTC_OVER_MAX])])

        asked_for_more = LeadProfile(lead_ref="lf-9", borrower=BorrowerProfile(credit_band_min_fico=640),
                                     loan_type=LoanType.FIX_AND_FLIP, loan_amount=D("350000"), state="FL")

        class Evaluator:
            def evaluate(self, profile, request):
                return capped if request.loan_amount == asked_for_more.loan_amount else FITTING

        out = _generate(soft_db, lead=asked_for_more, evaluator=Evaluator())
        assert out.status == "generated" and out.lender_key == LENDER

    def test_a_lender_declined_for_another_reason_is_not_sized(self, soft_db):
        wrong_state = LenderFitResult(non_fitting=[LenderMiss(lender_key=LENDER, lender_name="L", reasons=["state"],
                                                              reason_codes=[DeclineReason.STATE_NOT_COVERED])])
        assert _generate(soft_db, evaluator=FakeLenderFitEvaluator(wrong_state)).status == "no_fit"

    def test_backflip_is_tried_first_even_when_ranked_after_another_lender(self, soft_db):
        other = LenderRules(key="other_lender", name="Other", verified=True, max_ltv=D("0.75"))
        backflip = LenderRules(key="generic_backflip_flip", name="Backflip", verified=True, max_ltv=D("0.75"))
        ranked = LenderFitResult(fitting=[LenderFit(lender_key="other_lender", lender_name="O"),
                                          LenderFit(lender_key="generic_backflip_flip", lender_name="B")])
        both = {"other_lender": PARAMS[LENDER], "generic_backflip_flip": PARAMS[LENDER]}
        out = generate_soft_approval(
            soft_db, phone=PHONE, dialer_call_id="c", facts=FACTS, submitted_by=None,
            lead_source=FakeLeadProfileSource({PHONE: LEAD}), evaluator=FakeLenderFitEvaluator(ranked),
            rules_lookup=lambda k: {"other_lender": other, "generic_backflip_flip": backflip}.get(k),
            params=both, renderer=lambda ctx: b"%PDF")
        assert out.lender_key == "generic_backflip_flip"

    def test_next_lender_is_used_when_the_first_no_longer_fits_at_the_computed_loan(self, soft_db):
        second = LenderRules(key="second", name="Second", verified=True, max_ltv=D("0.75"))
        both = LenderFitResult(fitting=[LenderFit(lender_key=LENDER, lender_name="A"),
                                        LenderFit(lender_key="second", lender_name="B")])
        only_second = LenderFitResult(fitting=[LenderFit(lender_key="second", lender_name="B")])

        lead = LeadProfile(lead_ref="lf-4", borrower=BorrowerProfile(), loan_type=LoanType.FIX_AND_FLIP,
                           loan_amount=D("200000"), state="FL")

        class Evaluator:
            def evaluate(self, profile, request):
                return both if request.loan_amount == lead.loan_amount else only_second

        params = {**PARAMS, "second": PARAMS[LENDER]}
        out = generate_soft_approval(
            soft_db, phone=PHONE, dialer_call_id="c", facts=FACTS, submitted_by=None,
            lead_source=FakeLeadProfileSource({PHONE: lead}), evaluator=Evaluator(),
            rules_lookup=lambda k: {LENDER: RULES, "second": second}.get(k), params=params,
            renderer=lambda ctx: b"%PDF",
        )
        assert out.status == "generated" and out.lender_key == "second"

    def test_render_failure_stores_status_without_pdf(self, soft_db):
        def boom(ctx):
            raise RuntimeError("chromium missing")

        out = _generate(soft_db, renderer=boom)
        assert out.status == "render_failed" and _row(soft_db)["pdf"] is None

    def test_resubmitting_identical_facts_changes_nothing(self, soft_db):
        first = _generate(soft_db)
        again = _generate(soft_db, call_id="call-2")
        row = _row(soft_db)
        assert again.approval_id == first.approval_id and row["history"] == [] and row["dialer_call_id"] == "call-1"

    def test_changed_figures_update_the_one_row_and_keep_the_old_values(self, soft_db):
        first = _generate(soft_db)
        changed = SoftApprovalFacts(**{**FACTS.model_dump(), "purchase_price": D("280000")})
        second = _generate(soft_db, facts=changed, call_id="call-2")
        rows = soft_db.execute(text("SELECT count(*) FROM lending.soft_approvals WHERE phone = :p"), {"p": PHONE}).scalar()
        row = _row(soft_db)
        assert rows == 1 and second.approval_id == first.approval_id
        assert row["figures"]["net_loan"] == "300000" and row["dialer_call_id"] == "call-2"
        assert len(row["history"]) == 1 and row["history"][0]["figures"]["net_loan"] == "290000"

    def test_production_defaults_produce_a_flagged_pdf_on_the_placeholder_inputs(self, soft_db):
        """Real lead source + real evaluator (T-05) + real matrix: every lender is unconfirmed, so the PDF is made
        on Josh's placeholder box and flagged subject to lender confirmation (A1). The feature itself stays closed
        behind LENDING_SOFT_APPROVAL_ENABLED."""
        out = generate_soft_approval(soft_db, phone=PHONE, dialer_call_id="c", facts=FACTS, submitted_by=None,
                                     renderer=lambda ctx: b"%PDF-prod")
        assert out.status == "no_lead" and _row(soft_db)["pdf"] is None  # no LendingFlow lead for the phone
        _flow_lead(soft_db)
        out = generate_soft_approval(soft_db, phone=PHONE, dialer_call_id="c", facts=FACTS, submitted_by=None,
                                     renderer=lambda ctx: b"%PDF-prod")
        row = _row(soft_db)
        assert out.status == "generated" and out.lender_key == "generic_backflip_flip"
        assert row["figures"]["subject_to_confirmation"] is True and row["figures"]["binding_cap"] == "ltc"
        assert row["figures"]["net_loan"] == "246000"  # the placeholder 85% LTC cap on 290,000 total cost
        assert not any(r.verified for r in LENDER_MATRIX)

    def test_migration_is_idempotent(self, soft_db):
        apply_soft_approvals(soft_db.get_bind())
        apply_soft_approvals(soft_db.get_bind())


# ---- template ----------------------------------------------------------------------------------

CONTEXT = {"generated_date": "October 9, 2026", "property_address": "12 Palm Ave, Tampa, FL", "net_loan": "$290,000",
           "rehab_funding": "$40,000", "max_purchase_price": "$260,000", "purchase_price": "$250,000",
           "rehab_budget": "$40,000", "arv": "$400,000"}


class TestLendingFlowLeadSource:
    SOURCE = LendingFlowLeadProfileSource()

    def test_the_default_source_reads_lendingflow(self):
        assert isinstance(default_lead_source(), LendingFlowLeadProfileSource)

    def test_maps_the_leads_fields(self, soft_db):
        _flow_lead(soft_db)
        lead = self.SOURCE.for_phone(soft_db, PHONE)
        assert lead.loan_type is LoanType.FIX_AND_FLIP and lead.loan_amount == D(290000) and lead.state == "FL"
        assert lead.borrower.credit_band_min_fico == 640 and lead.has_core_fields()

    def test_a_closed_band_carries_its_upper_bound_so_a_higher_floor_is_a_decline(self, soft_db):
        """Review on #363: "680-719" must reach the evaluator as 680 to 719, never as an open-ended 680+."""
        _flow_lead(soft_db, fico=680)
        soft_db.execute(text("UPDATE lending.lendingflow_leads SET credit_band = '680-719' WHERE phone = :p"), {"p": PHONE})
        lead = self.SOURCE.for_phone(soft_db, PHONE)
        assert (lead.borrower.credit_band_min_fico, lead.borrower.credit_band_max_fico) == (680, 719)
        strict = LenderRules(key="strict", name="Strict", verified=True, loan_types=frozenset({LoanType.FIX_AND_FLIP}),
                             credit_floor=740)
        request = LoanRequest(loan_type=lead.loan_type, loan_amount=lead.loan_amount, state=lead.state)
        result = sa_service.MatrixLenderFitEvaluator(matrix=(strict,)).evaluate(lead.borrower, request)
        assert not result.fitting and not result.possible
        assert DeclineReason.CREDIT_BELOW_MIN in result.non_fitting[0].reason_codes

    def test_the_most_recent_lead_for_the_phone_wins(self, soft_db):
        _flow_lead(soft_db, vendor="old", amount=100000, received="2026-09-01 10:00:00+00")
        _flow_lead(soft_db, vendor="new", amount=250000, received="2026-10-01 10:00:00+00")
        assert self.SOURCE.for_phone(soft_db, PHONE).loan_amount == D(250000)

    def test_an_unformatted_phone_matches(self, soft_db):
        _flow_lead(soft_db)
        assert self.SOURCE.for_phone(soft_db, "(727) 555-0101") is not None

    @pytest.mark.parametrize("phone", ["+17275559999", "", "not a phone"])
    def test_no_lead_for_an_unknown_or_unusable_phone(self, soft_db, phone):
        _flow_lead(soft_db)
        assert self.SOURCE.for_phone(soft_db, phone) is None

    def test_a_suppressed_latest_lead_blocks_even_when_an_older_one_is_fine(self, soft_db):
        _flow_lead(soft_db, vendor="old", suppressed=False, received="2026-09-01 10:00:00+00")
        _flow_lead(soft_db, vendor="new", suppressed=True, received="2026-10-01 10:00:00+00")
        assert self.SOURCE.for_phone(soft_db, PHONE) is None

    @pytest.mark.parametrize("over", [{"loan_type": "TOWNHOUSE_LOAN"}, {"loan_type": None}, {"amount": None}, {"state": None}])
    def test_a_missing_or_unrecognised_core_value_is_reported_as_missing_never_guessed(self, soft_db, over):
        _flow_lead(soft_db, **over)
        lead = self.SOURCE.for_phone(soft_db, PHONE)
        assert lead is not None and not lead.has_core_fields()
        out = generate_soft_approval(soft_db, phone=PHONE, dialer_call_id="c", facts=FACTS, submitted_by=None)
        assert (out.status, out.reason) == ("no_lead", "lead_missing_core_fields") and _row(soft_db)["pdf"] is None

    def test_a_lead_in_the_database_reaches_a_stored_pdf_through_the_default_source(self, soft_db):
        _flow_lead(soft_db)
        out = generate_soft_approval(
            soft_db, phone=PHONE, dialer_call_id="c", facts=FACTS, submitted_by="U1",
            evaluator=FakeLenderFitEvaluator(FITTING), rules_lookup=lambda key: RULES if key == LENDER else None,
            params=PARAMS, renderer=lambda ctx: b"%PDF-test")
        assert out.status == "generated" and bytes(_row(soft_db)["pdf"]) == b"%PDF-test"


class TestTemplate:
    WATERMARK = "NON-BINDING SOFT APPROVAL ESTIMATE — FOR INFORMATIONAL PURPOSES ONLY"

    def test_shows_address_figures_and_watermark(self):
        html = render_html("soft_approval.html", CONTEXT, watermark=self.WATERMARK)
        for expected in ("12 Palm Ave, Tampa, FL", "$290,000", "$40,000", "$260,000", self.WATERMARK, "Soft Approval Summary"):
            assert expected in html
        assert "Pre-Qualification Estimate" not in html

    def test_copy_safety_no_rates_ratios_or_lender_names(self):
        body = render_html("soft_approval.html", CONTEXT, watermark=self.WATERMARK)
        body = body[body.index('<div class="content">'):body.index('<div class="footer-wrap">')]
        body = re.sub(r"<style>.*?</style>|<[^>]+>", " ", body, flags=re.S).lower()  # visible text only
        for forbidden in ("%", "interest", "apr", "ltc", "ltv", "origination", "points", "backflip", "rcn", "kiavi", "easy street"):
            assert forbidden not in body

    def test_b7_nothing_is_emailed_so_the_copy_never_says_reply_to_this_email(self):
        html = render_html("soft_approval.html", CONTEXT, watermark=self.WATERMARK)
        assert "reply to this email" not in html

    def test_the_pdf_is_signed_next_deal_lending_with_one_callback_number(self):
        # The signer and callback number come from the shared base.html (config/lending_prequal.py): the client wants
        # "Next Deal Lending" and (656) 221-1100 on every borrower-facing PDF, and no personal name.
        html = render_html("soft_approval.html", CONTEXT, watermark=self.WATERMARK)
        visible = re.sub(r"<style>.*?</style>|<[^>]+>", " ", html, flags=re.S)
        assert "Next Deal Lending" in visible and "(656) 221-1100" in visible
        for banned in ("Josh", "Kantor", "(727)", "436-9951"):
            assert banned not in visible

    def test_the_subject_to_confirmation_line_prints_only_when_flagged(self):
        flagged = render_html("soft_approval.html", {**CONTEXT, "subject_to_confirmation": True}, watermark=self.WATERMARK)
        plain = render_html("soft_approval.html", {**CONTEXT, "subject_to_confirmation": False}, watermark=self.WATERMARK)
        assert "subject to lender confirmation" in flagged and "subject to lender confirmation" not in plain
        assert "subject to lender confirmation" not in render_html("soft_approval.html", CONTEXT, watermark=self.WATERMARK)

    def test_omits_max_price_when_there_is_none(self):
        html = render_html("soft_approval.html", {**CONTEXT, "max_purchase_price": ""}, watermark=self.WATERMARK)
        assert "maximum purchase price" not in html

    def test_a_missing_context_key_raises(self):
        with pytest.raises(Exception):
            render_html("soft_approval.html", {k: v for k, v in CONTEXT.items() if k != "net_loan"}, watermark=self.WATERMARK)


# ---- card posting ------------------------------------------------------------------------------

class FakeSlack:
    def __init__(self):
        self.posts, self.views, self.fail = [], [], False
        self.ephemerals, self.files, self.upload_fail = [], [], False
        self.updates, self.update_fail = [], False

    def chat_postEphemeral(self, **kwargs):
        self.ephemerals.append(kwargs)

    def chat_update(self, **kwargs):
        if self.update_fail:
            raise RuntimeError("cannot edit the card")
        self.updates.append(kwargs)

    def files_upload_v2(self, **kwargs):
        if self.upload_fail:
            raise RuntimeError("files:write missing")
        self.files.append(kwargs)

    def chat_postMessage(self, **kwargs):
        if self.fail:
            raise RuntimeError("slack down")
        self.posts.append(kwargs)
        return {"ts": "1.1"}

    def views_open(self, **kwargs):
        self.views.append(kwargs)


def _settings(**over):
    base = dict(lending_soft_approval_enabled=True, lending_dial_tasks_channel="CDIAL",
                lending_soft_approval_user_ids="U1", lending_cora_approver_user_ids="",
                lending_slack_bot_token=SimpleNamespace(get_secret_value=lambda: "xoxb-test"),
                lending_slack_signing_secret=SimpleNamespace(get_secret_value=lambda: "sig-secret"))
    base.update(over)
    return SimpleNamespace(**base)


@pytest.fixture
def wired(monkeypatch, soft_db):
    @contextmanager
    def session():
        with soft_db.begin_nested():  # like lending_session: an error rolls the unit of work back
            yield soft_db

    monkeypatch.setattr(slack_card, "lending_session", session)
    monkeypatch.setattr(webhook, "lending_session", session)
    monkeypatch.setattr(slack_card, "get_settings", lambda: _settings())
    monkeypatch.setattr(webhook, "get_settings", lambda: _settings())
    slack = FakeSlack()
    monkeypatch.setattr(webhook, "slack_client", lambda: slack)
    return slack


def _call(db, *, call_id="call-1", talk=120, disposition=None, phone=PHONE):
    return db.execute(
        text("INSERT INTO lending.call_dispositions (dialer_call_id, phone, caller_name, talk_duration_sec, disposition, raw_event) "
             "VALUES (:c, :p, 'Sam', :t, :d, '{}'::jsonb) RETURNING id"),
        {"c": call_id, "p": phone, "t": talk, "d": disposition},
    ).scalar()


class TestCard:
    def test_card_is_posted_once_per_call_with_a_masked_phone_and_a_button(self, wired, soft_db):
        row_id = _call(soft_db)
        slack_card.post_soft_approval_card(row_id, client=wired)
        slack_card.post_soft_approval_card(row_id, client=wired)
        assert len(wired.posts) == 1
        post = wired.posts[0]
        assert post["channel"] == "CDIAL" and "call-1" in post["text"] and PHONE not in json.dumps(post)
        assert post["blocks"][1]["elements"][0]["value"] == "call-1"
        assert soft_db.execute(text("SELECT message_ts FROM lending.soft_approval_cards")).scalar() == "1.1"

    @pytest.mark.parametrize("kwargs", [{"talk": 0}, {"disposition": "DNC_REQUEST"}])
    def test_unanswered_and_dnc_calls_get_no_card(self, wired, soft_db, kwargs):
        slack_card.post_soft_approval_card(_call(soft_db, **kwargs), client=wired)
        assert wired.posts == []

    def test_disabled_feature_posts_nothing(self, wired, soft_db, monkeypatch):
        monkeypatch.setattr(slack_card, "get_settings", lambda: _settings(lending_soft_approval_enabled=False))
        slack_card.post_soft_approval_card(_call(soft_db), client=wired)
        assert wired.posts == []

    def test_a_failed_post_releases_the_claim_and_never_raises(self, wired, soft_db):
        row_id = _call(soft_db)
        wired.fail = True
        slack_card.post_soft_approval_card(row_id, client=wired)
        assert soft_db.execute(text("SELECT count(*) FROM lending.soft_approval_cards")).scalar() == 0
        wired.fail = False
        slack_card.post_soft_approval_card(row_id, client=wired)
        assert len(wired.posts) == 1

    def test_follow_up_schedules_the_card_only_for_a_finished_call(self):
        tasks = []
        base = dict(row_id=7, call_id="c", phone=PHONE, caller_seat=None, disposition=None, opt_out_propagated=False)
        follow_up(RecordedCall(call_ended=True, **base), lambda fn, *a: tasks.append(fn.__name__))
        assert "post_soft_approval_card" in tasks
        tasks.clear()
        follow_up(RecordedCall(call_ended=False, **base), lambda fn, *a: tasks.append(fn.__name__))
        assert "post_soft_approval_card" not in tasks


@pytest.fixture
def retry(wired, soft_db, monkeypatch):
    @contextmanager
    def session():
        with soft_db.begin_nested():
            yield soft_db

    monkeypatch.setattr(card_retry, "lending_session", session)
    monkeypatch.setattr(card_retry, "get_settings", lambda: _settings())
    monkeypatch.setattr(slack_card, "slack_client", lambda: wired)
    return card_retry


def _ended(db, row_id, minutes_ago=10):
    db.execute(
        text("UPDATE lending.call_dispositions SET call_ended_at = now() - make_interval(mins => :m) WHERE id = :id"),
        {"m": minutes_ago, "id": row_id},
    )


class TestCardRetry:
    def test_a_card_that_failed_to_post_is_posted_by_the_retry_cycle_exactly_once(self, wired, soft_db, retry):
        row_id = _call(soft_db)
        _ended(soft_db, row_id)
        recorded = RecordedCall(row_id=row_id, call_id="call-1", phone=PHONE, caller_seat=None, disposition=None,
                                opt_out_propagated=False, call_ended=True)
        wired.fail = True
        follow_up(recorded, lambda fn, *a: fn(*a) if fn.__name__ == "post_soft_approval_card" else None)
        assert wired.posts == []
        assert soft_db.execute(text("SELECT count(*) FROM lending.soft_approval_cards")).scalar() == 0

        wired.fail = False  # the CDR row is unchanged, so the poller never calls follow_up for it again
        assert retry.run() == 1
        assert retry.run() == 0
        assert len(wired.posts) == 1 and wired.posts[0]["blocks"][1]["elements"][0]["value"] == "call-1"
        assert soft_db.execute(text("SELECT message_ts FROM lending.soft_approval_cards")).scalar() == "1.1"

    def test_a_call_that_already_has_a_card_is_not_retried(self, wired, soft_db, retry):
        row_id = _call(soft_db)
        _ended(soft_db, row_id)
        slack_card.post_soft_approval_card(row_id, client=wired)
        assert retry.run() == 0
        assert len(wired.posts) == 1

    @pytest.mark.parametrize("call, minutes_ago", [
        ({"talk": 0}, 10),
        ({"disposition": "DNC_REQUEST"}, 10),
        ({}, 0),            # ended just now: its first attempt may still be running
        ({}, 7 * 60),       # older than the 6-hour retry window
        ({}, None),         # never ended
    ])
    def test_only_eligible_recent_calls_are_retried(self, wired, soft_db, retry, call, minutes_ago):
        row_id = _call(soft_db, **call)
        if minutes_ago is not None:
            _ended(soft_db, row_id, minutes_ago)
        assert retry.run() == 0
        assert wired.posts == []

    def test_disabled_feature_retries_nothing(self, wired, soft_db, retry, monkeypatch):
        row_id = _call(soft_db)
        _ended(soft_db, row_id)
        monkeypatch.setattr(retry, "get_settings", lambda: _settings(lending_soft_approval_enabled=False))
        assert retry.run() == 0
        assert wired.posts == []


# ---- signature + route -------------------------------------------------------------------------

SECRET = "sig-secret"


def _signed(body: bytes, *, secret=SECRET, ts=None):
    ts = str(int(ts if ts is not None else time.time()))
    sig = "v0=" + hmac.new(secret.encode(), b"v0:" + ts.encode() + b":" + body, hashlib.sha256).hexdigest()
    return {"X-Slack-Request-Timestamp": ts, "X-Slack-Signature": sig, "Content-Type": "application/x-www-form-urlencoded"}


def _payload(obj) -> bytes:
    return urlencode({"payload": json.dumps(obj)}).encode()


@pytest.fixture
def client(wired):
    app = FastAPI()
    app.include_router(webhook.router)
    return TestClient(app)


class TestMovedCoraCards:
    """Cora cards the lending app posted before Cora had her own app: a click retires the card."""

    CLICK = {"type": "block_actions", "channel": {"id": "CCORA"}, "message": {"ts": "9.9"},
             "actions": [{"action_id": "agent_core_approve", "value": "7"}]}

    def test_only_cora_buttons_count_as_moved_cora_clicks(self):
        assert webhook.is_moved_cora_click(self.CLICK)
        soft = {**self.CLICK, "actions": [{"action_id": "soft_approval_open", "value": "call-1"}]}
        assert not webhook.is_moved_cora_click(soft)
        assert not webhook.is_moved_cora_click({"type": "view_submission"})

    def test_the_old_card_loses_its_buttons_and_points_to_the_new_one(self, monkeypatch):
        updates: list[dict] = []
        client = SimpleNamespace(chat_update=lambda **kwargs: updates.append(kwargs))
        monkeypatch.setattr(webhook, "slack_client", lambda: client)
        webhook._retire_moved_cora_card(self.CLICK)
        assert updates[0]["channel"] == "CCORA" and updates[0]["ts"] == "9.9"
        assert updates[0]["text"] == webhook.MOVED_CORA_CARD_TEXT
        assert all(block["type"] != "actions" for block in updates[0]["blocks"])

    def test_a_slack_failure_while_retiring_is_swallowed(self, monkeypatch):
        def boom(**kwargs):
            raise RuntimeError("slack down")

        monkeypatch.setattr(webhook, "slack_client", lambda: SimpleNamespace(chat_update=boom))
        webhook._retire_moved_cora_card(self.CLICK)  # no exception


class TestSignature:
    def test_valid_signature(self):
        body = b"payload=x"
        h = _signed(body)
        assert slack_card.verify_slack_signature(SECRET, h["X-Slack-Request-Timestamp"], h["X-Slack-Signature"], body)

    def test_wrong_secret_stale_timestamp_and_missing_headers_fail(self):
        body = b"payload=x"
        h = _signed(body, secret="other")
        assert not slack_card.verify_slack_signature(SECRET, h["X-Slack-Request-Timestamp"], h["X-Slack-Signature"], body)
        old = _signed(body, ts=time.time() - 3600)
        assert not slack_card.verify_slack_signature(SECRET, old["X-Slack-Request-Timestamp"], old["X-Slack-Signature"], body)
        assert not slack_card.verify_slack_signature(SECRET, None, None, body)
        assert not slack_card.verify_slack_signature("", "1", "v0=x", body)


class TestRoute:
    URL = "/webhooks/lending/slack-interactivity"

    def test_closed_while_the_signing_secret_is_unset(self, client, monkeypatch):
        monkeypatch.setattr(webhook, "get_settings", lambda: _settings(lending_slack_signing_secret=None))
        body = _payload({"type": "block_actions"})
        assert client.post(self.URL, content=body, headers=_signed(body)).status_code == 503

    def test_bad_signature_is_rejected(self, client):
        body = _payload({"type": "block_actions"})
        assert client.post(self.URL, content=body, headers=_signed(body, secret="wrong")).status_code == 401

    def test_button_click_opens_the_form_for_that_call(self, client, wired):
        body = _payload({"type": "block_actions", "trigger_id": "trig", "user": {"id": "U1"},
                         "channel": {"id": "CDIAL"}, "message": {"ts": "1.1"},
                         "actions": [{"action_id": "soft_approval_open", "value": "call-1"}]})
        assert client.post(self.URL, content=body, headers=_signed(body)).status_code == 200
        view = wired.views[0]["view"]
        assert wired.views[0]["trigger_id"] == "trig"
        assert json.loads(view["private_metadata"])["call_id"] == "call-1"
        assert {b["block_id"] for b in view["blocks"]} == {"address", "purchase", "rehab", "arv", "ptype", "close"}

    def test_a_cora_card_click_is_not_handled_here(self, client, wired):
        """Cora's cards belong to her own Slack app; a stray agent_core click opens nothing."""
        body = _payload({"type": "block_actions", "trigger_id": "trig", "channel": {"id": "CCORA"},
                         "message": {"ts": "2.2"}, "actions": [{"action_id": "agent_core_approve", "value": "7"}]})
        assert client.post(self.URL, content=body, headers=_signed(body)).json() == {}
        assert wired.views == []

    def test_invalid_submission_returns_inline_errors_and_stores_nothing(self, client, soft_db):
        body = _payload({"type": "view_submission", "user": {"id": "U1"}, "view": {
            "callback_id": "soft_approval_form", "private_metadata": json.dumps({"call_id": "call-1"}),
            "state": {"values": _state(purchase="")}}})
        res = client.post(self.URL, content=body, headers=_signed(body)).json()
        assert res["response_action"] == "errors" and "purchase" in res["errors"]
        assert soft_db.execute(text("SELECT count(*) FROM lending.soft_approvals")).scalar() == 0

    def test_valid_submission_without_a_lead_stores_no_pdf_and_replies_in_thread(self, client, wired, soft_db):
        _call(soft_db)
        body = _payload({"type": "view_submission", "user": {"id": "U1"}, "view": {
            "callback_id": "soft_approval_form",
            "private_metadata": json.dumps({"call_id": "call-1", "channel": "CDIAL", "message_ts": "1.1"}),
            "state": {"values": _state()}}})
        assert client.post(self.URL, content=body, headers=_signed(body)).json() == {}
        row = _row(soft_db)
        assert row["status"] == "no_lead" and row["pdf"] is None and row["submitted_by"] == "U1"
        assert wired.posts[-1]["thread_ts"] == "1.1" and "No LendingFlow lead" in wired.posts[-1]["text"]
        assert "250000" not in json.dumps(wired.posts[-1])

    # ---- B11: who may open and submit the form -----------------------------------------------

    def _open_body(self, user):
        return _payload({"type": "block_actions", "trigger_id": "trig", "user": {"id": user},
                         "channel": {"id": "CDIAL"}, "message": {"ts": "1.1"},
                         "actions": [{"action_id": "soft_approval_open", "value": "call-1"}]})

    def _submit_body(self, user, **state):
        return _payload({"type": "view_submission", "user": {"id": user}, "view": {
            "callback_id": "soft_approval_form",
            "private_metadata": json.dumps({"call_id": "call-1", "channel": "CDIAL", "message_ts": "1.1"}),
            "state": {"values": _state(**state)}}})

    def test_a_user_not_on_the_allow_list_cannot_open_the_form(self, client, wired):
        body = self._open_body("UOTHER")
        assert client.post(self.URL, content=body, headers=_signed(body)).status_code == 200
        assert wired.views == [] and wired.ephemerals[0]["user"] == "UOTHER"

    def test_a_user_not_on_the_allow_list_cannot_submit_and_nothing_is_stored(self, client, wired, soft_db):
        _call(soft_db)
        body = self._submit_body("UOTHER")
        res = client.post(self.URL, content=body, headers=_signed(body)).json()
        assert res["response_action"] == "errors"
        assert soft_db.execute(text("SELECT count(*) FROM lending.soft_approvals")).scalar() == 0

    def test_an_empty_allow_list_blocks_everyone(self, client, wired, monkeypatch):
        monkeypatch.setattr(webhook, "get_settings", lambda: _settings(
            lending_soft_approval_user_ids="", lending_cora_approver_user_ids=""))
        body = self._open_body("U1")
        client.post(self.URL, content=body, headers=_signed(body))
        assert wired.views == []

    def test_the_cora_approver_ids_are_the_fallback_allow_list(self, client, wired, monkeypatch):
        monkeypatch.setattr(webhook, "get_settings", lambda: _settings(
            lending_soft_approval_user_ids="", lending_cora_approver_user_ids="UJOSH, U2"))
        body = self._open_body("UJOSH")
        client.post(self.URL, content=body, headers=_signed(body))
        assert len(wired.views) == 1

    # ---- B7 (PDF opened from the thread) and F5 (facts reach the lead card) -----------------

    @pytest.fixture
    def flow(self, soft_db, monkeypatch):
        _flow_lead(soft_db)
        _call(soft_db)
        monkeypatch.setattr(sa_service, "render_pdf", lambda *a, **k: b"%PDF-route")

    def test_the_stored_pdf_is_uploaded_to_the_cards_thread_with_a_flagged_status(self, client, wired, soft_db, flow):
        body = self._submit_body("U1")
        assert client.post(self.URL, content=body, headers=_signed(body)).json() == {}
        assert _row(soft_db)["status"] == "generated"
        upload = wired.files[0]
        assert (upload["channel"], upload["thread_ts"], upload["content"]) == ("CDIAL", "1.1", b"%PDF-route")
        assert upload["filename"] == "soft-approval-0101.pdf" and "12 Palm" not in json.dumps(upload, default=str)
        assert "Subject to lender confirmation" in wired.posts[-1]["text"]

    def test_a_failed_upload_never_fails_the_soft_approval(self, client, wired, soft_db, flow):
        wired.upload_fail = True
        body = self._submit_body("U1")
        client.post(self.URL, content=body, headers=_signed(body))
        assert _row(soft_db)["status"] == "generated" and wired.posts[-1]["thread_ts"] == "1.1"

    def test_no_upload_when_nothing_was_generated(self, client, wired, soft_db):
        _call(soft_db)  # no LendingFlow lead for the phone
        body = self._submit_body("U1")
        client.post(self.URL, content=body, headers=_signed(body))
        assert wired.files == []

    def test_the_card_shows_a_flagged_pdf_ready_line_and_offers_the_form_again(self, client, wired, soft_db, flow):
        body = self._submit_body("U1")
        client.post(self.URL, content=body, headers=_signed(body))
        update = wired.updates[0]
        assert (update["channel"], update["ts"]) == ("CDIAL", "1.1")
        status = next(b for b in update["blocks"] if b["type"] == "context")["elements"][0]["text"]
        assert "PDF ready" in status and "subject to lender confirmation" in status and "<!date^" in status
        button = next(b for b in update["blocks"] if b["type"] == "actions")["elements"][0]
        assert button["text"]["text"] == "Re-enter deal facts"
        assert (button["action_id"], button["value"]) == (webhook.OPEN_FORM_ACTION_ID, "call-1")
        dumped = json.dumps(update, default=str)
        assert "12 Palm" not in dumped and PHONE not in dumped

    def test_the_card_shows_why_no_pdf_was_made_and_keeps_the_original_button(self, client, wired, soft_db, flow):
        body = self._submit_body("U1", arv="40000")  # the ARV cap cuts the loan below the lender minimum
        client.post(self.URL, content=body, headers=_signed(body))
        blocks = wired.updates[0]["blocks"]
        status = next(b for b in blocks if b["type"] == "context")["elements"][0]["text"]
        assert "No PDF" in status and wired.files == []
        assert next(b for b in blocks if b["type"] == "actions")["elements"][0]["text"]["text"] == "Enter deal facts"

    def test_a_failed_card_update_never_fails_the_soft_approval(self, client, wired, soft_db, flow):
        wired.update_fail = True
        body = self._submit_body("U1")
        client.post(self.URL, content=body, headers=_signed(body))
        assert _row(soft_db)["status"] == "generated" and wired.files and wired.posts[-1]["thread_ts"] == "1.1"

    def test_the_card_is_left_alone_when_the_call_is_unknown(self, client, wired, soft_db):
        body = self._submit_body("U1")  # no call row seeded
        client.post(self.URL, content=body, headers=_signed(body))
        assert wired.updates == []

    def test_disabled_feature_ignores_a_signed_request(self, client, monkeypatch):
        monkeypatch.setattr(webhook, "get_settings", lambda: _settings(lending_soft_approval_enabled=False))
        body = _payload({"type": "block_actions"})
        assert client.post(self.URL, content=body, headers=_signed(body)).json() == {}
