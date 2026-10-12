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
from migrations.apply_lending_soft_approvals import apply_to as apply_soft_approvals
from src.lending import soft_approval_webhook as webhook
from src.lending.call_pipeline import follow_up
from src.lending.contracts import BorrowerProfile, LenderFit, LenderFitResult, LoanType
from src.lending.dispositions import RecordedCall
from src.lending.fakes import FakeLenderFitEvaluator
from src.lending.pdf.render import render_html
from src.lending.soft_approval import slack_card
from src.lending.soft_approval.calc import CalculationUnavailable, LenderTerms, calculate_figures
from src.lending.soft_approval.facts import SoftApprovalFacts
from src.lending.soft_approval.lead_source import FakeLeadProfileSource, LeadProfile
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
    apply_soft_approvals(lending_db.get_bind())
    return lending_db


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


# ---- calculation -------------------------------------------------------------------------------

class TestCalc:
    ARV_ONLY = LenderTerms(rehab_funding_pct=D(1), purchase_advance_pct=D(1), arv_cap=D("0.75"))

    def test_price_under_the_limit(self):
        f = calculate_figures(D(250000), D(40000), D(400000), self.ARV_ONLY)
        assert (f.net_loan, f.rehab_funding, f.max_purchase_price, f.cash_needed) == (D(290000), D(40000), D(260000), D(0))

    def test_price_over_the_limit_caps_the_loan(self):
        f = calculate_figures(D(280000), D(40000), D(400000), self.ARV_ONLY)
        assert (f.net_loan, f.max_purchase_price, f.cash_needed) == (D(300000), D(260000), D(20000))

    def test_ltc_limit_is_solved_for_price_not_taken_from_the_actual_price(self):
        terms = LenderTerms(rehab_funding_pct=D("0.5"), purchase_advance_pct=D("0.9"), ltc_cap=D("0.8"), max_loan=D(10_000_000))
        f = calculate_figures(D(200000), D(100000), D(1_000_000), terms)
        # a*P + f*R <= l*(P+R)  ->  P <= R*(l-f)/(a-l) = 100000*0.3/0.1
        assert f.max_purchase_price == D(300000)

    def test_ltc_that_cannot_give_the_full_advance_is_a_note_not_an_error(self):
        terms = LenderTerms(rehab_funding_pct=D(1), purchase_advance_pct=D("0.9"), ltc_cap=D("0.85"))
        f = calculate_figures(D(200000), D(40000), D(500000), terms)
        assert f.full_advance_unavailable and f.max_purchase_price is None
        assert f.net_loan == (D("0.85") * D(240000))

    def test_rehab_funding_never_exceeds_the_cap(self):
        terms = LenderTerms(rehab_funding_pct=D(1), purchase_advance_pct=D(1), max_loan=D(30000))
        f = calculate_figures(D(100000), D(50000), D(400000), terms)
        assert f.rehab_funding == D(30000) and f.net_loan == D(30000)

    def test_amounts_round_down_and_cash_needed_rounds_up(self):
        terms = LenderTerms(rehab_funding_pct=D(1), purchase_advance_pct=D(1), arv_cap=D("0.7777"))
        f = calculate_figures(D(100000), D(10000), D(150000), terms)
        assert f.net_loan == f.net_loan.to_integral_value() and f.net_loan < D("0.7777") * D(150000) + 1
        assert f.cash_needed >= D(110000) - D("0.7777") * D(150000)

    @pytest.mark.parametrize("a,f,reason", [(D(0), D(1), "invalid_lender_terms"), (D("1.1"), D(1), "invalid_lender_terms"),
                                            (D(1), D("-0.1"), "invalid_lender_terms")])
    def test_bad_lender_terms_are_refused(self, a, f, reason):
        with pytest.raises(CalculationUnavailable) as exc:
            calculate_figures(D(1000), D(100), D(2000), LenderTerms(rehab_funding_pct=f, purchase_advance_pct=a, arv_cap=D("0.7")))
        assert exc.value.reason == reason

    def test_no_limit_at_all_is_refused(self):
        with pytest.raises(CalculationUnavailable) as exc:
            calculate_figures(D(1000), D(100), D(2000), LenderTerms(rehab_funding_pct=D(1), purchase_advance_pct=D(1)))
        assert exc.value.reason == "no_loan_limit"

    @pytest.mark.parametrize("p,r,v", [(D(0), D(1), D(1)), (D(1), D(-1), D(1)), (D(1), D(1), D(0))])
    def test_bad_inputs_are_refused(self, p, r, v):
        with pytest.raises(CalculationUnavailable):
            calculate_figures(p, r, v, self.ARV_ONLY)


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
        assert seen["net_loan"] == "$290,000" and seen["property_address"] == FACTS.property_address
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

    def test_unconfirmed_lender_inputs_fail_closed(self, soft_db):
        unconfirmed = {LENDER: SoftApprovalParams(rehab_funding_pct=D(1), purchase_advance_pct=None)}
        out = _generate(soft_db, params=unconfirmed)
        assert out.status == "terms_unconfirmed" and _row(soft_db)["pdf"] is None

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

    def test_production_defaults_produce_no_pdf(self, soft_db):
        """Real evaluator + real matrix (all lenders unverified) + no LendingFlow store: closed, never a guess."""
        out = generate_soft_approval(soft_db, phone=PHONE, dialer_call_id="c", facts=FACTS, submitted_by=None)
        assert out.status == "no_lead" and _row(soft_db)["pdf"] is None
        out = generate_soft_approval(soft_db, phone=PHONE, dialer_call_id="c", facts=FACTS, submitted_by=None,
                                     lead_source=FakeLeadProfileSource({PHONE: LEAD}))
        assert out.status in {"no_fit", "terms_unconfirmed"} and not any(r.verified for r in LENDER_MATRIX)

    def test_migration_is_idempotent(self, soft_db):
        apply_soft_approvals(soft_db.get_bind())
        apply_soft_approvals(soft_db.get_bind())


# ---- template ----------------------------------------------------------------------------------

CONTEXT = {"generated_date": "October 9, 2026", "property_address": "12 Palm Ave, Tampa, FL", "net_loan": "$290,000",
           "rehab_funding": "$40,000", "max_purchase_price": "$260,000", "purchase_price": "$250,000",
           "rehab_budget": "$40,000", "arv": "$400,000"}


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

    def chat_postMessage(self, **kwargs):
        if self.fail:
            raise RuntimeError("slack down")
        self.posts.append(kwargs)
        return {"ts": "1.1"}

    def views_open(self, **kwargs):
        self.views.append(kwargs)


def _settings(**over):
    base = dict(lending_soft_approval_enabled=True, lending_dial_tasks_channel="CDIAL",
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
        body = _payload({"type": "block_actions", "trigger_id": "trig",
                         "channel": {"id": "CDIAL"}, "message": {"ts": "1.1"},
                         "actions": [{"action_id": "soft_approval_open", "value": "call-1"}]})
        assert client.post(self.URL, content=body, headers=_signed(body)).status_code == 200
        view = wired.views[0]["view"]
        assert wired.views[0]["trigger_id"] == "trig"
        assert json.loads(view["private_metadata"])["call_id"] == "call-1"
        assert {b["block_id"] for b in view["blocks"]} == {"address", "purchase", "rehab", "arv", "ptype", "close"}

    def test_invalid_submission_returns_inline_errors_and_stores_nothing(self, client, soft_db):
        body = _payload({"type": "view_submission", "user": {"id": "U1"}, "view": {
            "callback_id": "soft_approval_form", "private_metadata": json.dumps({"call_id": "call-1"}),
            "state": {"values": _state(purchase="")}}})
        res = client.post(self.URL, content=body, headers=_signed(body)).json()
        assert res["response_action"] == "errors" and "purchase" in res["errors"]
        assert soft_db.execute(text("SELECT count(*) FROM lending.soft_approvals")).scalar() == 0

    def test_valid_submission_without_a_lead_store_stores_no_pdf_and_replies_in_thread(self, client, wired, soft_db):
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

    def test_disabled_feature_ignores_a_signed_request(self, client, monkeypatch):
        monkeypatch.setattr(webhook, "get_settings", lambda: _settings(lending_soft_approval_enabled=False))
        body = _payload({"type": "block_actions"})
        assert client.post(self.URL, content=body, headers=_signed(body)).json() == {}
