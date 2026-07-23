"""
Unit tests for Vera's revenue truth report (VERA-v2.2 sub-task V3).

Covers only the pure functions (no live Stripe/DB connection) — the two-way
reconciliation classification, MRR delta computation, charge/refund/dispute
summarization, and the renderer. check_subscriber_reconciliation() /
check_mrr() / check_payment_activity() / check_refunds_and_disputes() /
run_revenue_truth() themselves need a live Stripe key + vera_readonly
connection and are exercised via `python -m src.agents.vera --revenue-truth`
in staging, same as V2's DB-touching functions.
"""
from datetime import date, datetime, timezone

from src.agents.vera.checks.revenue_truth import (
    MrrResult,
    PaymentActivityResult,
    ReconciliationResult,
    RefundsDisputesResult,
    _classify_access_not_paying,
    _classify_charges,
    _classify_paying_no_access,
    _compute_mrr_delta,
    _day_window_utc,
    _field,
    _select_prior_day_row,
    _stripe_mrr,
    _summarize_refunds_disputes,
    render_revenue_truth_report,
)


class _BracketOnly:
    """Mimics the real stripe-python 15.1.0 response object shape: supports
    __getitem__ (bracket access) but NOT .get() at all — hasattr(obj, 'get')
    is False on the real SDK objects, and calling .get() raises
    AttributeError. Confirmed by actually running check_mrr() against a real
    Stripe test-mode subscription during validation, which crashed outright
    on item.get("quantity") before _field() was introduced. Any test using
    plain dicts alone would never catch this — dicts support .get() fine."""

    def __init__(self, data: dict):
        self._data = data

    def __getitem__(self, key):
        return self._data[key]

    # Deliberately NO .get() method — this is the point.


# ─────────────────────────────────────────────────────────────────────────────
# Direction A — paying but no access
# ─────────────────────────────────────────────────────────────────────────────

def test_paying_no_access_flags_missing_subscriber():
    active_subs = {"cus_1": {}, "cus_2": {}}
    subscriber_by_customer = {"cus_1": {"status": "active"}}
    flagged = _classify_paying_no_access(active_subs, subscriber_by_customer)
    assert flagged == ["cus_2"]


def test_paying_no_access_flags_grace_status_too():
    # Stripe still says the subscription is live, but we've already marked
    # the subscriber grace/churned — a real mismatch, we may release their
    # ZIP while they're still actually paying. Grace is INCLUDED here, unlike
    # direction B.
    active_subs = {"cus_1": {}}
    subscriber_by_customer = {"cus_1": {"status": "grace"}}
    flagged = _classify_paying_no_access(active_subs, subscriber_by_customer)
    assert flagged == ["cus_1"]


def test_paying_no_access_clean_when_statuses_agree():
    active_subs = {"cus_1": {}}
    subscriber_by_customer = {"cus_1": {"status": "active"}}
    assert _classify_paying_no_access(active_subs, subscriber_by_customer) == []


# ─────────────────────────────────────────────────────────────────────────────
# Direction B — access but not paying
# ─────────────────────────────────────────────────────────────────────────────

def test_access_not_paying_flags_customer_not_in_active_stripe_set():
    active_subs = {"cus_1": {}}
    active_db_subscribers = [
        {"stripe_customer_id": "cus_1", "stripe_subscription_id": "sub_1", "status": "active"},
        {"stripe_customer_id": "cus_2", "stripe_subscription_id": "sub_2", "status": "active"},
    ]
    flagged = _classify_access_not_paying(active_subs, active_db_subscribers)
    assert flagged == [{"customer_id": "cus_2", "reason": "not_active_in_stripe"}]


def test_access_not_paying_flags_missing_subscription_id():
    active_subs = {}
    active_db_subscribers = [
        {"stripe_customer_id": "cus_1", "stripe_subscription_id": None, "status": "active"},
    ]
    flagged = _classify_access_not_paying(active_subs, active_db_subscribers)
    assert flagged == [{"customer_id": "cus_1", "reason": "no_stripe_subscription_id"}]


def test_access_not_paying_excludes_grace_by_design():
    # The caller filters to status == 'active' before calling this function —
    # a grace subscriber is EXPECTED to no longer be Stripe-active, so it must
    # never reach this function at all (flagging it would be a guaranteed
    # false positive every day). Passing an empty active_db_subscribers list
    # (as the caller would after filtering out grace) proves no flag fires.
    flagged = _classify_access_not_paying({}, [])
    assert flagged == []


# ─────────────────────────────────────────────────────────────────────────────
# MRR delta
# ─────────────────────────────────────────────────────────────────────────────

def test_mrr_delta_no_prior_row_returns_none():
    assert _compute_mrr_delta(1000, None) is None


def test_mrr_delta_computes_net_change():
    assert _compute_mrr_delta(1000, {"value_numeric": 800}) == 200


def test_mrr_delta_negative_change_allowed():
    assert _compute_mrr_delta(500, {"value_numeric": 800}) == -300


def test_mrr_delta_prior_row_with_null_numeric_returns_none():
    # First-ever run writes revenue.mrr.new_yesterday with no value_numeric
    # ("no prior day to compare") — reading that back as "prior" the next day
    # must not be misread as a real zero-value MRR.
    assert _compute_mrr_delta(1000, {"value_numeric": None}) is None


# ─────────────────────────────────────────────────────────────────────────────
# Prior-day row selection (the same-day-retry guard)
# ─────────────────────────────────────────────────────────────────────────────

def test_select_prior_day_row_skips_todays_own_row():
    # A same-day retry: the first run today already wrote a row (observed_at
    # = today). Without this guard, the second run would compare today
    # against itself instead of against yesterday's real value.
    today = date(2026, 7, 23)
    rows = [
        {"value_numeric": 20000, "observed_at": datetime(2026, 7, 23, 14, 0, tzinfo=timezone.utc)},
        {"value_numeric": 18000, "observed_at": datetime(2026, 7, 22, 7, 36, tzinfo=timezone.utc)},
    ]
    selected = _select_prior_day_row(rows, today)
    assert selected == rows[1]


def test_select_prior_day_row_first_ever_run_returns_none():
    assert _select_prior_day_row([], date(2026, 7, 23)) is None


def test_select_prior_day_row_skips_multiple_same_day_retries():
    today = date(2026, 7, 23)
    rows = [
        {"value_numeric": 20050, "observed_at": datetime(2026, 7, 23, 15, 0, tzinfo=timezone.utc)},
        {"value_numeric": 20000, "observed_at": datetime(2026, 7, 23, 14, 0, tzinfo=timezone.utc)},
        {"value_numeric": 18000, "observed_at": datetime(2026, 7, 22, 7, 36, tzinfo=timezone.utc)},
    ]
    selected = _select_prior_day_row(rows, today)
    assert selected == rows[2]


# ─────────────────────────────────────────────────────────────────────────────
# Day window
# ─────────────────────────────────────────────────────────────────────────────

def test_day_window_utc_is_exactly_one_day():
    gte, lt = _day_window_utc(date(2026, 7, 23))
    assert lt - gte == 86400


# ─────────────────────────────────────────────────────────────────────────────
# Charge classification (new/failed, subscription/one-time split)
# ─────────────────────────────────────────────────────────────────────────────

def test_classify_charges_splits_succeeded_and_failed():
    charges = [
        {"status": "succeeded", "amount": 5000, "invoice": "in_1"},
        {"status": "succeeded", "amount": 2000, "invoice": None},
        {"status": "failed", "amount": 3000, "invoice": "in_2"},
    ]
    result = _classify_charges(charges)
    assert result.new_count == 2
    assert result.new_amount_cents == 7000
    assert result.failed_count == 1
    assert result.failed_amount_cents == 3000


def test_classify_charges_splits_subscription_vs_one_time():
    charges = [
        {"status": "succeeded", "amount": 5000, "invoice": "in_1"},
        {"status": "succeeded", "amount": 2000, "invoice": None},
    ]
    result = _classify_charges(charges)
    assert result.subscription_count == 1 and result.subscription_amount_cents == 5000
    assert result.one_time_count == 1 and result.one_time_amount_cents == 2000


def test_classify_charges_ignores_other_statuses():
    charges = [{"status": "pending", "amount": 1000, "invoice": None}]
    result = _classify_charges(charges)
    assert result.new_count == 0 and result.failed_count == 0


# ─────────────────────────────────────────────────────────────────────────────
# Refunds & disputes
# ─────────────────────────────────────────────────────────────────────────────

def test_summarize_refunds_disputes():
    refunds = [{"amount": 1000}, {"amount": 500}]
    disputes = [{"id": "dp_1", "amount": 2000, "reason": "fraudulent"}]
    result = _summarize_refunds_disputes(refunds, disputes)
    assert result.refunds_count == 2 and result.refunds_amount_cents == 1500
    assert result.disputes_count == 1 and result.disputes_amount_cents == 2000
    assert result.disputes == [{"id": "dp_1", "amount": 2000, "reason": "fraudulent"}]


def test_summarize_refunds_disputes_empty():
    result = _summarize_refunds_disputes([], [])
    assert result.refunds_count == 0 and result.disputes_count == 0


# ─────────────────────────────────────────────────────────────────────────────
# Stripe-side MRR
# ─────────────────────────────────────────────────────────────────────────────

def test_stripe_mrr_normalizes_annual_to_monthly():
    subs = {
        "cus_1": {"items": {"data": [
            {"price": {"unit_amount": 10000, "recurring": {"interval": "monthly"}}, "quantity": 1},
        ]}},
        "cus_2": {"items": {"data": [
            {"price": {"unit_amount": 120000, "recurring": {"interval": "annual"}}, "quantity": 1},
        ]}},
    }
    # cus_1: $100/mo. cus_2: $1200/yr -> $100/mo. Total $200 = 20000 cents.
    assert _stripe_mrr(subs) == 20000


def test_stripe_mrr_skips_malformed_subscriptions():
    subs = {"cus_1": {"items": {"data": []}}}  # no line items — IndexError guarded
    assert _stripe_mrr(subs) == 0


def test_stripe_mrr_works_against_bracket_only_object():
    # Reproduces the exact shape that crashed check_mrr() against real
    # Stripe data: a subscription-item object with NO .get() method,
    # only bracket access. quantity is read via _field(), not item.get().
    item = _BracketOnly({"price": {"unit_amount": 10000, "recurring": {"interval": "monthly"}}})
    subs = {"cus_1": _BracketOnly({"items": {"data": [item]}})}
    assert _stripe_mrr(subs) == 10000


# ─────────────────────────────────────────────────────────────────────────────
# _field() — the fix for stripe-python 15.1.0 objects not supporting .get()
# ─────────────────────────────────────────────────────────────────────────────

def test_field_reads_from_plain_dict():
    assert _field({"status": "active"}, "status") == "active"


def test_field_reads_from_bracket_only_object():
    assert _field(_BracketOnly({"status": "active"}), "status") == "active"


def test_field_returns_default_on_missing_key_for_dict():
    assert _field({}, "status", "unknown") == "unknown"


def test_field_returns_default_on_missing_key_for_bracket_only_object():
    assert _field(_BracketOnly({}), "status", "unknown") == "unknown"


def test_field_returns_default_when_value_is_none():
    assert _field({"status": None}, "status", "unknown") == "unknown"


def test_classify_charges_works_against_bracket_only_objects():
    # The real production path: check_payment_activity() passes real Stripe
    # Charge objects (no .get()) into _classify_charges(), not plain dicts.
    charges = [
        _BracketOnly({"status": "succeeded", "amount": 5000, "invoice": "in_1"}),
        _BracketOnly({"status": "failed", "amount": 3000, "invoice": None}),
    ]
    result = _classify_charges(charges)
    assert result.new_count == 1 and result.new_amount_cents == 5000
    assert result.failed_count == 1 and result.failed_amount_cents == 3000
    assert result.subscription_count == 1


def test_summarize_refunds_disputes_works_against_bracket_only_objects():
    refunds = [_BracketOnly({"amount": 1000})]
    disputes = [_BracketOnly({"id": "dp_1", "amount": 2000, "reason": "fraudulent"})]
    result = _summarize_refunds_disputes(refunds, disputes)
    assert result.refunds_amount_cents == 1000
    assert result.disputes == [{"id": "dp_1", "amount": 2000, "reason": "fraudulent"}]


# ─────────────────────────────────────────────────────────────────────────────
# Renderer
# ─────────────────────────────────────────────────────────────────────────────

def _sample_inputs():
    reconciliation = ReconciliationResult(
        paying_no_access_count=1, paying_no_access_sample_ids=["cus_2"],
        access_not_paying_count=1, access_not_paying_sample_ids=["cus_4"],
        access_not_paying_details=[
            {"customer_id": "cus_4", "reason": "not_active_in_stripe", "stripe_status": "canceled"},
        ],
        active_subscriptions_by_customer={"cus_1": {}, "cus_2": {}},
    )
    mrr = MrrResult(db_total_cents=20000, active_null_plan_price_count=0,
                     stripe_total_cents=20000, drift_cents=0)
    payments = PaymentActivityResult(
        new_count=2, new_amount_cents=7000, failed_count=1, failed_amount_cents=3000,
        subscription_count=1, subscription_amount_cents=5000,
        one_time_count=1, one_time_amount_cents=2000,
    )
    refunds_disputes = RefundsDisputesResult(
        refunds_count=2, refunds_amount_cents=1500, disputes_count=1, disputes_amount_cents=2000,
        disputes=[{"id": "dp_1", "amount": 2000, "reason": "fraudulent"}],
    )
    return reconciliation, mrr, payments, refunds_disputes


def test_render_revenue_truth_report_shows_the_one_number():
    reconciliation, mrr, payments, refunds_disputes = _sample_inputs()
    subject, body = render_revenue_truth_report(
        reconciliation, mrr, 200, payments, refunds_disputes, report_date=date(2026, 7, 23),
    )
    assert "NEW MRR ADDED YESTERDAY: $2.00" in body
    assert body.rstrip().endswith("— Vera.")
    assert "2 reconciliation mismatch(es)" in subject
    assert "1 dispute(s)" in subject


def test_render_revenue_truth_report_no_prior_day():
    reconciliation, mrr, payments, refunds_disputes = _sample_inputs()
    body = render_revenue_truth_report(
        reconciliation, mrr, None, payments, refunds_disputes, report_date=date(2026, 7, 23),
    )[1]
    assert "no prior day to compare" in body


def test_render_revenue_truth_report_surfaces_reconciliation_detail():
    reconciliation, mrr, payments, refunds_disputes = _sample_inputs()
    body = render_revenue_truth_report(
        reconciliation, mrr, 200, payments, refunds_disputes, report_date=date(2026, 7, 23),
    )[1]
    assert "cus_2" in body  # paying-no-access sample
    assert "cus_4: not_active_in_stripe" in body  # access-not-paying detail
    assert "dp_1" in body and "fraudulent" in body  # dispute detail
    assert "subscription 1" in body and "one-time 1" in body  # split


# ─────────────────────────────────────────────────────────────────────────────
# Stripe-unreachable abstention (never fabricate "0 mismatches" as "verified
# clean" — a Stripe outage must read as "could not check", not a clean bill
# of health)
# ─────────────────────────────────────────────────────────────────────────────

def test_render_revenue_truth_report_surfaces_reconciliation_abstention():
    reconciliation, mrr, payments, refunds_disputes = _sample_inputs()
    reconciliation = ReconciliationResult(
        paying_no_access_count=0, paying_no_access_sample_ids=[],
        access_not_paying_count=0, access_not_paying_sample_ids=[],
        access_not_paying_details=[], active_subscriptions_by_customer=None,
        stripe_ok=False,
    )
    subject, body = render_revenue_truth_report(
        reconciliation, mrr, 200, payments, refunds_disputes, report_date=date(2026, 7, 23),
    )
    assert "STRIPE UNREACHABLE" in body
    assert "STRIPE UNREACHABLE for 1 section(s)" in subject
    # Must never silently render as a clean "0 mismatches" result.
    assert "Paying but no access: 0" not in body


def test_render_revenue_truth_report_surfaces_mrr_abstention():
    reconciliation, _mrr, payments, refunds_disputes = _sample_inputs()
    mrr = MrrResult(db_total_cents=20000, active_null_plan_price_count=0,
                     stripe_total_cents=0, drift_cents=0, stripe_ok=False)
    subject, body = render_revenue_truth_report(
        reconciliation, mrr, None, payments, refunds_disputes, report_date=date(2026, 7, 23),
    )
    assert "Stripe: UNREACHABLE" in body
    assert "DB: $200.00" in body  # DB-side number still reported even when Stripe fails
    assert "STRIPE UNREACHABLE for 1 section(s)" in subject


def test_render_revenue_truth_report_surfaces_payments_abstention():
    reconciliation, mrr, _payments, refunds_disputes = _sample_inputs()
    payments = PaymentActivityResult(stripe_ok=False)
    body = render_revenue_truth_report(
        reconciliation, mrr, 200, payments, refunds_disputes, report_date=date(2026, 7, 23),
    )[1]
    assert "STRIPE UNREACHABLE — payment activity could not be verified today." in body


def test_render_revenue_truth_report_surfaces_refunds_disputes_abstention():
    reconciliation, mrr, payments, _refunds_disputes = _sample_inputs()
    refunds_disputes = RefundsDisputesResult(stripe_ok=False)
    subject, body = render_revenue_truth_report(
        reconciliation, mrr, 200, payments, refunds_disputes, report_date=date(2026, 7, 23),
    )
    assert "STRIPE UNREACHABLE — refunds/disputes could not be verified today." in body
    assert body.rstrip().endswith("— Vera.")  # report still completes, doesn't truncate
    assert "STRIPE UNREACHABLE for 1 section(s)" in subject


def test_render_revenue_truth_report_all_sections_healthy_has_no_unreachable_subject():
    reconciliation, mrr, payments, refunds_disputes = _sample_inputs()
    subject, _body = render_revenue_truth_report(
        reconciliation, mrr, 200, payments, refunds_disputes, report_date=date(2026, 7, 23),
    )
    assert "UNREACHABLE" not in subject
