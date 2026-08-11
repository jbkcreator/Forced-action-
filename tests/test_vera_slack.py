"""Regression tests for PR #223 review findings: Slack must not drop
information the email report includes (unchecked sources, paying-no-access
IDs, truncation visibility)."""
from src.agents.vera.checks.discrepancy_digest import PromiseDigest
from src.agents.vera.checks.revenue_truth import ReconciliationResult
from src.services.vera_slack import build_digest_blocks, build_revenue_truth_blocks


def _blocks_text(blocks: list) -> str:
    out = []
    for b in blocks:
        if b["type"] in ("section", "header"):
            out.append(b["text"]["text"])
        elif b["type"] == "context":
            out.extend(e["text"] for e in b["elements"])
    return "\n".join(out)


def test_digest_blocks_warn_on_unchecked_sources():
    digest = PromiseDigest()
    blocks = build_digest_blocks(
        "subject", discrepancies=[], digest=digest, report_date="2026-08-11",
        unchecked=["deploy (V2)", "revenue (V3)"],
    )
    text = _blocks_text(blocks)
    assert "NOT CHECKED TODAY" in text
    assert "deploy (V2)" in text
    assert "revenue (V3)" in text


def test_digest_blocks_no_warning_when_nothing_unchecked():
    digest = PromiseDigest()
    blocks = build_digest_blocks(
        "subject", discrepancies=[], digest=digest, report_date="2026-08-11",
        unchecked=[],
    )
    assert "NOT CHECKED TODAY" not in _blocks_text(blocks)


def _mrr_stub():
    class _MRR:
        db_total_cents = 0
        stripe_ok = True
        stripe_total_cents = 0
        drift_cents = 0
        active_null_plan_price_count = 0
    return _MRR()


def _stripe_stub():
    class _S:
        stripe_ok = True
        new_count = 0
        new_amount_cents = 0
        subscription_count = 0
        one_time_count = 0
        failed_count = 0
        failed_amount_cents = 0
        refunds_count = 0
        refunds_amount_cents = 0
        disputes_count = 0
        disputes_amount_cents = 0
    return _S()


def test_revenue_truth_blocks_show_paying_no_access_sample_ids():
    reconciliation = ReconciliationResult(
        paying_no_access_count=2,
        paying_no_access_sample_ids=["cus_A1", "cus_B2"],
        access_not_paying_count=0,
        access_not_paying_sample_ids=[],
        access_not_paying_details=[],
    )
    blocks = build_revenue_truth_blocks(
        "subject", reconciliation, _mrr_stub(), 0, _stripe_stub(), _stripe_stub(),
        report_date="2026-08-11",
    )
    text = _blocks_text(blocks)
    assert "cus_A1" in text
    assert "cus_B2" in text


def test_revenue_truth_blocks_flag_truncated_sample():
    reconciliation = ReconciliationResult(
        paying_no_access_count=0,
        paying_no_access_sample_ids=[],
        access_not_paying_count=30,
        access_not_paying_sample_ids=[f"cus_{i}" for i in range(20)],
        access_not_paying_details=[
            {"customer_id": f"cus_{i}", "reason": "not_active_in_stripe", "stripe_status": "canceled"}
            for i in range(20)
        ],
    )
    blocks = build_revenue_truth_blocks(
        "subject", reconciliation, _mrr_stub(), 0, _stripe_stub(), _stripe_stub(),
        report_date="2026-08-11",
    )
    text = _blocks_text(blocks)
    assert "cus_0" in text
    assert "20 of 30" in text or "capped at 20 of 30" in text
