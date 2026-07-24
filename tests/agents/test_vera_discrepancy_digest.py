"""
Unit tests for Vera's promise & discrepancy digest (VERA-v2.2 sub-task V4).

Covers only the pure functions (no live DB) — discrepancy building from
pre-fetched facts, the promise digest split/sort, the seed-check comparator,
and the renderer. The DB-touching functions (record_promise, close_promise,
open_promises, run_discrepancy_digest, run_seed_check) are exercised on
staging via `python -m src.agents.vera --promise-digest / --seed-check`,
same convention as V2/V3.
"""
import time
from datetime import datetime, timedelta, timezone

from src.agents.vera.checks.discrepancy_digest import (
    Discrepancy,
    _compute_unchecked,
    build_discrepancies,
    build_promise_digest,
    detect_claim_discrepancy,
    render_digest_report,
)
from src.agents.vera.promises import PromiseRow


def _fact(fact_value=None, value_numeric=None):
    return {"fact_value": fact_value, "value_numeric": value_numeric}


def _promise(pid, *, owner="josh", desc="do X", mrr=None, due_offset_days=None,
             observed_days_ago=0):
    now = datetime.now(timezone.utc)
    due = None if due_offset_days is None else now + timedelta(days=due_offset_days)
    return PromiseRow(
        id=pid, thread_id=None, description=desc, owner=owner, source="manual",
        mrr_at_risk_cents=mrr, status="open", due_at=due,
        observed_at=now - timedelta(days=observed_days_ago),
    )


# ─────────────────────────────────────────────────────────────────────────────
# build_discrepancies
# ─────────────────────────────────────────────────────────────────────────────

def test_clean_state_yields_no_discrepancies():
    out = build_discrepancies(
        deploy_drift=_fact("in_sync"),
        pending_migrations=_fact(value_numeric=0),
        stale_cron=[],
        paying_no_access=_fact(value_numeric=0),
        access_not_paying=_fact(value_numeric=0),
        mrr_drift=_fact(value_numeric=0),
    )
    assert out == []


def test_deploy_drift_flagged():
    out = build_discrepancies(_fact("behind"), None, [], None, None, None)
    assert len(out) == 1
    assert out[0].source == "deploy.drift"
    assert "behind" in out[0].live


def test_deploy_drift_unknown_is_abstention_not_a_discrepancy():
    # V2 writes drift='unknown' when it can't verify (repo not on host / git
    # failed). That must NOT be reported as a confirmed discrepancy.
    out = build_discrepancies(_fact("unknown"), None, [], None, None, None)
    assert out == []


def test_pending_migrations_flagged_only_when_positive():
    assert build_discrepancies(None, _fact(value_numeric=0), [], None, None, None) == []
    out = build_discrepancies(None, _fact(value_numeric=3), [], None, None, None)
    assert len(out) == 1 and "3 migration" in out[0].live


def test_stale_cron_each_becomes_a_discrepancy():
    stale = [{"label": "foreclosures/hillsborough", "fact_value": "stale"},
             {"label": "permits", "fact_value": "stale"}]
    out = build_discrepancies(None, None, stale, None, None, None)
    assert len(out) == 2
    assert out[0].source == "cron.foreclosures/hillsborough.freshness"


def test_reconciliation_and_mrr_drift_flagged():
    out = build_discrepancies(
        None, None, [],
        paying_no_access=_fact(value_numeric=2),
        access_not_paying=_fact(value_numeric=1),
        mrr_drift=_fact(value_numeric=500),
    )
    sources = {d.source for d in out}
    assert sources == {
        "revenue.reconciliation.paying_no_access.count",
        "revenue.reconciliation.access_not_paying.count",
        "revenue.mrr.drift_cents",
    }
    mrr = next(d for d in out if d.source == "revenue.mrr.drift_cents")
    assert "$5.00" in mrr.live


def test_missing_fact_is_skipped_not_a_false_all_clear():
    # None facts (source check didn't run) must not fabricate a clean verdict.
    out = build_discrepancies(None, None, [], None, None, None)
    assert out == []  # skipped, and the orchestrator reports these as "unchecked"


# ─────────────────────────────────────────────────────────────────────────────
# detect_claim_discrepancy (seed-check comparator)
# ─────────────────────────────────────────────────────────────────────────────

def test_seed_check_catches_false_claim():
    assert detect_claim_discrepancy("bogus", "real_sha") is not None


def test_seed_check_catches_claim_even_when_head_unknown():
    # Non-prod host: real HEAD is None; a bogus claim still differs -> caught.
    d = detect_claim_discrepancy("bogus", None)
    assert d is not None and "unknown" in d.live


def test_seed_check_no_discrepancy_when_equal():
    assert detect_claim_discrepancy("abc", "abc") is None


# ─────────────────────────────────────────────────────────────────────────────
# build_promise_digest
# ─────────────────────────────────────────────────────────────────────────────

def test_overdue_vs_pending_split():
    now = datetime.now(timezone.utc)
    overdue = _promise(1, due_offset_days=-1)
    pending = _promise(2, due_offset_days=5)
    no_due = _promise(3, due_offset_days=None)  # no due date -> never overdue
    digest = build_promise_digest([overdue, pending, no_due], now=now)
    assert [p.id for p in digest.overdue] == [1]
    assert {p.id for p in digest.pending} == {2, 3}
    assert digest.open_count == 3


def test_digest_sorts_by_mrr_then_age():
    now = datetime.now(timezone.utc)
    low = _promise(1, due_offset_days=5, mrr=100, observed_days_ago=1)
    high = _promise(2, due_offset_days=5, mrr=9000, observed_days_ago=1)
    digest = build_promise_digest([low, high], now=now)
    assert [p.id for p in digest.pending] == [2, 1]  # higher MRR-at-risk first


# ─────────────────────────────────────────────────────────────────────────────
# render_digest_report — no DB, counts correct, empty sections say "none"
# ─────────────────────────────────────────────────────────────────────────────

def test_render_is_pure_and_reflects_counts():
    start = time.perf_counter()
    digest = build_promise_digest([_promise(1, due_offset_days=-1, mrr=1100)])
    discrepancies = [Discrepancy("claim", "live", "deploy.drift")]
    subject, body, html = render_digest_report(digest, discrepancies, unchecked=["revenue (V3)"])
    elapsed = time.perf_counter() - start
    assert elapsed < 1.0  # no live DB/network — the V3 pre-fetch lesson
    assert "1 discrepancy(ies)" in subject
    assert "1 overdue promise(s)" in subject
    assert "OPEN DISCREPANCIES: 1" in body
    assert "not checked today" in body.lower() or "revenue (V3)" in body
    assert "Promise &amp; Discrepancy Digest" in html
    assert "OPEN DISCREPANCIES" in html


def test_render_empty_state():
    subject, body, _ = render_digest_report(build_promise_digest([]), [], unchecked=[])
    assert "0 discrepancy(ies)" in subject
    assert "none — every checked claim matches live state." in body


# ─────────────────────────────────────────────────────────────────────────────
# Regression: PR #173 review — a Stripe outage must be reported as "revenue
# unchecked", never as a clean reconciliation, even though V3 now writes
# abstention facts (rows exist, but with no value_numeric).
# ─────────────────────────────────────────────────────────────────────────────

def test_unchecked_flags_revenue_when_all_facts_are_abstention_rows():
    # This is exactly what V3 writes after a Stripe outage: rows exist (not
    # None) but carry no value_numeric ("stripe unreachable").
    abstained = _fact("stripe unreachable", value_numeric=None)
    unchecked = _compute_unchecked(
        deploy_drift=_fact("in_sync"),
        paying_no_access=abstained,
        access_not_paying=abstained,
        mrr_drift=abstained,
    )
    assert "revenue (V3)" in unchecked


def test_unchecked_flags_revenue_when_facts_are_entirely_missing():
    unchecked = _compute_unchecked(
        deploy_drift=_fact("in_sync"),
        paying_no_access=None, access_not_paying=None, mrr_drift=None,
    )
    assert "revenue (V3)" in unchecked


def test_unchecked_does_not_flag_revenue_when_any_fact_is_real():
    unchecked = _compute_unchecked(
        deploy_drift=_fact("in_sync"),
        paying_no_access=_fact(value_numeric=0),
        access_not_paying=None,
        mrr_drift=None,
    )
    assert "revenue (V3)" not in unchecked


def test_unchecked_flags_deploy_on_unknown_drift():
    unchecked = _compute_unchecked(
        deploy_drift=_fact("unknown"),
        paying_no_access=_fact(value_numeric=0),
        access_not_paying=_fact(value_numeric=0),
        mrr_drift=_fact(value_numeric=0),
    )
    assert unchecked == ["deploy (V2)"]
