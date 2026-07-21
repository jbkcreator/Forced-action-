"""T-B8-01 — compute_operator_dashboard aggregation shape + per-KPI queries.

Mirrors tests/test_channel_metrics.py's pattern: real Postgres via fresh_db,
a far-future window (2099) so seeded rows are isolated from any real
committed data in shared tables.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from src.core.models import (
    ChurnPrediction,
    CoraIncident,
    CustomerAccount,
    HumanCloseEscalation,
    MrrMovement,
    ScraperAlertLog,
    Subscriber,
)
from src.services.operator_dashboard import compute_operator_dashboard

FRM = datetime(2099, 6, 1, tzinfo=timezone.utc)
TO = datetime(2099, 7, 1, tzinfo=timezone.utc)
_IN = datetime(2099, 6, 12, tzinfo=timezone.utc)
_OUT = datetime(2099, 5, 1, tzinfo=timezone.utc)

_UNAVAILABLE_KPIS = {
    "deals_submitted": "Block 5 (investor_deals) not yet built",
    "lender_matches": "Block 5 (lender-matrix rule engine) not yet built",
    "loans_funded": "Block 7 (lender integration tiers) not yet built",
    "commissions_owed": "Block 7 (referral commission ledger) not yet built",
}
_AVAILABLE_KPIS = {
    "mrr", "new_accounts", "churn_risk", "leads_delivered",
    "activation", "source_failures", "cora_approvals_waiting",
}
_HIGH_RISK_COUNT_SQL = text("""
    SELECT COUNT(*) FROM (
        SELECT DISTINCT ON (subscriber_id) subscriber_id, churn_risk_band
        FROM churn_predictions
        ORDER BY subscriber_id, predicted_at DESC
    ) latest
    WHERE latest.churn_risk_band IN ('high', 'very_high')
""")


def _sub(db):
    tag = uuid.uuid4().hex[:12]
    s = Subscriber(
        stripe_customer_id=f"cus_od_{tag}",
        tier="pro", vertical="roofing", county_id="hillsborough", status="active",
        event_feed_uuid=f"od-{tag}", email=f"od_{tag}@example.com",
    )
    db.add(s)
    db.flush()
    return s


def _acct(db, sub, *, mrr=0):
    a = CustomerAccount(subscriber_id=sub.id, status="active", mrr_cents=mrr)
    db.add(a)
    db.flush()
    return a


def test_all_11_kpis_present_with_correct_availability(fresh_db):
    result = compute_operator_dashboard(fresh_db, FRM, TO)

    assert result["from"] == FRM.isoformat()
    assert result["to"] == TO.isoformat()
    kpis = result["kpis"]
    assert set(kpis.keys()) == _AVAILABLE_KPIS | set(_UNAVAILABLE_KPIS.keys())

    for key, reason in _UNAVAILABLE_KPIS.items():
        assert kpis[key] == {"available": False, "reason": reason}

    for key in _AVAILABLE_KPIS:
        assert kpis[key]["available"] is True


def test_new_accounts_counts_only_new_movements_in_window(fresh_db):
    sub = _sub(fresh_db)
    acct = _acct(fresh_db, sub)
    fresh_db.add(MrrMovement(
        account_id=acct.account_id, movement_type="new",
        delta_cents=10000, mrr_after_cents=10000, effective_at=_IN,
    ))
    fresh_db.add(MrrMovement(
        account_id=acct.account_id, movement_type="new",
        delta_cents=5000, mrr_after_cents=15000, effective_at=_OUT,
    ))
    fresh_db.add(MrrMovement(
        account_id=acct.account_id, movement_type="churn",
        delta_cents=-10000, mrr_after_cents=5000, effective_at=_IN,
    ))
    fresh_db.flush()

    result = compute_operator_dashboard(fresh_db, FRM, TO)
    assert result["kpis"]["new_accounts"] == {"available": True, "value": 1}


def test_churn_risk_counts_latest_band_per_subscriber_only(fresh_db):
    # churn_risk is a current-state snapshot (mirrors compute_revenue_metrics'
    # un-windowed mrr_cents/active_accounts convention), so it isn't isolated
    # by the 2099 window — assert the delta this test's rows produce, not an
    # absolute count, since the shared dev DB may have other committed
    # high/very_high predictions.
    before = fresh_db.execute(_HIGH_RISK_COUNT_SQL).scalar()

    high_risk_sub = _sub(fresh_db)
    low_risk_sub = _sub(fresh_db)

    # high_risk_sub: stale 'low' prediction, then a newer 'high' one — only
    # the latest should count.
    fresh_db.add(ChurnPrediction(
        subscriber_id=high_risk_sub.id, predicted_at=_OUT,
        churn_risk_score=10, churn_risk_band="low",
    ))
    fresh_db.add(ChurnPrediction(
        subscriber_id=high_risk_sub.id, predicted_at=_IN,
        churn_risk_score=90, churn_risk_band="high",
    ))
    fresh_db.add(ChurnPrediction(
        subscriber_id=low_risk_sub.id, predicted_at=_IN,
        churn_risk_score=5, churn_risk_band="low",
    ))
    fresh_db.flush()

    result = compute_operator_dashboard(fresh_db, FRM, TO)
    assert result["kpis"]["churn_risk"]["available"] is True
    assert result["kpis"]["churn_risk"]["value"] - before == 1


def test_source_failures_counts_scraper_alerts_within_cooldown_window(fresh_db):
    # Canonical count (action_queue.source_failures) = scraper alerts within the
    # rolling alert-cooldown window (now - alert_cooldown_hours), NOT the
    # dashboard from/to window. Assert the delta against real-now timestamps so
    # the test is independent of the far-future FRM/TO and any shared-DB rows.
    now = datetime.now(timezone.utc)
    before = compute_operator_dashboard(fresh_db, FRM, TO)["kpis"]["source_failures"]["value"]

    fresh_db.add(ScraperAlertLog(source_type="foreclosures", alert_type="scraper_error", alerted_at=now))
    fresh_db.add(ScraperAlertLog(source_type="deeds", alert_type="zero_records", alerted_at=now))
    fresh_db.add(ScraperAlertLog(source_type="deeds", alert_type="zero_records",
                                 alerted_at=now - timedelta(hours=48)))  # outside cooldown
    fresh_db.flush()

    result = compute_operator_dashboard(fresh_db, FRM, TO)
    assert result["kpis"]["source_failures"]["available"] is True
    assert result["kpis"]["source_failures"]["value"] - before == 2


def test_cora_approvals_waiting_counts_legal_lane_incidents_only(fresh_db):
    # Canonical count (action_queue.cora_approvals_waiting) = open cora incidents
    # in the legal lane (human_escalated / feature_killed) ONLY. Auto-handled
    # incidents, resolved incidents, and human-close escalations are excluded.
    # Delta-based (current-state snapshot, shared DB) like churn_risk.
    before = compute_operator_dashboard(fresh_db, FRM, TO)["kpis"]["cora_approvals_waiting"]["value"]

    sub = _sub(fresh_db)

    fresh_db.add(CoraIncident(  # counted
        metric_name="dialable_rate", severity="red", action_taken="human_escalated",
        observed_value=0.1, threshold_value=0.5, breach_started=_IN, breach_resolved=None,
    ))
    fresh_db.add(CoraIncident(  # counted
        metric_name="reply_rate", severity="red", action_taken="feature_killed",
        observed_value=0.1, threshold_value=0.5, breach_started=_IN, breach_resolved=None,
    ))
    fresh_db.add(CoraIncident(  # NOT counted — auto-handled (ops lane)
        metric_name="dialable_rate", severity="yellow", action_taken="auto_paused",
        observed_value=0.4, threshold_value=0.5, breach_started=_IN, breach_resolved=None,
    ))
    fresh_db.add(CoraIncident(  # NOT counted — resolved
        metric_name="dialable_rate", severity="yellow", action_taken="human_escalated",
        observed_value=0.4, threshold_value=0.5, breach_started=_IN, breach_resolved=_IN,
    ))
    fresh_db.add(HumanCloseEscalation(  # NOT counted — human-close excluded from this KPI
        subscriber_id=sub.id, decision_id="dec-1",
        revenue_signal_score=90, interactions_count=3,
        target_tier="pro", channel="sms", outcome=None,
    ))
    fresh_db.flush()

    result = compute_operator_dashboard(fresh_db, FRM, TO)
    assert result["kpis"]["cora_approvals_waiting"]["available"] is True
    assert result["kpis"]["cora_approvals_waiting"]["value"] - before == 2
