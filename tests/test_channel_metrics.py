"""Block 4 (#23) — compute_channel_metrics (CAC / payback by channel).

Mirrors tests/scenarios/test_m11_revenue_metrics_e2e.py's pattern: real
Postgres via fresh_db, a far-future window (2099) so seeded mrr_movements/
marketing_spend rows are isolated from any real committed data in shared
tables. quora_topics.cumulative_spend and affiliate_payout_ledger have no
per-window isolation available (see revenue_metrics.compute_channel_metrics
docstring — a documented approximation), so those two channels are asserted
loosely (present, non-negative), while meta/google — driven entirely by
mrr_movements + marketing_spend inside the 2099 window — get exact assertions.
"""
from datetime import date, datetime, timezone

from src.core.models import CustomerAccount, MarketingSpend, MrrMovement, Subscriber
from src.services.revenue_metrics import compute_channel_metrics

FRM = datetime(2099, 6, 1, tzinfo=timezone.utc)
TO = datetime(2099, 7, 1, tzinfo=timezone.utc)
_IN = datetime(2099, 6, 12, tzinfo=timezone.utc)
_SPEND_START = date(2099, 6, 1)
_SPEND_END = date(2099, 6, 30)


def _channel_row(rows, channel):
    return next((r for r in rows if r["channel"] == channel), None)


def _sub(db, *, utm_source=None, signup_source="direct"):
    s = Subscriber(
        stripe_customer_id=f"cus_cm_{utm_source or signup_source}_{id(object())}",
        tier="pro", vertical="roofing", county_id="hillsborough", status="active",
        event_feed_uuid=f"cm-{utm_source or signup_source}-{id(object())}",
        email=f"cm_{utm_source or signup_source}_{id(object())}@example.com",
        utm_source=utm_source, signup_source=signup_source,
    )
    db.add(s)
    db.flush()
    return s


def _acct(db, sub, *, mrr):
    a = CustomerAccount(subscriber_id=sub.id, status="active", mrr_cents=mrr)
    db.add(a)
    db.flush()
    return a


def test_meta_channel_cac_and_payback(fresh_db):
    db = fresh_db
    sub = _sub(db, utm_source="facebook")
    acct = _acct(db, sub, mrr=10000)  # $100/mo
    db.add(MrrMovement(account_id=acct.account_id, movement_type="new",
                       delta_cents=10000, mrr_after_cents=10000, effective_at=_IN))
    db.add(MarketingSpend(
        channel="meta", period_start=_SPEND_START, period_end=_SPEND_END,
        amount_cents=50000,  # $500 spend, 1 new customer → CAC $500
    ))
    db.flush()

    rows = compute_channel_metrics(db, FRM, TO)
    meta = _channel_row(rows, "meta")
    assert meta is not None
    assert meta["new_customers"] == 1
    assert meta["revenue_cents"] == 10000
    assert meta["spend_cents"] == 50000
    assert meta["cac_cents"] == 50000
    # avg_mrr_cents is a live snapshot across ALL active meta-channel
    # accounts (mirrors compute_revenue_metrics' un-windowed mrr_cents
    # convention) — not scoped to this test's seed data, so other committed
    # meta accounts in the shared dev DB affect the average. Assert
    # shape/positivity rather than an exact value; cac_cents above is the
    # fully window-scoped, deterministic assertion.
    assert meta["payback_months"] is not None
    assert meta["payback_months"] > 0


def test_facebook_and_instagram_utm_source_combine_into_one_meta_channel(fresh_db):
    # Regression guard: Meta ad placements tag utm_source differently
    # (facebook vs instagram) but must roll up into ONE combined 'meta'
    # channel — mirroring how meta_capi_service.py already treats both
    # placements as a single Meta integration (via the shared fbclid).
    db = fresh_db
    fb_sub = _sub(db, utm_source="facebook")
    ig_sub = _sub(db, utm_source="instagram")
    fb_acct = _acct(db, fb_sub, mrr=10000)
    ig_acct = _acct(db, ig_sub, mrr=15000)
    db.add(MrrMovement(account_id=fb_acct.account_id, movement_type="new",
                       delta_cents=10000, mrr_after_cents=10000, effective_at=_IN))
    db.add(MrrMovement(account_id=ig_acct.account_id, movement_type="new",
                       delta_cents=15000, mrr_after_cents=15000, effective_at=_IN))
    db.flush()

    rows = compute_channel_metrics(db, FRM, TO)
    assert _channel_row(rows, "facebook") is None
    assert _channel_row(rows, "instagram") is None
    meta = _channel_row(rows, "meta")
    assert meta is not None
    assert meta["new_customers"] >= 2
    assert meta["revenue_cents"] >= 25000


def test_no_spend_yields_null_cac(fresh_db):
    db = fresh_db
    sub = _sub(db, utm_source="google")
    acct = _acct(db, sub, mrr=5000)
    db.add(MrrMovement(account_id=acct.account_id, movement_type="new",
                       delta_cents=5000, mrr_after_cents=5000, effective_at=_IN))
    db.flush()

    rows = compute_channel_metrics(db, FRM, TO)
    g = _channel_row(rows, "google")
    assert g is not None
    assert g["new_customers"] == 1
    assert g["spend_cents"] == 0
    assert g["cac_cents"] is None
    assert g["payback_months"] is None


def test_quora_and_affiliate_are_present_and_auto_sourced(fresh_db):
    # No manual marketing_spend row needed for these — spend is auto-pulled
    # from quora_topics / affiliate_payout_ledger, not marketing_spend.
    rows = compute_channel_metrics(fresh_db, FRM, TO)
    quora = _channel_row(rows, "quora")
    affiliate = _channel_row(rows, "affiliate")
    assert quora is not None
    assert affiliate is not None
    assert quora["spend_cents"] >= 0
    assert affiliate["spend_cents"] >= 0


def test_quora_campaign_without_utm_source_buckets_as_quora(fresh_db):
    # Regression guard: Quora's producer only stamps utm_campaign
    # ("quora_<slug>"), never utm_source — a subscriber attributed this way
    # must NOT fall through to signup_source='landing_page'.
    sub = Subscriber(
        stripe_customer_id=f"cus_cm_quora_{id(object())}",
        tier="pro", vertical="roofing", county_id="hillsborough", status="active",
        event_feed_uuid=f"cm-quora-{id(object())}",
        email=f"cm_quora_{id(object())}@example.com",
        utm_source=None, utm_campaign="quora_best_roofer_tampa", signup_source="landing_page",
    )
    fresh_db.add(sub)
    fresh_db.flush()
    acct = _acct(fresh_db, sub, mrr=8000)
    fresh_db.add(MrrMovement(account_id=acct.account_id, movement_type="new",
                             delta_cents=8000, mrr_after_cents=8000, effective_at=_IN))
    fresh_db.flush()

    rows = compute_channel_metrics(fresh_db, FRM, TO)
    quora = _channel_row(rows, "quora")
    assert quora is not None
    assert quora["new_customers"] >= 1
