"""M11 / B3 Revenue Reporting — DB-backed metric tests (real Postgres via fresh_db).

Seeds a coherent B1+B2 scenario and asserts the §4A.5 metric bundle — this is the
acceptance test: MRR, active accounts, leads delivered, free→paid, churn, and unit
economics all return correct values from seed data.
"""

from datetime import datetime, timezone

import pytest
from sqlalchemy import text

from src.core.models import (
    CustomerAccount, Delivery, EnrichmentUsageLog, FreeToPaidAttribution,
    MrrMovement, Property,
)
from src.services.revenue_metrics import compute_revenue_metrics

pytestmark = pytest.mark.scenario_platform

# Far-future window: snapshot tables (customer_accounts/deliveries) are new and
# empty of committed data, but enrichment_usage_logs is a pre-existing production
# table — fresh_db SELECTs see its committed rows. A 2099 window guarantees only
# our seeded rows fall inside it, so the period/cost assertions are deterministic.
FRM = datetime(2099, 6, 1, tzinfo=timezone.utc)
TO = datetime(2099, 7, 1, tzinfo=timezone.utc)
_CYCLE = datetime(2099, 7, 23, tzinfo=timezone.utc)
_IN = datetime(2099, 6, 12, tzinfo=timezone.utc)     # inside window
_OUT = datetime(2099, 5, 1, tzinfo=timezone.utc)      # before window
_FREE_AT = datetime(2099, 5, 20, tzinfo=timezone.utc)
_CONV_AT = datetime(2099, 6, 10, tzinfo=timezone.utc)
_ENR_AT = datetime(2099, 6, 8, tzinfo=timezone.utc)


def _acct(db, cust, *, status, mrr, entitlement=None):
    a = CustomerAccount(stripe_customer_id=cust, status=status, mrr_cents=mrr,
                        lead_entitlement=entitlement or {})
    db.add(a); db.flush()
    return a


def _prop(db, parcel):
    p = Property(parcel_id=parcel, zip="33601", county_id="hillsborough")
    db.add(p); db.flush()
    return p


def _delivery(db, acct, prop, *, grade, when, cycle_end):
    d = Delivery(property_id=prop.id, account_id=acct.account_id, grade=grade,
                 vertical="roofing", status="delivered", delivered_at=when,
                 billing_period_end=cycle_end)
    db.add(d); db.flush()
    return d


def test_revenue_metrics_bundle_from_seed(fresh_db):
    db = fresh_db

    # accounts: 1 active ($299), 1 past_due ($299), 1 churned
    a = _acct(db, "cus_m11_a", status="active", mrr=29900, entitlement={"gold": 20})
    _acct(db, "cus_m11_pd", status="past_due", mrr=29900)
    c = _acct(db, "cus_m11_ch", status="churned", mrr=0)

    # MRR movements: a 'new' in-window for A; a voluntary 'churn' in-window for C
    db.add(MrrMovement(account_id=a.account_id, movement_type="new", delta_cents=29900,
                       mrr_after_cents=29900, effective_at=_IN))
    db.add(MrrMovement(account_id=c.account_id, movement_type="churn", delta_cents=-29900,
                       mrr_after_cents=0, is_involuntary=False, effective_at=_IN))
    db.flush()

    # deliveries to A: 1 free Bronze (pre-window, cycle NULL), 3 Gold in-window, 1 Gold pre-window
    free = _delivery(db, a, _prop(db, "M11-free"), grade="Bronze",
                     when=_FREE_AT, cycle_end=None)
    for i in range(3):
        _delivery(db, a, _prop(db, f"M11-g{i}"), grade="Gold", when=_IN, cycle_end=_CYCLE)
    _delivery(db, a, _prop(db, "M11-old"), grade="Gold", when=_OUT, cycle_end=_CYCLE)

    # attribution: A converted in-window, 2 free leads, first-touch = the free delivery
    db.add(FreeToPaidAttribution(
        account_id=a.account_id, first_free_delivery_id=free.id, last_free_delivery_id=free.id,
        free_leads_count=2, converted_at=_CONV_AT, first_paid_plan="starter",
    ))
    # enrichment cost: 2 records @ 50c in-window → cost_per_record = 50
    for i in range(2):
        db.add(EnrichmentUsageLog(vendor="batchdata", purpose="batch_skip_trace",
                                  property_id=_prop(db, f"M11-enr{i}").id,
                                  cost_cents=50, success=True, created_at=_ENR_AT))
    db.flush()

    m = compute_revenue_metrics(db, FRM, TO)

    # snapshot
    assert m["mrr_cents"] == 29900
    assert m["active_accounts"] == 1
    assert m["past_due_count"] == 1
    assert m["at_risk_mrr_cents"] == 29900
    # period: movements
    assert m["new_mrr_cents"] == 29900
    assert m["voluntary_churn_cents"] == 29900
    assert m["involuntary_churn_cents"] == 0
    assert m["churned_count"] == 1
    # period: deliveries (windowed; the pre-window Gold and Bronze excluded)
    assert m["leads_delivered_by_grade"] == {"Gold": 3}
    assert m["leads_delivered_by_account"][str(a.account_id)] == 3
    assert m["entitlement_utilization"] == 0.15        # 3 delivered / 20 owned
    # free→paid
    assert m["free_to_paid_rate"] == 1.0               # 1 conversion / 1 free-lead account
    assert m["avg_time_to_convert_days"] == 21.0       # 2026-06-10 − 2026-05-20
    # unit economics
    assert m["cost_per_record_cents"] == 50
    assert m["revenue_per_lead_cents"] == round(29900 / 3)   # MRR / leads delivered


def test_empty_db_returns_zeroes(fresh_db):
    m = compute_revenue_metrics(fresh_db, FRM, TO)
    assert m["mrr_cents"] == 0
    assert m["active_accounts"] == 0
    assert m["leads_delivered_by_grade"] == {}
    assert m["free_to_paid_rate"] == 0.0
    assert m["avg_time_to_convert_days"] is None
    assert m["cost_per_record_cents"] == 0
    assert m["revenue_per_lead_cents"] == 0
