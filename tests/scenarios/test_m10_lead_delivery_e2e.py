"""M10 / B2 Lead Delivery — DB-backed claim/matching tests (real Postgres via fresh_db)."""

import threading
from datetime import datetime, timezone

import pytest
from sqlalchemy import text

from src.core.database import get_db_context
from src.core.models import CustomerAccount, Delivery, Property, Subscriber, ZipTerritory
from src.services.lead_delivery import Lead, claim, record_free_to_paid, reject_delivery, rejection_rate
from src.services.revenue_engine import record_subscription_active

pytestmark = pytest.mark.scenario_platform

_PERIOD_END = datetime(2026, 7, 23, tzinfo=timezone.utc)


def _ensure_plans(db):
    db.execute(text("""
        INSERT INTO plans (plan_id, name, tier, price_cents, interval, entitlements)
        VALUES ('starter','Starter','starter',29900,'monthly','{}'::jsonb),
               ('pro','Pro','pro',49900,'monthly','{}'::jsonb)
        ON CONFLICT (plan_id) DO NOTHING
    """))


def _mk_property(db, parcel, *, zip_code="33601", county="hillsborough"):
    p = Property(parcel_id=parcel, zip=zip_code, county_id=county)
    db.add(p)
    db.flush()
    return p


def _mk_account(db, *, cust, plan_tier="starter", entitlement, status="active",
                vertical="roofing", zip_code="33601", county="hillsborough",
                period_end=_PERIOD_END, lock_zip=True):
    sub = Subscriber(
        stripe_customer_id=cust, tier="starter", vertical=vertical,
        county_id=county, status="active",
    )
    db.add(sub)
    db.flush()
    acct = CustomerAccount(
        stripe_customer_id=cust, subscriber_id=sub.id, status=status,
        plan_tier=plan_tier, lead_entitlement=entitlement, current_period_end=period_end,
    )
    db.add(acct)
    db.flush()
    if lock_zip:
        db.add(ZipTerritory(
            zip_code=zip_code, vertical=vertical, county_id=county,
            subscriber_id=sub.id, status="locked",
        ))
        db.flush()
    return sub, acct


def _lead(prop, *, grade="Gold", verticals=("roofing",)):
    return Lead(property_id=prop.id, zip_code=prop.zip, county_id=prop.county_id,
                grade=grade, verticals=list(verticals))


def test_claim_single_candidate_delivers(fresh_db):
    _ensure_plans(fresh_db)
    prop = _mk_property(fresh_db, "M10TEST-1")
    _, acct = _mk_account(fresh_db, cust="cus_m10_1", entitlement={"gold": 20})

    d = claim(fresh_db, _lead(prop))
    fresh_db.flush()

    assert d is not None
    assert d.account_id == acct.account_id
    assert d.grade == "Gold"
    assert d.vertical == "roofing"
    assert d.billing_period_end == _PERIOD_END
    assert d.status == "delivered"


def test_claim_exclusivity_second_claim_returns_none(fresh_db):
    _ensure_plans(fresh_db)
    prop = _mk_property(fresh_db, "M10TEST-2")
    _mk_account(fresh_db, cust="cus_m10_2a", entitlement={"gold": 20})
    # a second contractor also covers it (different vertical the lead qualifies for)
    _mk_account(fresh_db, cust="cus_m10_2b", entitlement={"gold": 20},
                vertical="restoration")

    first = claim(fresh_db, _lead(prop, verticals=("roofing", "restoration")))
    fresh_db.flush()
    second = claim(fresh_db, _lead(prop, verticals=("roofing", "restoration")))
    fresh_db.flush()

    assert first is not None
    assert second is None  # exclusive: lead already claimed
    count = fresh_db.execute(
        text("SELECT count(*) FROM deliveries WHERE property_id = :p"), {"p": prop.id}
    ).scalar()
    assert count == 1


def test_no_coverage_returns_none(fresh_db):
    _ensure_plans(fresh_db)
    prop = _mk_property(fresh_db, "M10TEST-3", zip_code="99999")
    _mk_account(fresh_db, cust="cus_m10_3", entitlement={"gold": 20}, zip_code="33601")

    assert claim(fresh_db, _lead(prop)) is None  # property zip 99999 unlocked → undelivered pool


def test_exhausted_entitlement_skips_account(fresh_db):
    _ensure_plans(fresh_db)
    _, acct = _mk_account(fresh_db, cust="cus_m10_4", entitlement={"gold": 1})
    # burn the single gold slot this cycle
    p1 = _mk_property(fresh_db, "M10TEST-4a")
    assert claim(fresh_db, _lead(p1)) is not None
    fresh_db.flush()
    # next gold lead → account exhausted → no delivery (upsell pool)
    p2 = _mk_property(fresh_db, "M10TEST-4b")
    assert claim(fresh_db, _lead(p2)) is None


def test_not_entitled_to_grade_skips_account(fresh_db):
    _ensure_plans(fresh_db)
    _mk_account(fresh_db, cust="cus_m10_5", entitlement={"bronze": 5})  # no gold bucket
    prop = _mk_property(fresh_db, "M10TEST-5")

    assert claim(fresh_db, _lead(prop, grade="Gold")) is None


def test_tier_priority_picks_higher_plan(fresh_db):
    _ensure_plans(fresh_db)
    prop = _mk_property(fresh_db, "M10TEST-6")
    # roofing locked by a Starter account; restoration locked by a Pro account
    _mk_account(fresh_db, cust="cus_m10_6_starter", plan_tier="starter",
                entitlement={"gold": 50}, vertical="roofing")
    _, pro = _mk_account(fresh_db, cust="cus_m10_6_pro", plan_tier="pro",
                         entitlement={"gold": 20}, vertical="restoration")

    d = claim(fresh_db, _lead(prop, verticals=("roofing", "restoration")))
    fresh_db.flush()
    assert d.account_id == pro.account_id  # Pro outranks Starter despite less headroom


def test_churned_account_excluded(fresh_db):
    _ensure_plans(fresh_db)
    _mk_account(fresh_db, cust="cus_m10_7", entitlement={"gold": 20}, status="churned")
    prop = _mk_property(fresh_db, "M10TEST-7")

    assert claim(fresh_db, _lead(prop)) is None  # churned → not served


def test_past_due_account_still_served(fresh_db):
    _ensure_plans(fresh_db)
    _, acct = _mk_account(fresh_db, cust="cus_m10_8", entitlement={"gold": 20}, status="past_due")
    prop = _mk_property(fresh_db, "M10TEST-8")

    d = claim(fresh_db, _lead(prop))
    fresh_db.flush()
    assert d is not None and d.account_id == acct.account_id


def test_attribution_first_touch_on_conversion(fresh_db):
    """Free Bronze leads delivered during trial, then the account converts to paid
    (via B1) → exactly one attribution row, first-touch = earliest free delivery."""
    _ensure_plans(fresh_db)
    _, acct = _mk_account(fresh_db, cust="cus_m10_attr", plan_tier=None,
                          status="free_trial", entitlement={"bronze": 5}, period_end=None)
    d1 = claim(fresh_db, _lead(_mk_property(fresh_db, "M10ATTR-1"), grade="Bronze"))
    d2 = claim(fresh_db, _lead(_mk_property(fresh_db, "M10ATTR-2"), grade="Bronze"))
    fresh_db.flush()
    assert d1 and d2

    # B1 conversion → triggers the attribution hook
    record_subscription_active(
        fresh_db, acct, plan_id="starter", stripe_subscription_id="sub_attr",
        current_period_end=_PERIOD_END, stripe_event_id="evt_attr",
    )
    fresh_db.flush()

    row = fresh_db.execute(text("""
        SELECT first_free_delivery_id, last_free_delivery_id, free_leads_count, first_paid_plan
        FROM free_to_paid_attribution WHERE account_id = :a
    """), {"a": str(acct.account_id)}).fetchone()
    assert row is not None
    assert row.free_leads_count == 2
    assert row.first_free_delivery_id == d1.id   # first-touch
    assert row.last_free_delivery_id == d2.id
    assert row.first_paid_plan == "starter"


def test_attribution_is_idempotent(fresh_db):
    _ensure_plans(fresh_db)
    _, acct = _mk_account(fresh_db, cust="cus_m10_attr2", entitlement={"gold": 20})
    now = datetime(2026, 6, 23, tzinfo=timezone.utc)

    first = record_free_to_paid(fresh_db, acct, first_paid_plan="starter", converted_at=now)
    second = record_free_to_paid(fresh_db, acct, first_paid_plan="starter", converted_at=now)
    fresh_db.flush()

    assert first is not None
    assert second is None  # one row per account
    count = fresh_db.execute(
        text("SELECT count(*) FROM free_to_paid_attribution WHERE account_id = :a"),
        {"a": str(acct.account_id)},
    ).scalar()
    assert count == 1


def test_attribution_instant_pay_zero_free_leads(fresh_db):
    """Account that pays immediately with no prior free leads → row with count 0,
    NULL first-touch (still recorded; reporting filters count>0 for the milestone)."""
    _ensure_plans(fresh_db)
    _, acct = _mk_account(fresh_db, cust="cus_m10_attr3", plan_tier=None,
                          status="free_trial", entitlement={"bronze": 5}, period_end=None)

    record_subscription_active(
        fresh_db, acct, plan_id="starter", stripe_subscription_id="sub_attr3",
        current_period_end=_PERIOD_END, stripe_event_id="evt_attr3",
    )
    fresh_db.flush()

    row = fresh_db.execute(text("""
        SELECT first_free_delivery_id, free_leads_count
        FROM free_to_paid_attribution WHERE account_id = :a
    """), {"a": str(acct.account_id)}).fetchone()
    assert row is not None
    assert row.free_leads_count == 0
    assert row.first_free_delivery_id is None


def test_reject_marks_row_and_grants_credit(fresh_db):
    _ensure_plans(fresh_db)
    _, acct = _mk_account(fresh_db, cust="cus_m10_rej", entitlement={"gold": 20})
    d = claim(fresh_db, _lead(_mk_property(fresh_db, "M10REJ-1")))
    fresh_db.flush()

    reject_delivery(fresh_db, d.id, "disconnected")
    fresh_db.flush()
    fresh_db.refresh(d)
    fresh_db.refresh(acct)

    assert d.status == "rejected"
    assert d.rejection_reason == "disconnected"
    assert d.rejected_at is not None
    assert acct.lead_credits.get("gold") == 1


def test_reject_credit_yields_one_replacement_not_two(fresh_db):
    """Single compensation: reject of a 1-bucket account lets exactly one
    replacement through, then exhausted again (no double-credit)."""
    _ensure_plans(fresh_db)
    _mk_account(fresh_db, cust="cus_m10_rej2", entitlement={"gold": 1})
    a = claim(fresh_db, _lead(_mk_property(fresh_db, "M10REJ-2a")))
    fresh_db.flush()
    assert a is not None
    # exhausted
    assert claim(fresh_db, _lead(_mk_property(fresh_db, "M10REJ-2b"))) is None

    reject_delivery(fresh_db, a.id, "wrong_party")     # +1 credit
    fresh_db.flush()

    # one replacement gets through…
    c = claim(fresh_db, _lead(_mk_property(fresh_db, "M10REJ-2c")))
    fresh_db.flush()
    assert c is not None
    # …but not a second (net = 1 good lead for a 1-bucket plan)
    assert claim(fresh_db, _lead(_mk_property(fresh_db, "M10REJ-2d"))) is None


def test_reject_is_idempotent(fresh_db):
    _ensure_plans(fresh_db)
    _, acct = _mk_account(fresh_db, cust="cus_m10_rej3", entitlement={"gold": 20})
    d = claim(fresh_db, _lead(_mk_property(fresh_db, "M10REJ-3")))
    fresh_db.flush()

    reject_delivery(fresh_db, d.id, "deceased")
    reject_delivery(fresh_db, d.id, "deceased")   # second call: no-op
    fresh_db.flush()
    fresh_db.refresh(acct)

    assert acct.lead_credits.get("gold") == 1     # credited once only


def test_rejection_rate_guardrail(fresh_db):
    _ensure_plans(fresh_db)
    _, acct = _mk_account(fresh_db, cust="cus_m10_rej4", entitlement={"gold": 20})
    d1 = claim(fresh_db, _lead(_mk_property(fresh_db, "M10REJ-4a")))
    claim(fresh_db, _lead(_mk_property(fresh_db, "M10REJ-4b")))
    fresh_db.flush()
    reject_delivery(fresh_db, d1.id, "duplicate")
    fresh_db.flush()

    assert rejection_rate(fresh_db, acct.account_id) == 0.5


def test_ultra_grade_delivers_with_ultra_bucket(fresh_db):
    """M6 emits grade 'Ultra' (not CDS 'Ultra Platinum'); an account with an
    'ultra' entitlement bucket must receive it — proves the grade pipe is open
    end to end after adding the higher-grade buckets to the plan seeds."""
    _ensure_plans(fresh_db)
    _, acct = _mk_account(fresh_db, cust="cus_m10_ultra",
                          entitlement={"ultra": 2, "gold": 20})
    prop = _mk_property(fresh_db, "M10-ULTRA")

    d = claim(fresh_db, _lead(prop, grade="Ultra"))
    fresh_db.flush()
    assert d is not None and d.account_id == acct.account_id
    assert d.grade == "Ultra"


def test_concurrent_claims_deliver_only_once():
    """§12.3 HIGH SEVERITY: two matchers racing the same lead must produce exactly
    one delivery. Uses two independent committed sessions through a barrier so the
    property-row FOR UPDATE lock is genuinely exercised. Self-cleans (commits to DB)."""
    PARCEL = "M10CONC-1"
    CUSTS = ["cus_m10_conc_a", "cus_m10_conc_b"]
    results: dict[str, object] = {}
    barrier = threading.Barrier(2)

    try:
        with get_db_context() as s:
            # plan_tier=None → no plans FK, so this committed-then-cleaned fixture
            # never pollutes the shared `plans` catalog (exclusivity needs no ranking).
            prop = _mk_property(s, PARCEL)
            _mk_account(s, cust=CUSTS[0], plan_tier=None, entitlement={"gold": 20}, vertical="roofing")
            _mk_account(s, cust=CUSTS[1], plan_tier=None, entitlement={"gold": 20}, vertical="restoration")
            pid = prop.id
            s.commit()

        def worker(name):
            try:
                with get_db_context() as s:
                    lead = Lead(property_id=pid, zip_code="33601", county_id="hillsborough",
                                grade="Gold", verticals=["roofing", "restoration"])
                    barrier.wait(timeout=10)
                    d = claim(s, lead)
                    s.commit()
                    results[name] = (d is not None)
            except Exception as exc:  # noqa: BLE001
                results[name] = exc

        t1 = threading.Thread(target=worker, args=("a",))
        t2 = threading.Thread(target=worker, args=("b",))
        t1.start(); t2.start(); t1.join(); t2.join()

        with get_db_context() as s:
            count = s.execute(text("SELECT count(*) FROM deliveries WHERE property_id = :p"),
                              {"p": pid}).scalar()

        assert not any(isinstance(v, Exception) for v in results.values()), results
        assert count == 1                              # only one delivery row
        assert sum(bool(v) for v in results.values()) == 1   # exactly one winner
    finally:
        with get_db_context() as s:
            s.execute(text("DELETE FROM deliveries WHERE property_id IN "
                           "(SELECT id FROM properties WHERE parcel_id = :pc)"), {"pc": PARCEL})
            s.execute(text("DELETE FROM zip_territories WHERE subscriber_id IN "
                           "(SELECT id FROM subscribers WHERE stripe_customer_id = ANY(:c))"), {"c": CUSTS})
            s.execute(text("DELETE FROM customer_accounts WHERE stripe_customer_id = ANY(:c)"), {"c": CUSTS})
            s.execute(text("DELETE FROM subscribers WHERE stripe_customer_id = ANY(:c)"), {"c": CUSTS})
            s.execute(text("DELETE FROM properties WHERE parcel_id = :pc"), {"pc": PARCEL})
            s.commit()
