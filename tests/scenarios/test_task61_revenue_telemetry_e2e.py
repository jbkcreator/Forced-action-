"""Task 6.1 — Revenue OS Analytics Spine, DB-backed correctness tests (real Postgres via fresh_db).

Two functions, tested separately because they rest on different evidence:

  * compute_confirmed_delivery_margin — real per-property delivery events
    (SentLead for lead_unlock/lead_pack, PremiumPurchase for report/brief).
  * compute_zip_territory_margin / list_zip_territory_leads — a current-state
    snapshot inferred from ZIP-territory locks, with the multi-vertical
    collision resolved by highest vertical_score and no double-counting
    against properties already owned by the confirmed-delivery leg.
"""

from datetime import datetime, timezone

import pytest
from sqlalchemy import text as sa_text

from src.core.models import (
    DistressScore, EnrichmentUsageLog, LeadPackPurchase,
    Property, SentLead, Subscriber, SubscriptionInvoice, ZipTerritory,
)
from src.services.revenue_telemetry import (
    compute_confirmed_delivery_margin, compute_zip_territory_margin,
    list_zip_territory_leads,
)

pytestmark = pytest.mark.scenario_platform

FRM = datetime(2099, 6, 1, tzinfo=timezone.utc)
TO = datetime(2099, 7, 1, tzinfo=timezone.utc)
_IN = datetime(2099, 6, 12, tzinfo=timezone.utc)


def _subscriber(db, cust):
    s = Subscriber(stripe_customer_id=cust, tier="pro", vertical="roofing", county_id="hillsborough")
    db.add(s); db.flush()
    return s


def _prop(db, parcel, *, zip_code="33565", county_id="hillsborough"):
    p = Property(parcel_id=parcel, zip=zip_code, county_id=county_id)
    db.add(p); db.flush()
    return p


def _score(db, prop, *, vertical_scores, qualified=True):
    ds = DistressScore(property_id=prop.id, vertical_scores=vertical_scores,
                       county_id=prop.county_id, qualified=qualified,
                       score_date=datetime(2099, 6, 1, tzinfo=timezone.utc))
    db.add(ds); db.flush()
    return ds


def _premium_purchase(db, subscriber, *, sku, amount_cents, property_id, delivered_at,
                       stripe_payment_intent_id, refunded_at=None):
    # Raw SQL: PremiumPurchase.output_ref_expires_at (set by premium_engine's
    # real fulfillment code) has no migration and doesn't exist in this DB —
    # a pre-existing, unrelated bug. ORM .add() would insert every mapped
    # column and hit it; sidestep it here by inserting only real columns.
    db.execute(sa_text("""
        INSERT INTO premium_purchases (
            subscriber_id, sku, paid_via, amount_cents, property_id,
            status, purchased_at, delivered_at, stripe_payment_intent_id, refunded_at
        ) VALUES (
            :sid, :sku, 'card', :amount_cents, :pid,
            'delivered', :delivered_at, :delivered_at, :pi, :refunded_at
        )
    """), {
        "sid": subscriber.id, "sku": sku, "amount_cents": amount_cents, "pid": property_id,
        "delivered_at": delivered_at, "pi": stripe_payment_intent_id, "refunded_at": refunded_at,
    })
    db.flush()


def _enrich(db, prop, *, cost_cents, success=True):
    e = EnrichmentUsageLog(vendor="tracerfy", purpose="batch_skip_trace", property_id=prop.id,
                           cost_cents=cost_cents, success=success)
    db.add(e); db.flush()
    return e


def _territory(db, subscriber, *, zip_code, vertical, county_id="hillsborough", status="locked"):
    zt = ZipTerritory(zip_code=zip_code, vertical=vertical, county_id=county_id,
                      subscriber_id=subscriber.id, status=status,
                      locked_at=datetime(2099, 5, 1, tzinfo=timezone.utc))
    db.add(zt); db.flush()
    return zt


def _invoice(db, subscriber, *, amount_cents, paid_at=_IN, reversed_at=None, stripe_invoice_id):
    inv = SubscriptionInvoice(subscriber_id=subscriber.id, stripe_invoice_id=stripe_invoice_id,
                              amount_collected_cents=amount_cents, period_month=paid_at.date(),
                              paid_at=paid_at, reversed_at=reversed_at)
    db.add(inv); db.flush()
    return inv


def _row_for(rows, subscriber_id):
    return next((r for r in rows if r["subscriber_id"] == subscriber_id), None)


# ── compute_confirmed_delivery_margin ──────────────────────────────────────

def test_lead_unlock_margin(fresh_db):
    db = fresh_db
    sub = _subscriber(db, "cus_t61b_a")
    prop = _prop(db, "T61B-unlock")
    _enrich(db, prop, cost_cents=4)
    db.add(SentLead(subscriber_id=sub.id, property_id=prop.id, source="lead_unlock_payment",
                    sent_at=_IN, amount_cents=400))
    db.flush()

    rows = compute_confirmed_delivery_margin(db, FRM, TO)
    r = _row_for(rows, sub.id)
    assert r["delivered_leads_count"] == 1
    assert r["revenue_cents"] == 400
    assert r["attributed_cost_cents"] == 4
    assert r["net_margin_cents"] == 396


def test_lead_pack_revenue_split_across_five_leads(fresh_db):
    db = fresh_db
    sub = _subscriber(db, "cus_t61b_b")
    props = [_prop(db, f"T61B-pack{i}") for i in range(5)]
    for p in props:
        _enrich(db, p, cost_cents=7)
    purchase = LeadPackPurchase(
        subscriber_id=sub.id, zip_code="33565", vertical="roofing", county_id="hillsborough",
        stripe_payment_intent_id="pi_t61b_pack", status="delivered",
        purchased_at=_IN, delivered_at=_IN, amount_cents=9900,
        lead_ids=[p.id for p in props],
    )
    db.add(purchase); db.flush()
    for p in props:
        db.add(SentLead(subscriber_id=sub.id, property_id=p.id, source="lead_pack",
                        sent_at=_IN, stripe_payment_intent_id="pi_t61b_pack"))
    db.flush()

    rows = compute_confirmed_delivery_margin(db, FRM, TO)
    r = _row_for(rows, sub.id)
    assert r["delivered_leads_count"] == 5
    assert r["revenue_cents"] == 9900          # split 1980*5, summed back to the full purchase
    assert r["attributed_cost_cents"] == 35    # 7 * 5


def test_premium_report_margin(fresh_db):
    db = fresh_db
    sub = _subscriber(db, "cus_t61b_c")
    prop = _prop(db, "T61B-report")
    _enrich(db, prop, cost_cents=2)
    _premium_purchase(db, sub, sku="report", amount_cents=700, property_id=prop.id,
                      delivered_at=_IN, stripe_payment_intent_id="pi_t61b_report")

    rows = compute_confirmed_delivery_margin(db, FRM, TO)
    r = _row_for(rows, sub.id)
    assert r["revenue_cents"] == 700
    assert r["attributed_cost_cents"] == 2


def test_refunded_premium_purchase_excluded(fresh_db):
    db = fresh_db
    sub = _subscriber(db, "cus_t61b_d")
    prop = _prop(db, "T61B-refunded")
    _premium_purchase(db, sub, sku="brief", amount_cents=1200, property_id=prop.id,
                      delivered_at=_IN, refunded_at=_IN,
                      stripe_payment_intent_id="pi_t61b_refunded")

    rows = compute_confirmed_delivery_margin(db, FRM, TO)
    assert _row_for(rows, sub.id) is None


def test_confirmed_delivery_no_enrichment_row_is_zero_cost_no_exception(fresh_db):
    db = fresh_db
    sub = _subscriber(db, "cus_t61b_e")
    prop = _prop(db, "T61B-freecost")
    db.add(SentLead(subscriber_id=sub.id, property_id=prop.id, source="lead_unlock_payment",
                    sent_at=_IN, amount_cents=250))
    db.flush()

    rows = compute_confirmed_delivery_margin(db, FRM, TO)
    r = _row_for(rows, sub.id)
    assert r["attributed_cost_cents"] == 0
    assert r["net_margin_cents"] == 250
    assert r["margin_pct"] == 1.0


def test_confirmed_delivery_sort_order_desc(fresh_db):
    db = fresh_db
    sub_hi = _subscriber(db, "cus_t61b_f_hi")
    db.add(SentLead(subscriber_id=sub_hi.id, property_id=_prop(db, "T61B-hi").id,
                    source="lead_unlock_payment", sent_at=_IN, amount_cents=700))
    sub_lo = _subscriber(db, "cus_t61b_f_lo")
    db.add(SentLead(subscriber_id=sub_lo.id, property_id=_prop(db, "T61B-lo").id,
                    source="lead_unlock_payment", sent_at=_IN, amount_cents=250))
    db.flush()

    rows = compute_confirmed_delivery_margin(db, FRM, TO)
    ids = [r["subscriber_id"] for r in rows if r["subscriber_id"] in (sub_hi.id, sub_lo.id)]
    assert ids.index(sub_hi.id) < ids.index(sub_lo.id)


# ── compute_zip_territory_margin / list_zip_territory_leads ────────────────

def test_territory_holder_with_no_qualifying_property_still_reported(fresh_db):
    db = fresh_db
    sub = _subscriber(db, "cus_t61c_a")
    _territory(db, sub, zip_code="90001", vertical="roofing")
    _invoice(db, sub, amount_cents=80000, stripe_invoice_id="in_t61c_a")

    rows = compute_zip_territory_margin(db)
    r = _row_for(rows, sub.id)
    assert r is not None
    assert r["territory_leads_count"] == 0
    assert r["revenue_cents"] == 80000
    assert r["territory_attributed_cost_cents"] == 0
    assert r["net_margin_cents"] == 80000
    assert r["basis"] == "current_territory_snapshot"


def test_single_vertical_territory_margin(fresh_db):
    db = fresh_db
    sub = _subscriber(db, "cus_t61c_b")
    _territory(db, sub, zip_code="90002", vertical="roofing")
    _invoice(db, sub, amount_cents=80000, stripe_invoice_id="in_t61c_b")
    prop = _prop(db, "T61C-single", zip_code="90002")
    _score(db, prop, vertical_scores={"roofing": 90})
    _enrich(db, prop, cost_cents=7)

    rows = compute_zip_territory_margin(db)
    r = _row_for(rows, sub.id)
    assert r["territory_leads_count"] == 1
    assert r["territory_attributed_cost_cents"] == 7
    assert r["net_margin_cents"] == 79993


def test_multi_vertical_collision_highest_score_wins(fresh_db):
    db = fresh_db
    sub_roof = _subscriber(db, "cus_t61c_c_roof")
    sub_resto = _subscriber(db, "cus_t61c_c_resto")
    _territory(db, sub_roof, zip_code="90003", vertical="roofing")
    _territory(db, sub_resto, zip_code="90003", vertical="restoration")
    prop = _prop(db, "T61C-collision", zip_code="90003")
    _score(db, prop, vertical_scores={"roofing": 60, "restoration": 85})
    _enrich(db, prop, cost_cents=7)

    rows = compute_zip_territory_margin(db)
    r_roof = _row_for(rows, sub_roof.id)
    r_resto = _row_for(rows, sub_resto.id)
    assert r_resto["territory_attributed_cost_cents"] == 7
    assert r_roof["territory_attributed_cost_cents"] == 0

    leads_roof = list_zip_territory_leads(db, sub_roof.id)
    leads_resto = list_zip_territory_leads(db, sub_resto.id)
    assert leads_resto[0]["is_owner"] is True
    assert leads_resto[0]["cost_attribution_note"] is None
    assert leads_roof[0]["is_owner"] is False
    assert leads_roof[0]["attributed_cost_cents"] == 0
    assert "restoration" in leads_roof[0]["cost_attribution_note"]


def test_property_already_confirmed_elsewhere_excluded_from_territory(fresh_db):
    db = fresh_db
    sub = _subscriber(db, "cus_t61c_d")
    _territory(db, sub, zip_code="90004", vertical="roofing")
    prop = _prop(db, "T61C-doublecount", zip_code="90004")
    _score(db, prop, vertical_scores={"roofing": 90})
    _enrich(db, prop, cost_cents=7)
    # Already has a confirmed delivery event elsewhere (different subscriber/product)
    other_sub = _subscriber(db, "cus_t61c_d_other")
    db.add(SentLead(subscriber_id=other_sub.id, property_id=prop.id,
                    source="lead_unlock_payment", sent_at=_IN, amount_cents=400))
    db.flush()

    rows = compute_zip_territory_margin(db)
    r = _row_for(rows, sub.id)
    assert r["territory_leads_count"] == 0
    assert r["territory_attributed_cost_cents"] == 0

    leads = list_zip_territory_leads(db, sub.id)
    assert leads[0]["is_owner"] is False
    assert "confirmed delivery event" in leads[0]["cost_attribution_note"]


def test_territory_reversed_invoice_excluded_from_revenue(fresh_db):
    db = fresh_db
    sub = _subscriber(db, "cus_t61c_e")
    _territory(db, sub, zip_code="90005", vertical="roofing")
    _invoice(db, sub, amount_cents=80000, stripe_invoice_id="in_t61c_e_good")
    _invoice(db, sub, amount_cents=80000, reversed_at=_IN, stripe_invoice_id="in_t61c_e_bad")

    rows = compute_zip_territory_margin(db)
    r = _row_for(rows, sub.id)
    assert r["revenue_cents"] == 80000


def test_property_not_scoring_for_vertical_not_owned(fresh_db):
    db = fresh_db
    sub = _subscriber(db, "cus_t61c_f")
    _territory(db, sub, zip_code="90006", vertical="roofing")
    prop = _prop(db, "T61C-novertical", zip_code="90006")
    _score(db, prop, vertical_scores={"restoration": 90})  # no roofing score at all
    _enrich(db, prop, cost_cents=7)

    rows = compute_zip_territory_margin(db)
    r = _row_for(rows, sub.id)
    assert r["territory_leads_count"] == 0
