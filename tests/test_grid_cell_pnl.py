"""
CLONE-v2.2 CL2 — grid-cell P&L framework (src/services/grid_cell_pnl.py).
"""

from __future__ import annotations

import uuid
from datetime import date

from sqlalchemy import text

from src.services.grid_cell_pnl import (
    compute_contribution_margin_cents,
    estimate_payback_days,
    rollup_cell_from_ledger,
    upsert_cell_pnl,
)

CELL = dict(
    county_id="hillsborough",
    distress_type="foreclosures",
    buyer_vertical="wholesalers",
    offer_step="paid_unlock",
)


def test_compute_contribution_margin_cents():
    assert compute_contribution_margin_cents(10000, 4000) == 6000
    assert compute_contribution_margin_cents(1000, 1500) == -500


def test_estimate_payback_days_recoups_within_series():
    assert estimate_payback_days([1000, 1000, 1000], cac_cents=2500) == 3


def test_estimate_payback_days_never_recoups_returns_none():
    assert estimate_payback_days([100, 100, 100], cac_cents=10000) is None


def test_estimate_payback_days_zero_cac_is_instant():
    assert estimate_payback_days([], cac_cents=0) == 0


def _cleanup(db) -> None:
    db.execute(text("DELETE FROM grid_cell_pnl WHERE county_id = :c"), {"c": CELL["county_id"]})
    db.commit()


def test_upsert_cell_pnl_inserts_then_overwrites_same_period(fresh_db):
    period_start, period_end = date(2026, 1, 1), date(2026, 1, 31)
    try:
        first_id = upsert_cell_pnl(
            fresh_db, **CELL, period_start=period_start, period_end=period_end,
            revenue_cents=10000, cost_cents=4000, deal_count=2,
        )
        second_id = upsert_cell_pnl(
            fresh_db, **CELL, period_start=period_start, period_end=period_end,
            revenue_cents=15000, cost_cents=5000, deal_count=3,
        )
        assert first_id == second_id

        row = fresh_db.execute(text("""
            SELECT revenue_cents, cost_cents, contribution_margin_cents, deal_count
            FROM grid_cell_pnl WHERE id = :id
        """), {"id": first_id}).one()
        assert row.revenue_cents == 15000
        assert row.cost_cents == 5000
        assert row.contribution_margin_cents == 10000
        assert row.deal_count == 3
    finally:
        _cleanup(fresh_db)


def _cleanup_rollup_fixtures(db, *, property_ids, subscriber_id, deal_id) -> None:
    """fresh_db commits for real (see _cleanup above) — every table this
    test seeds must be torn down explicitly, or rows accumulate across runs
    and silently inflate later runs' rollup sums."""
    db.execute(text("DELETE FROM platform_cost_attribution WHERE subscriber_id = :sub"), {"sub": subscriber_id})
    db.execute(text("DELETE FROM platform_revenue_ledger WHERE subscriber_id = :sub"), {"sub": subscriber_id})
    db.execute(text("DELETE FROM enrichment_usage_logs WHERE subscriber_id = :sub"), {"sub": subscriber_id})
    db.execute(text("DELETE FROM golden_close_chains WHERE deal_id = :deal_id"), {"deal_id": deal_id})
    db.execute(text("DELETE FROM deal_outcomes WHERE id = :deal_id"), {"deal_id": deal_id})
    db.execute(text("DELETE FROM properties WHERE id = ANY(:pids)"), {"pids": property_ids})
    db.execute(text("DELETE FROM subscribers WHERE id = :sub"), {"sub": subscriber_id})
    _cleanup(db)


def test_rollup_cell_from_ledger_sums_tagged_properties_only(fresh_db):
    parcel_a, parcel_b = f"parcel_{uuid.uuid4().hex[:8]}", f"parcel_other_{uuid.uuid4().hex[:8]}"
    sub_uuid = str(uuid.uuid4())
    period_start, period_end = date(2026, 3, 1), date(2026, 3, 31)
    property_id = other_property_id = subscriber_id = deal_id = None
    try:
        property_id = fresh_db.execute(
            text("INSERT INTO properties (parcel_id, created_at, updated_at) VALUES (:p, NOW(), NOW()) RETURNING id"), {"p": parcel_a},
        ).scalar_one()
        other_property_id = fresh_db.execute(
            text("INSERT INTO properties (parcel_id, created_at, updated_at) VALUES (:p, NOW(), NOW()) RETURNING id"), {"p": parcel_b},
        ).scalar_one()
        subscriber_id = fresh_db.execute(text("""
            INSERT INTO subscribers (
                stripe_customer_id, county_id, tier, vertical, status,
                founding_member, has_saved_card, auto_mode_enabled, created_at, updated_at
            )
            VALUES (
                :stripe_customer_id, 'hillsborough', 'pro', 'wholesalers', 'active',
                FALSE, FALSE, FALSE, NOW(), NOW()
            )
            RETURNING id
        """), {"stripe_customer_id": f"cus_test_{sub_uuid}"}).scalar_one()
        deal_id = fresh_db.execute(text("""
            INSERT INTO deal_outcomes (deal_size_bucket, pipeline_stage, created_at)
            VALUES ('10_25k', 'closed_won', NOW()) RETURNING id
        """)).scalar_one()

        # Tag only `property_id` (not other_property_id) with this exact cell.
        fresh_db.execute(text("""
            INSERT INTO golden_close_chains
                (deal_id, property_id, county_id, distress_type, buyer_vertical, offer_step, authored_by)
            VALUES (:deal_id, :pid, :county_id, :distress_type, :buyer_vertical, :offer_step, 'cl2_test')
        """), {"deal_id": deal_id, "pid": property_id, **CELL})

        # Revenue/cost on the tagged property should be counted...
        fresh_db.execute(text("""
            INSERT INTO platform_revenue_ledger
                (subscriber_id, product_type, amount_cents, property_id, source_table, source_id, occurred_at)
            VALUES (:sub, 'lead_unlock', 5000, :pid, 'test_source', :sid, :occurred_at)
        """), {"sub": subscriber_id, "pid": property_id, "sid": deal_id, "occurred_at": date(2026, 3, 15)})

        enrichment_log_id = fresh_db.execute(text("""
            INSERT INTO enrichment_usage_logs (vendor, purpose, subscriber_id, property_id, cost_cents, success, created_at)
            VALUES ('batchdata', 'lead_unlock', :sub, :pid, 1200, TRUE, :created_at)
            RETURNING id
        """), {"sub": subscriber_id, "pid": property_id, "created_at": date(2026, 3, 10)}).scalar_one()
        fresh_db.execute(text("""
            INSERT INTO platform_cost_attribution
                (enrichment_usage_log_id, subscriber_id, property_id, attribution_method, attributed_cost_cents, created_at)
            VALUES (:log_id, :sub, :pid, 'direct_purchase', 1200, :created_at)
        """), {"log_id": enrichment_log_id, "sub": subscriber_id, "pid": property_id, "created_at": date(2026, 3, 10)})

        # ...but revenue on the untagged property must not leak into this cell.
        fresh_db.execute(text("""
            INSERT INTO platform_revenue_ledger
                (subscriber_id, product_type, amount_cents, property_id, source_table, source_id, occurred_at)
            VALUES (:sub, 'lead_unlock', 999999, :pid, 'test_source', :sid, :occurred_at)
        """), {"sub": subscriber_id, "pid": other_property_id, "sid": deal_id + 1, "occurred_at": date(2026, 3, 20)})

        cell_id = rollup_cell_from_ledger(
            fresh_db, **CELL, period_start=period_start, period_end=period_end,
        )

        row = fresh_db.execute(text("""
            SELECT revenue_cents, cost_cents, contribution_margin_cents, deal_count
            FROM grid_cell_pnl WHERE id = :id
        """), {"id": cell_id}).one()
        assert row.revenue_cents == 5000
        assert row.cost_cents == 1200
        assert row.contribution_margin_cents == 3800
        assert row.deal_count == 1
    finally:
        if deal_id is not None:
            _cleanup_rollup_fixtures(
                fresh_db,
                property_ids=[pid for pid in (property_id, other_property_id) if pid is not None],
                subscriber_id=subscriber_id,
                deal_id=deal_id,
            )
        else:
            _cleanup(fresh_db)
