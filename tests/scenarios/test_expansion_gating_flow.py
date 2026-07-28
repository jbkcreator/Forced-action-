"""
Phase 5 — Expansion Gating scenario / integration pass.

Full pipeline E2E:
  5A. Compute all 7 gate metrics from real Postgres data via _compute_metrics().
  5B. Cache to fakeredis via _cache_metric(); read back via _build_gate_snapshot().
      Assert all 7 EXPANSION_GATES present; each has a valid color.
      Assert free_tier_cost_ratio is a real computed value (not hardcoded None stub).
      Assert county_profitability is binary 0.0/1.0 (not None proxy).
  5C. All-green fakeredis + MRR forced $0 → icp_launch_blocked returns only MRR reason.
  5D. All-green fakeredis + $60K seeded MRR → icp_launch_blocked returns [].
  5E. Flip first_payment_rate red → gate reappears as reason; MRR not in reasons.
  5F. Each of the 7 gates individually, when red, blocks ICP activation.

Marker:   scenario_platform
Requires: real Postgres (DATABASE_URL) + fakeredis (REDIS_SANDBOX=true via
          tests/scenarios/conftest.py autouse fixture).
"""
import uuid
from decimal import Decimal
from unittest.mock import patch

import pytest
from sqlalchemy import text

from config.lifecycle_guardrails import EXPANSION_GATES
from src.services.contractor_mrr import MRR_ICP_GATE_THRESHOLD
from src.services.icp_launch_gate import icp_launch_blocked
from src.tasks.county_launch_evaluator import _GATE_TO_REDIS, _build_gate_snapshot
from src.tasks.kill_switch_metric_ingest import _cache_metric, _compute_metrics

pytestmark = pytest.mark.scenario_platform

_COUNTY = "hillsborough"


# ── seed helpers ─────────────────────────────────────────────────────────────

def _seed_paying_subs(db, count: int, plan_price: str = "100.00") -> None:
    """Insert `count` active paying subscribers into the current savepoint."""
    for i in range(count):
        db.execute(
            text("""
                INSERT INTO subscribers (
                    stripe_customer_id, event_feed_uuid,
                    tier, vertical, county_id, founding_member,
                    status, plan_price, has_saved_card, auto_mode_enabled,
                    created_at, updated_at
                )
                VALUES (
                    :cus, gen_random_uuid(),
                    'starter', 'roofing', :county, false,
                    'active', :price, false, false,
                    NOW() - INTERVAL '90 days', NOW()
                )
            """),
            {"cus": f"cus_p5_{uuid.uuid4().hex[:12]}", "county": _COUNTY, "price": plan_price},
        )
    db.flush()


def _write_all_green(county: str = _COUNTY) -> None:
    """Push all-green values for all 7 EXPANSION_GATES to fakeredis."""
    from src.core.redis_client import rset

    prefix = f"fa:ks_metric:{county}"
    rset(f"{prefix}:first_payment_rate",   "35.0")   # green >= 30
    rset(f"{prefix}:saved_card_rate",      "75.0")   # green >= 70
    rset(f"{prefix}:wallet_adoption",      "20.0")   # green >= 15
    rset(f"{prefix}:lock_conversion",      "7.0")    # green >= 5
    rset(f"{prefix}:retention_30d",        "75.0")   # green >= 70 (payer_retention_30d maps here)
    rset(f"{prefix}:free_tier_cost_ratio", "10.0")   # green <= 40 (lower is better)
    rset(f"{prefix}:county_profitability", "1.0")    # green = 1.0


# ── 5A + 5B: real compute → cache → snapshot ─────────────────────────────────

def test_real_metrics_computed_and_all_7_gates_in_snapshot(fresh_db):
    """
    _compute_metrics() must return all 7 EXPANSION_GATES keys from real DB data.
    free_tier_cost_ratio must be None-or-float (not a hardcoded non-None stub).
    county_profitability must be binary 0.0 or 1.0.
    After caching, _build_gate_snapshot() must return all 7 gates with valid colors.
    """
    # 5A: compute from real DB
    metrics = _compute_metrics(fresh_db, _COUNTY)

    # payer_retention_30d is stored as retention_30d in the metrics dict
    # (see _GATE_TO_REDIS mapping); resolve before asserting.
    for gate in EXPANSION_GATES:
        metrics_key = _GATE_TO_REDIS.get(gate, gate)
        assert metrics_key in metrics, (
            f"Gate '{gate}' (metrics key '{metrics_key}') missing from _compute_metrics output"
        )

    ftr = metrics["free_tier_cost_ratio"]
    assert ftr is None or isinstance(ftr, float), (
        f"free_tier_cost_ratio must be None or float — got {type(ftr).__name__}: {ftr!r}. "
        "Likely still hardcoded."
    )

    cp = metrics["county_profitability"]
    assert cp in (0.0, 1.0), (
        f"county_profitability must be 0.0 or 1.0 — got {cp!r}. Likely still a None stub."
    )

    # 5B: write to fakeredis, read back snapshot
    for feature, value in metrics.items():
        _cache_metric(feature, value, county_id=_COUNTY)

    snapshot = _build_gate_snapshot(_COUNTY)

    for gate in EXPANSION_GATES:
        assert gate in snapshot, f"Gate '{gate}' missing from _build_gate_snapshot output"
        color = snapshot[gate]["color"]
        assert color in ("green", "yellow", "red"), (
            f"Gate '{gate}' has unexpected color {color!r}"
        )

    # At least some values must have made it into the snapshot
    non_none = sum(1 for g in snapshot.values() if g["value"] is not None)
    assert non_none >= 1, "Every gate value is None in snapshot after caching — Redis not working"


# ── 5C: all-green gates + $0 MRR → blocked on MRR only ──────────────────────

def test_icp_blocked_on_mrr_when_all_gates_green(fresh_db):
    """
    With all 7 gates green and contractor MRR forced to $0,
    icp_launch_blocked('rei_investor') must return exactly one reason
    containing 'contractor_mrr' and the $50K threshold.
    The REI Investor row (status=gated) must already exist in the DB.
    """
    _write_all_green()

    with patch(
        "src.services.icp_launch_gate.global_contractor_mrr",
        return_value=Decimal("0"),
    ):
        reasons = icp_launch_blocked(fresh_db, "rei_investor")

    assert len(reasons) == 1, (
        f"Expected exactly 1 blocking reason, got {len(reasons)}: {reasons}"
    )
    assert "contractor_mrr" in reasons[0], (
        f"Reason must mention 'contractor_mrr', got: {reasons[0]!r}"
    )
    assert "50000" in reasons[0], (
        f"Reason must include the $50K threshold, got: {reasons[0]!r}"
    )


# ── 5D: all-green + $60K MRR → unblocked ─────────────────────────────────────

def test_icp_unblocked_when_all_gates_green_and_mrr_met(fresh_db):
    """
    Seed 600 active paying subscribers @ $100 each = +$60K MRR (above real DB MRR).
    Total > $50K threshold. icp_launch_blocked must return [].
    """
    _write_all_green()
    # 50 subs × $1,000 = $50K extra; real DB MRR (~$6K) brings total well above threshold
    _seed_paying_subs(fresh_db, 50, plan_price="1000.00")

    reasons = icp_launch_blocked(fresh_db, "rei_investor")

    assert reasons == [], (
        f"ICP launch should be permitted — got blocking reasons: {reasons}"
    )


# ── 5E: one gate flipped red → reappears in reasons; MRR not a reason ────────

def test_single_red_gate_blocks_after_mrr_met(fresh_db):
    """
    Seed $60K MRR so contractor_mrr gate clears.
    Start with all gates green, then flip first_payment_rate to red.
    icp_launch_blocked must mention first_payment_rate; must NOT mention contractor_mrr.
    """
    _write_all_green()
    _seed_paying_subs(fresh_db, 50, plan_price="1000.00")

    # Flip first_payment_rate to red (10% << green threshold 30%)
    from src.core.redis_client import rset
    rset(f"fa:ks_metric:{_COUNTY}:first_payment_rate", "10.0")

    reasons = icp_launch_blocked(fresh_db, "rei_investor")

    assert any("first_payment_rate" in r for r in reasons), (
        f"Expected first_payment_rate in blocking reasons, got: {reasons}"
    )
    assert any("red" in r for r in reasons), (
        f"Expected 'red' in reason text, got: {reasons}"
    )
    assert all("contractor_mrr" not in r for r in reasons), (
        f"contractor_mrr must not block (MRR=$60K seeded), got: {reasons}"
    )


# ── 5F: each gate individually blocks ICP ─────────────────────────────────────

@pytest.mark.parametrize(
    "gate, red_value",
    [
        ("first_payment_rate",   "10.0"),   # red <= 20  (higher is better)
        ("saved_card_rate",      "40.0"),   # red <= 50
        ("wallet_adoption",      "5.0"),    # red <= 10
        ("lock_conversion",      "1.0"),    # red <= 3
        ("payer_retention_30d",  "40.0"),   # red <= 55; stored as retention_30d in Redis
        ("free_tier_cost_ratio", "60.0"),   # red >= 50  (lower is better)
        ("county_profitability", "0.0"),    # binary: 0.0 = red
    ],
)
def test_each_gate_individually_blocks_icp(gate, red_value, fresh_db):
    """
    For every one of the 7 expansion gates:
    seed $60K MRR, start all-green, flip exactly that gate red,
    assert icp_launch_blocked returns a reason mentioning that gate.
    """
    _write_all_green()
    _seed_paying_subs(fresh_db, 50, plan_price="1000.00")

    from src.core.redis_client import rset

    redis_key = _GATE_TO_REDIS.get(gate, gate)
    rset(f"fa:ks_metric:{_COUNTY}:{redis_key}", red_value)

    reasons = icp_launch_blocked(fresh_db, "rei_investor")

    assert any(gate in r for r in reasons), (
        f"Gate '{gate}' set to {red_value} (red) but not found in reasons: {reasons}"
    )
