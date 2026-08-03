"""
Unit tests for the NBRA population sweep (REVINT I1 populate loop).

Verifies the money-value math the sweep owns (horizon, margin, RGP) and that it
drives PR #185's primitives correctly — offer selection, RESPA/no-price skips,
and consistent p_close between the RGP numerator and the stored prior. No DB.
"""
from unittest.mock import MagicMock

import pytest

from src.core.models import RevenueType
from src.services import nbra_populate as np
from src.services.nbra_populate import (
    DEFAULT_GROSS_MARGIN,
    SUBSCRIPTION_HORIZON_MONTHS,
    _revenue_estimate,
    populate_opportunity_scores,
)
from src.services.opportunity_score import COLD_START_PRIORS


class TestRevenueEstimate:
    def test_monthly_subscription_12mo_horizon(self):
        rev, mrr, interval = _revenue_estimate(RevenueType.SUBSCRIPTION, 110000, "monthly")
        assert rev == 110000 * SUBSCRIPTION_HORIZON_MONTHS
        assert mrr == 110000
        assert interval == "monthly"

    def test_annual_subscription_face_value(self):
        rev, mrr, interval = _revenue_estimate(RevenueType.SUBSCRIPTION, 1100000, "annual")
        assert rev == 1100000
        assert mrr == round(1100000 / 12)
        assert interval == "annual"

    def test_one_time_face_no_mrr(self):
        rev, mrr, interval = _revenue_estimate(RevenueType.ONE_TIME, 9900, None)
        assert rev == 9900
        assert mrr is None
        assert interval is None


def _patch_common(monkeypatch, whales, offer="founder_tier"):
    monkeypatch.setattr(np, "get_ranked_whales", lambda *a, **k: whales)
    monkeypatch.setattr(np, "recommend_offer_for_entity",
                        lambda be: {"offer": offer, "matched_rule_id": "r", "confidence": 0.6})
    monkeypatch.setattr(np, "_resolve_plan_price", lambda db, pid: (110000, "monthly"))
    calls = []
    monkeypatch.setattr(np, "get_or_create_score",
                        lambda db, **kw: calls.append(kw) or MagicMock())
    return calls


class TestPopulate:
    def test_whale_founder_tier_rgp_matches_worked_example(self, monkeypatch):
        whales = [{"opportunity_thread_id": "OPP-2026-00001", "entity_id": 1,
                   "entity_type": "LLC", "total_purchase_count": 5}]
        calls = _patch_common(monkeypatch, whales)
        summary = populate_opportunity_scores(MagicMock())

        assert summary["scored"] == 1
        kw = calls[0]
        # p_close(0.05) × ($1,100×12) × 0.90 margin = 59,400c = $594
        assert kw["expected_revenue_cents"] == 110000 * 12
        assert kw["expected_retained_gross_profit_cents"] == 59400
        assert kw["revenue_type"] == RevenueType.SUBSCRIPTION
        assert kw["segment"] == "whale"
        assert kw["source_action_type"] == "call_outreach"  # → 15 josh-minutes in #185
        assert kw["expected_mrr_cents"] == 110000

    def test_rgp_uses_same_prior_as_stored(self, monkeypatch):
        whales = [{"opportunity_thread_id": "OPP-2026-00002", "entity_id": 2,
                   "entity_type": "LLC", "total_purchase_count": 4}]
        calls = _patch_common(monkeypatch, whales)
        populate_opportunity_scores(MagicMock())
        kw = calls[0]
        expected = round(COLD_START_PRIORS["whale"]["p_close"] * kw["expected_revenue_cents"] * DEFAULT_GROSS_MARGIN)
        assert kw["expected_retained_gross_profit_cents"] == expected

    def test_respa_gated_offer_skipped(self, monkeypatch):
        whales = [{"opportunity_thread_id": "OPP-2026-00003", "entity_id": 3,
                   "entity_type": "Individual", "total_purchase_count": 3}]
        calls = _patch_common(monkeypatch, whales, offer="hard_money_intro")
        summary = populate_opportunity_scores(MagicMock())
        assert summary["scored"] == 0
        assert summary["skipped_respa"] == 1
        assert calls == []

    def test_no_price_offer_skipped(self, monkeypatch):
        whales = [{"opportunity_thread_id": "OPP-2026-00004", "entity_id": 4,
                   "entity_type": "LLC", "total_purchase_count": 6}]
        calls = _patch_common(monkeypatch, whales)
        # founder_tier maps to a plan, but the plan resolves to no price
        monkeypatch.setattr(np, "_resolve_plan_price", lambda db, pid: (0, None))
        summary = populate_opportunity_scores(MagicMock())
        assert summary["scored"] == 0
        assert summary["skipped_no_price"] == 1

    def test_threads_without_id_excluded(self, monkeypatch):
        whales = [
            {"opportunity_thread_id": None, "entity_id": 5, "entity_type": "LLC", "total_purchase_count": 3},
            {"opportunity_thread_id": "OPP-2026-00006", "entity_id": 6, "entity_type": "LLC", "total_purchase_count": 3},
        ]
        calls = _patch_common(monkeypatch, whales)
        summary = populate_opportunity_scores(MagicMock())
        assert summary["whales"] == 1   # None-thread whale filtered before scoring
        assert summary["scored"] == 1
        assert calls[0]["opportunity_thread_id"] == "OPP-2026-00006"
