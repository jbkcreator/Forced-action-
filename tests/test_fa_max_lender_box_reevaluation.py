"""WP-T2-10/11 — Lender Box rule changes resurface previously-declined deals.

Spec §Lender box (item 23): out-of-box routes to a human, "a discarded
exception is a lost deal"; §Abandonment: "previously declined borrowers who
have since become fundable". Decisions: GRILL-DECISIONS.md G4.
"""
from __future__ import annotations

from unittest.mock import MagicMock

from src.services.opportunity_router import router
from src.services.opportunity_router.models import GyrColor, RoutingDecision


class _Ctx:
    def __init__(self, opportunity_id: str):
        self.opportunity_id = opportunity_id


def _decision(color: GyrColor) -> RoutingDecision:
    return RoutingDecision(
        color=color, expected_revenue_cents=100_00, reason_codes=[], disqualifying_rule=None,
        queue="MONEY" if color != GyrColor.RED else "EXCEPTIONS",
    )


def test_out_of_box_to_green_is_reengagement():
    assert router.is_reengagement("red", ["out_of_box", "max_ltc"], _decision(GyrColor.GREEN))


def test_out_of_box_to_yellow_is_reengagement():
    assert router.is_reengagement("red", ["out_of_box"], _decision(GyrColor.YELLOW))


def test_red_for_other_reason_is_not_reengagement():
    assert not router.is_reengagement("red", ["borrower_suppressed"], _decision(GyrColor.GREEN))


def test_still_red_is_not_reengagement():
    assert not router.is_reengagement("red", ["out_of_box"], _decision(GyrColor.RED))


def test_reevaluate_posts_reengagement_card_for_flipped_deal(monkeypatch):
    db = MagicMock()
    db.execute.return_value.mappings.return_value.all.return_value = [
        {"opportunity_id": "flip", "gyr_color": "red", "gyr_reason": ["out_of_box"]},
        {"opportunity_id": "same", "gyr_color": "green", "gyr_reason": []},
    ]
    monkeypatch.setattr(router, "assemble_batch", lambda ids, _db: [_Ctx(i) for i in ids])
    monkeypatch.setattr(router, "classify", lambda ctx, _cfg: _decision(GyrColor.GREEN))
    monkeypatch.setattr(router, "_persist", lambda *a: None)
    monkeypatch.setattr(router, "_log_decision", lambda *a: None)
    monkeypatch.setattr(router, "post_to_slack", lambda *a, **k: None)
    cards = []
    monkeypatch.setattr(router, "post_reengagement", lambda ctx, d: cards.append(ctx.opportunity_id))

    router.reevaluate(["flip", "same"], db)

    assert cards == ["flip"]


def test_evaluate_batch_loads_programs_once(monkeypatch):
    from decimal import Decimal

    from src.services import lender_box

    calls = []

    def _fake_load(db):
        calls.append(1)
        return []

    monkeypatch.setattr(lender_box, "load_active_programs", _fake_load)
    deal = lender_box.DealInput(
        property_type="acquisition", state="FL", proposed_loan_amount=Decimal("100000"),
        purchase_price=None, rehab_estimate=None, arv=None, borrower_prior_loans=None,
    )

    lender_box.evaluate_batch([("a", deal), ("b", deal), ("c", deal)], MagicMock())

    assert len(calls) == 1
