"""WP-T2-11 router — MONEY is delivered highest expected revenue first.

Spec §Slack queues: MONEY "highest expected revenue first"; §Router (item 25):
"Ranking within color by expected dollars, always." Decisions: GRILL-DECISIONS.md G3.
"""
from __future__ import annotations

from src.services.opportunity_router.models import GyrColor, RoutingDecision
from src.services.opportunity_router.router import rank_for_delivery


class _Ctx:
    def __init__(self, opportunity_id: str):
        self.opportunity_id = opportunity_id


def _pair(opp: str, color: GyrColor, cents: int, queue: str):
    return _Ctx(opp), RoutingDecision(
        color=color, expected_revenue_cents=cents, reason_codes=[], disqualifying_rule=None, queue=queue,
    )


def test_green_before_yellow_then_dollars_descending():
    pairs = [
        _pair("y-big", GyrColor.YELLOW, 900_00, "MONEY"),
        _pair("g-small", GyrColor.GREEN, 100_00, "MONEY"),
        _pair("g-big", GyrColor.GREEN, 500_00, "MONEY"),
    ]

    ranked = rank_for_delivery(pairs)

    assert [ctx.opportunity_id for ctx, _ in ranked["MONEY"]] == ["g-big", "g-small", "y-big"]


def test_exceptions_ranked_by_dollars_and_terminal_dropped():
    pairs = [
        _pair("r-small", GyrColor.RED, 10_00, "EXCEPTIONS"),
        _pair("r-big", GyrColor.RED, 70_00, "EXCEPTIONS"),
        _pair("dead", GyrColor.RED, 99_00, None),
    ]

    ranked = rank_for_delivery(pairs)

    assert [ctx.opportunity_id for ctx, _ in ranked["EXCEPTIONS"]] == ["r-big", "r-small"]
    assert "dead" not in {ctx.opportunity_id for q in ranked.values() for ctx, _ in q}


def test_zero_revenue_sorts_last_within_color():
    pairs = [
        _pair("g-zero", GyrColor.GREEN, 0, "MONEY"),
        _pair("g-some", GyrColor.GREEN, 1, "MONEY"),
    ]

    ranked = rank_for_delivery(pairs)

    assert [ctx.opportunity_id for ctx, _ in ranked["MONEY"]] == ["g-some", "g-zero"]


def test_sweep_posts_header_then_cards_in_rank_order(monkeypatch):
    from unittest.mock import MagicMock

    from src.services.opportunity_router import router

    decisions = {
        "a": (GyrColor.GREEN, 100_00), "b": (GyrColor.GREEN, 900_00), "c": (GyrColor.YELLOW, 50_00),
    }
    db = MagicMock()
    db.execute.return_value.scalars.return_value.all.return_value = list(decisions)
    monkeypatch.setattr(router, "assemble_batch", lambda ids, _db: [_Ctx(i) for i in ids])
    monkeypatch.setattr(router, "classify", lambda ctx, _cfg: RoutingDecision(
        color=decisions[ctx.opportunity_id][0], expected_revenue_cents=decisions[ctx.opportunity_id][1],
        reason_codes=[], disqualifying_rule=None, queue="MONEY",
    ))
    monkeypatch.setattr(router, "_persist", lambda *a: None)
    monkeypatch.setattr(router, "_log_decision", lambda *a: None)
    posts = []
    monkeypatch.setattr(router, "post_queue_header", lambda q, **kw: posts.append(("header", q, kw["count"])))
    monkeypatch.setattr(router, "post_to_slack", lambda ctx, d, rank, total: posts.append((ctx.opportunity_id, rank, total)))

    router.run_sweep(db)

    assert posts == [("header", "MONEY", 3), ("b", 1, 3), ("a", 2, 3), ("c", 3, 3)]
