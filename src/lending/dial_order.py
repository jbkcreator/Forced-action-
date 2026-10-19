"""Dial order for the launch queues (Runbook Principle 1 + §4.3).

Queues are dialed in ``DIAL_ORDER``; inside a queue the highest score goes first.
The score source is pluggable: T-16's 0-10 ``lead_score`` when present, else the
existing maturity ``rank``. Unscored records follow the scored ones in their own
queue and never drop below a lower queue. No exploration slice at launch.
"""
from __future__ import annotations

import random
from typing import Any, Callable, Mapping, Optional, Sequence

from config.lending_queues import DIAL_ORDER
from src.lending.lead_scoring import RankedLead, order_queue

ScoreSource = Callable[[Mapping[str, Any]], Optional[int]]


def record_score(record: Mapping[str, Any]) -> Optional[int]:
    for key in ("lead_score", "rank"):
        value = record.get(key)
        if value is not None:
            return int(value)
    return None


def _queue_of(record: Mapping[str, Any]) -> Optional[str]:
    return record.get("queue") or record.get("pool")


def order_for_dialing(
    records: Sequence[Mapping[str, Any]], *, score: ScoreSource = record_score,
) -> list[Mapping[str, Any]]:
    """Records in launch queues, by ``DIAL_ORDER`` then highest score first (stable)."""
    by_queue: dict[str, list[Mapping[str, Any]]] = {q: [] for q in DIAL_ORDER}
    for record in records:
        queue = _queue_of(record)
        if queue in by_queue:
            by_queue[queue].append(record)
    ordered: list[Mapping[str, Any]] = []
    for queue in DIAL_ORDER:
        scored, unscored = [], []
        for record in by_queue[queue]:
            value = score(record)
            (unscored if value is None else scored).append((record, value))
        ordered += order_queue([RankedLead(item=r, rank=v) for r, v in scored],
                               rng=random.Random(0), exploration_share=0.0)
        ordered += [r for r, _ in unscored]
    return ordered


def queue_to_dial_now(remaining: Mapping[str, int]) -> Optional[str]:
    """First queue in ``DIAL_ORDER`` that still has records to dial, else None."""
    return next((q for q in DIAL_ORDER if remaining.get(q, 0) > 0), None)


def sort_for_load(records: Sequence[Mapping[str, Any]], *, score: ScoreSource = record_score) -> list[Mapping[str, Any]]:
    """Every record, launch queues in dial order first; the rest keep their order at the
    end so the dialer load still sees (and refuses) any unmapped pool."""
    others = [r for r in records if _queue_of(r) not in DIAL_ORDER]
    return order_for_dialing(records, score=score) + others
