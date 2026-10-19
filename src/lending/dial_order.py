"""Dial order for the launch queues (Runbook Principle 1 + §4.3).

Queues are dialed in ``DIAL_ORDER``; inside a queue the highest score goes first.
Scores on different scales are never compared: each queue sorts on one field, the
first of ``SCORE_FIELDS`` any of its records carries (T-16's 0-10 ``lead_score``,
else the existing 1-10 maturity ``rank``). Records without that field follow the
scored ones in their own queue and never drop below a lower queue. Ties and
unscored records keep their input order. No exploration slice at launch.
"""
from __future__ import annotations

import random
from typing import Any, Mapping, Optional, Sequence

from config.lending_queues import DIAL_ORDER
from src.lending.lead_scoring import RankedLead, order_queue

# Highest priority first; T-16 plugs in as ``lead_score`` with no code change here.
SCORE_FIELDS: tuple[str, ...] = ("lead_score", "rank")

Record = Mapping[str, Any]


def _queue_of(record: Record) -> Optional[str]:
    return record.get("queue") or record.get("pool")


def score_field(records: Sequence[Record]) -> Optional[str]:
    """The one score field a queue sorts on: the first of SCORE_FIELDS any record carries."""
    return next((f for f in SCORE_FIELDS if any(r.get(f) is not None for r in records)), None)


def _order_one_queue(records: Sequence[Record]) -> list[Record]:
    field = score_field(records)
    if field is None:
        return list(records)
    scored = [r for r in records if r.get(field) is not None]
    unscored = [r for r in records if r.get(field) is None]
    ranked = order_queue([RankedLead(item=r, rank=int(r[field])) for r in scored],
                         rng=random.Random(0), exploration_share=0.0)
    return ranked + unscored


def order_for_dialing(records: Sequence[Record]) -> list[Record]:
    """Records in launch queues, by ``DIAL_ORDER`` then highest score first."""
    by_queue: dict[str, list[Record]] = {q: [] for q in DIAL_ORDER}
    for record in records:
        queue = _queue_of(record)
        if queue in by_queue:
            by_queue[queue].append(record)
    return [r for queue in DIAL_ORDER for r in _order_one_queue(by_queue[queue])]


def sort_for_load(records: Sequence[Record]) -> list[Record]:
    """Every record, launch queues in dial order first; the rest keep their order at the
    end so the dialer load still sees (and refuses) any unmapped pool."""
    others = [r for r in records if _queue_of(r) not in DIAL_ORDER]
    return order_for_dialing(records) + others


def queue_to_dial_now(remaining: Mapping[str, int]) -> Optional[str]:
    """First queue in ``DIAL_ORDER`` that still has records to dial, else None."""
    return next((q for q in DIAL_ORDER if remaining.get(q, 0) > 0), None)
