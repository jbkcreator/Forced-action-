"""
Producer ranking (SPEC Stage D).

rank_partners ranks partner rows within each class by:
  1. observed_transaction_count desc
  2. last_observed_at desc  (tie-break 1)
  3. total_cash_volume desc (tie-break 2)

Top 25 per class → status='active'; rest → status='identified'.
All rows are returned (non-destructive).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Optional

from collections import defaultdict


@dataclass
class PartnerRow:
    buyer_entity_id: int
    canonical_name: str
    partner_class: str
    observed_transaction_count: int
    last_observed_at: date
    first_observed_at: Optional[date] = None
    total_cash_volume: float = 0.0
    # Populated by rank_partners:
    rank: int = 0
    status: str = "identified"
    # Populated by enrich_top_partners (Stage E):
    needs_enrichment: bool = False


_TOP_N = 25


def rank_partners(rows: list[PartnerRow]) -> list[PartnerRow]:
    """
    Assign rank and status to each row, per partner_class.
    Returns all rows (non-destructive — dropouts stay, rank is updated).
    """
    by_class: dict[str, list[PartnerRow]] = defaultdict(list)
    for row in rows:
        by_class[row.partner_class].append(row)

    result: list[PartnerRow] = []
    for class_rows in by_class.values():
        class_rows.sort(
            key=lambda r: (
                -r.observed_transaction_count,
                -(r.last_observed_at.toordinal()),
                -r.total_cash_volume,
            )
        )
        for i, row in enumerate(class_rows):
            row.rank = i + 1
            row.status = "active" if i < _TOP_N else "identified"
        result.extend(class_rows)

    return result
