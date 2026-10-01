"""Deterministic 1-10 score, input labels and caller questions for one lead.

Pure functions over explicit inputs: no clock, no database. Every input
carries a provenance label (known, estimated, missing), and only a known input
can earn rank. An estimated maturity inside the window never counts as a
deadline; it becomes a question for the caller instead.
"""
from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
from enum import Enum
from typing import Generic, Optional, Sequence, TypeVar

from config.lead_scoring import (
    BASE_RANK_BY_SIGNALS_MET,
    EQUITY_THRESHOLD_PCT,
    EXPLORATION_SHARE,
    LARGE_LOAN_BONUS,
    LARGE_LOAN_THRESHOLD,
    MATURITY_WINDOW_DAYS,
    MAX_RANK_BELOW_TOP,
    REPEAT_OPERATOR_BONUS,
    REPEAT_OPERATOR_MIN_PERMITS,
    REPEAT_OPERATOR_MIN_PROPERTIES,
    SUNBIZ_GOOD_STANDING,
    TOP_RANK,
)

T = TypeVar("T")


class Provenance(str, Enum):
    KNOWN = "known"
    ESTIMATED = "estimated"
    MISSING = "missing"


@dataclass(frozen=True)
class Signal(Generic[T]):
    """One scoring input and how much it can be trusted."""

    value: Optional[T]
    provenance: Provenance

    @classmethod
    def known(cls, value: T) -> "Signal[T]":
        return cls(value, Provenance.KNOWN)

    @classmethod
    def estimated(cls, value: T) -> "Signal[T]":
        return cls(value, Provenance.ESTIMATED)

    @classmethod
    def missing(cls) -> "Signal[T]":
        return cls(None, Provenance.MISSING)

    @property
    def is_known(self) -> bool:
        return self.provenance is Provenance.KNOWN and self.value is not None


def _missing():
    return Signal.missing()


@dataclass(frozen=True)
class LeadSignals:
    """Everything the score reads. Any input may be missing."""

    maturity_date: Signal[date] = field(default_factory=_missing)
    entity_status: Signal[str] = field(default_factory=_missing)
    equity_pct: Signal[Decimal] = field(default_factory=_missing)
    decision_maker_confirmed: Signal[bool] = field(default_factory=_missing)
    loan_amount: Signal[Decimal] = field(default_factory=_missing)
    entity_property_count: Signal[int] = field(default_factory=_missing)
    entity_recent_permit_count: Signal[int] = field(default_factory=_missing)


@dataclass(frozen=True)
class LeadScore:
    rank: int
    signals_met: tuple[str, ...]
    bonuses: tuple[str, ...]
    labels: dict[str, str]
    caller_questions: tuple[str, ...]


def _maturity_within_window(maturity: Optional[date], today: date) -> bool:
    return maturity is not None and today <= maturity <= today + timedelta(days=MATURITY_WINDOW_DAYS)


def _core_signals_met(signals: LeadSignals, today: date) -> tuple[str, ...]:
    """The rank-10 signals that are known and met."""
    met: list[str] = []
    if signals.maturity_date.is_known and _maturity_within_window(signals.maturity_date.value, today):
        met.append("maturity_within_window")
    if signals.entity_status.is_known and signals.entity_status.value == SUNBIZ_GOOD_STANDING:
        met.append("entity_in_good_standing")
    if signals.equity_pct.is_known and signals.equity_pct.value > EQUITY_THRESHOLD_PCT:
        met.append("equity_above_threshold")
    if signals.decision_maker_confirmed.is_known and signals.decision_maker_confirmed.value:
        met.append("decision_maker_confirmed")
    return tuple(met)


def _is_repeat_operator(signals: LeadSignals) -> bool:
    properties = signals.entity_property_count
    permits = signals.entity_recent_permit_count
    return (
        (properties.is_known and properties.value >= REPEAT_OPERATOR_MIN_PROPERTIES)
        or (permits.is_known and permits.value >= REPEAT_OPERATOR_MIN_PERMITS)
    )


def _bonuses(signals: LeadSignals) -> tuple[str, ...]:
    earned: list[str] = []
    if _is_repeat_operator(signals):
        earned.append("repeat_operator")
    if signals.loan_amount.is_known and signals.loan_amount.value >= LARGE_LOAN_THRESHOLD:
        earned.append("large_loan")
    return tuple(earned)


def _caller_questions(signals: LeadSignals, today: date) -> tuple[str, ...]:
    maturity = signals.maturity_date
    if maturity.provenance is Provenance.ESTIMATED and _maturity_within_window(maturity.value, today):
        return (f"Is the loan coming due around {maturity.value:%B %Y}?",)
    return ()


def score_lead(signals: LeadSignals, *, today: date) -> LeadScore:
    """Rank one lead from 1 to 10. Rank 10 needs all four core signals known and met."""
    met = _core_signals_met(signals, today)
    bonuses = _bonuses(signals)
    if len(met) == len(BASE_RANK_BY_SIGNALS_MET):
        rank = TOP_RANK
    else:
        bonus_points = (REPEAT_OPERATOR_BONUS if "repeat_operator" in bonuses else 0) + (
            LARGE_LOAN_BONUS if "large_loan" in bonuses else 0
        )
        rank = min(BASE_RANK_BY_SIGNALS_MET[len(met)] + bonus_points, MAX_RANK_BELOW_TOP)
    labels = {name: getattr(signals, name).provenance.value for name in LeadSignals.__dataclass_fields__}
    return LeadScore(
        rank=rank,
        signals_met=met,
        bonuses=bonuses,
        labels=labels,
        caller_questions=_caller_questions(signals, today),
    )


@dataclass(frozen=True)
class RankedLead(Generic[T]):
    item: T
    rank: int
    tiebreak: Decimal = Decimal(0)


def order_queue(
    leads: Sequence[RankedLead[T]],
    *,
    rng: random.Random,
    exploration_share: float = EXPLORATION_SHARE,
) -> list[T]:
    """Highest rank first, with a small exploration slice spread through the queue.

    The slice is drawn at random from the lower half of the ranking and placed
    at even intervals, so callers keep reaching records the rules rate lower
    and the rules can be checked against how those calls go. ``rng`` is passed
    in so a queue can be rebuilt exactly for audit.
    """
    ranked = sorted(leads, key=lambda lead: (-lead.rank, -lead.tiebreak))
    explore_count = int(round(len(ranked) * exploration_share))
    if explore_count == 0:
        return [lead.item for lead in ranked]

    lower_half_start = len(ranked) // 2
    explore_positions = set(rng.sample(range(lower_half_start, len(ranked)), explore_count))
    explored = [ranked[i] for i in sorted(explore_positions)]
    main = [lead for i, lead in enumerate(ranked) if i not in explore_positions]

    step = max(len(ranked) // explore_count, 1)
    queue: list[RankedLead[T]] = []
    explored_iter = iter(explored)
    for index, lead in enumerate(main, start=1):
        queue.append(lead)
        if index % (step - 1 or 1) == 0:
            nxt = next(explored_iter, None)
            if nxt is not None:
                queue.append(nxt)
    queue.extend(explored_iter)
    return [lead.item for lead in queue]
