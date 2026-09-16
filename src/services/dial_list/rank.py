"""WP-9 Dial List — pure expected-revenue ranking core.

Entrypoint: rank_dial_list(candidates, as_of, config) -> DialList

No I/O, no DB, no network, no LLM. Deterministic and fully unit-testable.
Score (Q9): expected_revenue = probability × expected_loan × commission × urgency,
then × builder_multiplier when the opportunity is a builder (Q10).
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

from .config import DEFAULT_CONFIG, DialListConfig
from .models import DialCandidate, DialList, DialListEntry, LoanConfidence

_ZERO = Decimal("0")
_ONE = Decimal("1")


class _Score:
    __slots__ = ("expected_revenue", "probability", "expected_loan",
                 "commission", "urgency", "loan_confidence")

    def __init__(self, expected_revenue, probability, expected_loan,
                 commission, urgency, loan_confidence):
        self.expected_revenue = expected_revenue
        self.probability = probability
        self.expected_loan = expected_loan
        self.commission = commission
        self.urgency = urgency
        self.loan_confidence = loan_confidence


def _probability(c: DialCandidate, config: DialListConfig) -> Decimal:
    if c.intent_tier is not None:
        return config.intent_probability[c.intent_tier]
    return config.detector_only_probability_floor


def _expected_loan(c: DialCandidate, config: DialListConfig) -> Tuple[Decimal, LoanConfidence]:
    if c.arv is not None and c.arv > _ZERO and c.max_ltc is not None and c.max_ltc > _ZERO:
        return c.max_ltc * c.arv, "high"
    if c.assessed_value_mkt is not None and c.assessed_value_mkt > _ZERO:
        return config.expected_loan_fallback_fraction * c.assessed_value_mkt, "low"
    if c.last_sale_price is not None and c.last_sale_price > _ZERO:
        return config.expected_loan_fallback_fraction * c.last_sale_price, "low"
    return _ZERO, "low"


def _urgency(c: DialCandidate, as_of: date, config: DialListConfig) -> Decimal:
    base = max(
        (config.urgency_weights.get(t, config.urgency_default) for t in c.triggers),
        default=config.urgency_default,
    )
    if c.urgency_date is None:
        return base
    days = (c.urgency_date - as_of).days
    # Recent-or-imminent within the window earns a proximity boost; the closer to
    # as_of (|days| small), the larger, up to (1 + urgency_recent_boost).
    if abs(days) <= config.urgency_recent_days:
        proximity = _ONE - (Decimal(abs(days)) / Decimal(config.urgency_recent_days))
        return base * (_ONE + config.urgency_recent_boost * proximity)
    return base


def _score(c: DialCandidate, as_of: date, config: DialListConfig) -> _Score:
    probability = _probability(c, config)
    expected_loan, loan_conf = _expected_loan(c, config)
    commission = config.commission_rate * expected_loan
    urgency = _urgency(c, as_of, config)
    expected_revenue = probability * expected_loan * commission * urgency
    if c.is_builder:
        expected_revenue = expected_revenue * config.builder_multiplier
    return _Score(expected_revenue, probability, expected_loan,
                  commission, urgency, loan_conf)


def _dedup_key(c: DialCandidate) -> Tuple[str, object]:
    """Resolved borrowers collapse by buyer_entity_id; unresolved by property."""
    if c.buyer_entity_id is not None:
        return ("borrower", c.buyer_entity_id)
    return ("property", c.property_id)


def _merge_triggers(existing: List[str], incoming: List[str]) -> List[str]:
    seen = set(existing)
    merged = list(existing)
    for t in incoming:
        if t not in seen:
            seen.add(t)
            merged.append(t)
    return merged


def _reason(c: DialCandidate, triggers: List[str]) -> Tuple[str, List[str]]:
    tset = set(triggers)
    parts: List[str] = []
    if "builder" in tset:
        parts.append("Builder / new-construction signal — larger loan likely.")
    if "cash_purchase" in tset:
        parts.append("Cash purchase, no financing — may want leverage next deal.")
    if "auction_probate" in tset:
        parts.append("Bought at auction/probate — fresh project likely forming.")
    if "stalled_flip" in tset:
        parts.append("Stalled flip (open permit, no completion) — may need a bridge/takeout.")
    if "permits_no_financing" in tset:
        parts.append("Permits pulled, no recorded financing — funding work out of pocket.")
    if "out_of_state" in tset:
        parts.append("Out-of-state owner — likely needs a local lending relationship.")
    if "financing_intent" in tset and not parts:
        parts.append("Financing-intent signals present.")
    if not parts:
        parts.append("Financing-intent opportunity.")

    talking_points: List[str] = []
    if c.properties_owned is not None:
        talking_points.append(f"Owns {c.properties_owned} properties")
    if c.last_deal_months_ago is not None:
        talking_points.append(f"Last deal {c.last_deal_months_ago} months ago")
    if "out_of_state" in tset:
        talking_points.append("Out-of-state owner")

    reason = " ".join(parts)
    return reason, talking_points


def rank_dial_list(
    candidates: List[DialCandidate],
    as_of: date,
    config: Optional[DialListConfig] = None,
) -> DialList:
    cfg = config or DEFAULT_CONFIG

    # 1) dedup — one entry per resolved borrower (else per property), keep the
    #    highest-scoring candidate, merge the losers' triggers into it.
    best: Dict[Tuple[str, object], Tuple[DialCandidate, _Score, List[str]]] = {}
    for c in candidates:
        s = _score(c, as_of, cfg)
        key = _dedup_key(c)
        if key not in best:
            best[key] = (c, s, list(c.triggers))
        else:
            keep_c, keep_s, keep_triggers = best[key]
            merged = _merge_triggers(keep_triggers, c.triggers)
            if s.expected_revenue > keep_s.expected_revenue:
                best[key] = (c, s, _merge_triggers(list(c.triggers), keep_triggers))
            else:
                best[key] = (keep_c, keep_s, merged)

    # 2) sort — expected_revenue desc, deterministic tiebreak by
    #    opportunity_id then property_id (stable, reproducible).
    rows = list(best.values())
    rows.sort(key=lambda r: (r[0].opportunity_id or "", r[0].property_id))
    rows.sort(key=lambda r: r[1].expected_revenue, reverse=True)

    # 3) cut to top N, build entries with ranks.
    entries: List[DialListEntry] = []
    for i, (c, s, triggers) in enumerate(rows[: cfg.list_size], start=1):
        reason, talking_points = _reason(c, triggers)
        if s.loan_confidence == "low" and s.expected_loan > _ZERO:
            talking_points.append("Est. loan size low-confidence (fallback basis)")
        entries.append(
            DialListEntry(
                property_id=c.property_id,
                opportunity_id=c.opportunity_id,
                buyer_entity_id=c.buyer_entity_id,
                triggers=triggers,
                expected_revenue=s.expected_revenue,
                probability=s.probability,
                expected_loan=s.expected_loan,
                commission=s.commission,
                urgency=s.urgency,
                expected_loan_confidence=s.loan_confidence,
                borrower_resolved=c.buyer_entity_id is not None,
                reason=reason,
                talking_points=talking_points,
                rank=i,
            )
        )

    return DialList(
        generated_for=as_of,
        entries=entries,
        candidate_count=len(candidates),
        config_version=cfg.config_version,
    )
