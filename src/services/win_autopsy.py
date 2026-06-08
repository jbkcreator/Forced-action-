"""Win autopsy memory cards for Cora's counterfactual learning layer."""

from __future__ import annotations

import json
from collections import Counter
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.core.models import DealOutcome, DistressScore, LearningCard, Owner, Property, Subscriber

CARD_TYPE = "win_autopsy"
MAX_WINS_PER_CARD = 50
LEARNING_CARD_CACHE_TTL = 8 * 86400


def _jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _latest_score(db: Session, property_id: int) -> Optional[DistressScore]:
    return (
        db.execute(
            select(DistressScore)
            .where(DistressScore.property_id == property_id)
            .order_by(DistressScore.score_date.desc())
            .limit(1)
        )
        .scalars()
        .first()
    )


def _distress_signal_names(distress_types: Any) -> list[str]:
    if isinstance(distress_types, list):
        return [str(v) for v in distress_types if v]
    if isinstance(distress_types, dict):
        names = []
        for key, value in distress_types.items():
            if value is True:
                names.append(str(key))
            elif isinstance(value, (int, float, Decimal)) and value > 0:
                names.append(str(key))
            elif isinstance(value, list) and value:
                names.append(str(key))
        return names
    return []


def _owner_snapshot(owner: Optional[Owner]) -> dict:
    if owner is None:
        return {
            "owner_type": None,
            "absentee_status": None,
            "contact_info_confidence": None,
            "contact_info_confidence_score": None,
            "phone_type": None,
            "phone_reachable": None,
        }
    phone_meta = owner.phone_metadata if isinstance(owner.phone_metadata, dict) else {}
    phone_1_meta = phone_meta.get("phone_1") if isinstance(phone_meta.get("phone_1"), dict) else {}
    return {
        "owner_type": owner.owner_type,
        "absentee_status": owner.absentee_status,
        "contact_info_confidence": owner.contact_info_confidence,
        "contact_info_confidence_score": _jsonable(owner.contact_info_confidence_score),
        "phone_type": phone_1_meta.get("type"),
        "phone_reachable": phone_1_meta.get("reachable"),
    }


def _build_patterns(win: dict) -> list[str]:
    patterns = []
    if win.get("county_id"):
        patterns.append(f"county:{win['county_id']}")
    if win.get("trade_vertical"):
        patterns.append(f"vertical:{win['trade_vertical']}")
    if win.get("lead_tier"):
        patterns.append(f"tier:{win['lead_tier']}")
    owner = win.get("owner") or {}
    if owner.get("contact_info_confidence"):
        patterns.append(f"contact_confidence:{owner['contact_info_confidence']}")
    if owner.get("phone_type"):
        patterns.append(f"phone_type:{owner['phone_type']}")
    for signal in win.get("distress_signals") or []:
        patterns.append(f"signal:{signal}")
    return patterns


def build_win_snapshot(deal: DealOutcome, db: Session) -> dict:
    sub = db.get(Subscriber, deal.subscriber_id)
    prop = db.get(Property, deal.property_id) if deal.property_id else None
    owner = (
        db.execute(select(Owner).where(Owner.property_id == deal.property_id)).scalars().first()
        if deal.property_id
        else None
    )
    score = _latest_score(db, deal.property_id) if deal.property_id else None
    signals = _distress_signal_names(score.distress_types if score else None)
    county_id = deal.county_id or (prop.county_id if prop else None) or (sub.county_id if sub else None)
    vertical = deal.trade_vertical or (sub.vertical if sub else None)

    snapshot = {
        "deal_id": deal.id,
        "subscriber_id": deal.subscriber_id,
        "property_id": deal.property_id,
        "deal_size_bucket": deal.deal_size_bucket,
        "deal_amount": _jsonable(deal.deal_amount),
        "days_to_close": deal.days_to_close,
        "deal_date": _jsonable(deal.deal_date),
        "county_id": county_id,
        "trade_vertical": vertical,
        "zip": prop.zip if prop else None,
        "lead_source": deal.lead_source,
        "lead_tier": score.lead_tier if score else None,
        "final_cds_score": _jsonable(score.final_cds_score) if score else None,
        "urgency_level": score.urgency_level if score else None,
        "distress_signals": signals,
        "vertical_scores": score.vertical_scores if score else None,
        "owner": _owner_snapshot(owner),
        "captured_at": datetime.now(timezone.utc).isoformat(),
    }
    snapshot["patterns"] = _build_patterns(snapshot)
    return snapshot


def _summarize(wins: list[dict], pattern_counts: dict[str, int]) -> str:
    if not wins:
        return "No win autopsies captured yet."
    latest = wins[-1]
    strongest = sorted(pattern_counts.items(), key=lambda item: (-item[1], item[0]))[:3]
    bits = ", ".join(f"{name} x{count}" for name, count in strongest) or "no repeated pattern yet"
    return (
        f"Win autopsy: {len(wins)} recent wins. Latest="
        f"{latest.get('trade_vertical') or 'unknown'} / {latest.get('county_id') or 'unknown'} / "
        f"{latest.get('deal_size_bucket') or 'unknown'}; strongest patterns: {bits}."
    )


def _prime_cache(card: LearningCard) -> None:
    try:
        from src.core.redis_client import rset
        payload = {
            "card_date": card.card_date.isoformat(),
            "card_type": card.card_type,
            "summary_text": card.summary_text,
            "data": card.data_json or {},
            "action_taken": card.action_taken,
        }
        rset(f"learning_card:{card.card_type}", json.dumps(payload), ttl_seconds=LEARNING_CARD_CACHE_TTL)
    except Exception:
        pass


def record_win_autopsy(deal_outcome_id: int, db: Session) -> Optional[LearningCard]:
    """
    Append one closed-won deal snapshot into today's win_autopsy learning card.

    Loss autopsies ask why a conversion failed. This mirrors that loop for wins
    so Cora can reuse positive patterns in future prioritization.
    """
    deal = db.get(DealOutcome, deal_outcome_id)
    if deal is None or deal.deal_size_bucket == "skip":
        return None

    today = date.today()
    snapshot = build_win_snapshot(deal, db)
    card = (
        db.execute(
            select(LearningCard).where(
                LearningCard.card_date == today,
                LearningCard.card_type == CARD_TYPE,
            )
        )
        .scalars()
        .first()
    )

    data = dict(card.data_json or {}) if card else {}
    wins = list(data.get("wins") or [])
    wins = [w for w in wins if w.get("deal_id") != deal.id]
    wins.append(snapshot)
    wins = wins[-MAX_WINS_PER_CARD:]

    counts: Counter[str] = Counter()
    for win in wins:
        counts.update(win.get("patterns") or [])
    pattern_counts = dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))

    data = {
        "wins": wins,
        "pattern_counts": pattern_counts,
        "latest_win": snapshot,
    }
    summary = _summarize(wins, pattern_counts)

    if card:
        card.summary_text = summary
        card.data_json = data
        card.action_taken = "reuse_success_patterns"
    else:
        card = LearningCard(
            card_date=today,
            card_type=CARD_TYPE,
            summary_text=summary,
            data_json=data,
            action_taken="reuse_success_patterns",
        )
        db.add(card)
    db.flush()
    _prime_cache(card)
    return card
