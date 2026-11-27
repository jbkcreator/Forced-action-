"""R1 0.9 auto-send judges: pure rules over features frozen at card creation, no paid API. Rule change → bump the version."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

CLASS_VERSION_BASELINE = 1
CLASS_VERSION_V1 = 1

_BASELINE_MIN_HISTORY = 5
_BASELINE_REJECT_RATE = 0.20
_BASELINE_EDIT_RATE = 0.10
_MIN_BODY_LENGTH = 20
_QUICK_FOLLOW_UP_HOURS = 24.0


@dataclass(frozen=True)
class CardFeatures:
    template_version: Optional[str]
    past_total: int
    past_edit: int
    past_reject: int
    borrower_replied_since_last_send: bool = False
    from_agent_job: bool = False
    agent_job_held: bool = False
    prev_draft_edited: bool = False
    is_first_touch: bool = True
    hours_since_last_message: Optional[float] = None
    body_length: int = 0
    has_link: bool = False
    missing_first_name: bool = False


@dataclass(frozen=True)
class Prediction:
    decision: str
    confidence: Optional[float]


def predict_baseline(features: CardFeatures) -> Prediction:
    """Template history only: under 5 past decisions → approve with no confidence; reject ≥ 20%; edit ≥ 10%."""
    if features.past_total < _BASELINE_MIN_HISTORY:
        return Prediction(decision="approve", confidence=None)

    reject_rate = features.past_reject / features.past_total
    edit_rate = features.past_edit / features.past_total

    if reject_rate >= _BASELINE_REJECT_RATE:
        return Prediction(decision="reject", confidence=min(1.0, round(reject_rate / _BASELINE_REJECT_RATE, 4)))
    if edit_rate >= _BASELINE_EDIT_RATE:
        return Prediction(decision="edit", confidence=min(1.0, round(edit_rate / _BASELINE_EDIT_RATE, 4)))

    headroom = min(_BASELINE_REJECT_RATE - reject_rate, _BASELINE_EDIT_RATE - edit_rate)
    return Prediction(decision="approve", confidence=round(min(1.0, headroom / _BASELINE_EDIT_RATE), 4))


def _edit_signals(features: CardFeatures) -> list[str]:
    checks = {
        "borrower_replied": features.borrower_replied_since_last_send,
        "agent_job_held": features.agent_job_held,
        "prev_draft_edited": features.prev_draft_edited,
        "quick_follow_up": (features.hours_since_last_message is not None
                            and features.hours_since_last_message < _QUICK_FOLLOW_UP_HOURS),
        "link_on_first_touch": features.has_link and features.is_first_touch,
        "body_too_short": features.body_length < _MIN_BODY_LENGTH,
        "missing_first_name": features.missing_first_name,
    }
    return [name for name, fired in checks.items() if fired]


def predict_v1(features: CardFeatures) -> Prediction:
    """The baseline, downgraded from approve to edit when any per-card red flag fires; never upgrades a reject."""
    base = predict_baseline(features)
    signals = _edit_signals(features)
    if signals and base.decision == "approve":
        return Prediction(decision="edit", confidence=round(min(1.0, len(signals) * 0.25), 4))
    return base
