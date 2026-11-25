"""R1 0.9 Auto-send judges: pure rules, frozen inputs, no paid API.

Two shadow classes are logged per card:

* ``auto-send-judge-baseline`` — template history only (edit rate, reject rate, n).
  Thresholds: reject ≥ 20 %, edit ≥ 10 %, else approve; <5 decisions → approve / no confidence.
* ``auto-send-judge-v1`` — adds per-card signals on top of the baseline rule:
    - borrower_replied_since_draft
    - agent_job_verdict (pass | fail | unknown)
    - hold_history_count (times this contact was held before)
    - prev_draft_edited (previous draft for this contact was edited)
    - is_first_touch (True = first message to this contact)
    - hours_since_last_message (None if first touch)
    - body_length (characters in the draft body)
    - has_link (body contains a URL)
    - missing_first_name (body starts without the borrower's first name)

Rule change → bump CLASS_VERSION.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

CLASS_VERSION_BASELINE = 1
CLASS_VERSION_V1 = 1

# Baseline thresholds (from P1.4 graduation config, D11)
_BASELINE_MIN_HISTORY = 5       # fewer → approve / null confidence
_BASELINE_REJECT_RATE = 0.20    # ≥ 20 % rejected → predict reject
_BASELINE_EDIT_RATE = 0.10      # ≥ 10 % edited → predict edit


@dataclass(frozen=True)
class CardFeatures:
    """Frozen inputs for the judge.  All fields are known at card creation; none come from post-decision data.

    ``template_version`` is the template key on the card's payload.
    ``past_total`` / ``past_edit`` / ``past_reject`` are counts read from pending_actions at card creation.
    Per-card signals are from the card itself and the borrower's history up to that moment.
    """

    # template history
    template_version: Optional[str]
    past_total: int           # total decided cards for this template version
    past_edit: int            # edited then approved
    past_reject: int          # rejected

    # per-card signals (v1)
    borrower_replied_since_draft: bool = False
    agent_job_verdict: str = "unknown"       # "pass" | "fail" | "unknown"
    hold_history_count: int = 0              # times this contact was held before
    prev_draft_edited: bool = False          # previous draft for this contact was edited
    is_first_touch: bool = True
    hours_since_last_message: Optional[float] = None
    body_length: int = 0
    has_link: bool = False
    missing_first_name: bool = False


@dataclass(frozen=True)
class Prediction:
    decision: str                 # "approve" | "edit" | "reject"
    confidence: Optional[float]   # 0–1; None for rules classes with thin history


def predict_baseline(features: CardFeatures) -> Prediction:
    """Template-history-only baseline judge.

    Returns approve with confidence=None when history is too thin (<5 decisions).
    With enough history: reject ≥ 20 %, edit ≥ 10 %, else approve.
    Confidence is a rough proxy: 1 − distance to the nearest threshold.
    """
    if features.past_total < _BASELINE_MIN_HISTORY:
        return Prediction(decision="approve", confidence=None)

    reject_rate = features.past_reject / features.past_total
    edit_rate = features.past_edit / features.past_total

    if reject_rate >= _BASELINE_REJECT_RATE:
        confidence = min(1.0, round(reject_rate / _BASELINE_REJECT_RATE, 4))
        return Prediction(decision="reject", confidence=confidence)

    if edit_rate >= _BASELINE_EDIT_RATE:
        confidence = min(1.0, round(edit_rate / _BASELINE_EDIT_RATE, 4))
        return Prediction(decision="edit", confidence=confidence)

    # approve: confidence = 1 − how close we are to the lower threshold
    # (farther from thresholds = more confident)
    headroom = min(
        _BASELINE_REJECT_RATE - reject_rate,
        _BASELINE_EDIT_RATE - edit_rate,
    )
    total_range = min(_BASELINE_REJECT_RATE, _BASELINE_EDIT_RATE)
    confidence = round(min(1.0, headroom / total_range), 4) if total_range > 0 else 1.0
    return Prediction(decision="approve", confidence=confidence)


def predict_v1(features: CardFeatures) -> Prediction:
    """Card-signals judge.

    Starts from the baseline, then applies per-card veto signals that flip to
    'edit' or 'reject'.  The idea: if any red-flag signal fires, downgrade.

    Signals that push toward edit/reject:
    * borrower replied since the draft → edit (we might need to adjust)
    * agent-job verdict = fail → reject
    * hold_history_count ≥ 2 → edit (this contact repeatedly causes holds)
    * prev_draft_edited → edit (previous message to this person needed changes)
    * body length < 20 chars → edit (too short to be a real message)
    * missing_first_name → edit (personalisation not done)

    Signals that push toward approve:
    * borrower replied since the draft AND is_first_touch → edit (a reply on a first
      touch is unusual; keep but surface for review)

    None of these signals can flip a 'reject' baseline to approve.
    """
    base = predict_baseline(features)

    # Signals that override toward reject first (highest risk)
    if features.agent_job_verdict == "fail":
        return Prediction(decision="reject", confidence=0.90)

    # Then toward edit
    edit_signals: list[str] = []
    if features.borrower_replied_since_draft:
        edit_signals.append("borrower_replied")
    if features.hold_history_count >= 2:
        edit_signals.append("hold_history")
    if features.prev_draft_edited:
        edit_signals.append("prev_edited")
    if features.body_length < 20:
        edit_signals.append("body_too_short")
    if features.missing_first_name:
        edit_signals.append("missing_first_name")

    if edit_signals and base.decision == "approve":
        # Downgrade to edit; confidence = fraction of signals that fired (capped)
        signal_confidence = round(min(1.0, len(edit_signals) * 0.25), 4)
        return Prediction(decision="edit", confidence=signal_confidence)

    return base
