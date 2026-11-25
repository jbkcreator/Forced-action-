"""R1 0.9 shadow classes: name → decision-maker and comparator. Thin until a real second class arrives."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional

from src.lending.shadow.judge import (
    CLASS_VERSION_BASELINE,
    CLASS_VERSION_V1,
    CardFeatures,
    Prediction,
    predict_baseline,
    predict_v1,
)

NO_DECISION = "no_decision"


def compare_decision(proposed: dict, actual: Optional[str]) -> Optional[bool]:
    """Match when the proposed decision equals Josh's; None while there is no decision to compare with."""
    if actual is None or actual == NO_DECISION:
        return None
    return proposed.get("decision") == actual


@dataclass(frozen=True)
class ShadowClassDef:
    name: str
    version: int
    predict: Callable[[CardFeatures], Prediction]
    compare: Callable[[dict, Optional[str]], Optional[bool]]
    subject_kind: str


_CLASSES = (
    ShadowClassDef("auto-send-judge-baseline", CLASS_VERSION_BASELINE, predict_baseline, compare_decision,
                   "pending_action"),
    ShadowClassDef("auto-send-judge-v1", CLASS_VERSION_V1, predict_v1, compare_decision, "pending_action"),
)
_REGISTRY = {c.name: c for c in _CLASSES}

BASELINE_CLASS = "auto-send-judge-baseline"


def get(class_name: str) -> Optional[ShadowClassDef]:
    return _REGISTRY.get(class_name)


def all_classes() -> list[ShadowClassDef]:
    return list(_CLASSES)
