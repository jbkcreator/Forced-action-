"""R1 0.9 Shadow class registry.

Maps class names to their predict callable and version.  New classes are added here and nowhere else.
No plugin framework — this list stays thin until a real second class arrives.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional

from src.lending.shadow.judge import (
    CardFeatures,
    CLASS_VERSION_BASELINE,
    CLASS_VERSION_V1,
    Prediction,
    predict_baseline,
    predict_v1,
)


@dataclass(frozen=True)
class ShadowClassDef:
    name: str
    version: int
    predict: Callable[[CardFeatures], Prediction]
    subject_kind: str = "pending_action"


_REGISTRY: dict[str, ShadowClassDef] = {}


def _register(defn: ShadowClassDef) -> None:
    _REGISTRY[defn.name] = defn


_register(ShadowClassDef(
    name="auto-send-judge-baseline",
    version=CLASS_VERSION_BASELINE,
    predict=predict_baseline,
))

_register(ShadowClassDef(
    name="auto-send-judge-v1",
    version=CLASS_VERSION_V1,
    predict=predict_v1,
))


def get(class_name: str) -> Optional[ShadowClassDef]:
    return _REGISTRY.get(class_name)


def all_classes() -> list[ShadowClassDef]:
    return list(_REGISTRY.values())
