"""R1 1.1 unified timeline for one spine contact (the spec's ``lending.timeline_events``, read in Python: CLAUDE.md
forbids a view on ``call_dispositions``, and the live GHL and Instantly history cannot be in a database view)."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Sequence

from sqlalchemy.orm import Session

from config.lending_contact_spine import TIMELINE_LIMIT
from src.lending.spine.touches import Touch, TouchSource, contact_keys, local_touches

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Timeline:
    touches: tuple[Touch, ...]
    unavailable: tuple[str, ...]


def _newest_first(touch: Touch) -> tuple[bool, float]:
    return (touch.at is None, -touch.at.timestamp() if touch.at else 0.0)


def timeline(db: Session, contact_id: int, *, remote: Sequence[TouchSource], limit: int = TIMELINE_LIMIT) -> Timeline:
    keys = contact_keys(db, contact_id)
    by_ref = {touch.ref: touch for touch in local_touches(db, keys)}
    unavailable: list[str] = []
    for source in remote:
        if not getattr(source, "configured", True):
            unavailable.append(source.system)
            continue
        try:
            found = source.touches(keys)
        except Exception as exc:  # class only: a provider error can echo request detail
            logger.warning("contact timeline: %s unavailable (%s)", source.system, type(exc).__name__)
            unavailable.append(source.system)
            continue
        by_ref.update((touch.ref, touch) for touch in found)  # the provider's own record wins
    ordered = sorted(by_ref.values(), key=_newest_first)
    return Timeline(touches=tuple(ordered[:limit]), unavailable=tuple(unavailable))
