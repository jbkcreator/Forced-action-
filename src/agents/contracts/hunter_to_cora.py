"""
Hunter->Cora handoff contract (QUALITY-v2.2 Q3).

Formalizes src/agents/hunter/gating.py's ad hoc UNVERIFIED_FLOOR=70 /
is_citable() single-field gate into an explicit required-fields schema,
checked at the real seam Cora's drafting pipeline reads Hunter's data
through: src.services.whale_ranking.get_ranked_whales() (re-exported
unmodified as src.agents.cora.tools.read_tools.get_ranked_whales) for Cell
#1, and read_tools.get_recent_auction_fast_follow_whales() for Cell #2.

Required fields, per QUALITY-v2.2 analysis doc decision D1:
  - opportunity_thread_id (entity/target ID)   -- OPP-YYYY-##### format.
  - confidence_score (>=70 to be citable)      -- gating.is_citable(). This
    field did not exist in get_ranked_whales()'s output before this task
    (see the fix in this same task, whale_ranking.py) -- the contract
    cannot check a field that was never selected.
  - source citation                            -- source_ref (defaulted;
    every row this contract validates comes from exactly one of the two
    named Hunter functions above, so the citation is structural, not a
    per-row input Hunter has to supply).
  - dated/freshness class                      -- derived from
    whale_flagged_at (_freshness_class below), not supplied directly --
    Hunter always stamps whale_flagged_at the moment an entity qualifies
    (src/services/whale_detection.py), so the contract computes an
    age-based label rather than requiring a redundant string field.
  - contact-channel confidence per channel      -- contact_channel /
    contact_confidence (real; contact_confidence was silently dropped by
    read_tools.get_contact_channel() before this task's fix, see Task 3).
  - buyer classification (type, portfolio size, whale flag) -- entity_type +
    total_purchase_count / total_cash_volume. is_whale is NOT re-validated
    here: every row this contract ever sees already passed a
    `WHERE be.is_whale` clause in its source query (both
    get_ranked_whales() and get_recent_auction_fast_follow_whales()
    confirmed by reading both queries directly) -- re-checking it here
    would be validating something structurally already true.
  - "why now" temporal catalyst (spec §1.1.8: stale gold is barred) --
    EITHER why_now (a formatted string, Cell #1 shape) OR
    latest_auction_deed_date (Cell #2 shape -- verified by reading
    read_tools.get_recent_auction_fast_follow_whales()'s SELECT, which has
    no why_now column at all). At least one must be present.
  - warm-intro annotation, if present           -- warm_intro (Optional;
    no warm-intro concept exists anywhere in the codebase today, confirmed
    by grep -- always None until a future Hunter subtask adds one; present
    in the schema now so adding real data later is not a breaking change).
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator, model_validator
from sqlalchemy.orm import Session

from src.agents.contracts.base import HandoffRejected, reject_and_notify
from src.agents.hunter.gating import is_citable

logger = logging.getLogger(__name__)

_THREAD_ID_RE = re.compile(r"^OPP-\d{4}-\d{5}$")
_FRESH_DAYS = 14
_STALE_AFTER_DAYS = 90


def _freshness_class(whale_flagged_at: Optional[Any]) -> str:
    if whale_flagged_at is None:
        return "undated"
    if isinstance(whale_flagged_at, str):
        try:
            whale_flagged_at = datetime.fromisoformat(whale_flagged_at)
        except ValueError:
            return "undated"
    if whale_flagged_at.tzinfo is None:
        whale_flagged_at = whale_flagged_at.replace(tzinfo=timezone.utc)
    age_days = (datetime.now(timezone.utc) - whale_flagged_at).days
    if age_days < 0 or age_days <= _FRESH_DAYS:
        return "fresh"
    if age_days <= _STALE_AFTER_DAYS:
        return "aging"
    return "stale"


class HunterToCoraHandoff(BaseModel):
    opportunity_thread_id: str
    confidence_score: int = Field(ge=0, le=100)
    entity_type: str
    total_purchase_count: int = Field(ge=0)
    total_cash_volume: Decimal
    contact_channel: str
    contact_confidence: int = Field(ge=0, le=100)
    why_now: Optional[str] = None
    latest_auction_deed_date: Optional[str] = None
    source_ref: str = "hunter_whale_ranking"
    freshness_class: str = "undated"
    warm_intro: Optional[str] = None

    @field_validator("opportunity_thread_id")
    @classmethod
    def _thread_id_format(cls, v: str) -> str:
        if not v or not _THREAD_ID_RE.match(v):
            raise ValueError(f"opportunity_thread_id {v!r} does not match OPP-YYYY-##### format")
        return v

    @field_validator("contact_channel")
    @classmethod
    def _contact_channel_known(cls, v: str) -> str:
        if v not in ("phone", "email", "none"):
            raise ValueError(f"contact_channel {v!r} not one of phone/email/none")
        return v

    @model_validator(mode="after")
    def _why_now_present(self) -> "HunterToCoraHandoff":
        has_why_now = bool(self.why_now and self.why_now.strip())
        has_auction_date = bool(self.latest_auction_deed_date)
        if not has_why_now and not has_auction_date:
            raise ValueError(
                "neither why_now nor latest_auction_deed_date present — "
                "spec §1.1.8 requires a temporal catalyst (stale gold is barred)"
            )
        return self


def validate_handoff(ranked_whale: dict[str, Any]) -> HunterToCoraHandoff:
    """Builds and validates the contract from one row of
    whale_ranking.get_ranked_whales() / read_tools.get_recent_auction_fast_follow_whales()
    output (post fallback_ranking.rank_targets(), which preserves all
    original keys and only adds fallback_score).

    Raises pydantic.ValidationError on any structurally invalid or missing
    field. Callers must catch that and route through reject_handoff() below
    rather than let one bad row crash the whole producer sweep.
    """
    return HunterToCoraHandoff(
        opportunity_thread_id=ranked_whale.get("opportunity_thread_id"),
        confidence_score=ranked_whale.get("confidence_score"),
        entity_type=ranked_whale.get("entity_type"),
        total_purchase_count=ranked_whale.get("total_purchase_count"),
        total_cash_volume=ranked_whale.get("total_cash_volume"),
        contact_channel=ranked_whale.get("contact_channel"),
        contact_confidence=ranked_whale.get("contact_confidence"),
        why_now=ranked_whale.get("why_now"),
        latest_auction_deed_date=(
            str(ranked_whale["latest_auction_deed_date"])
            if ranked_whale.get("latest_auction_deed_date") else None
        ),
        freshness_class=_freshness_class(ranked_whale.get("whale_flagged_at")),
        warm_intro=ranked_whale.get("warm_intro"),
    )


def is_handoff_citable(handoff: HunterToCoraHandoff) -> bool:
    """The formalized version of gating.is_citable() -- same threshold,
    now checked against a full validated handoff rather than a bare int.

    Also enforces spec §1.1.8 ("stale gold is barred"): a freshness_class of
    'stale' (whale_flagged_at older than _STALE_AFTER_DAYS) is never citable,
    regardless of confidence_score -- a previously-qualified whale must not
    stay in the ranking indefinitely just because nothing re-checks its age."""
    return is_citable(handoff.confidence_score) and handoff.freshness_class != "stale"


def reject_handoff(session: Session, ranked_whale: dict[str, Any], errors: list[str]) -> HandoffRejected:
    """Records + Slack-notifies a Hunter->Cora rejection (decision D2).
    Called by target_producer.py when validate_handoff() raises, or when
    is_handoff_citable() is False after a structurally valid build."""
    return reject_and_notify(
        session,
        boundary="hunter_to_cora",
        missing_fields=errors,
        reference_id=ranked_whale.get("opportunity_thread_id"),
        payload_snapshot=ranked_whale,
    )
