"""WP-8B canonical ARV persistence + the WP-7 consumption projection.

Two D6-WP7-contract deliverables live here (property-keyed, spine-independent —
no dependency on WP-1 opportunities / state_engine):

  1. persist_arv_result(...)  -- write the canonical ARV row for a property,
                                 idempotent-or-supersede on determinative inputs,
                                 rounding low/high/point to $5,000 at the single
                                 write site.
  2. get_published_arv(...)   -- project the narrow, privacy-allowlisted
                                 PublishedARV DTO other surfaces (WP-7) consume.
                                 selected_comps + locality_tier physically cannot
                                 reach it — defense in depth at the source.

The async recompute consumer (contract deliverable #3: read WP-7's fire-and-
forget enqueue -> compute_arv_for_property -> persist) is DEFERRED: it lands
once WP-7's enqueue shape exists. Until then WP-7's enqueue no-ops harmlessly.
"""
from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Literal, Optional

from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from .arv_models import ARVResult

logger = logging.getLogger(__name__)

# Bump when compute logic (formula, rounding, tiering) changes. A new version is
# a distinct dimension of the uniqueness key — old rows are never overwritten.
ARV_CALC_VERSION = "wp8b-1.0.0"

_ROUNDING_STEP = Decimal("5000")


def round_to_5k(value: Optional[Decimal]) -> Optional[Decimal]:
    """Round to the nearest $5,000 (half up). None -> None.

    The single rounding site per the D6-WP7 contract: internal and external
    reads see identical numbers because the rounded figure is what's stored.
    """
    if value is None:
        return None
    if not isinstance(value, Decimal):
        value = Decimal(str(value))
    return (value / _ROUNDING_STEP).quantize(Decimal("1"), rounding=ROUND_HALF_UP) * _ROUNDING_STEP


def _canon(value) -> Optional[str]:
    """Canonical string for hashing — numerically equal Decimals hash equal."""
    if value is None:
        return None
    if isinstance(value, Decimal):
        return "0" if value == 0 else str(value.normalize())
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def compute_arv_input_hash(result: ARVResult) -> str:
    """Deterministic sha256 over the determinative fields of a computed ARV.

    Two computations with the same unrounded valuation and full comp
    provenance hash equal, so a true retry is a no-op. Display rounding is not
    applied here: materially different inputs must remain separately auditable
    even when they happen to round to the same published $5,000 increment.
    """
    selected_comps = sorted(
        (comp.model_dump(mode="json") for comp in result.selected_comps),
        key=lambda comp: (
            comp["property_id"], comp["sale_yr"], comp["sale_mo"]
        ),
    )
    payload = {
        "low": _canon(result.low),
        "high": _canon(result.high),
        "point": _canon(result.point),
        "confidence": _canon(result.confidence),
        "comp_count": _canon(result.comp_count),
        "weak_comp": _canon(result.weak_comp),
        "locality_tier": _canon(result.locality_tier),
        "recency_window_months": _canon(result.recency_window_months),
        "after_repair_condition": _canon(result.after_repair_condition),
        "inferred_condition_count": _canon(result.inferred_condition_count),
        "arv_unknown": _canon(result.arv_unknown),
        "unknown_reason": _canon(result.unknown_reason),
        "source": _canon(result.source),
        "selected_comps": selected_comps,
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class PublishedARV(BaseModel):
    """Narrow, privacy-allowlisted ARV projection for external consumers (WP-7).

    Frozen field set from the D6-WP7 contract. Deliberately omits locality_tier
    (internal ranking) and selected_comps (never on an unauthenticated surface)
    — they physically cannot be attached to this model.
    """

    model_config = {"frozen": True}

    arv_result_id: str
    low: Optional[Decimal] = None
    high: Optional[Decimal] = None
    point: Optional[Decimal] = None
    confidence: Optional[str] = None
    comp_count: int = 0
    weak_comp: bool = True
    computed_at: datetime
    source: str
    # Manual-override audit trail (WP-8B: "allow reviewed manual override
    # with audit trail"). low/high/point above are already the override
    # values when overridden=True — a consumer that only reads low/high/point
    # always gets the figure that should govern, without needing to know
    # override happened. These extra fields exist so a reviewer-facing
    # surface can still show "this was overridden, by whom, why."
    overridden: bool = False
    overridden_by: Optional[str] = None
    override_reason: Optional[str] = None


# ---------------------------------------------------------------------------
# Idempotent-or-supersede decision — pure. Caller performs the INSERT/UPDATE.
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ExistingArvResult:
    arv_result_id: str
    input_hash: str
    calculation_version: str


@dataclass(frozen=True)
class ArvPersistDecision:
    action: Literal["noop", "insert", "insert_supersede"]
    existing_result_id: Optional[str] = None
    supersedes_result_id: Optional[str] = None


def decide_arv_persistence(
    latest: Optional[ExistingArvResult],
    new_hash: str,
    new_version: str,
) -> ArvPersistDecision:
    """No prior row -> insert. Same (hash, version) -> no-op. Else supersede."""
    if latest is None:
        return ArvPersistDecision(action="insert")
    if latest.input_hash == new_hash and latest.calculation_version == new_version:
        return ArvPersistDecision(action="noop", existing_result_id=latest.arv_result_id)
    return ArvPersistDecision(
        action="insert_supersede", supersedes_result_id=latest.arv_result_id
    )


# ---------------------------------------------------------------------------
# DB access
# ---------------------------------------------------------------------------

_LATEST_COMPUTED_SQL = text(
    """
    SELECT arv_result_id, input_hash, calculation_version
    FROM fa_max_arv_results
    WHERE property_id = :pid AND status = 'computed'
    ORDER BY computed_at DESC
    LIMIT 1
    """
)

_LOCK_PROPERTY_SQL = text("SELECT pg_advisory_xact_lock(:pid)")

_INSERT_SQL = text(
    """
    INSERT INTO fa_max_arv_results (
        property_id, low, high, point, confidence, comp_count, weak_comp,
        locality_tier, selected_comps, source, arv_unknown,
        calculation_version, input_hash, status, supersedes_result_id
    ) VALUES (
        :property_id, :low, :high, :point, :confidence, :comp_count, :weak_comp,
        :locality_tier, CAST(:selected_comps AS JSONB), :source, :arv_unknown,
        :calculation_version, :input_hash, 'computed', :supersedes_result_id
    )
    RETURNING arv_result_id
    """
)

_MARK_SUPERSEDED_SQL = text(
    "UPDATE fa_max_arv_results SET status = 'superseded' WHERE arv_result_id = :rid"
)

_PUBLISHED_SQL = text(
    """
    SELECT arv_result_id, low, high, point, confidence, comp_count,
           weak_comp, computed_at, source, arv_unknown, status,
           override_low, override_point, override_high,
           overridden_by, override_reason
    FROM fa_max_arv_results
    WHERE property_id = :pid AND status IN ('computed', 'overridden')
    ORDER BY computed_at DESC
    LIMIT 1
    """
)

_OVERRIDE_SQL = text(
    """
    UPDATE fa_max_arv_results
    SET status = 'overridden',
        override_low = :override_low,
        override_point = :override_point,
        override_high = :override_high,
        override_reason = :override_reason,
        overridden_by = :overridden_by,
        overridden_at = now()
    WHERE arv_result_id = :arv_result_id AND status = 'computed'
    RETURNING arv_result_id
    """
)


def persist_arv_result(
    session: Session,
    *,
    property_id: int,
    result: ARVResult,
    computed_by: str,
) -> str:
    """Persist a computed ARV for a property, idempotent-or-supersede.

    Returns the ``arv_result_id`` of the row that now represents this property's
    current ARV (an existing row on a no-op, else the freshly inserted one).

    Raises SQLAlchemyError (after rollback + ERROR log) on any DB failure.
    """
    new_hash = compute_arv_input_hash(result)
    try:
        # Serialize recomputes for one property. Without this, two workers can
        # both read the same current row and each create a new canonical row.
        if session.get_bind().dialect.name == "postgresql":
            session.execute(_LOCK_PROPERTY_SQL, {"pid": property_id})

        row = session.execute(
            _LATEST_COMPUTED_SQL, {"pid": property_id}
        ).mappings().first()
        latest = (
            ExistingArvResult(
                arv_result_id=str(row["arv_result_id"]),
                input_hash=row["input_hash"],
                calculation_version=row["calculation_version"],
            )
            if row is not None
            else None
        )
        decision = decide_arv_persistence(latest, new_hash, ARV_CALC_VERSION)

        if decision.action == "noop":
            return decision.existing_result_id  # type: ignore[return-value]

        selected_comps_json = json.dumps(
            [c.model_dump(mode="json") for c in result.selected_comps]
        )
        params = {
            "property_id": property_id,
            "low": round_to_5k(result.low),
            "high": round_to_5k(result.high),
            "point": round_to_5k(result.point),
            "confidence": result.confidence,
            "comp_count": result.comp_count,
            "weak_comp": result.weak_comp,
            "locality_tier": result.locality_tier,
            "selected_comps": selected_comps_json,
            "source": result.source,
            "arv_unknown": result.arv_unknown,
            "calculation_version": ARV_CALC_VERSION,
            "input_hash": new_hash,
            "supersedes_result_id": decision.supersedes_result_id,
        }
        if decision.action == "insert_supersede":
            session.execute(
                _MARK_SUPERSEDED_SQL, {"rid": decision.supersedes_result_id}
            )

        new_id = session.execute(_INSERT_SQL, params).scalar_one()

        session.commit()
        logger.info(
            "arv_persistence: %s ARV for property_id=%s (result_id=%s, by=%s)",
            decision.action, property_id, new_id, computed_by,
        )
        return str(new_id)
    except SQLAlchemyError:
        session.rollback()
        logger.error(
            "arv_persistence: failed to persist ARV for property_id=%s",
            property_id, exc_info=True,
        )
        raise


def get_published_arv(session: Session, property_id: int) -> Optional[PublishedARV]:
    """Project the narrow, allowlisted ARV for a property, or None.

    None when no computed row exists OR the latest is an unknown result — an
    unknown ARV reads as "absent" to consumers (WP-7 omits the field entirely,
    never a blank/guessed value). Only allowlisted columns are selected;
    selected_comps + locality_tier cannot reach this surface.
    """
    row = session.execute(_PUBLISHED_SQL, {"pid": property_id}).mappings().first()
    if row is None or row["arv_unknown"]:
        return None
    overridden = row["status"] == "overridden"
    return PublishedARV(
        arv_result_id=str(row["arv_result_id"]),
        # An override REPLACES the governing figure for every downstream
        # consumer of this projection — they read low/high/point only and
        # should never need to know an override happened to get the right
        # number. The original computed values stay untouched on the row
        # itself (queryable directly for audit/reversal — see
        # override_arv_result()'s docstring).
        low=row["override_low"] if overridden else row["low"],
        high=row["override_high"] if overridden else row["high"],
        point=row["override_point"] if overridden else row["point"],
        confidence=row["confidence"],
        comp_count=row["comp_count"],
        weak_comp=row["weak_comp"],
        computed_at=row["computed_at"],
        source=row["source"],
        overridden=overridden,
        overridden_by=row["overridden_by"] if overridden else None,
        override_reason=row["override_reason"] if overridden else None,
    )


def override_arv_result(
    session: Session,
    *,
    arv_result_id: str,
    override_low: Decimal,
    override_point: Decimal,
    override_high: Decimal,
    reason: str,
    overridden_by: str,
) -> bool:
    """Manually override a computed ARV, with a mandatory audit trail
    (WP-8B: "allow reviewed manual override with audit trail").

    Reviewed override only — targets a row currently in 'computed' status
    (the WHERE clause in _OVERRIDE_SQL enforces this: an already-overridden
    or already-superseded row cannot be re-overridden through this path,
    preventing a stale/duplicate review from silently winning). The
    ORIGINAL computed low/high/point/confidence/selected_comps are never
    modified — this only adds override_* columns and flips status, so the
    computation this override is correcting stays fully inspectable
    (a bad override is reversible: re-run compute_arv_for_property() +
    persist_arv_result() to insert a fresh 'computed' row, exactly the
    same "never destroy history" pattern used everywhere else in this
    codebase for merges/state transitions).

    Raises ValueError if reason is blank, or if low/point/high are not in
    non-decreasing order (same ordering invariant the compute engine itself
    guarantees — an override must not silently produce an inverted range).

    Returns True if the override was applied, False if no row in
    'computed' status matched arv_result_id (already overridden/superseded,
    or the id doesn't exist) — never raises for that case, since a
    double-click on a review button is a normal UI race, not an error.
    """
    if not reason or not reason.strip():
        raise ValueError("override_arv_result: reason is required")
    if not (override_low <= override_point <= override_high):
        raise ValueError(
            f"override_arv_result: range must be non-decreasing, got "
            f"low={override_low} point={override_point} high={override_high}"
        )

    result = session.execute(
        _OVERRIDE_SQL,
        {
            "arv_result_id": arv_result_id,
            "override_low": round_to_5k(override_low),
            "override_point": round_to_5k(override_point),
            "override_high": round_to_5k(override_high),
            "override_reason": reason.strip(),
            "overridden_by": overridden_by,
        },
    ).fetchone()
    applied = result is not None
    if applied:
        logger.info(
            "arv_persistence: ARV %s overridden by %s (reason=%r)",
            arv_result_id, overridden_by, reason,
        )
    else:
        logger.warning(
            "arv_persistence: override_arv_result no-op — %s is not in 'computed' status "
            "(already overridden/superseded, or does not exist)",
            arv_result_id,
        )
    return applied
