"""
WP-8A Quote Ready — persistence core (spine-independent, pure parts).

The DB write and domain-event publish depend on Dev 1's WP-1 spine
(fa_max_opportunities, state_engine.transition, event publisher), which is NOT
yet on `dev`. Everything here is pure and unit-testable now; the spine-bound
half is expressed behind QuoteReadyEventSink / a persist entrypoint that a
later change wires to WP-1.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Literal, Optional, Protocol

from .models import (
    Confidence,
    QuoteReadyInput,
    QuoteReadyResult,
    default_rehab_confidence,
    min_confidence,
)

# Bump when compute logic (formula, rounding, precedence) changes. Old rows are
# never overwritten — a new version yields a new input_hash key dimension.
QUOTE_READY_CALC_VERSION = "wp8a-1.0.0"

ResultStatus = Literal["computed", "incomplete", "needs_review", "approved", "rejected", "superseded"]

# Input fields that define an "effective input" for hashing. opportunity_id and
# calculation_version are separate dimensions of the uniqueness key, so excluded.
_HASH_FIELDS = (
    "property_id",
    "purchase_price",
    "estimated_value",
    "assessed_value_mkt",
    "last_sale_price",
    "rehab_estimate",
    "rehab_source",
    "rehab_confidence",
    "arv",
    "arv_source",
    "arv_confidence",
    "max_ltc",
    "max_ltv",
)


def _canon(value) -> Optional[str]:
    """Canonical string for hashing. Numerically equal Decimals hash equal:
    0.80 == 0.8, 100 == 100.00, and signed zero collapses to "0"."""
    if value is None:
        return None
    if isinstance(value, Decimal):
        if value == 0:
            return "0"
        return str(value.normalize())
    return str(value)


def compute_input_hash(inp: QuoteReadyInput) -> str:
    """Deterministic sha256 over canonicalized effective inputs."""
    payload = {name: _canon(getattr(inp, name)) for name in _HASH_FIELDS}
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def derive_status(result: QuoteReadyResult) -> ResultStatus:
    """A freshly computed result is 'computed' when whole, else 'incomplete'.

    Review states (needs_review/approved/rejected) belong to the review layer;
    'superseded' is applied on recompute.
    """
    return "incomplete" if result.missing else "computed"


def overall_confidence(result: QuoteReadyResult) -> Confidence | Literal["unknown"]:
    """Weakest confidence across present figures; 'unknown' when none present."""
    confs = [
        fig.confidence
        for fig in (result.project_cost, result.proposed_loan, result.ltc, result.ltv)
        if fig is not None
    ]
    if not confs:
        return "unknown"
    return min_confidence(*confs)


# ---------------------------------------------------------------------------
# Supersede decision — pure. The caller performs the actual INSERT/UPDATE.
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ExistingResult:
    result_id: str
    input_hash: str
    calculation_version: str


@dataclass(frozen=True)
class PersistDecision:
    action: Literal["noop", "insert", "insert_supersede"]
    existing_result_id: Optional[str] = None   # noop → the row to reuse
    supersedes_result_id: Optional[str] = None  # insert_supersede → prior row


def decide_persistence(
    latest: Optional[ExistingResult],
    new_hash: str,
    new_version: str,
) -> PersistDecision:
    """Idempotent-or-supersede decision for a recomputation.

    - No prior row → insert.
    - Prior row with identical (hash, version) → no-op, reuse it.
    - Prior row differing on either → insert new + supersede the prior.
    """
    if latest is None:
        return PersistDecision(action="insert")
    if latest.input_hash == new_hash and latest.calculation_version == new_version:
        return PersistDecision(action="noop", existing_result_id=latest.result_id)
    return PersistDecision(action="insert_supersede", supersedes_result_id=latest.result_id)


# ---------------------------------------------------------------------------
# Row assembly — pure. Maps the domain result into the fa_max_quote_ready_results
# column payloads. result_id is generated DB-side (generate_uuidv7()).
# ---------------------------------------------------------------------------

def build_result_row(
    inp: QuoteReadyInput,
    result: QuoteReadyResult,
    *,
    computed_by: str,
    supersedes_result_id: Optional[str] = None,
) -> dict:
    figures = {
        name: (None if fig is None else fig.model_dump(mode="json"))
        for name, fig in (
            ("project_cost", result.project_cost),
            ("proposed_loan", result.proposed_loan),
            ("ltc", result.ltc),
            ("ltv", result.ltv),
        )
    }
    inputs_json = {name: _canon(getattr(inp, name)) for name in _HASH_FIELDS}
    # Persist the EFFECTIVE rehab confidence actually used by the calculation,
    # not the raw nullable field — otherwise the audit record contradicts the
    # computed confidence when the caller omits rehab_confidence.
    effective_rehab_conf = inp.rehab_confidence or default_rehab_confidence(inp.rehab_source)
    provenance = {
        "rehab": {"source": inp.rehab_source, "confidence": effective_rehab_conf},
        "arv": {"source": inp.arv_source, "confidence": inp.arv_confidence},
    }
    confidence = {
        "overall": overall_confidence(result),
        "per_figure": {k: (v["confidence"] if v else None) for k, v in figures.items()},
    }
    return {
        "opportunity_id": str(inp.opportunity_id),
        "property_id": inp.property_id,
        "calculation_version": QUOTE_READY_CALC_VERSION,
        "input_hash": compute_input_hash(inp),
        "status": derive_status(result),
        "inputs": inputs_json,
        "outputs": figures,
        "provenance": provenance,
        "confidence": confidence,
        "missing_inputs": result.missing,
        "computed_by": computed_by,
        "supersedes_result_id": supersedes_result_id,
    }


# ---------------------------------------------------------------------------
# Event emission — behind a protocol. WP-1's real publisher is not on `dev` yet.
# ---------------------------------------------------------------------------

def domain_event_for(result: QuoteReadyResult) -> str:
    return "fa_max.quote_ready.incomplete" if result.missing else "fa_max.quote_ready.computed"


def build_idempotency_key(inp: QuoteReadyInput) -> str:
    return f"quote-ready:{inp.opportunity_id}:{compute_input_hash(inp)}:{QUOTE_READY_CALC_VERSION}"


class QuoteReadyEventSink(Protocol):
    def emit(self, *, event_type: str, opportunity_id: str, idempotency_key: str, payload: dict) -> None:
        ...


class NoOpEventSink:
    """Local/default sink until WP-1's domain-event publisher lands."""
    def emit(self, *, event_type: str, opportunity_id: str, idempotency_key: str, payload: dict) -> None:
        return None
