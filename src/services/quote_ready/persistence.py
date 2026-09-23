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


# ---------------------------------------------------------------------------
# DB write path — the actual INSERT this module's own docstring flagged as
# spine-bound and "NOT yet wired": build_result_row() only ever built the
# row dict; nothing in this codebase called INSERT with it (confirmed by a
# real e2e test run finding fa_max_quote_ready_results empty in production
# despite compute_quote_ready() being called from builder_sizing.py). This
# closes that gap using the exact idempotent-or-supersede pattern
# arv_persistence.persist_arv_result() already uses for WP-8B.
# ---------------------------------------------------------------------------

from sqlalchemy import text as _text
from sqlalchemy.orm import Session as _Session

_LATEST_COMPUTED_SQL = _text(
    """
    SELECT result_id::text AS result_id, input_hash, calculation_version
    FROM fa_max_quote_ready_results
    WHERE opportunity_id = :opportunity_id ::uuid AND status = 'computed'
    ORDER BY computed_at DESC
    LIMIT 1
    """
)

# Matches uq_quote_ready_opp_hash_version's actual scope: (opportunity_id,
# input_hash, calculation_version) with NO status filter (code-review
# finding, eighth round, 2026-09). _LATEST_COMPUTED_SQL above only looks at
# 'computed' rows for the supersede DECISION — correct for that purpose
# (never clobber a reviewer's 'needs_review'/'approved'/'rejected' row) —
# but persist_quote_ready_result() used ONLY that lookup to also decide
# insert-vs-noop, so a recompute producing the exact same (hash, version)
# as an existing NON-'computed' row (most commonly 'incomplete' — an
# opportunity missing ARV, retried unchanged) found no 'latest', tried to
# insert, and crashed on the unique constraint instead of returning the
# existing row. Caught by the $20k/$50k precedence integration test's own
# repeat-trigger-no-duplicate case.
_EXACT_MATCH_ANY_STATUS_SQL = _text(
    """
    SELECT result_id::text AS result_id
    FROM fa_max_quote_ready_results
    WHERE opportunity_id = :opportunity_id ::uuid
      AND input_hash = :input_hash AND calculation_version = :calculation_version
    LIMIT 1
    """
)

_MARK_SUPERSEDED_SQL = _text(
    "UPDATE fa_max_quote_ready_results SET status = 'superseded' WHERE result_id = :rid ::uuid"
)

_INSERT_SQL = _text(
    """
    INSERT INTO fa_max_quote_ready_results (
        opportunity_id, property_id, calculation_version, input_hash, status,
        inputs, outputs, provenance, confidence, missing_inputs, computed_by,
        supersedes_result_id
    ) VALUES (
        :opportunity_id ::uuid, :property_id, :calculation_version, :input_hash, :status,
        CAST(:inputs AS JSONB), CAST(:outputs AS JSONB), CAST(:provenance AS JSONB),
        CAST(:confidence AS JSONB), CAST(:missing_inputs AS JSONB), :computed_by,
        :supersedes_result_id ::uuid
    )
    RETURNING result_id::text
    """
)


def persist_quote_ready_result(
    session: _Session,
    *,
    inp: QuoteReadyInput,
    result: QuoteReadyResult,
    computed_by: str,
) -> str:
    """Persist a computed Quote Ready result, idempotent-or-supersede on
    (opportunity_id, effective inputs, calculation_version) — same shape as
    arv_persistence.persist_arv_result(). Returns the result_id that now
    represents this opportunity's current scenario (an existing row on a
    no-op, else the freshly inserted one).

    Only ever supersedes a row still in 'computed' status — a row already
    'needs_review'/'approved'/'rejected' by a human reviewer is left alone;
    persist_quote_ready_result() finding no 'computed' row for the
    opportunity simply inserts a fresh one, which is exactly what
    dossier._handle_quote_ready_modify_submission wants: Modify always
    produces a new row to re-review, never silently overwrites the one a
    reviewer already looked at.
    """
    new_hash = compute_input_hash(inp)
    row = session.execute(
        _LATEST_COMPUTED_SQL, {"opportunity_id": str(inp.opportunity_id)}
    ).mappings().first()
    latest = (
        ExistingResult(
            result_id=row["result_id"],
            input_hash=row["input_hash"],
            calculation_version=row["calculation_version"],
        )
        if row is not None
        else None
    )
    decision = decide_persistence(latest, new_hash, QUOTE_READY_CALC_VERSION)
    if decision.action == "noop":
        return decision.existing_result_id  # type: ignore[return-value]

    # decide_persistence() only saw 'computed' rows — an exact (hash,
    # version) match against a NON-'computed' row (e.g. 'incomplete') is
    # invisible to it, but still collides with uq_quote_ready_opp_hash_version.
    # Check the constraint's actual scope directly before attempting the
    # insert, rather than letting Postgres reject it.
    exact_match_id = session.execute(
        _EXACT_MATCH_ANY_STATUS_SQL,
        {
            "opportunity_id": str(inp.opportunity_id),
            "input_hash": new_hash,
            "calculation_version": QUOTE_READY_CALC_VERSION,
        },
    ).scalar_one_or_none()
    if exact_match_id is not None:
        return exact_match_id

    row_dict = build_result_row(
        inp, result, computed_by=computed_by, supersedes_result_id=decision.supersedes_result_id,
    )
    if decision.action == "insert_supersede":
        session.execute(_MARK_SUPERSEDED_SQL, {"rid": decision.supersedes_result_id})

    new_id = session.execute(
        _INSERT_SQL,
        {
            **row_dict,
            "inputs": json.dumps(row_dict["inputs"]),
            "outputs": json.dumps(row_dict["outputs"]),
            "provenance": json.dumps(row_dict["provenance"]),
            "confidence": json.dumps(row_dict["confidence"]),
            "missing_inputs": json.dumps(row_dict["missing_inputs"]),
        },
    ).scalar_one()
    return new_id
