"""
FA Max Qualification Service (WP-T3-7).

Single write path for fa_max_opportunity_facts. Evaluates sufficiency against
the per-opportunity-type checklist from config/fa_max_qualification.py,
writes a decision row to fa_max_qualification_decisions, and enqueues the
appropriate downstream work.

Autonomy: internal agent — no outbound contact, no tier gate required.

Cross-owner dependencies explicitly tracked here (not silently assumed):
  • Dev 4 (WP-8A/8B): owns and must wire the "fa_max_quote_ready" queue
    consumer. T3-7 enqueues into that queue on every sufficient decision;
    Dev 4's consumer must re-check facts_revision and checklist_version at
    result-publication time, not only at claim time (stale-handoff window).
  • T3-8 (Fundability Agent, same developer): owns the enrichment-exhaustion
    promotion trigger. Until T3-8 defines a terminal enrichment-failed state,
    pending_enrichment gaps remain indefinitely pending from T3-7's side.

Compliance boundary (SOT.md Part 1):
    No borrower financial data (credit score, income, bank statement, tax
    return, SSN) may be read, written, or inferred by this module. Any fact
    key in the checklist that could hold such data is rejected at code review
    regardless of source.
"""
from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Literal, Optional
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.fa_max_qualification import (
    CHECKLIST_VERSION,
    FactSpec,
    FORBIDDEN_FINANCIAL_TERMS,
    GapType,
    NONNEGATIVE_NUMERIC_FACT_KEYS,
    PENDING_CONTRACT_APPROVAL_TYPES,
    POSITIVE_NUMERIC_FACT_KEYS,
    get_checklist,
)

# Free-text-ish fact fields whose VALUE (not just field name) must never
# carry a forbidden financial term — currently only arv_source (a
# provenance label, not a data-bearing field). current_use is a closed
# Literal enum already and needs no runtime check.
_CONTENT_CHECKED_FACT_KEYS: frozenset[str] = frozenset({"arv_source"})

logger = logging.getLogger(__name__)

# The queue name the qualification worker consumes. This name is also used
# by the admin endpoint and any other work-item producer for qualification.
FA_MAX_QUAL_QUEUE_NAME = "fa_max_qualification"

# The queue name for Scenario Builder work — owned and consumed by Dev 4 (WP-8A/8B).
# T3-7 enqueues into it on sufficiency; Dev 4 must wire the consumer.
# DEPENDENCY: not yet built as of T3-7 — see module docstring.
FA_MAX_QUOTE_READY_QUEUE_NAME = "fa_max_quote_ready"

FactSource = Literal["client", "enrichment", "auto"]
# "sufficient_pending_contract": the checklist would otherwise say
# sufficient, but this opportunity_type's checklist is unconfirmed against
# a real downstream consumer (config.PENDING_CONTRACT_APPROVAL_TYPES) — no
# handoff occurs until Dev 4 confirms. See that constant's comment.
Verdict = Literal[
    "sufficient", "insufficient", "pending_enrichment", "sufficient_pending_contract"
]


# ---------------------------------------------------------------------------
# Gap type
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Gap:
    fact_key: str
    display_name: str
    reason: str
    gap_type: GapType


@dataclass
class SufficiencyResult:
    verdict: Verdict
    gaps: list[Gap] = field(default_factory=list)
    opportunity_id: str = ""
    facts_revision: int = 0
    checklist_version: str = CHECKLIST_VERSION

    @property
    def gap_content_hash(self) -> Optional[str]:
        if not self.gaps:
            return None
        sorted_pairs = sorted((g.fact_key, g.reason) for g in self.gaps)
        raw = json.dumps(sorted_pairs, separators=(",", ":"))
        return hashlib.sha256(raw.encode()).hexdigest()


# ---------------------------------------------------------------------------
# Facts reader
# ---------------------------------------------------------------------------

_FACT_COLUMNS = (
    "facts_revision",
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
    "expected_exit_strategy",
    "current_use",
    "existing_sqft",
    "facts_provenance",
    "updated_at",
)


def get_opportunity_facts(
    *, session: Session, opportunity_id: str
) -> Optional[Dict[str, Any]]:
    """Load the current fact row for an opportunity. None if no facts row yet."""
    row = session.execute(
        text(
            "SELECT " + ", ".join(_FACT_COLUMNS) +
            " FROM fa_max_opportunity_facts"
            " WHERE opportunity_id = :oid ::uuid"
        ),
        {"oid": opportunity_id},
    ).mappings().first()
    if row is None:
        return None
    result = dict(row)
    # Deserialize provenance if returned as string
    if isinstance(result.get("facts_provenance"), str):
        result["facts_provenance"] = json.loads(result["facts_provenance"])
    return result


def _ensure_facts_row(session: Session, opportunity_id: str) -> None:
    """Upsert an empty facts row for an opportunity if none exists."""
    session.execute(
        text(
            "INSERT INTO fa_max_opportunity_facts (opportunity_id)"
            " VALUES (:oid ::uuid)"
            " ON CONFLICT (opportunity_id) DO NOTHING"
        ),
        {"oid": opportunity_id},
    )


# ---------------------------------------------------------------------------
# Facts writer — typed setters
# ---------------------------------------------------------------------------

_ALLOWED_UPDATES: frozenset[str] = frozenset({
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
    "expected_exit_strategy",
    "current_use",
    "existing_sqft",
})

# Facts that can be auto-populated from the existing enrichment pipeline
# (estimated_value → financials.estimated_value, arv → financials.arv, etc.)
# Absent enrichment-sourceable facts yield gap_type='pending_enrichment' rather
# than 'client_gap'. See T3-8 for the retry/exhaustion policy.
ENRICHMENT_SOURCEABLE_FACTS: frozenset[str] = frozenset({
    "estimated_value",
    "assessed_value_mkt",
    "last_sale_price",
    "arv",
})


def set_facts(
    *,
    session: Session,
    opportunity_id: str,
    updates: Dict[str, Any],
    source: FactSource,
    set_by: str,
) -> int:
    """Write a batch of fact updates, returning the new facts_revision.

    Precedence rules:
      - Client overrides (source='client') always win, permanently, for that fact.
      - Enrichment may refresh enrichment-owned values when source_freshness/priority
        allows — but not when a client override already exists for that fact.
      - Only effective changes (new value differs from stored value) bump
        facts_revision. A no-op write returns the current revision unchanged.

    Raises ValueError for unknown fact keys (blocks typos from silently landing
    in provenance with no DB column to validate against), and for a
    content-checked field (arv_source) whose value contains a forbidden
    financial term.

    The forbidden-term check previously existed only in the API request
    validator (admin_router.py) — this is the actual, authoritative write
    path (the API is just one caller), so a direct call from any other
    caller (a future T3-8 enrichment writer, a script, a test) could bypass
    it entirely (code-review finding, fifth round, 2026-09; SOT.md Part 1
    requires the boundary enforced at the schema/write-path level, not only
    the API layer).
    """
    unknown = set(updates) - _ALLOWED_UPDATES
    if unknown:
        raise ValueError(f"Unknown fact keys: {sorted(unknown)}")

    for key in _CONTENT_CHECKED_FACT_KEYS & set(updates):
        val = updates[key]
        if val and any(term in str(val).lower() for term in FORBIDDEN_FINANCIAL_TERMS):
            raise ValueError(
                f"{key} must not reference borrower financial data: {val!r}"
            )

    if not updates:
        # Nothing to write; ensure the row exists and return current revision.
        _ensure_facts_row(session, opportunity_id)
        row = session.execute(
            text("SELECT facts_revision FROM fa_max_opportunity_facts"
                 " WHERE opportunity_id = :oid ::uuid"),
            {"oid": opportunity_id},
        ).scalar()
        return row or 0

    _ensure_facts_row(session, opportunity_id)

    # FOR UPDATE: serializes concurrent set_facts() calls for the same
    # opportunity. Without this, two concurrent writers can both read the
    # same current_provenance snapshot and each commit a facts_provenance
    # JSONB that only contains their own key, silently erasing the other's
    # provenance entry (code-review finding, 2026-09) — including a client's
    # override ownership, which is exactly the guarantee this precedence
    # logic exists to protect. The row lock is held for the remainder of
    # this transaction (released on commit/rollback by the caller).
    existing = session.execute(
        text(
            "SELECT facts_revision, facts_provenance, " +
            ", ".join(str(k) for k in _ALLOWED_UPDATES) +
            " FROM fa_max_opportunity_facts"
            " WHERE opportunity_id = :oid ::uuid"
            " FOR UPDATE"
        ),
        {"oid": opportunity_id},
    ).mappings().first()

    current_revision: int = existing["facts_revision"] if existing else 0
    current_provenance: dict = existing["facts_provenance"] if existing else {}
    if isinstance(current_provenance, str):
        current_provenance = json.loads(current_provenance)

    effective: Dict[str, Any] = {}
    new_provenance = dict(current_provenance)

    now_iso = datetime.now(timezone.utc).isoformat()

    for key, new_val in updates.items():
        existing_prov = current_provenance.get(key, {})
        existing_source = existing_prov.get("source")

        # Client override protection: if a client override already owns this
        # fact and the new source is not 'client', skip.
        if existing_source == "client" and source != "client":
            logger.debug(
                "fa_max_qual.set_facts: skipping enrichment overwrite of"
                " client-owned fact=%r for opportunity=%s",
                key, opportunity_id,
            )
            continue

        # Ownership transfer: a client confirming the same value an
        # enrichment source previously wrote must still take ownership of
        # that fact, or a later enrichment write would silently overwrite
        # it again (the value-equality short-circuit below previously
        # treated this as a pure no-op and dropped the ownership change —
        # code-review finding, 2026-09).
        ownership_changes = source == "client" and existing_source != "client"

        existing_val = existing[key] if existing else None
        if _values_equal(existing_val, new_val) and not ownership_changes:
            continue  # no effective change → no revision bump for this key

        effective[key] = new_val
        new_provenance[key] = {"source": source, "set_by": set_by, "set_at": now_iso}

    if not effective:
        return current_revision

    # Build SET clause — only for columns that actually changed.
    set_parts = [f"{k} = :{k}" for k in effective]
    set_parts.append("facts_revision = facts_revision + 1")
    set_parts.append("facts_provenance = :provenance ::jsonb")
    set_parts.append("updated_at = NOW()")

    params: Dict[str, Any] = {
        "oid": opportunity_id,
        "provenance": json.dumps(new_provenance),
    }
    for key, val in effective.items():
        params[key] = val

    row = session.execute(
        text(
            "UPDATE fa_max_opportunity_facts"
            " SET " + ", ".join(set_parts) +
            " WHERE opportunity_id = :oid ::uuid"
            " RETURNING facts_revision"
        ),
        params,
    ).scalar_one()

    logger.info(
        "fa_max_qual.set_facts: opportunity=%s revision bumped to %d"
        " (changed=%r source=%s by=%s)",
        opportunity_id, row, sorted(effective.keys()), source, set_by,
    )
    return row


def _values_equal(a: Any, b: Any) -> bool:
    """True when two fact values are effectively equal (handles Decimal/int/str)."""
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    try:
        return Decimal(str(a)) == Decimal(str(b))
    except Exception:
        return str(a) == str(b)


# ---------------------------------------------------------------------------
# Sufficiency evaluator
# ---------------------------------------------------------------------------

def evaluate_sufficiency(
    *,
    session: Session,
    opportunity_id: str,
    opportunity_type: str,
    facts: Dict[str, Any],
    facts_revision: int,
) -> SufficiencyResult:
    """Pure checklist evaluation. Writes a decision row to
    fa_max_qualification_decisions and returns a SufficiencyResult.

    Does NOT itself enqueue downstream work or route EXCEPTIONS — that is the
    qualification worker's responsibility (src/agents/fa_max/qualification_worker.py).

    The caller (worker) must pass the facts_revision it loaded before evaluation
    so the decision row is keyed to the exact snapshot it evaluated.
    """
    try:
        specs = get_checklist(opportunity_type)
    except KeyError:
        logger.error(
            "fa_max_qual.evaluate_sufficiency: unknown opportunity_type=%r"
            " for opportunity=%s — no checklist defined",
            opportunity_type, opportunity_id,
        )
        # Write an insufficient decision so the state stays visible, then raise.
        gap = Gap(
            fact_key="_opportunity_type",
            display_name="Opportunity Type",
            reason=f"No checklist defined for type={opportunity_type!r}",
            gap_type="client_gap",
        )
        result = SufficiencyResult(
            verdict="insufficient",
            gaps=[gap],
            opportunity_id=opportunity_id,
            facts_revision=facts_revision,
        )
        _write_decision(session, opportunity_id, result)
        return result

    gaps: list[Gap] = []
    # Track which alternative groups have at least one satisfied member
    satisfied_groups: set[str] = set()

    for spec in specs:
        val = facts.get(spec.fact_key)
        if _fact_is_usable(spec.fact_key, val):
            if spec.alternative_group:
                satisfied_groups.add(spec.alternative_group)
            continue

        # This fact is absent — only add a gap if it isn't covered by a satisfied group
        if spec.alternative_group and spec.alternative_group in satisfied_groups:
            continue

        gaps.append(Gap(
            fact_key=spec.fact_key,
            display_name=spec.display_name,
            reason=spec.gap_reason,
            gap_type=spec.gap_type,
        ))

    # Post-pass: remove alternative-group gaps where any member is satisfied
    gaps = [
        g for g in gaps
        if not _is_alt_group_satisfied(g, specs, facts, satisfied_groups)
    ]

    if not gaps:
        verdict: Verdict = "sufficient"
    elif any(g.gap_type == "client_gap" for g in gaps):
        verdict = "insufficient"
    else:
        # All remaining gaps are pending_enrichment
        verdict = "pending_enrichment"

    # A genuine gap (client_gap or pending_enrichment) is always reported
    # as-is regardless of type — the client still needs to supply what the
    # checklist says is missing. Only a WOULD-BE-sufficient verdict is
    # downgraded here: that's the one outcome that would otherwise trigger
    # an authoritative handoff to a Scenario Builder contract Dev 4 has not
    # confirmed for this type (code-review finding, fourth round, 2026-09).
    if verdict == "sufficient" and opportunity_type in PENDING_CONTRACT_APPROVAL_TYPES:
        verdict = "sufficient_pending_contract"

    result = SufficiencyResult(
        verdict=verdict,
        gaps=gaps,
        opportunity_id=opportunity_id,
        facts_revision=facts_revision,
    )
    _write_decision(session, opportunity_id, result)
    return result


def _fact_is_usable(fact_key: str, val: Any) -> bool:
    """True when a fact value is present AND (for numeric facts) usable.

    Presence alone is not sufficiency: a $0 purchase_price/arv is not a real
    scenario input, but `0 is not None` and `str(0) != ""`, so a
    presence-only check let it through as "sufficient" (code-review finding,
    2026-09). Validity is field-specific, matching
    src/services/quote_ready/compute.py's own ground truth exactly:
    purchase-basis facts and arv must be strictly positive;
    rehab_estimate=0 is genuinely valid (a no-rehab deal) and must NOT be
    rejected — a second code-review round (2026-09) caught an earlier fix
    that applied `> 0` to rehab_estimate too, which disagreed with the
    actual Scenario Builder and rejected legitimate $0-rehab inputs.
    Non-numeric facts (exit strategy, current_use, ids) keep the original
    presence check — emptiness is the only invalid state for those.
    """
    if val is None:
        return False
    if str(val).strip() == "":
        return False
    if fact_key in POSITIVE_NUMERIC_FACT_KEYS:
        try:
            return Decimal(str(val)) > 0
        except Exception:
            return False
    if fact_key in NONNEGATIVE_NUMERIC_FACT_KEYS:
        try:
            return Decimal(str(val)) >= 0
        except Exception:
            return False
    return True


def _is_alt_group_satisfied(
    gap: Gap, specs: list[FactSpec], facts: Dict[str, Any], satisfied: set[str]
) -> bool:
    """True when this gap belongs to an alternative group that has another satisfied member."""
    if not any(s.fact_key == gap.fact_key and s.alternative_group for s in specs):
        return False
    for spec in specs:
        if spec.fact_key == gap.fact_key and spec.alternative_group:
            return spec.alternative_group in satisfied
    return False


def _write_decision(
    session: Session, opportunity_id: str, result: SufficiencyResult
) -> None:
    session.execute(
        text(
            "INSERT INTO fa_max_qualification_decisions"
            " (opportunity_id, facts_revision, checklist_version,"
            "  verdict, gaps, gap_content_hash, decided_at, decided_by)"
            " VALUES"
            " (:oid ::uuid, :facts_revision, :checklist_version,"
            "  :verdict, :gaps ::jsonb, :gap_hash, NOW(), :decided_by)"
        ),
        {
            "oid": opportunity_id,
            "facts_revision": result.facts_revision,
            "checklist_version": result.checklist_version,
            "verdict": result.verdict,
            "gaps": json.dumps([
                {"fact_key": g.fact_key, "display_name": g.display_name,
                 "reason": g.reason, "gap_type": g.gap_type}
                for g in result.gaps
            ]),
            "gap_hash": result.gap_content_hash,
            "decided_by": "agent:qualification",
        },
    )


# ---------------------------------------------------------------------------
# Work-queue producers
# ---------------------------------------------------------------------------

def enqueue_qualification_recheck(
    *,
    session: Session,
    opportunity_id: str,
    facts_revision: int,
    person_id: Optional[str] = None,
) -> Optional[str]:
    """Enqueue a qualification recheck for the given opportunity+revision.

    Called immediately after set_facts() bumps the revision. Idempotent: the
    same (opportunity_id, facts_revision, checklist_version) triple produces
    the same idempotency_key, so a duplicate enqueue is a no-op.
    """
    from src.services.state_engine import enqueue_work_item

    idempotency_key = (
        f"qual:{opportunity_id}:{facts_revision}:{CHECKLIST_VERSION}"
    )
    return enqueue_work_item(
        session=session,
        queue_name=FA_MAX_QUAL_QUEUE_NAME,
        payload={
            "opportunity_id": opportunity_id,
            "facts_revision": facts_revision,
            "checklist_version": CHECKLIST_VERSION,
        },
        idempotency_key=idempotency_key,
        person_id=person_id,
    )


def enqueue_quote_ready_work(
    *,
    session: Session,
    opportunity_id: str,
    facts_revision: int,
    person_id: Optional[str] = None,
) -> Optional[str]:
    """Enqueue Scenario Builder work for a sufficient opportunity.

    DEPENDENCY: The 'fa_max_quote_ready' queue consumer is owned by Dev 4
    (WP-8A/8B) and is NOT YET BUILT. This enqueue is durable; the work item
    will sit pending until Dev 4's consumer comes online.

    The consumer MUST re-verify (opportunity_id, facts_revision, checklist_version)
    at result-publication time, not only at claim time, to detect a correction
    that landed mid-calculation and map the in-flight result to 'superseded'
    rather than the current scenario.
    """
    from src.services.state_engine import enqueue_work_item

    idempotency_key = (
        f"quote_ready:{opportunity_id}:{facts_revision}:{CHECKLIST_VERSION}"
    )
    return enqueue_work_item(
        session=session,
        queue_name=FA_MAX_QUOTE_READY_QUEUE_NAME,
        payload={
            "opportunity_id": opportunity_id,
            "facts_revision": facts_revision,
            "checklist_version": CHECKLIST_VERSION,
        },
        idempotency_key=idempotency_key,
        person_id=person_id,
    )


# ---------------------------------------------------------------------------
# Fact resolution for the Scenario Builder (WP-8A/8B integration)
# ---------------------------------------------------------------------------

# QuoteReadyInput fields T3-7 can supply, in the order the fallback chain
# in src/services/quote_ready/compute.py._purchase_basis() itself checks
# them. This is the agreed precedence contract for the WP-8A/8B connection
# (code-review finding, eighth round, 2026-09): T3-7's client-confirmed
# fa_max_opportunity_facts value wins whenever it is non-NULL; a NULL value
# (never set, or explicitly cleared via the admin endpoint) falls through
# to the caller's own fallback source (financials / published ARV). This
# is the SAME precedence rule set_facts() already enforces for writes
# (client always wins over enrichment) — this function is the read-side of
# that same contract, so a consumer building a QuoteReadyInput never has to
# duplicate the precedence logic itself.
RESOLVABLE_QUOTE_READY_FACT_KEYS: frozenset[str] = frozenset({
    "purchase_price", "estimated_value", "assessed_value_mkt", "last_sale_price",
    "rehab_estimate", "rehab_source", "rehab_confidence",
    "arv", "arv_source", "arv_confidence",
})


def resolve_quote_ready_facts(
    *, session: Session, opportunity_id: str, fallback: Dict[str, Any],
) -> Dict[str, Any]:
    """Resolve QuoteReadyInput fields for one opportunity, giving T3-7's
    client-confirmed facts precedence over the caller's own fallback source
    (e.g. dossier.py's financials/published-ARV read) — see
    RESOLVABLE_QUOTE_READY_FACT_KEYS's docstring for the exact contract.

    LOCKS the facts row (FOR UPDATE) for the remainder of the CALLER's
    transaction — the caller is expected to persist its computed result
    (and commit, releasing this lock) before returning, so a concurrent
    set_facts() call cannot land between this resolution and that persist
    and go unnoticed (code-review finding, eighth round, 2026-09: "protect
    against publishing superseded facts"). This mirrors
    _lock_and_check_eligibility's pattern in the qualification worker.

    Returns a dict with every key in `fallback` present: T3-7's value where
    the fact row has one, the caller's own fallback value otherwise (never
    None-over-a-real-value, and never invents a value neither side has).
    Also includes 'facts_revision' — the caller may record this in its own
    persisted provenance for auditability, though the lock already
    guarantees no revision skew within a single resolve+persist call.

    No facts row (opportunity never received a T3-7 intake write) is a
    normal case, not an error — every resolved field falls back to the
    caller's own value, and facts_revision is 0.
    """
    row = session.execute(
        text(
            "SELECT " + ", ".join(sorted(RESOLVABLE_QUOTE_READY_FACT_KEYS)) +
            ", facts_revision"
            " FROM fa_max_opportunity_facts"
            " WHERE opportunity_id = :oid ::uuid"
            " FOR UPDATE"
        ),
        {"oid": opportunity_id},
    ).mappings().first()

    resolved = dict(fallback)
    facts_revision = 0
    if row is not None:
        facts_revision = row["facts_revision"] or 0
        for key in RESOLVABLE_QUOTE_READY_FACT_KEYS:
            value = row[key]
            if value is not None:
                resolved[key] = value

    resolved["facts_revision"] = facts_revision
    return resolved
