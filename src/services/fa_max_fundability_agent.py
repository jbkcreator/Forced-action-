"""FA Max Fundability Agent service (WP-T3-8).

Enriches an open opportunity with property/public data before a human reviews
it — specifically the ARV fact that the Scenario Builder (WP-8A/8B) needs
but cannot receive from the client (it's pipeline-sourced, not client-
supplied). This is the production implementation of the enrichment-exhaustion
policy that T3-7's qualification worker deferred:

    "T3-8 owns the enrichment-exhaustion policy for pending enrichment gaps
    (pending_enrichment → client_gap after retries are exhausted)"
    — src/agents/fa_max/qualification_worker.py

Architecture:

  1. populate_arv_for_opportunity()  -- tries to write published WP-8B ARV
       into fa_max_opportunity_facts for ONE opportunity. Calls set_facts()
       (T3-7's single write path) with source='enrichment'. If the ARV is
       genuinely unavailable, leaves the record alone — the daily sweep
       cadence IS the retry (no separate backoff timer).

  2. populate_arv_for_property()    -- event-driven hook called after
       arv_sweep computes/persists a new ARV for a property. Finds all open
       opportunities whose subject property matches and calls
       populate_arv_for_opportunity() for each.

  3. run_fundability_sweep()        -- daily backstop. Queries opportunities
       whose latest qualification decision is pending_enrichment (ARV still
       absent), calls populate_arv_for_opportunity() for each, then checks
       exhaustion (elapsed days since the first pending_enrichment decision).
       Escalates once via the EXCEPTIONS lane after ENRICHMENT_EXHAUSTION_DAYS.

Autonomy: internal agent (enrichment). No outbound contact. No autonomy
tier gate required (SOT.md Part 2: "sourcing, enrichment, ... internal
agents run fully autonomously from day one").

Compliance boundary (SOT.md Part 1):
  - Only property/project facts are written (arv, arv_source, arv_confidence,
    assessed_value_mkt, last_sale_price, estimated_value). No borrower
    financial data (credit score, income, bank statement, tax return, SSN)
    may be written or read by this module.
  - set_facts() is the single, authoritative write path — this module uses
    nothing else to modify fa_max_opportunity_facts.

Dependency on T3-7 (WP-T3-7, PR #300):
  - fa_max_opportunity_facts table (apply_fa_max_opportunity_facts.py)
  - fa_max_qualification_decisions table (apply_fa_max_opportunity_facts.py)
  - set_facts() from src.services.fa_max_qualification
  - enqueue_qualification_recheck() from src.services.fa_max_qualification
  DEPENDENCY NOT MERGED: this module must not be called in production until
  PR #300 is merged and apply_fa_max_opportunity_facts.py has been applied.
  The cron entry in crontab.txt is therefore guarded by a pre-flight check in
  the sweep task (fa_max_fundability_sweep.py) that fails gracefully if the
  table does not exist.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.fa_max_fundability import (
    ENRICHMENT_EXHAUSTION_DAYS,
    FA_MAX_VENTURE_KEY,
    FUNDABILITY_ARV_SOURCE,
    FUNDABILITY_ELIGIBLE_OUTCOMES,
    FUNDABILITY_ELIGIBLE_STAGES,
)
from src.services.quote_ready.arv_persistence import get_published_arv
from src.services.relay.exceptions_alert_queue import enqueue_and_attempt

# T3-7 (PR #300) dependency: imported at module level for patchability in tests.
# If T3-7 tables haven't been applied yet, this import fails at process start —
# the pre-flight check in fa_max_fundability_sweep.py handles the run-time guard;
# the module-level import ensures tests can patch these names cleanly.
try:
    from src.services.fa_max_qualification import (
        enqueue_qualification_recheck,
        set_facts,
    )
except ImportError:  # T3-7 not merged — used only in the sweep task (guarded)
    enqueue_qualification_recheck = None  # type: ignore[assignment]
    set_facts = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal query helpers
# ---------------------------------------------------------------------------

_SUBJECT_PROPERTY_SQL = text(
    "SELECT op.opportunity_id ::text, opp.current_stage, opp.outcome, opp.person_id ::text"
    " FROM fa_max_opportunity_properties op"
    " JOIN fa_max_opportunities opp ON opp.opportunity_id = op.opportunity_id"
    " WHERE op.property_id = :pid AND op.role = 'subject'"
    "   AND opp.outcome IN :outcomes"
    "   AND opp.current_stage IN :stages"
)

_PENDING_ARV_OPPS_SQL = text(
    """
    WITH latest_decision AS (
        SELECT DISTINCT ON (qd.opportunity_id)
            qd.opportunity_id,
            qd.gaps
        FROM fa_max_qualification_decisions qd
        ORDER BY qd.opportunity_id, qd.decided_at DESC
    ),
    first_pending AS (
        SELECT opportunity_id, MIN(decided_at) AS first_pending_at
        FROM fa_max_qualification_decisions
        WHERE gaps @> '[{"gap_type": "pending_enrichment"}]'
        GROUP BY opportunity_id
    )
    SELECT
        ld.opportunity_id ::text,
        opp.current_stage,
        opp.outcome,
        opp.person_id ::text,
        (SELECT op.property_id
           FROM fa_max_opportunity_properties op
          WHERE op.opportunity_id = ld.opportunity_id AND op.role = 'subject'
          LIMIT 1) AS property_id,
        fp.first_pending_at
    FROM latest_decision ld
    JOIN fa_max_opportunities opp ON opp.opportunity_id = ld.opportunity_id
    JOIN first_pending fp ON fp.opportunity_id = ld.opportunity_id
    WHERE opp.outcome IN :outcomes
      AND opp.current_stage IN :stages
      AND ld.gaps @> '[{"gap_type": "pending_enrichment"}]'
    """
)

_FIRST_PENDING_AT_SQL = text(
    """
    SELECT MIN(decided_at) AS first_pending_at
    FROM fa_max_qualification_decisions
    WHERE opportunity_id = :oid ::uuid
      AND gaps @> '[{"gap_type": "pending_enrichment"}]'
    """
)


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------

@dataclass
class FundabilityResult:
    opportunity_id: str
    property_id: Optional[int]
    arv_written: bool = False
    facts_revision: Optional[int] = None
    arv_unavailable: bool = False
    escalated: bool = False
    skipped: bool = False
    skip_reason: str = ""
    error: Optional[str] = None


@dataclass
class SweepStats:
    total_candidates: int = 0
    arv_written: int = 0
    arv_unavailable: int = 0
    escalated: int = 0
    skipped: int = 0
    errors: int = 0
    results: list[FundabilityResult] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Core enrichment function
# ---------------------------------------------------------------------------

def populate_arv_for_opportunity(
    *,
    session: Session,
    opportunity_id: str,
    person_id: Optional[str] = None,
    property_id: Optional[int] = None,
) -> FundabilityResult:
    """Attempt to write a published ARV into fa_max_opportunity_facts.

    Looks up the subject property (via fa_max_opportunity_properties) if
    property_id is not supplied. Reads get_published_arv() from WP-8B.
    If available, writes it via set_facts() (T3-7's single authoritative
    write path, source='enrichment'). A no-op if the client has already
    overridden the ARV with source='client' — set_facts() enforces that
    precedence internally.

    Never writes a guessed or placeholder ARV — only a real WP-8B published
    value. If unavailable, leaves the record alone and returns arv_unavailable=True.

    Also populates enrichment-sourceable financial fallbacks (assessed_value_mkt,
    last_sale_price, estimated_value) from the financials row when present and
    not already client-overridden. These reduce the likelihood of a
    pending_enrichment gap persisting past the first nightly sweep.
    """
    result = FundabilityResult(
        opportunity_id=opportunity_id,
        property_id=property_id,
    )

    # Resolve subject property_id if not supplied.
    if property_id is None:
        property_row = session.execute(
            text(
                "SELECT op.property_id"
                " FROM fa_max_opportunity_properties op"
                " WHERE op.opportunity_id = :oid ::uuid AND op.role = 'subject'"
                " LIMIT 1"
            ),
            {"oid": opportunity_id},
        ).first()
        if property_row is None:
            result.skipped = True
            result.skip_reason = "no_subject_property"
            logger.debug(
                "fa_max.fundability: opportunity=%s has no subject property link — skipping",
                opportunity_id,
            )
            return result
        property_id = property_row[0]
        result.property_id = property_id

    # Load published ARV from WP-8B's canonical store.
    published_arv = get_published_arv(session, property_id)

    # Load financials row for supplementary enrichment-sourceable fields.
    fin_row = session.execute(
        text(
            "SELECT assessed_value_mkt, last_sale_price"
            " FROM financials WHERE property_id = :pid LIMIT 1"
        ),
        {"pid": property_id},
    ).mappings().first()

    # Build the enrichment update. Only include fields with real values.
    updates: dict = {}

    if published_arv is not None and published_arv.point is not None:
        updates["arv"] = float(published_arv.point)
        updates["arv_source"] = FUNDABILITY_ARV_SOURCE
        if published_arv.confidence is not None:
            # Map WP-8B confidence vocab to the facts field's allowed set.
            # get_published_arv() returns confidence from ARVResult — the
            # field uses the same "high"/"medium"/"low"/"unknown" vocabulary.
            confidence = published_arv.confidence.lower()
            if confidence in ("high", "medium", "low"):
                updates["arv_confidence"] = confidence

    if fin_row:
        if fin_row["assessed_value_mkt"] is not None:
            updates["assessed_value_mkt"] = float(fin_row["assessed_value_mkt"])
        if fin_row["last_sale_price"] is not None:
            updates["last_sale_price"] = float(fin_row["last_sale_price"])

    if not updates:
        result.arv_unavailable = True
        logger.debug(
            "fa_max.fundability: opportunity=%s property=%s — ARV unavailable and"
            " no financial fallbacks; will retry on next sweep",
            opportunity_id, property_id,
        )
        return result

    # Capture the pre-write revision so we can tell an effective change
    # (set_facts bumps it) from a no-op write (set_facts returns it unchanged,
    # e.g. the client already locked in the same ARV via override precedence).
    prior_revision_row = session.execute(
        text(
            "SELECT facts_revision FROM fa_max_opportunity_facts"
            " WHERE opportunity_id = :oid ::uuid"
        ),
        {"oid": opportunity_id},
    ).first()
    prior_revision = prior_revision_row[0] if prior_revision_row else 0

    # Write facts via T3-7's single authoritative path. Client-override
    # precedence is enforced inside set_facts() — we don't need to check it
    # here. If the ARV was already client-overridden, set_facts() returns the
    # current revision unchanged (no facts_revision bump, no requeue).
    new_revision = set_facts(
        session=session,
        opportunity_id=opportunity_id,
        updates=updates,
        source="enrichment",
        set_by="fa_max_fundability_agent",
    )
    session.commit()

    revision_changed = new_revision != prior_revision

    # Only re-queue if the revision actually changed (set_facts returns the
    # CURRENT revision when no effective change was made — avoid a spurious
    # recheck if the client already had the same ARV locked in).
    if "arv" in updates and revision_changed:
        result.arv_written = True
        result.facts_revision = new_revision
        enqueue_qualification_recheck(
            session=session,
            opportunity_id=opportunity_id,
            facts_revision=new_revision,
            person_id=person_id,
        )
        session.commit()
        logger.info(
            "fa_max.fundability: opportunity=%s property=%s ARV=%.0f written"
            " (revision=%s), qualification recheck enqueued",
            opportunity_id, property_id, updates["arv"], new_revision,
        )
    elif "arv" in updates:
        # ARV was available but the write was a no-op (revision unchanged) —
        # already recorded (e.g. a prior sweep wrote it, or a client override
        # already locked in the same value). Not a fresh write; don't re-queue.
        result.facts_revision = new_revision
        logger.debug(
            "fa_max.fundability: opportunity=%s property=%s — ARV already"
            " current (revision=%s unchanged), no recheck needed",
            opportunity_id, property_id, new_revision,
        )
    else:
        # Financial fallbacks written but no ARV — still incomplete for
        # the Scenario Builder's ARV requirement, but the values are cached.
        result.arv_unavailable = True
        result.facts_revision = new_revision
        logger.debug(
            "fa_max.fundability: opportunity=%s property=%s — financial fallbacks"
            " written (revision=%s) but ARV still unavailable",
            opportunity_id, property_id, new_revision,
        )

    return result


# ---------------------------------------------------------------------------
# Event-driven hook (called after arv_sweep publishes for a property)
# ---------------------------------------------------------------------------

def populate_arv_for_property(
    *,
    session: Session,
    property_id: int,
) -> list[FundabilityResult]:
    """Find open opportunities for a property and try to write ARV for each.

    Called by arv_sweep after it persists a new or updated ARV for a property.
    This event-driven path means an opportunity that was waiting on ARV can
    proceed to qualification the SAME day the ARV lands, without waiting for
    the next day's sweep.
    """
    rows = session.execute(
        _SUBJECT_PROPERTY_SQL,
        {
            "pid": property_id,
            "outcomes": tuple(FUNDABILITY_ELIGIBLE_OUTCOMES),
            "stages": tuple(FUNDABILITY_ELIGIBLE_STAGES),
        },
    ).mappings().all()

    if not rows:
        return []

    results = []
    for row in rows:
        opp_id = row["opportunity_id"]
        try:
            r = populate_arv_for_opportunity(
                session=session,
                opportunity_id=opp_id,
                person_id=row.get("person_id"),
                property_id=property_id,
            )
            results.append(r)
        except Exception:
            logger.exception(
                "fa_max.fundability: error enriching opportunity=%s for property=%s",
                opp_id, property_id,
            )
            results.append(
                FundabilityResult(
                    opportunity_id=opp_id,
                    property_id=property_id,
                    error="unexpected_error",
                )
            )
    return results


# ---------------------------------------------------------------------------
# Exhaustion check and escalation
# ---------------------------------------------------------------------------

def _check_exhaustion(
    *,
    session: Session,
    opportunity_id: str,
    property_id: Optional[int],
) -> bool:
    """Return True if this opportunity has been pending_enrichment for more
    than ENRICHMENT_EXHAUSTION_DAYS and an EXCEPTIONS escalation should fire.

    Exhaustion is measured from the earliest pending_enrichment qualification
    decision for this opportunity — regardless of facts_revision, because
    each new revision produces its own decision row and the clock started
    when the pipeline first failed to fill the ARV.
    """
    row = session.execute(
        _FIRST_PENDING_AT_SQL,
        {"oid": opportunity_id},
    ).mappings().first()

    if row is None or row["first_pending_at"] is None:
        return False

    first_pending_at: datetime = row["first_pending_at"]
    if first_pending_at.tzinfo is None:
        first_pending_at = first_pending_at.replace(tzinfo=timezone.utc)

    elapsed_days = (datetime.now(timezone.utc) - first_pending_at).days
    return elapsed_days >= ENRICHMENT_EXHAUSTION_DAYS


def _escalate_exhaustion(
    *,
    opportunity_id: str,
    property_id: Optional[int],
) -> bool:
    """Send one EXCEPTIONS escalation pointing Josh at the Override ARV
    Slack button (WP-8B). Deduplication is handled by enqueue_and_attempt()
    (DEDUP_WINDOW_HOURS = 12) so repeated sweep runs don't spam.

    Autonomy: EXCEPTIONS lane → Josh only, no borrower contact.
    """
    rule = f"fundability_exhaustion:{opportunity_id}"
    property_note = f" (property_id={property_id})" if property_id else ""
    message = (
        f"*ARV enrichment exhausted* — opportunity `{opportunity_id}`{property_note} "
        f"has been waiting on a comparable-sales ARV for >{ENRICHMENT_EXHAUSTION_DAYS} days. "
        f"The Scenario Builder cannot proceed without it.\n\n"
        f"To unblock: use the *Override ARV* button in the Quote Ready dossier (WP-8B) "
        f"to enter an ARV manually with an audit trail. The qualification agent will "
        f"re-evaluate automatically once the override is applied.\n\n"
        f"_This alert will not repeat for {12}h._"
    )
    delivered = enqueue_and_attempt(
        venture_key=FA_MAX_VENTURE_KEY,
        rule=rule,
        message=message,
    )
    logger.info(
        "fa_max.fundability: exhaustion escalation for opportunity=%s — delivered=%s",
        opportunity_id, delivered,
    )
    return delivered


# ---------------------------------------------------------------------------
# Daily backstop sweep
# ---------------------------------------------------------------------------

def run_fundability_sweep(session: Session) -> SweepStats:
    """Daily backstop: find opportunities with pending_enrichment ARV gaps,
    try to populate ARV from the WP-8B published store, and escalate
    after ENRICHMENT_EXHAUSTION_DAYS.

    Called by src/tasks/fa_max_fundability_sweep.py (cron, after arv_sweep).
    The nightly cadence IS the retry — no separate backoff loop. Each run
    either finds a published ARV and resolves the gap, or leaves the record
    alone for the next day.

    A mid-run crash leaves each failed opportunity in its prior durable state.
    The next run re-evaluates it — safe because populate_arv_for_opportunity()
    is idempotent (set_facts() only bumps revision on effective change).
    """
    stats = SweepStats()

    rows = session.execute(
        _PENDING_ARV_OPPS_SQL,
        {
            "outcomes": tuple(FUNDABILITY_ELIGIBLE_OUTCOMES),
            "stages": tuple(FUNDABILITY_ELIGIBLE_STAGES),
        },
    ).mappings().all()

    stats.total_candidates = len(rows)
    logger.info(
        "fa_max.fundability: sweep starting — %d pending-enrichment opportunities",
        stats.total_candidates,
    )

    for row in rows:
        opp_id = row["opportunity_id"]
        pid = row["property_id"]

        try:
            r = populate_arv_for_opportunity(
                session=session,
                opportunity_id=opp_id,
                person_id=row.get("person_id"),
                property_id=pid,
            )
            stats.results.append(r)

            if r.arv_written:
                stats.arv_written += 1
            elif r.skipped:
                stats.skipped += 1
            elif r.arv_unavailable:
                stats.arv_unavailable += 1
                # Check exhaustion only when ARV is still absent.
                if _check_exhaustion(
                    session=session,
                    opportunity_id=opp_id,
                    property_id=pid,
                ):
                    escalated = _escalate_exhaustion(
                        opportunity_id=opp_id,
                        property_id=pid,
                    )
                    if escalated:
                        stats.escalated += 1
                        r.escalated = True

        except Exception:
            logger.exception(
                "fa_max.fundability: sweep error on opportunity=%s",
                opp_id,
            )
            stats.errors += 1
            stats.results.append(
                FundabilityResult(
                    opportunity_id=opp_id,
                    property_id=pid,
                    error="unexpected_error",
                )
            )

    logger.info(
        "fa_max.fundability: sweep complete — candidates=%d arv_written=%d"
        " unavailable=%d escalated=%d skipped=%d errors=%d",
        stats.total_candidates,
        stats.arv_written,
        stats.arv_unavailable,
        stats.escalated,
        stats.skipped,
        stats.errors,
    )
    return stats
