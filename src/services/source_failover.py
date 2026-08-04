"""
QUALITY-v2.2 Q4 — named-alternate source failover PLUMBING (Hunter, per
decision A2-revised).

Builds the mechanism only: the named-alternate column pair on
county_sources (added by migrations/apply_revenue_canaries_and_failover.py),
the switching logic here, and a confidence-labeling helper. Hooked into
src/tasks/heartbeat_monitor.py at the exact point an SLA breach is about to
produce a genuinely new (not cooldown-suppressed) alert — this module does
NOT rebuild SLA-miss detection, which is real and working there already.

Per decision E3-revised, every alternate starts NULL/unpopulated and the
failover path stays INERT until a real alternate is researched and named
for a given (county, signal_type) — that research is explicitly out of
scope for this build. On an SLA breach with no alternate configured, this
module logs + alerts "SLA breach, no alternate configured" rather than
silently doing nothing OR defaulting a source to itself (the original,
since-corrected E3 answer — a retry, not a fallback; see the analysis
doc's §5b for why that was rejected: it duplicates three existing
mechanisms — §1.8's retry-3-then-dead-letter, requests_get_with_retry, and
CountySource.scrape_mode's playwright_then_ai).

Vera's own separate pre-approved manual-override remediation path is
untouched by this module — that's Vera's existing job (the second sentence
of spec §9.5's failover clause), not new Q4 build.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


@dataclass
class FailoverResult:
    source_type: str
    county_id: str
    event: str          # 'switched_to_alternate' | 'no_alternate_configured' | 'switched_back_to_primary'
    detail: str


def maybe_failover(db: Session, source_type: str, county_id: str) -> Optional[FailoverResult]:
    """Called from heartbeat_monitor.py at the moment a NEW (not-cooldown-
    suppressed) stale alert is about to fire for (source_type, county_id).

    Looks up the matching county_sources row (source_type == signal_type).
    Some HEARTBEAT_SLAS entries (the Lifecycle Data Engine outcome
    connectors — *_outcomes, dor_sale_outcomes) have no county_sources row
    at all; those return None and do nothing — failover plumbing is scoped
    to actual scraper sources only.

    Returns None if there's nothing new to do (no matching row, or already
    switched for this incident).
    """
    row = db.execute(sa_text("""
        SELECT id, active_source, alternate_url, alternate_source_name
        FROM county_sources
        WHERE county_id = :county_id AND signal_type = :signal_type
    """), {"county_id": county_id, "signal_type": source_type}).fetchone()

    if row is None:
        logger.debug(
            "[SourceFailover] no county_sources row for %s/%s — not a scraper "
            "source (likely a Lifecycle Data Engine connector); skipping",
            source_type, county_id,
        )
        return None

    if row.active_source == "alternate":
        # Already switched for this incident — nothing new to do.
        return None

    if not row.alternate_url:
        _log_event(db, source_type, county_id, "no_alternate_configured",
                    "SLA breach, no alternate configured")
        logger.warning(
            "[SourceFailover] SLA breach for %s/%s — no alternate configured. "
            "Plumbing is inert until a real backup source is researched and named.",
            source_type, county_id,
        )
        return FailoverResult(source_type, county_id, "no_alternate_configured",
                               "SLA breach, no alternate configured")

    db.execute(sa_text("""
        UPDATE county_sources
        SET active_source = 'alternate', switched_to_alternate_at = NOW()
        WHERE id = :id
    """), {"id": row.id})
    detail = f"switched to alternate '{row.alternate_source_name or row.alternate_url}'"
    _log_event(db, source_type, county_id, "switched_to_alternate", detail)
    logger.warning("[SourceFailover] %s/%s %s after SLA breach", source_type, county_id, detail)
    return FailoverResult(source_type, county_id, "switched_to_alternate", detail)


def mark_recovered(db: Session, source_type: str, county_id: str) -> None:
    """Called from heartbeat_monitor.py's recovery path (a source that WAS
    stale just produced a fresh successful run). If the source was on its
    alternate, switch it back to primary — the primary recovering is
    exactly the condition under which switching back makes sense."""
    row = db.execute(sa_text("""
        SELECT id FROM county_sources
        WHERE county_id = :county_id AND signal_type = :signal_type AND active_source = 'alternate'
    """), {"county_id": county_id, "signal_type": source_type}).fetchone()
    if row is None:
        return
    db.execute(sa_text("""
        UPDATE county_sources
        SET active_source = 'primary', switched_to_alternate_at = NULL
        WHERE id = :id
    """), {"id": row.id})
    _log_event(db, source_type, county_id, "switched_back_to_primary",
                "primary source recovered — switched back")
    logger.info("[SourceFailover] %s/%s switched back to primary (recovered)", source_type, county_id)


def _log_event(db: Session, source_type: str, county_id: str, event_type: str, detail: str) -> None:
    db.execute(sa_text("""
        INSERT INTO source_failover_log (source_type, county_id, event_type, detail)
        VALUES (:source_type, :county_id, :event_type, :detail)
    """), {"source_type": source_type, "county_id": county_id, "event_type": event_type, "detail": detail})


def confidence_penalty_for(db: Session, source_type: str, county_id: str) -> int:
    """Confidence points to subtract from a record sourced from this
    (source_type, county_id) while active_source == 'alternate' (decision
    E4 — "always apply" labeling). Returns 0 when on the primary source or
    when no county_sources row exists.

    Consumers (a future Hunter confidence-gating call, once scrapers are
    wired to read active_source/alternate_url — out of scope for this
    build per E3-revised's "plumbing only, inert" scope) would do:
        confidence = base_confidence - confidence_penalty_for(db, source_type, county_id)
    """
    row = db.execute(sa_text("""
        SELECT active_source, failover_confidence_penalty FROM county_sources
        WHERE county_id = :county_id AND signal_type = :signal_type
    """), {"county_id": county_id, "signal_type": source_type}).fetchone()
    if row is None or row.active_source != "alternate":
        return 0
    return int(row.failover_confidence_penalty)


def label_backup_sourced(record: dict, source_type: str, county_id: str, penalty: int) -> dict:
    """Stamp a record dict with backup-source labeling (decision E4 —
    "always apply", never conditional). Returns a NEW dict; does not mutate
    the input. source_label / confidence_penalty_applied are the two
    fields any future consumer table would need as real columns before
    this becomes load-bearing — see this plan's Self-Review for why no
    existing ingestion table is altered by this build."""
    out = dict(record)
    out["source_label"] = f"backup:{source_type}/{county_id}"
    out["confidence_penalty_applied"] = penalty
    return out
