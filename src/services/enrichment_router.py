"""Task 6.2 — Algorithmic Variance Control Layer: the enrichment router.

The single entry point every paid-enrichment call site should go through
instead of calling src.services.skip_trace_waterfall.run_cascade() directly.
Gates on src.services.budget_manager.is_paid_enrichment_allowed(); on the
allowed path it calls run_cascade() with the exact same arguments the real
callers already pass (its signature/behavior is untouched), so this is a
thin wrapper, not a rewrite. On the blocked path it falls back to a free
voter-registry cross-match and never silently falls through to the paid
path afterward unless override=True was explicitly passed for that call.

Every call writes exactly one AlgorithmicVarianceLog row, regardless of
outcome, so routing decisions are always auditable.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.settings import get_settings
from src.services.budget_manager import is_paid_enrichment_allowed
from src.services.phone_utils import normalize as normalize_phone

logger = logging.getLogger(__name__)


@dataclass
class LeadRecord:
    """Minimal per-lead context available at each real call site.
    property_id is the only hard requirement — owner_id is needed for the
    free-match write path (Owner.property_id is unique, so it's always
    resolvable from property_id if not supplied directly)."""
    property_id: int
    owner_id: Optional[int] = None
    subscriber_id: Optional[int] = None
    county_id: str = "hillsborough"
    vertical: Optional[str] = None
    lead_tier: Optional[str] = None
    lead_id: Optional[int] = None


def execute_free_voter_registry_cross_match(lead_record: LeadRecord, db: Session) -> dict:
    """Single-lead free lookup against the voters table (zero cost). Writes
    owners.phone_1 + enriched_contacts(source='voters') on a clean hit,
    checked against sms_opt_outs (DNC) before any write.

    Not a refactor of enrichment_background_loop.py:_seed_voters() — that
    function is architected around a pre-fetched batch with side effects
    tied to its own claim/DNC orchestration and isn't imported anywhere
    else. This is a fresh, narrow, single-lead version instead.

    Appraiser cross-match is intentionally NOT implemented here: Property
    Appraiser data scrapes at 07:45 UTC, after enrichment runs at 07:30, so
    it structurally cannot gate same-day.

    Returns {"found": bool, "source": "voters"|None,
             "reason": "no_owner_id"|"no_voter_match"|"dnc_blocked"|None,
             "mobile_phone": str|None} — always a structured result.
    """
    if not lead_record.owner_id:
        return {"found": False, "source": None, "reason": "no_owner_id", "mobile_phone": None}

    voter_row = db.execute(text("""
        SELECT phone_1 AS voter_phone
        FROM voters
        WHERE property_id = :pid AND phone_1 IS NOT NULL AND trim(phone_1) != ''
        ORDER BY updated_at DESC
        LIMIT 1
    """), {"pid": lead_record.property_id}).fetchone()

    if not voter_row:
        return {"found": False, "source": None, "reason": "no_voter_match", "mobile_phone": None}

    normalized = normalize_phone(voter_row.voter_phone)
    if not normalized:
        return {"found": False, "source": None, "reason": "no_voter_match", "mobile_phone": None}

    dnc_row = db.execute(text("""
        SELECT 1 FROM sms_opt_outs WHERE phone = :phone LIMIT 1
    """), {"phone": normalized}).fetchone()
    if dnc_row:
        return {"found": False, "source": None, "reason": "dnc_blocked", "mobile_phone": None}

    db.execute(text("""
        UPDATE owners SET phone_1 = :phone
        WHERE id = :owner_id AND (phone_1 IS NULL OR trim(phone_1) = '')
    """), {"phone": normalized, "owner_id": lead_record.owner_id})

    existing = db.execute(text("""
        SELECT id FROM enriched_contacts WHERE property_id = :pid AND source = 'voters' LIMIT 1
    """), {"pid": lead_record.property_id}).fetchone()

    if existing:
        db.execute(text("""
            UPDATE enriched_contacts SET mobile_phone = :phone, match_success = TRUE, enriched_at = :now
            WHERE id = :ec_id
        """), {"phone": normalized, "ec_id": existing.id, "now": datetime.now(timezone.utc)})
    else:
        db.execute(text("""
            INSERT INTO enriched_contacts (property_id, county_id, source, mobile_phone, match_success, enriched_at)
            VALUES (:pid, :county_id, 'voters', :phone, TRUE, :now)
        """), {
            "pid": lead_record.property_id, "county_id": lead_record.county_id,
            "phone": normalized, "now": datetime.now(timezone.utc),
        })

    return {"found": True, "source": "voters", "reason": None, "mobile_phone": normalized}


def _log_decision(
    db: Session,
    lead_record: LeadRecord,
    *,
    detail: dict,
    provider: Optional[str],
    lookup_success: bool,
    cost_cents: int,
) -> None:
    """Write exactly one AlgorithmicVarianceLog row for this decision.
    Best-effort — a logging failure must never block the routing decision
    itself from taking effect."""
    try:
        db.execute(text("""
            INSERT INTO algorithmic_variance_log (
                lead_id, property_id, subscriber_id, county, vertical, lead_tier,
                spend_ratio, threshold, selected_path, provider,
                paid_lookup_allowed, routing_reason, lookup_success, cost_cents
            ) VALUES (
                :lead_id, :property_id, :subscriber_id, :county, :vertical, :lead_tier,
                :spend_ratio, :threshold, :selected_path, :provider,
                :paid_lookup_allowed, :routing_reason, :lookup_success, :cost_cents
            )
        """), {
            "lead_id": lead_record.lead_id,
            "property_id": lead_record.property_id,
            "subscriber_id": lead_record.subscriber_id,
            "county": lead_record.county_id,
            "vertical": lead_record.vertical,
            "lead_tier": lead_record.lead_tier,
            "spend_ratio": detail.get("ratio"),
            "threshold": detail["threshold"],
            "selected_path": detail["selected_path"],
            "provider": provider,
            "paid_lookup_allowed": detail["selected_path"] in ("paid_trace", "override_paid"),
            "routing_reason": detail["routing_reason"],
            "lookup_success": lookup_success,
            "cost_cents": cost_cents,
        })
    except Exception:
        logger.warning(
            "[EnrichmentRouter] decision log failed: property_id=%s reason=%s",
            lead_record.property_id, detail.get("routing_reason"), exc_info=True,
        )


class EnrichmentRouter:
    def fetch_contact_profile(
        self,
        lead_record: LeadRecord,
        db: Session,
        override: bool = False,
    ) -> dict:
        """Gate-then-route a single lead.

        Returns {contact_found: bool, source: 'paid'|'free'|None,
                 routing_reason: str, spend_ratio: float|None, threshold: float,
                 selected_path: str, cascade_stats: WaterfallStats|None}.

        Never silently falls through free -> paid. If the free match fails,
        returns contact_found=False unless override=True was explicitly
        passed by the caller for THIS call (override is per-call, not
        sticky) — this is the fix for the exact bug in the task's own
        reference pseudocode, which called the paid path anyway when the
        free fallback failed.
        """
        try:
            allowed, detail = is_paid_enrichment_allowed(db, override=override)
        except Exception:
            logger.error(
                "[EnrichmentRouter] budget check failed, blocking paid enrichment: property_id=%s",
                lead_record.property_id, exc_info=True,
            )
            allowed, detail = False, {
                "ratio": None, "threshold": get_settings().enrichment_spend_ratio_threshold,
                "routing_reason": "missing_telemetry_guard",
                "selected_path": "blocked", "override_applied": False,
                "spend_cents": None, "revenue_cents": None,
            }

        result = {
            "contact_found": False, "source": None, "cascade_stats": None,
            "routing_reason": detail["routing_reason"], "spend_ratio": detail.get("ratio"),
            "threshold": detail["threshold"], "selected_path": detail["selected_path"],
        }

        if allowed:
            from src.services.skip_trace_waterfall import run_cascade
            owner_ids = [lead_record.owner_id] if lead_record.owner_id else None
            stats = run_cascade(county_id=lead_record.county_id, owner_ids=owner_ids)
            result["cascade_stats"] = stats
            hits = getattr(stats, "hits", 0) or 0
            result["contact_found"] = hits > 0
            result["source"] = "paid" if hits > 0 else None
            _log_decision(
                db, lead_record, detail=detail, provider="tracerfy_waterfall",
                lookup_success=result["contact_found"], cost_cents=0,
            )
            return result

        free_result = execute_free_voter_registry_cross_match(lead_record, db)
        result["contact_found"] = free_result["found"]
        result["source"] = "free" if free_result["found"] else None
        _log_decision(
            db, lead_record, detail=detail, provider="voters" if free_result["found"] else None,
            lookup_success=free_result["found"], cost_cents=0,
        )
        return result

    def fetch_contact_profiles_batch(
        self,
        lead_records: list[LeadRecord],
        db: Session,
        override: bool = False,
    ) -> dict:
        """Batch variant for the real cron/event call sites, which process
        10-200 owner_ids in one run_cascade() call for efficiency (shared
        HTTP batching, one triangulation pass at cascade end) rather than
        one call per lead. The routing decision is computed ONCE for the
        whole batch (one SQL query, not N identical ones), but exactly one
        AlgorithmicVarianceLog row is still written per lead for full
        per-lead auditability.

        Returns {"selected_path": str, "cascade_stats": WaterfallStats|None,
                 "free_results": {property_id: dict}} — callers that need
        per-property results (e.g. to update their own status columns)
        should inspect cascade_stats / free_results, not re-derive routing.
        """
        if not lead_records:
            return {"selected_path": "blocked", "cascade_stats": None, "free_results": {}}

        try:
            allowed, detail = is_paid_enrichment_allowed(db, override=override)
        except Exception:
            logger.error(
                "[EnrichmentRouter] batch budget check failed, blocking paid enrichment",
                exc_info=True,
            )
            allowed, detail = False, {
                "ratio": None, "threshold": get_settings().enrichment_spend_ratio_threshold,
                "routing_reason": "missing_telemetry_guard",
                "selected_path": "blocked", "override_applied": False,
                "spend_cents": None, "revenue_cents": None,
            }

        if allowed:
            from src.services.skip_trace_waterfall import run_cascade
            owner_ids = [lr.owner_id for lr in lead_records if lr.owner_id]
            county_id = lead_records[0].county_id
            stats = run_cascade(county_id=county_id, owner_ids=owner_ids or None)

            # Matches the existing hit-detection filter in
            # enrichment_background_loop.py's own Step 9 exactly: paid
            # sources only, excludes superseded/stale contacts, so a
            # voters-sourced or superseded row is never miscounted as a
            # cascade hit here.
            property_ids = [lr.property_id for lr in lead_records]
            hit_property_ids: set = set()
            if property_ids:
                rows = db.execute(text("""
                    SELECT DISTINCT property_id
                    FROM enriched_contacts
                    WHERE property_id = ANY(:pids)
                      AND match_success = TRUE
                      AND superseded_at IS NULL
                      AND source IN ('tracerfy', 'batch_skip_tracing', 'idi', 'pdl')
                """), {"pids": property_ids}).fetchall()
                hit_property_ids = {r.property_id for r in rows}

            for lr in lead_records:
                lookup_success = lr.property_id in hit_property_ids
                _log_decision(
                    db, lr, detail=detail, provider="tracerfy_waterfall",
                    lookup_success=lookup_success, cost_cents=0,
                )
            return {"selected_path": detail["selected_path"], "cascade_stats": stats, "free_results": {}}

        free_results = {}
        for lr in lead_records:
            free_result = execute_free_voter_registry_cross_match(lr, db)
            free_results[lr.property_id] = free_result
            _log_decision(
                db, lr, detail=detail, provider="voters" if free_result["found"] else None,
                lookup_success=free_result["found"], cost_cents=0,
            )
        return {"selected_path": detail["selected_path"], "cascade_stats": None, "free_results": free_results}
