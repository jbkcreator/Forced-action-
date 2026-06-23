"""
Skip trace cascade coordinator (v4).

Cascade order (cheapest → priciest, ADR 0016):
  Step 0: Free Sources  (voters + tax_collector — seed-only, ADR 0013)
  Tier 1: Tracerfy Standard    ($0.02/hit, $0.00/miss)
  Tier 2: Tracerfy Address-Only($0.04/hit, $0.00/miss) — misses + entity-skips
  Tier 3: BatchData            ($0.07/lookup always)
  Tier 4: IDI idiCORE          (key-gated, cost TBD when provisioned)
  Tier 5: PDL                  ($0.28/hit, $0.00/miss — ADR 0017 terminal fallback)

Stop condition: confidence >= skip_trace_confidence_threshold (default 0.70)
Cost ceiling:   skip_trace_cost_ceiling_cents per lead (default 80 = $0.80)

Worst case today (IDI unkeyed): $0.02 + $0.04 + $0.07 + $0.28 = $0.41 — under ceiling.

Triangulation runs ONCE at cascade end (not per tier) so all sources are
seen together for best cross-source corroboration (ADR 0015).

Per-provider hit rate and cost tracked via enrichment_usage_log table.
"""
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import text as sa_text

from config.settings import get_settings
from src.core.database import get_db_context
from src.core.models import DistressScore, EnrichedContact, Owner, Property
from src.services.enrichment_log import log_usage
from src.services.event_bus import emit_event, mark_processed
from src.services.prospect_service import (
    best_ec as _best_ec,
    dedupe_after_cascade,
    get_or_create_prospect as _get_or_create_prospect,
)
from src.services.skip_trace_result import compute_confidence
from src.utils.logger import get_logger

logger = get_logger(__name__)

_GOLD_PLUS_TIERS = {"Ultra Platinum", "Platinum", "Gold"}

_PROVIDER_COST_CENTS = {
    "tracerfy":          2,   # $0.02/hit, $0.00/miss
    "tracerfy_advanced": 4,   # $0.04/hit, $0.00/miss (Address-Only pass)
    "batchdata":         7,   # $0.07/lookup (always charged)
    "idi":               0,   # placeholder — update when contracted rate is known
    "pdl":               28,  # $0.28/hit, $0.00/miss
}


@dataclass
class WaterfallStats:
    total_leads:      int  = 0
    hits:             int  = 0
    misses:           int  = 0
    total_cost_cents: int  = 0
    per_provider: dict = field(default_factory=dict)


# ─── Candidate selection ─────────────────────────────────────────────────────

def _select_candidates(session, county_id: str, limit: int, today_only: bool) -> list:
    """Return Owner objects eligible for enrichment: Gold+ tier, no phone, not yet traced."""
    from sqlalchemy import and_, or_, func, exists as sa_exists

    # Exclude owners already attempted by either Tier 1 (Tracerfy) or Tier 2 (BatchData).
    already_traced = (
        sa_exists()
        .where(
            and_(
                EnrichedContact.property_id == Owner.property_id,
                EnrichedContact.source.in_(("tracerfy", "batch_skip_tracing")),
            )
        )
    )

    no_phone = or_(
        Owner.phone_1.is_(None),
        func.length(func.trim(Owner.phone_1)) == 0,
    )

    gold_plus = (
        sa_exists()
        .where(
            and_(
                DistressScore.property_id == Owner.property_id,
                DistressScore.lead_tier.in_(("Gold", "Platinum", "Ultra Platinum")),
            )
        )
    )

    q = (
        session.query(Owner)
        .join(Property, Owner.property_id == Property.id)
        .filter(
            Owner.county_id == county_id,
            no_phone,
            ~already_traced,
            gold_plus,
            Property.address.isnot(None),
            Property.address != "",
            Property.zip.isnot(None),
            Property.zip != "",
        )
        .order_by(Owner.id)
    )

    if today_only:
        today = datetime.now(timezone.utc).date()
        gold_plus_today = (
            sa_exists()
            .where(
                and_(
                    DistressScore.property_id == Owner.property_id,
                    DistressScore.lead_tier.in_(("Gold", "Platinum", "Ultra Platinum")),
                    func.date(DistressScore.score_date) == today,
                )
            )
        )
        q = q.filter(gold_plus_today)

    return q.limit(limit).all()


# ─── DB helpers ──────────────────────────────────────────────────────────────

def _read_ec(session, property_id: int, source: str) -> Optional[EnrichedContact]:
    return (
        session.query(EnrichedContact)
        .filter_by(property_id=property_id, source=source)
        .order_by(EnrichedContact.enriched_at.desc())
        .first()
    )


def _confidence_from_ec(owner: Owner, ec: Optional[EnrichedContact]) -> float:
    if not ec or not ec.match_success:
        return 0.0
    reachability = None
    if owner.phone_metadata:
        reachability = (owner.phone_metadata.get("phone_1") or {}).get("score")
    return compute_confidence(
        ec.mobile_phone, ec.landline, ec.email, ec.mailing_address, reachability,
    )


def _stamp_confidence(session, property_id: int, source: str, confidence: float) -> None:
    ec = _read_ec(session, property_id, source)
    if ec:
        ec.confidence = confidence
        session.flush()




# ─── Triangulation inline hook ───────────────────────────────────────────────

def _triangulate_owner(session, owner_id: int) -> None:
    """Recompute triangulation for a single owner after a successful skip-trace write."""
    settings = get_settings()
    if not settings.triangulation_enabled:
        return
    try:
        from src.services.contact_triangulation import TriangulationService
        TriangulationService(session).run_for_owner(owner_id)
    except Exception:
        logger.warning("[Waterfall] triangulation recompute failed for owner_id=%d", owner_id,
                       exc_info=True)


# ─── PDL persistence ─────────────────────────────────────────────────────────

def _persist_pdl(session, owner: Owner, result) -> None:
    from src.services.phone_utils import normalize as normalize_phone

    ec = _read_ec(session, owner.property_id, "pdl")
    if not ec:
        ec = EnrichedContact(
            property_id=owner.property_id,
            county_id=owner.county_id or "hillsborough",
            source="pdl",
            enriched_at=datetime.now(timezone.utc),
        )
        session.add(ec)

    ec.confidence      = result.confidence
    ec.mobile_phone    = result.mobile_phone
    ec.landline        = result.landline
    ec.email           = result.email
    ec.mailing_address = result.mailing_address
    ec.match_success   = result.success

    if not owner.phone_1:
        raw = result.mobile_phone or result.landline
        if raw:
            owner.phone_1 = normalize_phone(raw)
    if not owner.email_1 and result.email:
        owner.email_1 = result.email
    if result.success:
        owner.skip_trace_success = True
    session.flush()


# ─── Shared cascade function ─────────────────────────────────────────────────

def run_cascade(
    county_id: str = "hillsborough",
    limit: int = 200,
    owner_ids: Optional[list] = None,
    today_only: bool = True,
) -> "WaterfallStats":
    """
    Run the full enrichment cascade for a set of owners (ADR 0016).

    Cascade: Step 0 (free) → Tracerfy Standard → Tracerfy Address-Only →
             BatchData → IDI (key-gated) → PDL. Hard per-lead cost ceiling.

    owner_ids=None: selects candidates via _select_candidates (nightly batch mode).
    owner_ids=[...]: processes the given owners directly (event-driven mode).

    Triangulation runs ONCE at cascade end — not per tier (ADR 0015).
    """
    from src.services.tracerfy_fallback import run_tracerfy_fallback
    from src.services.skip_trace import run_skip_trace
    from src.services.pdl_skip_trace import run_pdl_lookup
    from src.services.idi_fallback import run_idi_fallback

    settings  = get_settings()
    ceiling   = settings.skip_trace_cost_ceiling_cents
    threshold = settings.skip_trace_confidence_threshold
    stats     = WaterfallStats()
    for p in ("tracerfy", "tracerfy_advanced", "batchdata", "idi", "pdl"):
        stats.per_provider[p] = {"attempts": 0, "hits": 0, "cost_cents": 0}

    # ── Candidate selection ───────────────────────────────────────────────────
    with get_db_context() as session:
        if owner_ids is not None:
            candidates = (
                session.query(Owner)
                .filter(Owner.id.in_(owner_ids))
                .all()
            )
        else:
            candidates = _select_candidates(session, county_id, limit, today_only)

    if not candidates:
        logger.info("[Cascade] No candidates for %s", county_id)
        return stats

    stats.total_leads = len(candidates)
    all_owner_ids     = [o.id for o in candidates]
    logger.info("[Cascade] %d candidates in %s", len(candidates), county_id)

    # ── Step 0: Free Sources ──────────────────────────────────────────────────
    # Voters are already loaded in the voters table (triangulation reads them).
    # Tax-collector mailing addresses are in enriched_contacts(source='tax_collector')
    # from prior tax uploads — nothing to fetch here.
    # These free sources feed triangulation at cascade end (ADR 0013).
    logger.debug("[Cascade] Step 0: free sources (voters + tax_collector) → triangulation seed")

    # Per-owner accounting
    spent_cents: dict[int, int] = {oid: 0 for oid in all_owner_ids}
    resolved_ids: set[int]      = set()   # confirmed contact, stop cascading
    hit_owner_ids: set[int]     = set()   # any tier hit (for end triangulation)

    # ── Stage 1: Tracerfy Standard ($0.02/hit) ────────────────────────────────
    entity_skip_ids: list[int] = []

    if settings.tracerfy_api_key:
        tracerfy_stats = run_tracerfy_fallback(
            owner_ids=all_owner_ids,
            county_id=county_id,
            trace_type="normal",
        )
        entity_skip_ids = tracerfy_stats.get("entity_skip_ids") or []

        with get_db_context() as session:
            for owner in list(candidates):
                owner = session.get(Owner, owner.id)
                if not owner:
                    continue
                ec         = _read_ec(session, owner.property_id, "tracerfy")
                confidence = _confidence_from_ec(owner, ec)
                cost = _PROVIDER_COST_CENTS["tracerfy"] if (ec and ec.match_success) else 0

                spent_cents[owner.id] += cost
                stats.total_cost_cents += cost
                stats.per_provider["tracerfy"]["attempts"] += 1
                stats.per_provider["tracerfy"]["cost_cents"] += cost

                if ec and ec.match_success and confidence >= threshold:
                    _stamp_confidence(session, owner.property_id, "tracerfy", confidence)
                    resolved_ids.add(owner.id)
                    hit_owner_ids.add(owner.id)
                    stats.hits += 1
                    stats.per_provider["tracerfy"]["hits"] += 1
            session.commit()
    else:
        logger.warning("[Cascade] TRACERFY_API_KEY not set — Stage 1 skipped")

    # ── Stage 2: Tracerfy Address-Only ($0.04/hit) ────────────────────────────
    # Targets: Standard misses + entity-skips (ADR 0016).
    # Entity-skips → INSERT new EC row; Standard misses → UPDATE existing miss row.
    with get_db_context() as session:
        miss_owner_ids: list[int] = [
            r[0] for r in session.execute(sa_text("""
                SELECT DISTINCT o.id
                FROM enriched_contacts ec
                JOIN owners o ON o.property_id = ec.property_id
                WHERE ec.source = 'tracerfy'
                  AND ec.match_success = FALSE
                  AND o.id = ANY(:oids)
            """), {"oids": all_owner_ids}).fetchall()
        ]

    ao_entity_ids = [oid for oid in entity_skip_ids if oid not in resolved_ids]
    ao_miss_ids   = [oid for oid in miss_owner_ids  if oid not in resolved_ids]
    ao_candidates = ao_entity_ids + ao_miss_ids

    if ao_candidates and settings.tracerfy_api_key:
        # Entity-skips: no prior tracerfy row → standard insert path with advanced trace
        if ao_entity_ids:
            run_tracerfy_fallback(
                owner_ids=ao_entity_ids,
                county_id=county_id,
                trace_type="advanced",
            )
        # Standard misses: update existing miss row on hit
        if ao_miss_ids:
            run_tracerfy_fallback(
                owner_ids=ao_miss_ids,
                county_id=county_id,
                trace_type="advanced",
                retrace_misses=True,
            )

        with get_db_context() as session:
            for owner_id in ao_candidates:
                owner = session.get(Owner, owner_id)
                if not owner:
                    continue
                ec         = _read_ec(session, owner.property_id, "tracerfy")
                confidence = _confidence_from_ec(owner, ec)
                # Advanced hit only if the EC row now has match_success=True
                cost = _PROVIDER_COST_CENTS["tracerfy_advanced"] if (ec and ec.match_success) else 0

                spent_cents[owner_id] = spent_cents.get(owner_id, 0) + cost
                stats.total_cost_cents += cost
                stats.per_provider["tracerfy_advanced"]["attempts"] += 1
                stats.per_provider["tracerfy_advanced"]["cost_cents"] += cost

                if ec and ec.match_success and confidence >= threshold:
                    _stamp_confidence(session, owner.property_id, "tracerfy", confidence)
                    resolved_ids.add(owner_id)
                    hit_owner_ids.add(owner_id)
                    stats.hits += 1
                    stats.per_provider["tracerfy_advanced"]["hits"] += 1
            session.commit()

    # ── Stage 3: BatchData ($0.07/lookup) ─────────────────────────────────────
    tier3_ids = [
        oid for oid in all_owner_ids
        if oid not in resolved_ids
        and spent_cents.get(oid, 0) + _PROVIDER_COST_CENTS["batchdata"] <= ceiling
    ]

    if tier3_ids and settings.batch_skip_tracing_api_key:
        run_skip_trace(owner_ids=tier3_ids, county_id=county_id, today_only=False)

        with get_db_context() as session:
            for owner_id in tier3_ids:
                owner = session.get(Owner, owner_id)
                if not owner:
                    continue
                ec         = _read_ec(session, owner.property_id, "batch_skip_tracing")
                confidence = _confidence_from_ec(owner, ec)
                cost       = _PROVIDER_COST_CENTS["batchdata"]

                spent_cents[owner_id] = spent_cents.get(owner_id, 0) + cost
                log_usage(session, vendor="batchdata", purpose="skip_trace",
                          success=bool(ec and ec.match_success),
                          cost_cents=cost, property_id=owner.property_id)

                stats.total_cost_cents += cost
                stats.per_provider["batchdata"]["attempts"] += 1
                stats.per_provider["batchdata"]["cost_cents"] += cost

                if ec and ec.match_success and confidence >= threshold:
                    _stamp_confidence(session, owner.property_id, "batch_skip_tracing", confidence)
                    resolved_ids.add(owner_id)
                    hit_owner_ids.add(owner_id)
                    stats.hits += 1
                    stats.per_provider["batchdata"]["hits"] += 1
            session.commit()
    elif tier3_ids:
        logger.warning("[Cascade] BATCH_SKIP_TRACING_API_KEY not set — Stage 3 skipped")

    # ── Stage 4: IDI idiCORE (key-gated, ADR 0017) ───────────────────────────
    # IDI cost entry in _PROVIDER_COST_CENTS is 0 until contracted rate is known.
    # The key guard in run_idi_fallback makes this a safe no-op when unkeyed.
    tier4_ids = [
        oid for oid in all_owner_ids
        if oid not in resolved_ids
        and spent_cents.get(oid, 0) + _PROVIDER_COST_CENTS["idi"] <= ceiling
    ]

    if tier4_ids:
        idi_result = run_idi_fallback(owner_ids=tier4_ids, county_id=county_id)
        if not idi_result.get("skipped"):
            with get_db_context() as session:
                for owner_id in tier4_ids:
                    owner = session.get(Owner, owner_id)
                    if not owner:
                        continue
                    ec         = _read_ec(session, owner.property_id, "idi")
                    confidence = _confidence_from_ec(owner, ec)
                    cost       = _PROVIDER_COST_CENTS["idi"]

                    spent_cents[owner_id] = spent_cents.get(owner_id, 0) + cost
                    stats.total_cost_cents += cost
                    stats.per_provider["idi"]["attempts"] += 1
                    stats.per_provider["idi"]["cost_cents"] += cost

                    if ec and ec.match_success and confidence >= threshold:
                        _stamp_confidence(session, owner.property_id, "idi", confidence)
                        resolved_ids.add(owner_id)
                        hit_owner_ids.add(owner_id)
                        stats.hits += 1
                        stats.per_provider["idi"]["hits"] += 1
                session.commit()

    # ── Stage 5: PDL (terminal fallback, ADR 0017) ───────────────────────────
    tier5_ids = [
        oid for oid in all_owner_ids
        if oid not in resolved_ids
        and spent_cents.get(oid, 0) + _PROVIDER_COST_CENTS["pdl"] <= ceiling
    ]

    if tier5_ids:
        if not settings.pdl_api_key:
            logger.warning("[Cascade] PDL_API_KEY not set — Stage 5 skipped, %d leads unresolved",
                           len(tier5_ids))
            stats.misses += len(tier5_ids)
        else:
            with get_db_context() as session:
                for owner_id in tier5_ids:
                    owner = session.get(Owner, owner_id)
                    if not owner:
                        continue
                    prop = owner.property
                    if not prop:
                        stats.misses += 1
                        continue

                    first, *rest = (owner.owner_name or "").split(" ", 1)
                    result = run_pdl_lookup(
                        first_name=first,
                        last_name=rest[0] if rest else "",
                        street=prop.address or "",
                        city=prop.city or "",
                        state=prop.state or "FL",
                        zip_code=prop.zip or "",
                    )

                    cost = result.cost_cents
                    log_usage(session, vendor="pdl", purpose="skip_trace",
                              success=result.success,
                              cost_cents=cost, property_id=owner.property_id,
                              error=result.error)

                    spent_cents[owner_id] = spent_cents.get(owner_id, 0) + cost
                    stats.total_cost_cents += cost
                    stats.per_provider["pdl"]["attempts"] += 1
                    stats.per_provider["pdl"]["cost_cents"] += cost

                    if result.success and result.confidence >= threshold:
                        _persist_pdl(session, owner, result)
                        resolved_ids.add(owner_id)
                        hit_owner_ids.add(owner_id)
                        stats.hits += 1
                        stats.per_provider["pdl"]["hits"] += 1
                    else:
                        stats.misses += 1
                        try:
                            from src.services.direct_mail import flag_direct_mail_eligible
                            flag_direct_mail_eligible(owner.property_id, session)
                        except Exception as _dm_err:
                            logger.warning(
                                "[Cascade] direct_mail flag failed for property_id=%d: %s",
                                owner.property_id, _dm_err,
                            )
                session.commit()

    # ── Triangulation — once at cascade end per hit owner (ADR 0015) ──────────
    # Called AFTER all stages so every source is visible for cross-source
    # corroboration. Closes the gap where IDI/Address-Only hits previously
    # waited for the nightly sweep to get confidence labels.
    if hit_owner_ids:
        with get_db_context() as session:
            for owner_id in hit_owner_ids:
                _triangulate_owner(session, owner_id)
            session.commit()

    stats.misses = stats.total_leads - stats.hits

    # ── M2: Prospect creation + contactability stamping + event emission ───────
    # Runs once after all cascade stages. Uses resolved_ids and spent_cents
    # which are already computed above — no extra API calls.
    with get_db_context() as session:
        property_ids: dict[int, int] = {}   # owner_id → property_id
        for owner_id in all_owner_ids:
            owner = session.get(Owner, owner_id)
            if owner:
                property_ids[owner_id] = owner.property_id

        for owner_id, property_id in property_ids.items():
            try:
                prospect_id = _get_or_create_prospect(session, property_id)

                if owner_id in resolved_ids:
                    state      = "contactable"
                    event_type = "enrichment.completed"
                    ec         = _best_ec(session, property_id)
                else:
                    state      = "exhausted"
                    event_type = "enrichment.failed"
                    ec         = None

                session.execute(sa_text("""
                    UPDATE prospects
                    SET contactability_state = :state, updated_at = NOW()
                    WHERE prospect_id = :pid
                """), {"state": state, "pid": prospect_id})

                emit_event(
                    session,
                    event_type=event_type,
                    actor="cascade",
                    source_component="skip_trace_waterfall",
                    prospect_id=prospect_id,
                    payload={
                        "property_id":        property_id,
                        "contactability_state": state,
                        "enriched_contact_id": ec.id if ec else None,
                        "source":             ec.source if ec else None,
                        "confidence":         float(ec.confidence) if ec and ec.confidence else None,
                        "total_cost_cents":   spent_cents.get(owner_id, 0),
                    },
                )
            except Exception:
                logger.warning(
                    "[Cascade] M2 prospect stamp failed for owner_id=%d property_id=%d",
                    owner_id, property_id, exc_info=True,
                )

        dedupe_after_cascade(session, list(property_ids.values()))
        session.commit()

    logger.info(
        "[Cascade] DONE leads=%d hits=%d misses=%d cost_cents=%d",
        stats.total_leads, stats.hits, stats.misses, stats.total_cost_cents,
    )
    return stats


# ─── Event-driven entry point ────────────────────────────────────────────────

_CASCADE_CONSUMER = "cascade"
_EVENT_POLL_LIMIT = 500


def consume_prospect_created(county_id: str = "hillsborough") -> WaterfallStats:
    """
    Event-driven cascade: poll unprocessed prospect.created events and enrich
    each eligible property via run_cascade().

    Eligibility guard (same as _select_candidates): owner has no phone and
    has not been traced by Tracerfy or BatchData. Already-enriched properties
    are skipped but their events are still marked processed.

    Delivery: at-least-once. Events are marked processed AFTER cascade so a
    crash mid-run allows the next invocation to re-pick them up. The cascade
    itself is idempotent (already-traced guard, ON CONFLICT DO NOTHING on
    prospect insert).
    """
    with get_db_context() as session:
        event_rows = session.execute(sa_text("""
            SELECT e.event_id,
                   (e.payload->>'property_id')::int AS property_id
            FROM events e
            LEFT JOIN processed_events pe
                ON pe.event_id = e.event_id
               AND pe.consumer  = :consumer
            WHERE e.event_type = 'prospect.created'
              AND pe.event_id IS NULL
            ORDER BY e.occurred_at
            LIMIT :lim
        """), {"consumer": _CASCADE_CONSUMER, "lim": _EVENT_POLL_LIMIT}).fetchall()

    if not event_rows:
        logger.info("[Cascade] no unprocessed prospect.created events")
        return WaterfallStats()

    property_event: dict[int, object] = {r.property_id: r.event_id for r in event_rows}
    property_ids = list(property_event.keys())

    logger.info("[Cascade] consume_prospect_created — %d events to process", len(event_rows))

    # Translate to owner_ids, applying eligibility guard
    with get_db_context() as session:
        owner_rows = session.execute(sa_text("""
            SELECT o.id AS owner_id, o.property_id
            FROM owners o
            JOIN properties pr ON pr.id = o.property_id
            WHERE o.property_id = ANY(:pids)
              AND (o.phone_1 IS NULL OR trim(o.phone_1) = '')
              AND NOT EXISTS (
                  SELECT 1 FROM enriched_contacts ec
                  WHERE ec.property_id = o.property_id
                    AND ec.source IN ('tracerfy', 'batch_skip_tracing')
              )
              AND pr.address IS NOT NULL AND pr.address != ''
              AND pr.zip    IS NOT NULL AND pr.zip    != ''
        """), {"pids": property_ids}).fetchall()

    owner_ids = [r.owner_id for r in owner_rows]

    stats = WaterfallStats()
    if owner_ids:
        stats = run_cascade(owner_ids=owner_ids)
    else:
        logger.info("[Cascade] all %d properties already enriched — marking events processed",
                    len(property_ids))

    # Mark all polled events processed regardless of eligibility
    with get_db_context() as session:
        for event_id in property_event.values():
            mark_processed(session, event_id, _CASCADE_CONSUMER)
        session.commit()

    logger.info(
        "[Cascade] consume_prospect_created done — events=%d eligible=%d hits=%d misses=%d",
        len(event_rows), len(owner_ids), stats.hits, stats.misses,
    )
    return stats


# ─── Legacy entry point ───────────────────────────────────────────────────────

def run_waterfall(
    county_id: str = "hillsborough",
    limit: int = 200,
    today_only: bool = True,
    tracerfy_only: bool = False,
    tracerfy_retrace_misses: bool = False,
    individual_only: bool = False,
    entity_only: bool = False,
) -> WaterfallStats:
    """
    Run the multi-provider skip trace waterfall for all eligible owners.

    Selects Gold+ candidates with no phone, then passes them through
    Tracerfy → BatchData → PDL, escalating only leads that remain below
    the confidence threshold after each tier.
    """
    from src.services.tracerfy_fallback import run_tracerfy_fallback
    from src.services.skip_trace import run_skip_trace
    from src.services.pdl_skip_trace import run_pdl_lookup

    settings  = get_settings()
    ceiling   = settings.skip_trace_cost_ceiling_cents
    threshold = settings.skip_trace_confidence_threshold
    stats     = WaterfallStats()
    for p in ("tracerfy", "batchdata", "pdl"):
        stats.per_provider[p] = {"attempts": 0, "hits": 0, "cost_cents": 0}

    # ── Candidate selection ───────────────────────────────────────────────
    with get_db_context() as session:
        candidates = _select_candidates(session, county_id, limit, today_only)

    if not candidates:
        logger.info("[Waterfall] No candidates for %s", county_id)
        return stats

    stats.total_leads = len(candidates)
    all_ids = [o.id for o in candidates]
    logger.info("[Waterfall] %d candidates in %s", len(candidates), county_id)

    # ── Tier 1: Tracerfy ─────────────────────────────────────────────────
    # 0 credits on miss — safe to run first. DNC flags written to
    # sms_opt_outs inline by run_tracerfy_fallback.
    tier2_ids: list[int] = []

    if settings.tracerfy_api_key:
        run_tracerfy_fallback(
            owner_ids=None if tracerfy_retrace_misses else all_ids,
            county_id=county_id,
            retrace_misses=tracerfy_retrace_misses,
            individual_only=individual_only,
            entity_only=entity_only,
            limit=limit,
        )

        # tracerfy_only: skip the per-owner EC loop entirely — no tier2 needed,
        # and stats are authoritative in enrichment_usage_logs.
        if tracerfy_only:
            return stats

        with get_db_context() as session:
            for owner in candidates:
                owner = session.get(Owner, owner.id)
                if not owner:
                    continue
                ec         = _read_ec(session, owner.property_id, "tracerfy")
                confidence = _confidence_from_ec(owner, ec)
                # Tracerfy charges only on hit; use list price for ceiling conservatism
                cost = _PROVIDER_COST_CENTS["tracerfy"] if (ec and ec.match_success) else 0

                # log_usage is handled inside run_tracerfy_fallback (with queue_id).
                stats.total_cost_cents += cost
                stats.per_provider["tracerfy"]["attempts"] += 1
                stats.per_provider["tracerfy"]["cost_cents"] += cost

                if ec and ec.match_success and confidence >= threshold:
                    _stamp_confidence(session, owner.property_id, "tracerfy", confidence)
                    _triangulate_owner(session, owner.id)
                    stats.hits += 1
                    stats.per_provider["tracerfy"]["hits"] += 1
                else:
                    # Use list price for ceiling check (conservative)
                    if _PROVIDER_COST_CENTS["tracerfy"] + _PROVIDER_COST_CENTS["batchdata"] <= ceiling:
                        tier2_ids.append(owner.id)
            session.commit()
    else:
        logger.warning("[Waterfall] TRACERFY_API_KEY not set — Tier 1 skipped")
        tier2_ids = all_ids

    # ── Tier 2: BatchData ────────────────────────────────────────────────
    tier3_ids: list[int] = []

    if tier2_ids:
        if settings.batch_skip_tracing_api_key:
            run_skip_trace(owner_ids=tier2_ids, county_id=county_id, today_only=False)

            with get_db_context() as session:
                for owner_id in tier2_ids:
                    owner = session.get(Owner, owner_id)
                    if not owner:
                        continue
                    ec         = _read_ec(session, owner.property_id, "batch_skip_tracing")
                    confidence = _confidence_from_ec(owner, ec)
                    cost       = _PROVIDER_COST_CENTS["batchdata"]  # always charged
                    spent_so_far = _PROVIDER_COST_CENTS["tracerfy"] + cost

                    log_usage(session, vendor="batchdata", purpose="skip_trace",
                              success=bool(ec and ec.match_success),
                              cost_cents=cost, property_id=owner.property_id)

                    stats.total_cost_cents += cost
                    stats.per_provider["batchdata"]["attempts"] += 1
                    stats.per_provider["batchdata"]["cost_cents"] += cost

                    if ec and ec.match_success and confidence >= threshold:
                        _stamp_confidence(session, owner.property_id, "batch_skip_tracing", confidence)
                        _triangulate_owner(session, owner.id)
                        stats.hits += 1
                        stats.per_provider["batchdata"]["hits"] += 1
                    else:
                        if spent_so_far + _PROVIDER_COST_CENTS["pdl"] <= ceiling:
                            tier3_ids.append(owner_id)
                session.commit()
        else:
            logger.warning("[Waterfall] BATCH_SKIP_TRACING_API_KEY not set — Tier 2 skipped")
            tier3_ids = tier2_ids

    # ── Tier 3: PeopleDataLabs ───────────────────────────────────────────
    if tier3_ids:
        if not settings.pdl_api_key:
            logger.warning("[Waterfall] PDL_API_KEY not set — Tier 3 skipped, %d leads unresolved", len(tier3_ids))
            stats.misses += len(tier3_ids)
        else:
            with get_db_context() as session:
                for owner_id in tier3_ids:
                    owner = session.get(Owner, owner_id)
                    if not owner:
                        continue
                    prop = owner.property
                    if not prop:
                        stats.misses += 1
                        continue

                    first, *rest = (owner.owner_name or "").split(" ", 1)

                    result = run_pdl_lookup(
                        first_name=first,
                        last_name=rest[0] if rest else "",
                        street=prop.address or "",
                        city=prop.city or "",
                        state=prop.state or "FL",
                        zip_code=prop.zip or "",
                    )

                    cost = result.cost_cents
                    log_usage(session, vendor="pdl", purpose="skip_trace",
                              success=result.success,
                              cost_cents=cost, property_id=owner.property_id,
                              error=result.error)

                    stats.total_cost_cents += cost
                    stats.per_provider["pdl"]["attempts"] += 1
                    stats.per_provider["pdl"]["cost_cents"] += cost

                    if result.success and result.confidence >= threshold:
                        _persist_pdl(session, owner, result)
                        _triangulate_owner(session, owner.id)
                        stats.hits += 1
                        stats.per_provider["pdl"]["hits"] += 1
                    else:
                        stats.misses += 1
                        # Final miss — flag for direct mail if a mailing address exists
                        try:
                            from src.services.direct_mail import flag_direct_mail_eligible
                            flag_direct_mail_eligible(owner.property_id, session)
                        except Exception as _dm_err:
                            logger.warning(
                                "[Waterfall] direct_mail flag failed for property_id=%d: %s",
                                owner.property_id, _dm_err,
                            )

                session.commit()

    logger.info(
        "[Waterfall] DONE leads=%d hits=%d misses=%d cost_cents=%d",
        stats.total_leads, stats.hits, stats.misses, stats.total_cost_cents,
    )
    return stats
