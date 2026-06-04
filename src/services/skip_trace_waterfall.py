"""
Skip trace waterfall coordinator.

Tier 1: Tracerfy  ($0.25/hit, $0.00/miss) — active when TRACERFY_API_KEY set
Tier 2: BatchData ($0.02/lookup always)    — active when BATCH_SKIP_TRACING_API_KEY set
Tier 3: PDL       ($0.28/hit, $0.00/miss)  — active when PDL_API_KEY set

Stop condition: confidence >= skip_trace_confidence_threshold (default 0.70)
Cost ceiling:   skip_trace_cost_ceiling_cents per lead (default 80 = $0.80)

Worst case: $0.25 + $0.02 + $0.28 = $0.55 — under the ceiling.

Tracerfy is Tier 1 because it charges 0 on a miss (no spend on leads it
can't find) and returns DNC/litigator flags inline on every hit, giving
compliance data before any send happens.

Per-provider hit rate and cost tracked via enrichment_usage_log table.
"""
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from config.settings import get_settings
from src.core.database import get_db_context
from src.core.models import DistressScore, EnrichedContact, Owner, Property
from src.services.enrichment_log import log_usage
from src.services.skip_trace_result import compute_confidence
from src.utils.logger import get_logger

logger = get_logger(__name__)

_PROVIDER_COST_CENTS = {"tracerfy": 2, "batchdata": 2, "pdl": 28}


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

    q = (
        session.query(Owner)
        .join(Property, Owner.property_id == Property.id)
        .join(
            DistressScore,
            and_(
                DistressScore.property_id == Property.id,
                DistressScore.lead_tier.in_(("Gold", "Platinum", "Ultra Platinum")),
            ),
        )
        .filter(
            Owner.county_id == county_id,
            no_phone,
            ~already_traced,
            Property.address.isnot(None),
            Property.address != "",
            Property.zip.isnot(None),
            Property.zip != "",
        )
        .order_by(DistressScore.score_date.desc())
    )

    if today_only:
        today = datetime.now(timezone.utc).date()
        q = q.filter(func.date(DistressScore.score_date) == today)

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


# ─── Main entry point ────────────────────────────────────────────────────────

def run_waterfall(
    county_id: str = "hillsborough",
    limit: int = 200,
    today_only: bool = True,
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
        run_tracerfy_fallback(owner_ids=all_ids, county_id=county_id)

        with get_db_context() as session:
            for owner in candidates:
                owner = session.get(Owner, owner.id)
                if not owner:
                    continue
                ec         = _read_ec(session, owner.property_id, "tracerfy")
                confidence = _confidence_from_ec(owner, ec)
                # Tracerfy charges only on hit; use list price for ceiling conservatism
                cost = _PROVIDER_COST_CENTS["tracerfy"] if (ec and ec.match_success) else 0

                log_usage(session, vendor="tracerfy", purpose="skip_trace",
                          success=bool(ec and ec.match_success),
                          cost_cents=cost, property_id=owner.property_id)

                stats.total_cost_cents += cost
                stats.per_provider["tracerfy"]["attempts"] += 1
                stats.per_provider["tracerfy"]["cost_cents"] += cost

                if ec and ec.match_success and confidence >= threshold:
                    _stamp_confidence(session, owner.property_id, "tracerfy", confidence)
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
                        stats.hits += 1
                        stats.per_provider["pdl"]["hits"] += 1
                    else:
                        stats.misses += 1

                session.commit()

    logger.info(
        "[Waterfall] DONE leads=%d hits=%d misses=%d cost_cents=%d",
        stats.total_leads, stats.hits, stats.misses, stats.total_cost_cents,
    )
    return stats
