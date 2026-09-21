"""WP-9 Dial List — retrieval / assembly adapter.

DB-facing layer that runs the live financing-intent detectors against FA's
existing tables, unions their hits with the `financing_intent_scores` feed,
resolves each property to a canonical buyer entity for dedup + relationship
facts, and maps the result into `DialCandidate` objects for the pure
`rank_dial_list` core. Retrieval only — all scoring/ranking lives in `rank`.

Detector coverage mirrors GRILL-DECISIONS.md: the six live triggers are built
here; the three API-gap triggers (maturities, MLS price-drops, 1031) are NOT —
no data source exists yet.

All SQL is `text()` with named bind params. Each detector is one set-returning
query; the union is enriched in a single batched query (expanding bindparam),
so there is no per-property round trip.
"""
from __future__ import annotations

import json
import logging
from datetime import date, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Set

from sqlalchemy import bindparam, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from config.settings import get_settings

from src.core.models import DialListSnapshot
from src.services.builder_patterns import (
    map_hits_to_property_signals,
    run_builder_detectors,
)
from src.services.builder_relationships import (
    load_builder_queue_state,
    surface_relationship_hits,
)
from src.services.builder_sizing import size_builder_hits
from src.services.buyer_entity_resolution import run_incremental_permits

from .config import DEFAULT_CONFIG, DialListConfig
from .models import DialCandidate, DialList
from .rank import rank_dial_list

logger = logging.getLogger(__name__)

# Detector lookback windows (days). Kept as retrieval knobs — they gate which
# records are recent enough to be worth a call, distinct from the pure core's
# scoring config.
_CASH_LOOKBACK_DAYS = 365
_PERMIT_LOOKBACK_DAYS = 540
_STALLED_MIN_AGE_DAYS = 365
_AUCTION_LOOKBACK_DAYS = 365
_PROBATE_LOOKBACK_DAYS = 365

# Permit statuses that mean the job is done — a stalled flip must NOT be in one.
# Heuristic: there is no explicit completion field (see GRILL-DECISIONS.md #3).
_PERMIT_CLOSED_STATUSES = (
    "final", "finaled", "closed", "completed", "co issued", "expired", "cancelled",
)

# DOR major use-code prefixes that are in-scope for hard-money residential
# lending (flips + new construction): vacant residential, single family,
# mobile home, condo, misc residential, multi-family under 10 units. Excludes
# 03 (10+ unit apartments) and the commercial/industrial/ag/institutional/govt
# ranges — those are not Backflip's borrower and their assessed values would
# otherwise dominate the dollar-ranking. See src/loaders/dor_use_codes.py.
_RESIDENTIAL_USE_PREFIXES = frozenset({"00", "01", "02", "04", "07", "08"})


def _is_residential(use_code: Optional[object]) -> bool:
    if use_code is None:
        return False
    code = str(use_code).strip()
    if not code.isdigit():
        return False
    return code.zfill(4)[:2] in _RESIDENTIAL_USE_PREFIXES


def _compose_address(
    street: Optional[object], city: Optional[object], zip_: Optional[object]
) -> Optional[str]:
    street_s = str(street).strip() if street else ""
    locality = " ".join(
        p for p in (str(city).strip() if city else "", str(zip_).strip() if zip_ else "") if p
    )
    parts = [p for p in (street_s, locality) if p]
    return ", ".join(parts) if parts else None


# ---------------------------------------------------------------------------
# Detectors — each returns {property_id: Optional[urgency_date]}
# ---------------------------------------------------------------------------

_FIS_SQL = text(
    """
    SELECT f.property_id AS property_id, f.intent_tier AS intent_tier
    FROM financing_intent_scores f
    JOIN (
        SELECT property_id, MAX(score_date) AS md
        FROM financing_intent_scores
        WHERE score_date <= :as_of
          AND (:county IS NULL OR county_id = :county)
        GROUP BY property_id
    ) latest
      ON latest.property_id = f.property_id AND latest.md = f.score_date
    WHERE (:county IS NULL OR f.county_id = :county)
    """
)

_CASH_SQL = text(
    """
    SELECT property_id AS property_id, MAX(record_date) AS urgency_date
    FROM deeds
    WHERE sale_price IS NOT NULL AND sale_price > 0
      AND mortgage_amount IS NULL
      AND record_date IS NOT NULL
      AND record_date <= :as_of AND record_date >= :since
      AND (:county IS NULL OR county_id = :county)
    GROUP BY property_id
    """
)

_PERMITS_NO_FIN_SQL = text(
    """
    SELECT bp.property_id AS property_id, MAX(bp.issue_date) AS urgency_date
    FROM building_permits bp
    WHERE bp.issue_date IS NOT NULL
      AND bp.issue_date <= :as_of AND bp.issue_date >= :since
      AND (:county IS NULL OR bp.county_id = :county)
      AND NOT EXISTS (
          SELECT 1 FROM deeds d
          WHERE d.property_id = bp.property_id
            AND d.mortgage_amount IS NOT NULL AND d.mortgage_amount > 0
      )
    GROUP BY bp.property_id
    """
)

_STALLED_SQL = text(
    """
    SELECT bp.property_id AS property_id, MIN(bp.issue_date) AS urgency_date
    FROM building_permits bp
    WHERE bp.issue_date IS NOT NULL AND bp.issue_date <= :stalled_before
      AND bp.is_enforcement_permit = :not_enforcement
      AND (bp.status IS NULL OR LOWER(bp.status) NOT IN :closed_statuses)
      AND (:county IS NULL OR bp.county_id = :county)
      AND NOT EXISTS (
          SELECT 1 FROM deeds d
          WHERE d.property_id = bp.property_id
            AND d.record_date IS NOT NULL AND d.record_date > bp.issue_date
            AND d.sale_price IS NOT NULL AND d.sale_price > 0
      )
    GROUP BY bp.property_id
    """
).bindparams(bindparam("closed_statuses", expanding=True))

_FORECLOSURE_AUCTION_SQL = text(
    """
    SELECT property_id AS property_id, MAX(auction_date) AS urgency_date
    FROM foreclosures
    WHERE auction_date IS NOT NULL
      AND auction_date < :as_of_next AND auction_date >= :since
      AND (:county IS NULL OR county_id = :county)
    GROUP BY property_id
    """
)

_TAX_DEED_AUCTION_SQL = text(
    """
    SELECT property_id AS property_id, MAX(auction_date) AS urgency_date
    FROM tax_deed_auctions
    WHERE property_id IS NOT NULL AND sold_to IS NOT NULL
      AND auction_date <= :as_of AND auction_date >= :since
      AND (:county IS NULL OR county_id = :county)
    GROUP BY property_id
    """
)

_PROBATE_SQL = text(
    """
    SELECT property_id AS property_id, MAX(filing_date) AS urgency_date
    FROM legal_proceedings
    WHERE record_type = 'Probate'
      AND filing_date IS NOT NULL
      AND filing_date <= :as_of AND filing_date >= :since
      AND (:county IS NULL OR county_id = :county)
    GROUP BY property_id
    """
)

_OUT_OF_STATE_SQL = text(
    """
    SELECT o.property_id AS property_id
    FROM owners o
    JOIN properties p ON p.id = o.property_id
    WHERE o.absentee_status IS NOT NULL
      AND LOWER(o.absentee_status) LIKE '%out%state%'
      AND (:county IS NULL OR p.county_id = :county)
    """
)

# Config-gated, off by default (client item 13). No true loan-maturity field
# exists; heuristic = a mortgage lien (document_type ML) whose filing_date +
# an assumed hard-money term lands inside the "approaching" window. Enable via
# config.enable_maturities_trigger once real origination/maturity data lands.
_MATURITIES_SQL = text(
    """
    SELECT property_id AS property_id, MAX(filing_date) AS urgency_date
    FROM legal_and_liens
    WHERE filing_date IS NOT NULL
      AND filing_date >= :mat_since AND filing_date <= :mat_until
      AND (UPPER(COALESCE(document_type, '')) = 'ML'
           OR UPPER(COALESCE(document_type, '')) LIKE '%MORTGAGE%')
      AND (:county IS NULL OR county_id = :county)
    GROUP BY property_id
    """
)

# Config-gated, off by default. No 1031 field exists; weak text signal only
# (grantee mentions 1031 / exchange — a handful of rows fleet-wide). Enable via
# config.enable_1031_trigger if a real exchange signal is ingested.
_EXCHANGE_1031_SQL = text(
    """
    SELECT property_id AS property_id, MAX(record_date) AS urgency_date
    FROM deeds
    WHERE record_date IS NOT NULL
      AND record_date <= :as_of AND record_date >= :since
      AND (LOWER(COALESCE(grantee, '')) LIKE '%1031%'
           OR LOWER(COALESCE(grantee, '')) LIKE '%exchange%')
      AND (:county IS NULL OR county_id = :county)
    GROUP BY property_id
    """
)

_TERMINAL_OUTCOMES_SQL = text(
    """
    SELECT opportunity_thread_id
    FROM agent_lane_opportunity_outcomes
    WHERE outcome IN ('won', 'lost')
      AND opportunity_thread_id IN :thread_ids
    """
).bindparams(bindparam("thread_ids", expanding=True))

# Scraper source_types that feed the dial-list detectors (real values from the
# scraper engines). If one is behind SLA the digest flags it stale.
_DIAL_LIST_SOURCE_TYPES = (
    "deeds", "permits", "foreclosures", "tax_deed_auction", "probate",
)

# Most-recent successful run per source; a source with no successful run at all
# is absent from the result (treated as stale by the caller).
_SOURCE_FRESHNESS_SQL = text(
    """
    SELECT source_type, county_id,
           MAX(CASE WHEN run_success THEN run_date END) AS last_success
    FROM scraper_run_stats
    WHERE source_type IN :source_types
      AND (:county IS NULL OR county_id = :county)
    GROUP BY source_type, county_id
    """
).bindparams(bindparam("source_types", expanding=True))

_NEEDS_ENRICHMENT_UPSERT_SQL = text(
    """
    INSERT INTO dial_list_needs_enrichment
        (property_id, reason, first_seen, last_seen, retry_count)
    VALUES (:pid, 'no_contact', :as_of, :as_of, 0)
    ON CONFLICT (property_id) DO UPDATE
        SET last_seen = :as_of,
            retry_count = dial_list_needs_enrichment.retry_count + 1
    """
)

_SNAPSHOT_LATEST_SQL = text(
    """
    SELECT payload
    FROM dial_list_snapshot
    WHERE ((:county IS NULL AND county_id IS NULL) OR county_id = :county)
    ORDER BY generated_for DESC, created_at DESC
    LIMIT 1
    """
)

_ENRICH_SQL = text(
    """
    SELECT
        p.id                     AS property_id,
        p.property_use_code      AS property_use_code,
        p.address                AS address,
        p.city                   AS city,
        p.zip                    AS zip,
        o.owner_name             AS owner_name,
        o.phone_1                AS phone_1,
        f.assessed_value_mkt     AS assessed_value_mkt,
        f.last_sale_price        AS last_sale_price,
        f.last_sale_date         AS last_sale_date,
        bel.buyer_entity_id      AS buyer_entity_id,
        be.canonical_name        AS canonical_name,
        be.opportunity_thread_id AS opportunity_thread_id,
        be.total_purchase_count  AS total_purchase_count
    FROM properties p
    LEFT JOIN financials f ON f.property_id = p.id
    LEFT JOIN owners o ON o.property_id = p.id
    LEFT JOIN buyer_entity_links bel
        ON bel.source_table = 'owners' AND bel.source_id = o.id
    LEFT JOIN buyer_entities be ON be.id = bel.buyer_entity_id
    WHERE p.id IN :pids
    """
).bindparams(bindparam("pids", expanding=True))


class _Acc:
    """Per-property accumulator built during detector fan-out."""

    __slots__ = (
        "triggers", "is_builder", "intent_tier", "urgency_date",
        "builder_entity_id", "builder_name",
        "builder_loan", "builder_loan_confidence",
    )

    def __init__(self) -> None:
        self.triggers: Set[str] = set()
        self.is_builder: bool = False
        self.intent_tier: Optional[str] = None
        self.urgency_date: Optional[date] = None
        self.builder_entity_id: Optional[int] = None
        self.builder_name: Optional[str] = None
        # 85% LTC construction sizing (Stage D) — overrides the generic
        # assessed-value loan fallback in the ranker for builder candidates.
        self.builder_loan: Optional[Decimal] = None
        self.builder_loan_confidence: Optional[str] = None

    def add_date(self, d: Optional[object]) -> None:
        d2 = _as_date(d)
        if d2 is None:
            return
        # keep the most recent actionable date (closest to as_of)
        if self.urgency_date is None or d2 > self.urgency_date:
            self.urgency_date = d2


def _as_date(value: Optional[object]) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, date) and not hasattr(value, "hour"):
        return value
    if hasattr(value, "date"):
        return value.date()  # datetime
    if isinstance(value, str):
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    return value if isinstance(value, date) else None


def _dec(value: Optional[object]) -> Optional[Decimal]:
    if value is None:
        return None
    try:
        d = Decimal(str(value))
    except (ValueError, ArithmeticError):
        return None
    return d if d >= 0 else None


_BATCH_ARV_SQL = text(
    """
    SELECT DISTINCT ON (property_id)
        property_id, point, arv_unknown
    FROM fa_max_arv_results
    WHERE property_id IN :pids
      AND status = 'computed'
    ORDER BY property_id, computed_at DESC
    """
)

_DIAL_LIST_MAX_LTC = Decimal("0.70")


def _batch_published_arv(
    session: Session, property_ids: list[int],
) -> dict[int, Decimal]:
    """Return {property_id: arv_point} for properties with a computed, non-unknown ARV.

    One query for the whole candidate set — no per-property round trips.
    Builder candidates don't use arv/max_ltc (they have expected_loan_override),
    but we fetch for all pids and let the caller skip builders.
    """
    if not property_ids:
        return {}
    rows = session.execute(
        _BATCH_ARV_SQL.bindparams(bindparam("pids", expanding=True)),
        {"pids": list(set(property_ids))},
    ).mappings()
    return {
        r["property_id"]: r["point"]
        for r in rows
        if not r["arv_unknown"] and r["point"] is not None
    }


def assemble_dial_candidates(
    session: Session,
    *,
    as_of: date,
    county_id: Optional[str] = None,
    config: Optional[DialListConfig] = None,
    surface_relationships: bool = False,
) -> List[DialCandidate]:
    """Run the live detectors, union + resolve, return ranking-ready candidates.

    Raises `SQLAlchemyError` (after logging) on any DB failure — a daily batch
    should fail loud, not silently emit an empty list that reads as "no calls".

    surface_relationships: when True, also posts RELATIONSHIPS Slack cards and
    writes their dedup ledger. Defaults False so a --dry-run generation produces
    NO delivery side effects (only the live delivery path opts in).
    """
    cfg = config or DEFAULT_CONFIG
    try:
        acc: Dict[int, _Acc] = {}

        def _bucket(pid: int) -> _Acc:
            a = acc.get(pid)
            if a is None:
                a = _Acc()
                acc[pid] = a
            return a

        # Stage E: resolve newly ingested permit principals, run all five
        # principal-aware builder detectors, then bridge their entity-grained
        # hits into WP-9's property-keyed accumulator.
        run_incremental_permits(session, county_id=county_id)
        builder_hits = run_builder_detectors(
            session, as_of=as_of, county_id=county_id,
        )
        # RELATIONSHIPS Slack delivery is a side effect — gated to the live path.
        if surface_relationships:
            surface_relationship_hits(session, builder_hits)

        # Operator decisions + Stage D sizing only matter when builders fired.
        dismissed_builders: Set[int] = set()
        builder_loans: Dict[int, tuple[Decimal, str]] = {}
        if builder_hits:
            dismissed_builders, _snoozed = load_builder_queue_state(session)
            # 85% LTC construction sizing per builder entity (Stage D); keep the
            # largest loan when a builder spans multiple properties.
            for sizing in size_builder_hits(session, builder_hits):
                if sizing.estimated_loan is None:
                    continue
                prev = builder_loans.get(sizing.buyer_entity_id)
                if prev is None or sizing.estimated_loan > prev[0]:
                    builder_loans[sizing.buyer_entity_id] = (
                        sizing.estimated_loan, sizing.confidence,
                    )

        for pid, signal in map_hits_to_property_signals(session, builder_hits).items():
            if signal.buyer_entity_id in dismissed_builders:
                continue
            a = _bucket(pid)
            a.triggers.add("builder")
            a.is_builder = True
            a.builder_entity_id = signal.buyer_entity_id
            a.builder_name = signal.principal_name
            a.add_date(signal.urgency_date)
            loan = builder_loans.get(signal.buyer_entity_id)
            if loan is not None:
                a.builder_loan, a.builder_loan_confidence = loan

        cash_since = as_of - timedelta(days=_CASH_LOOKBACK_DAYS)
        permit_since = as_of - timedelta(days=_PERMIT_LOOKBACK_DAYS)
        stalled_before = as_of - timedelta(days=_STALLED_MIN_AGE_DAYS)
        auction_since = as_of - timedelta(days=_AUCTION_LOOKBACK_DAYS)
        probate_since = as_of - timedelta(days=_PROBATE_LOOKBACK_DAYS)
        as_of_next = as_of + timedelta(days=1)

        # 1) financing-intent feed (union baseline)
        for row in session.execute(
            _FIS_SQL, {"as_of": as_of, "county": county_id}
        ).mappings():
            a = _bucket(row["property_id"])
            a.triggers.add("financing_intent")
            a.intent_tier = row["intent_tier"]

        # 2) cash purchases
        for row in session.execute(
            _CASH_SQL, {"as_of": as_of, "since": cash_since, "county": county_id}
        ).mappings():
            a = _bucket(row["property_id"])
            a.triggers.add("cash_purchase")
            a.add_date(row["urgency_date"])

        # 3) permits, no recorded financing
        for row in session.execute(
            _PERMITS_NO_FIN_SQL,
            {"as_of": as_of, "since": permit_since, "county": county_id},
        ).mappings():
            a = _bucket(row["property_id"])
            a.triggers.add("permits_no_financing")
            a.add_date(row["urgency_date"])

        # 4) stalled flips (heuristic — no explicit completion field)
        for row in session.execute(
            _STALLED_SQL,
            {
                "stalled_before": stalled_before,
                "not_enforcement": False,
                "closed_statuses": list(_PERMIT_CLOSED_STATUSES),
                "county": county_id,
            },
        ).mappings():
            a = _bucket(row["property_id"])
            a.triggers.add("stalled_flip")
            a.add_date(row["urgency_date"])

        # 5) auction / probate purchases (three sources → one trigger)
        for row in session.execute(
            _FORECLOSURE_AUCTION_SQL,
            {"as_of_next": as_of_next, "since": auction_since, "county": county_id},
        ).mappings():
            a = _bucket(row["property_id"])
            a.triggers.add("auction_probate")
            a.add_date(row["urgency_date"])
        for row in session.execute(
            _TAX_DEED_AUCTION_SQL,
            {"as_of": as_of, "since": auction_since, "county": county_id},
        ).mappings():
            a = _bucket(row["property_id"])
            a.triggers.add("auction_probate")
            a.add_date(row["urgency_date"])
        for row in session.execute(
            _PROBATE_SQL,
            {"as_of": as_of, "since": probate_since, "county": county_id},
        ).mappings():
            a = _bucket(row["property_id"])
            a.triggers.add("auction_probate")
            a.add_date(row["urgency_date"])

        # 7) out-of-state owners (no natural urgency date)
        for row in session.execute(
            _OUT_OF_STATE_SQL, {"county": county_id}
        ).mappings():
            _bucket(row["property_id"]).triggers.add("out_of_state")

        # 8) maturities approaching (config-gated, off by default — heuristic on
        #    mortgage-lien filing_date + assumed hard-money term; no true field).
        if cfg.enable_maturities_trigger:
            term_days = cfg.maturity_assumed_term_months * 30
            mat_since = as_of - timedelta(days=term_days)
            mat_until = as_of + timedelta(days=cfg.maturity_window_days - term_days)
            for row in session.execute(
                _MATURITIES_SQL,
                {"mat_since": mat_since, "mat_until": mat_until, "county": county_id},
            ).mappings():
                a = _bucket(row["property_id"])
                a.triggers.add("maturities")
                a.add_date(row["urgency_date"])

        # 9) 1031 exchange (config-gated, off by default — weak grantee text
        #    signal only; no dedicated field).
        if cfg.enable_1031_trigger:
            for row in session.execute(
                _EXCHANGE_1031_SQL,
                {"as_of": as_of, "since": cash_since, "county": county_id},
            ).mappings():
                a = _bucket(row["property_id"])
                a.triggers.add("exchange_1031")
                a.add_date(row["urgency_date"])

        # 10) price drops / expired investor listings (config-gated, off) — NO
        #     data source: there is no MLS/listing table. Declared so they light
        #     up when a listing feed lands; no detector query can run until then.
        if cfg.enable_price_drop_trigger or cfg.enable_expired_listing_trigger:
            logger.warning(
                "dial_list: price_drop/expired_listing enabled but no listing "
                "data source exists — no candidates produced for them."
            )

        if not acc:
            return []

        # Batch enrichment for the whole union in one round trip.
        enrich: Dict[int, Dict] = {}
        for row in session.execute(
            _ENRICH_SQL, {"pids": list(acc.keys())}
        ).mappings():
            # a property with >1 owner link could appear twice; first wins
            enrich.setdefault(row["property_id"], dict(row))

        # Batch-fetch canonical ARV (WP-8B) for all candidates — one query.
        published_arv: Dict[int, Decimal] = _batch_published_arv(
            session, list(acc.keys())
        )

        # Exclude opportunities already coded won/lost — they should not
        # resurface on the next day's list. Scope the lookup to this run's
        # candidate threads so the query never scans the full outcomes table.
        candidate_threads = {
            e["opportunity_thread_id"]
            for e in enrich.values()
            if e.get("opportunity_thread_id")
        }
        terminal_threads: Set[str] = set()
        if candidate_threads:
            terminal_threads = {
                row[0]
                for row in session.execute(
                    _TERMINAL_OUTCOMES_SQL, {"thread_ids": list(candidate_threads)}
                )
                if row[0] is not None
            }

    except SQLAlchemyError:
        logger.error(
            "dial_list retrieval failed (as_of=%s, county=%s)",
            as_of, county_id, exc_info=True,
        )
        raise

    candidates: List[DialCandidate] = []
    needs_enrichment: List[int] = []
    for pid, a in acc.items():
        e = enrich.get(pid, {})
        # Scope to residential-investor property types. Commercial / industrial
        # / apartment / ag parcels are not hard-money borrowers and their
        # assessed values would otherwise top the dollar-ranked list.
        if not _is_residential(e.get("property_use_code")):
            continue
        thread_id = e.get("opportunity_thread_id")
        if thread_id and thread_id in terminal_threads:
            continue
        # Enrichment returned no usable contact — never surface a call with a
        # guessed contact. Hold it in the needs-enrichment queue for retry on
        # the next batch (amendment failure-behavior).
        if not e.get("phone_1"):
            needs_enrichment.append(pid)
            continue
        last_sale_date = _as_date(e.get("last_sale_date"))
        address = _compose_address(e.get("address"), e.get("city"), e.get("zip"))
        last_deal_months = None
        if last_sale_date is not None:
            last_deal_months = max((as_of - last_sale_date).days // 30, 0)
        candidates.append(
            DialCandidate(
                property_id=pid,
                opportunity_id=e.get("opportunity_thread_id"),
                buyer_entity_id=a.builder_entity_id or e.get("buyer_entity_id"),
                triggers=sorted(a.triggers),
                intent_tier=a.intent_tier,
                arv=published_arv.get(pid),
                max_ltc=_DIAL_LIST_MAX_LTC if published_arv.get(pid) is not None else None,
                # Builder candidates carry Stage D's 85% LTC construction sizing;
                # the ranker uses this override ahead of the generic 70% assessed
                # fallback (a builder's loan basis is the build, not the parcel).
                expected_loan_override=a.builder_loan,
                expected_loan_override_confidence=(
                    "high" if a.builder_loan_confidence == "high" else "low"
                ) if a.builder_loan is not None else None,
                assessed_value_mkt=_dec(e.get("assessed_value_mkt")),
                last_sale_price=_dec(e.get("last_sale_price")),
                is_builder=a.is_builder,
                urgency_date=a.urgency_date,
                borrower_name=a.builder_name or e.get("canonical_name"),
                owner_name=e.get("owner_name"),
                property_address=address,
                phone=e.get("phone_1"),
                properties_owned=e.get("total_purchase_count"),
                last_deal_months_ago=last_deal_months,
            )
        )

    if needs_enrichment:
        _record_needs_enrichment(session, needs_enrichment, as_of)
        logger.info(
            "dial_list: %d candidate(s) held for enrichment (no contact) — "
            "not surfaced, will retry next batch", len(needs_enrichment),
        )
    return candidates


def _record_needs_enrichment(
    session: Session, property_ids: List[int], as_of: date
) -> None:
    """Upsert no-contact candidates into the needs-enrichment queue. Best
    effort — a bookkeeping failure must not sink the whole list."""
    try:
        for pid in property_ids:
            session.execute(
                _NEEDS_ENRICHMENT_UPSERT_SQL, {"pid": pid, "as_of": as_of}
            )
        session.commit()
    except SQLAlchemyError:
        session.rollback()
        logger.warning(
            "dial_list: needs-enrichment bookkeeping failed for %d property(ies)",
            len(property_ids), exc_info=True,
        )


def stale_dial_list_sources(
    session: Session,
    *,
    as_of: date,
    sla_days: int,
    county_id: Optional[str] = None,
) -> List[str]:
    """Return dial-list source_types whose last successful scraper run is older
    than the SLA (or that never succeeded). Never raises — staleness reporting
    must not break the digest."""
    try:
        cutoff = as_of - timedelta(days=sla_days)
        fresh: Dict[tuple[str, Optional[str]], Optional[date]] = {}
        counties: Set[Optional[str]] = set()
        for row in session.execute(
            _SOURCE_FRESHNESS_SQL,
            {"source_types": list(_DIAL_LIST_SOURCE_TYPES), "county": county_id},
        ).mappings():
            source = row["source_type"]
            row_county = row["county_id"]
            fresh[(source, row_county)] = _as_date(row["last_success"])
            counties.add(row_county)
        stale: List[str] = []
        scope_counties = {county_id} if county_id is not None else counties
        if not scope_counties:
            scope_counties = {None}
        for scope_county in scope_counties:
            for src in _DIAL_LIST_SOURCE_TYPES:
                last = fresh.get((src, scope_county))
                if last is None or last < cutoff:
                    stale.append(
                        src if county_id is not None or scope_county is None
                        else f"{src}/{scope_county}"
                    )
        return stale
    except SQLAlchemyError:
        logger.warning("dial_list: source-staleness check failed", exc_info=True)
        return []


def write_dial_list_snapshot(session: Session, dial_list: DialList,
                             *, county_id: Optional[str] = None) -> None:
    """Persist a successfully generated list so a future failed run can still
    post from cached state. Best effort — never sinks the live post."""
    try:
        session.add(
            DialListSnapshot(
                county_id=county_id,
                generated_for=dial_list.generated_for,
                payload=dial_list.model_dump(mode="json"),
            )
        )
        session.commit()
    except SQLAlchemyError:
        session.rollback()
        logger.warning("dial_list: snapshot write failed", exc_info=True)


def load_latest_dial_list_snapshot(
    session: Session, *, county_id: Optional[str] = None
) -> Optional[DialList]:
    """Load the most recent cached list for the failure-fallback path."""
    try:
        row = session.execute(
            _SNAPSHOT_LATEST_SQL, {"county": county_id}
        ).mappings().first()
    except SQLAlchemyError:
        logger.warning("dial_list: snapshot load failed", exc_info=True)
        return None
    if row is None:
        return None
    payload = row["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    dial_list = DialList.model_validate(payload)
    dial_list.from_cache = True
    return dial_list


def generate_dial_list(
    session: Session,
    *,
    as_of: date,
    county_id: Optional[str] = None,
    config: Optional[DialListConfig] = None,
    surface_relationships: bool = False,
) -> DialList:
    """Assemble candidates from the DB and rank them with the pure core.

    surface_relationships defaults False so a dry-run generation posts nothing
    and writes no RELATIONSHIPS dedup ledger; the live delivery path passes True.
    """
    cfg = config or DEFAULT_CONFIG
    candidates = assemble_dial_candidates(
        session, as_of=as_of, county_id=county_id, config=cfg,
        surface_relationships=surface_relationships,
    )
    dial_list = rank_dial_list(candidates, as_of, cfg)
    dial_list.stale_sources = stale_dial_list_sources(
        session, as_of=as_of, sla_days=get_settings().dial_list_source_sla_days,
        county_id=county_id,
    )
    return dial_list
