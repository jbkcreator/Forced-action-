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

import logging
from datetime import date, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Set

from sqlalchemy import bindparam, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

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

_NEW_CONSTRUCTION_PATTERNS = (
    "%new construction%",
    "%new single family%",
    "%new residential%",
    "%new sfr%",
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

_BUILDER_SQL = text(
    """
    SELECT bp.property_id AS property_id, MAX(bp.issue_date) AS urgency_date
    FROM building_permits bp
    WHERE bp.issue_date IS NOT NULL
      AND bp.issue_date <= :as_of AND bp.issue_date >= :since
      AND (:county IS NULL OR bp.county_id = :county)
      AND (
          LOWER(COALESCE(bp.permit_type, '')) LIKE :p0
          OR LOWER(COALESCE(bp.permit_type, '')) LIKE :p1
          OR LOWER(COALESCE(bp.permit_type, '')) LIKE :p2
          OR LOWER(COALESCE(bp.permit_type, '')) LIKE :p3
          OR LOWER(COALESCE(bp.description, '')) LIKE :p0
      )
    GROUP BY bp.property_id
    """
)

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

    __slots__ = ("triggers", "is_builder", "intent_tier", "urgency_date")

    def __init__(self) -> None:
        self.triggers: Set[str] = set()
        self.is_builder: bool = False
        self.intent_tier: Optional[str] = None
        self.urgency_date: Optional[date] = None

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


def assemble_dial_candidates(
    session: Session,
    *,
    as_of: date,
    county_id: Optional[str] = None,
) -> List[DialCandidate]:
    """Run the live detectors, union + resolve, return ranking-ready candidates.

    Raises `SQLAlchemyError` (after logging) on any DB failure — a daily batch
    should fail loud, not silently emit an empty list that reads as "no calls".
    """
    try:
        acc: Dict[int, _Acc] = {}

        def _bucket(pid: int) -> _Acc:
            a = acc.get(pid)
            if a is None:
                a = _Acc()
                acc[pid] = a
            return a

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

        # 5) builder / new-construction signal
        for row in session.execute(
            _BUILDER_SQL,
            {
                "as_of": as_of,
                "since": permit_since,
                "county": county_id,
                "p0": _NEW_CONSTRUCTION_PATTERNS[0],
                "p1": _NEW_CONSTRUCTION_PATTERNS[1],
                "p2": _NEW_CONSTRUCTION_PATTERNS[2],
                "p3": _NEW_CONSTRUCTION_PATTERNS[3],
            },
        ).mappings():
            a = _bucket(row["property_id"])
            a.triggers.add("builder")
            a.is_builder = True
            a.add_date(row["urgency_date"])

        # 6) auction / probate purchases (three sources → one trigger)
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

        if not acc:
            return []

        # Batch enrichment for the whole union in one round trip.
        enrich: Dict[int, Dict] = {}
        for row in session.execute(
            _ENRICH_SQL, {"pids": list(acc.keys())}
        ).mappings():
            # a property with >1 owner link could appear twice; first wins
            enrich.setdefault(row["property_id"], dict(row))

    except SQLAlchemyError:
        logger.error(
            "dial_list retrieval failed (as_of=%s, county=%s)",
            as_of, county_id, exc_info=True,
        )
        raise

    candidates: List[DialCandidate] = []
    for pid, a in acc.items():
        e = enrich.get(pid, {})
        # Scope to residential-investor property types. Commercial / industrial
        # / apartment / ag parcels are not hard-money borrowers and their
        # assessed values would otherwise top the dollar-ranked list.
        if not _is_residential(e.get("property_use_code")):
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
                buyer_entity_id=e.get("buyer_entity_id"),
                triggers=sorted(a.triggers),
                intent_tier=a.intent_tier,
                # arv/max_ltc come from the WP-8B published ARV once its
                # persistence lands (blocked on WP-1); until then the core
                # falls back to assessed value / last sale.
                arv=None,
                max_ltc=None,
                assessed_value_mkt=_dec(e.get("assessed_value_mkt")),
                last_sale_price=_dec(e.get("last_sale_price")),
                is_builder=a.is_builder,
                urgency_date=a.urgency_date,
                borrower_name=e.get("canonical_name"),
                owner_name=e.get("owner_name"),
                property_address=address,
                phone=e.get("phone_1"),
                properties_owned=e.get("total_purchase_count"),
                last_deal_months_ago=last_deal_months,
            )
        )
    return candidates


def generate_dial_list(
    session: Session,
    *,
    as_of: date,
    county_id: Optional[str] = None,
    config: Optional[DialListConfig] = None,
) -> DialList:
    """Assemble candidates from the DB and rank them with the pure core."""
    cfg = config or DEFAULT_CONFIG
    candidates = assemble_dial_candidates(session, as_of=as_of, county_id=county_id)
    return rank_dial_list(candidates, as_of, cfg)
