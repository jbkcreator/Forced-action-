"""WP-T2-8 Stage C — Builder pattern detectors.

Each detector queries UNION(building_permits, permit_staging) via the resolved
buyer_entity_id from buyer_entity_links and emits a BuilderHit for every
matched principal.

Five patterns (all thresholds config-driven, Q3 in GRILL-DECISIONS.md):
  repeat_builder  — same principal, ≥2 permits in 24 months
  concurrent_builder — same principal, ≥2 currently-active permits
  townhome_infill — permit-type clustering (multi-unit / townhome)
  land_to_permit  — deed recorded <90d before permit, same principal
  spec_cadence    — permit every 60–90 days, repeating (≥2 cycles)

Stage E wires these into dial_list/repository.py once WP-9 merges to dev.
Until then this module is purely a library: import run_builder_detectors() and
consume the returned list of BuilderHit objects.

All SQL uses sqlalchemy.text() + named bind parameters (never string concat).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
from typing import List, Literal, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Config defaults (override per Q3 GRILL-DECISIONS.md)
# ─────────────────────────────────────────────────────────────────────────────

REPEAT_BUILDER_MIN_PERMITS: int = 2          # ≥ this many permits in the window
REPEAT_BUILDER_WINDOW_DAYS: int = 730        # 24 months
CONCURRENT_BUILDER_MIN_ACTIVE: int = 2       # ≥ this many active permits at once
LAND_TO_PERMIT_DEED_WINDOW_DAYS: int = 90    # deed recorded ≤ this many days before permit
SPEC_CADENCE_MIN_CYCLES: int = 2             # ≥ this many permit cycles
SPEC_CADENCE_MIN_GAP_DAYS: int = 60          # min days between consecutive permits
SPEC_CADENCE_MAX_GAP_DAYS: int = 90          # max days between consecutive permits

# permit_type substrings that indicate multi-unit/townhome/infill construction
_TOWNHOME_PATTERNS = (
    "%townhome%",
    "%town home%",
    "%multi%family%",
    "%multifamily%",
    "%duplex%",
    "%triplex%",
    "%quadplex%",
    "%4-plex%",
    "%infill%",
    "%row home%",
    "%rowhome%",
)

# status values that count as "active" for concurrent_builder
_ACTIVE_STATUSES = frozenset({"active", "issued", "open", "in review", "under review", "approved"})

PatternType = Literal[
    "repeat_builder",
    "concurrent_builder",
    "townhome_infill",
    "land_to_permit",
    "spec_cadence",
]


@dataclass
class BuilderHit:
    """One detected builder opportunity — emitted per pattern per principal."""

    pattern: PatternType
    buyer_entity_id: int
    principal_name: str
    evidence_permit_ids: list[int] = field(default_factory=list)   # building_permits.id list
    staging_permit_ids: list[int] = field(default_factory=list)    # permit_staging.id list
    county_id: Optional[str] = None
    # Nearest permit issue_date — used as urgency_date when wired to DialCandidate
    latest_permit_date: Optional[date] = None
    # job_value sum across matched permits — input to Stage D sizing fallback
    total_job_value: Optional[Decimal] = None
    # property_id if a matched building_permits row exists (may be None for staging-only)
    property_id: Optional[int] = None


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _union_select(extra_columns: str = "", where: str = "", params: dict | None = None) -> str:
    """
    Build a SQL fragment that UNION ALL's building_permits (matched, with
    property_id) and permit_staging (unmatched). The caller supplies
    extra_columns (comma-prefixed), a WHERE clause, and bind params.
    Both sides alias their PK as permit_row_id and carry a source_flag.
    """
    return f"""
        SELECT
            bel.buyer_entity_id,
            p.id           AS permit_row_id,
            p.permit_number,
            p.permit_type,
            p.holder_name,
            p.county_id,
            p.issue_date,
            p.expire_date,
            p.completion_status,
            p.job_value,
            p.property_id  AS property_id,
            'building_permits' AS source_flag
            {extra_columns}
        FROM building_permits p
        JOIN buyer_entity_links bel
            ON bel.source_table = 'building_permits' AND bel.source_id = p.id
        {where}

        UNION ALL

        SELECT
            bel.buyer_entity_id,
            s.id           AS permit_row_id,
            s.permit_number,
            s.permit_type,
            s.holder_name,
            s.county_id,
            s.issue_date,
            s.expire_date,
            s.completion_status,
            s.job_value,
            s.matched_property_id AS property_id,
            'permit_staging' AS source_flag
            {extra_columns}
        FROM permit_staging s
        JOIN buyer_entity_links bel
            ON bel.source_table = 'permit_staging' AND bel.source_id = s.id
        {where}
    """


# ─────────────────────────────────────────────────────────────────────────────
# Detector 1 — repeat_builder
# ─────────────────────────────────────────────────────────────────────────────

_REPEAT_BUILDER_SQL = text("""
    WITH permit_union AS (
        SELECT
            bel.buyer_entity_id,
            p.id        AS permit_row_id,
            p.property_id,
            p.issue_date,
            p.job_value,
            p.county_id,
            'building_permits' AS src
        FROM building_permits p
        JOIN buyer_entity_links bel ON bel.source_table = 'building_permits' AND bel.source_id = p.id
        WHERE p.issue_date >= :since AND p.issue_date <= :as_of

        UNION ALL

        SELECT
            bel.buyer_entity_id,
            s.id,
            s.matched_property_id,
            s.issue_date,
            s.job_value,
            s.county_id,
            'permit_staging'
        FROM permit_staging s
        JOIN buyer_entity_links bel ON bel.source_table = 'permit_staging' AND bel.source_id = s.id
        WHERE s.issue_date >= :since AND s.issue_date <= :as_of
    ),
    entity_summary AS (
        SELECT
            buyer_entity_id,
            COUNT(*)                          AS permit_count,
            MAX(issue_date)                   AS latest_date,
            SUM(COALESCE(job_value, 0))       AS total_jv,
            MAX(property_id)                  AS any_property_id,
            MAX(county_id)                    AS county_id
        FROM permit_union
        GROUP BY buyer_entity_id
        HAVING COUNT(*) >= :min_permits
    )
    SELECT
        es.*,
        be.canonical_name,
        ARRAY(
            SELECT pu.permit_row_id FROM permit_union pu
            WHERE pu.buyer_entity_id = es.buyer_entity_id AND pu.src = 'building_permits'
        ) AS bp_ids,
        ARRAY(
            SELECT pu.permit_row_id FROM permit_union pu
            WHERE pu.buyer_entity_id = es.buyer_entity_id AND pu.src = 'permit_staging'
        ) AS ps_ids
    FROM entity_summary es
    JOIN buyer_entities be ON be.id = es.buyer_entity_id
""")


def detect_repeat_builders(
    session: Session,
    as_of: date | None = None,
    min_permits: int = REPEAT_BUILDER_MIN_PERMITS,
    window_days: int = REPEAT_BUILDER_WINDOW_DAYS,
    county_id: str | None = None,
) -> list[BuilderHit]:
    as_of = as_of or date.today()
    since = as_of - timedelta(days=window_days)
    rows = session.execute(_REPEAT_BUILDER_SQL, {
        "since": since, "as_of": as_of, "min_permits": min_permits,
    }).fetchall()
    hits = []
    for r in rows:
        if county_id and r.county_id != county_id:
            continue
        hits.append(BuilderHit(
            pattern="repeat_builder",
            buyer_entity_id=r.buyer_entity_id,
            principal_name=r.canonical_name,
            evidence_permit_ids=list(r.bp_ids or []),
            staging_permit_ids=list(r.ps_ids or []),
            county_id=r.county_id,
            latest_permit_date=r.latest_date,
            total_job_value=Decimal(str(r.total_jv)) if r.total_jv else None,
            property_id=r.any_property_id,
        ))
    return hits


# ─────────────────────────────────────────────────────────────────────────────
# Detector 2 — concurrent_builder
# ─────────────────────────────────────────────────────────────────────────────

_CONCURRENT_BUILDER_SQL = text("""
    WITH active_union AS (
        SELECT bel.buyer_entity_id, p.id, p.property_id, p.issue_date, p.job_value, p.county_id,
               'building_permits' AS src
        FROM building_permits p
        JOIN buyer_entity_links bel ON bel.source_table = 'building_permits' AND bel.source_id = p.id
        WHERE LOWER(COALESCE(p.completion_status, p.status, '')) = ANY(:active_statuses)

        UNION ALL

        SELECT bel.buyer_entity_id, s.id, s.matched_property_id, s.issue_date, s.job_value, s.county_id,
               'permit_staging'
        FROM permit_staging s
        JOIN buyer_entity_links bel ON bel.source_table = 'permit_staging' AND bel.source_id = s.id
        WHERE LOWER(COALESCE(s.completion_status, s.status, '')) = ANY(:active_statuses)
    ),
    summary AS (
        SELECT buyer_entity_id, COUNT(*) AS active_count,
               MAX(issue_date) AS latest_date, SUM(COALESCE(job_value, 0)) AS total_jv,
               MAX(property_id) AS any_property_id, MAX(county_id) AS county_id
        FROM active_union
        GROUP BY buyer_entity_id
        HAVING COUNT(*) >= :min_active
    )
    SELECT s.*, be.canonical_name,
        ARRAY(SELECT id FROM active_union au WHERE au.buyer_entity_id = s.buyer_entity_id AND au.src = 'building_permits') AS bp_ids,
        ARRAY(SELECT id FROM active_union au WHERE au.buyer_entity_id = s.buyer_entity_id AND au.src = 'permit_staging')   AS ps_ids
    FROM summary s
    JOIN buyer_entities be ON be.id = s.buyer_entity_id
""")


def detect_concurrent_builders(
    session: Session,
    min_active: int = CONCURRENT_BUILDER_MIN_ACTIVE,
    county_id: str | None = None,
) -> list[BuilderHit]:
    active_list = list(_ACTIVE_STATUSES)
    rows = session.execute(_CONCURRENT_BUILDER_SQL, {
        "active_statuses": active_list, "min_active": min_active,
    }).fetchall()
    hits = []
    for r in rows:
        if county_id and r.county_id != county_id:
            continue
        hits.append(BuilderHit(
            pattern="concurrent_builder",
            buyer_entity_id=r.buyer_entity_id,
            principal_name=r.canonical_name,
            evidence_permit_ids=list(r.bp_ids or []),
            staging_permit_ids=list(r.ps_ids or []),
            county_id=r.county_id,
            latest_permit_date=r.latest_date,
            total_job_value=Decimal(str(r.total_jv)) if r.total_jv else None,
            property_id=r.any_property_id,
        ))
    return hits


# ─────────────────────────────────────────────────────────────────────────────
# Detector 3 — townhome_infill
# ─────────────────────────────────────────────────────────────────────────────

def _townhome_ilike_clause(table_alias: str) -> str:
    return " OR ".join(
        f"LOWER({table_alias}.permit_type) LIKE {p!r}" for p in _TOWNHOME_PATTERNS
    )


def detect_townhome_infill(
    session: Session,
    as_of: date | None = None,
    county_id: str | None = None,
) -> list[BuilderHit]:
    as_of = as_of or date.today()
    # Build dynamic ILIKE clauses for both sides of the union
    ilike_bp = " OR ".join(f"LOWER(p.permit_type) LIKE {pat!r}" for pat in _TOWNHOME_PATTERNS)
    ilike_ps = " OR ".join(f"LOWER(s.permit_type) LIKE {pat!r}" for pat in _TOWNHOME_PATTERNS)
    county_filter_bp = "AND p.county_id = :county_id" if county_id else ""
    county_filter_ps = "AND s.county_id = :county_id" if county_id else ""

    sql = text(f"""
        WITH multi_unit AS (
            SELECT bel.buyer_entity_id, p.id AS pid, p.property_id, p.issue_date,
                   p.job_value, p.county_id, 'building_permits' AS src
            FROM building_permits p
            JOIN buyer_entity_links bel ON bel.source_table = 'building_permits' AND bel.source_id = p.id
            WHERE ({ilike_bp}) {county_filter_bp}

            UNION ALL

            SELECT bel.buyer_entity_id, s.id, s.matched_property_id, s.issue_date,
                   s.job_value, s.county_id, 'permit_staging'
            FROM permit_staging s
            JOIN buyer_entity_links bel ON bel.source_table = 'permit_staging' AND bel.source_id = s.id
            WHERE ({ilike_ps}) {county_filter_ps}
        ),
        summary AS (
            SELECT buyer_entity_id, COUNT(*) AS match_count,
                   MAX(issue_date) AS latest_date, SUM(COALESCE(job_value, 0)) AS total_jv,
                   MAX(property_id) AS any_property_id, MAX(county_id) AS county_id
            FROM multi_unit
            GROUP BY buyer_entity_id
        )
        SELECT s.*, be.canonical_name,
            ARRAY(SELECT pid FROM multi_unit m WHERE m.buyer_entity_id = s.buyer_entity_id AND m.src = 'building_permits') AS bp_ids,
            ARRAY(SELECT pid FROM multi_unit m WHERE m.buyer_entity_id = s.buyer_entity_id AND m.src = 'permit_staging')   AS ps_ids
        FROM summary s
        JOIN buyer_entities be ON be.id = s.buyer_entity_id
    """)
    params = {"county_id": county_id} if county_id else {}
    rows = session.execute(sql, params).fetchall()
    return [
        BuilderHit(
            pattern="townhome_infill",
            buyer_entity_id=r.buyer_entity_id,
            principal_name=r.canonical_name,
            evidence_permit_ids=list(r.bp_ids or []),
            staging_permit_ids=list(r.ps_ids or []),
            county_id=r.county_id,
            latest_permit_date=r.latest_date,
            total_job_value=Decimal(str(r.total_jv)) if r.total_jv else None,
            property_id=r.any_property_id,
        )
        for r in rows
    ]


# ─────────────────────────────────────────────────────────────────────────────
# Detector 4 — land_to_permit
# Principal acquires land (deed) then pulls a permit within 90 days.
# ─────────────────────────────────────────────────────────────────────────────

_LAND_TO_PERMIT_SQL = text("""
    WITH permit_union AS (
        SELECT bel.buyer_entity_id, p.id AS permit_row_id, p.property_id, p.issue_date,
               p.job_value, p.county_id, 'building_permits' AS src
        FROM building_permits p
        JOIN buyer_entity_links bel ON bel.source_table = 'building_permits' AND bel.source_id = p.id
        WHERE p.issue_date IS NOT NULL

        UNION ALL

        SELECT bel.buyer_entity_id, s.id, s.matched_property_id, s.issue_date,
               s.job_value, s.county_id, 'permit_staging'
        FROM permit_staging s
        JOIN buyer_entity_links bel ON bel.source_table = 'permit_staging' AND bel.source_id = s.id
        WHERE s.issue_date IS NOT NULL
    ),
    deed_entities AS (
        SELECT bel.buyer_entity_id, d.property_id, d.record_date
        FROM deeds d
        JOIN buyer_entity_links bel ON bel.source_table = 'deeds' AND bel.source_id = d.id
        WHERE d.record_date IS NOT NULL
    ),
    matches AS (
        SELECT DISTINCT
            pu.buyer_entity_id,
            pu.permit_row_id,
            pu.property_id,
            pu.issue_date,
            pu.job_value,
            pu.county_id,
            pu.src,
            de.record_date AS deed_date
        FROM permit_union pu
        JOIN deed_entities de
            ON de.buyer_entity_id = pu.buyer_entity_id
           AND (pu.property_id IS NULL OR de.property_id = pu.property_id)
        WHERE pu.issue_date - de.record_date BETWEEN 0 AND :deed_window_days
    )
    SELECT
        buyer_entity_id,
        MAX(issue_date)                       AS latest_date,
        SUM(COALESCE(job_value, 0))           AS total_jv,
        MAX(property_id)                      AS any_property_id,
        MAX(county_id)                        AS county_id,
        be.canonical_name,
        ARRAY_AGG(permit_row_id) FILTER (WHERE src = 'building_permits') AS bp_ids,
        ARRAY_AGG(permit_row_id) FILTER (WHERE src = 'permit_staging')   AS ps_ids
    FROM matches
    JOIN buyer_entities be ON be.id = matches.buyer_entity_id
    GROUP BY buyer_entity_id, be.canonical_name
""")


def detect_land_to_permit(
    session: Session,
    deed_window_days: int = LAND_TO_PERMIT_DEED_WINDOW_DAYS,
    county_id: str | None = None,
) -> list[BuilderHit]:
    rows = session.execute(_LAND_TO_PERMIT_SQL, {"deed_window_days": deed_window_days}).fetchall()
    hits = []
    for r in rows:
        if county_id and r.county_id != county_id:
            continue
        hits.append(BuilderHit(
            pattern="land_to_permit",
            buyer_entity_id=r.buyer_entity_id,
            principal_name=r.canonical_name,
            evidence_permit_ids=list(r.bp_ids or []),
            staging_permit_ids=list(r.ps_ids or []),
            county_id=r.county_id,
            latest_permit_date=r.latest_date,
            total_job_value=Decimal(str(r.total_jv)) if r.total_jv else None,
            property_id=r.any_property_id,
        ))
    return hits


# ─────────────────────────────────────────────────────────────────────────────
# Detector 5 — spec_cadence
# Permit every 60–90 days, repeating (≥2 consecutive gaps in that band).
# ─────────────────────────────────────────────────────────────────────────────

_SPEC_CADENCE_SQL = text("""
    WITH permit_union AS (
        SELECT bel.buyer_entity_id, p.id AS permit_row_id, p.issue_date,
               p.property_id, p.job_value, p.county_id, 'building_permits' AS src
        FROM building_permits p
        JOIN buyer_entity_links bel ON bel.source_table = 'building_permits' AND bel.source_id = p.id
        WHERE p.issue_date IS NOT NULL

        UNION ALL

        SELECT bel.buyer_entity_id, s.id, s.issue_date,
               s.matched_property_id, s.job_value, s.county_id, 'permit_staging'
        FROM permit_staging s
        JOIN buyer_entity_links bel ON bel.source_table = 'permit_staging' AND bel.source_id = s.id
        WHERE s.issue_date IS NOT NULL
    ),
    ordered AS (
        SELECT *,
               LAG(issue_date) OVER (PARTITION BY buyer_entity_id ORDER BY issue_date) AS prev_date
        FROM permit_union
    ),
    gap_check AS (
        SELECT *,
               (issue_date - prev_date) AS gap_days,
               CASE
                   WHEN prev_date IS NOT NULL
                    AND (issue_date - prev_date) BETWEEN :min_gap AND :max_gap
                   THEN 1 ELSE 0
               END AS in_cadence
        FROM ordered
    ),
    entity_cadence AS (
        SELECT buyer_entity_id,
               SUM(in_cadence)         AS cadence_count,
               MAX(issue_date)         AS latest_date,
               SUM(COALESCE(job_value, 0)) AS total_jv,
               MAX(property_id)        AS any_property_id,
               MAX(county_id)          AS county_id
        FROM gap_check
        GROUP BY buyer_entity_id
        HAVING SUM(in_cadence) >= :min_cycles
    )
    SELECT ec.*, be.canonical_name,
        ARRAY(SELECT gc.permit_row_id FROM gap_check gc
              WHERE gc.buyer_entity_id = ec.buyer_entity_id AND gc.src = 'building_permits') AS bp_ids,
        ARRAY(SELECT gc.permit_row_id FROM gap_check gc
              WHERE gc.buyer_entity_id = ec.buyer_entity_id AND gc.src = 'permit_staging')   AS ps_ids
    FROM entity_cadence ec
    JOIN buyer_entities be ON be.id = ec.buyer_entity_id
""")


def detect_spec_cadence(
    session: Session,
    min_gap_days: int = SPEC_CADENCE_MIN_GAP_DAYS,
    max_gap_days: int = SPEC_CADENCE_MAX_GAP_DAYS,
    min_cycles: int = SPEC_CADENCE_MIN_CYCLES,
    county_id: str | None = None,
) -> list[BuilderHit]:
    rows = session.execute(_SPEC_CADENCE_SQL, {
        "min_gap": min_gap_days, "max_gap": max_gap_days, "min_cycles": min_cycles,
    }).fetchall()
    hits = []
    for r in rows:
        if county_id and r.county_id != county_id:
            continue
        hits.append(BuilderHit(
            pattern="spec_cadence",
            buyer_entity_id=r.buyer_entity_id,
            principal_name=r.canonical_name,
            evidence_permit_ids=list(r.bp_ids or []),
            staging_permit_ids=list(r.ps_ids or []),
            county_id=r.county_id,
            latest_permit_date=r.latest_date,
            total_job_value=Decimal(str(r.total_jv)) if r.total_jv else None,
            property_id=r.any_property_id,
        ))
    return hits


# ─────────────────────────────────────────────────────────────────────────────
# Entry point — run all 5 detectors
# ─────────────────────────────────────────────────────────────────────────────

def run_builder_detectors(
    session: Session,
    as_of: date | None = None,
    county_id: str | None = None,
) -> List[BuilderHit]:
    """
    Run all 5 pattern detectors. Deduplication (same entity, multiple patterns)
    is intentional — each pattern is a separate signal; Stage E maps them to
    DialCandidate.triggers which is a list.
    """
    as_of = as_of or date.today()
    hits: List[BuilderHit] = []
    hits += detect_repeat_builders(session, as_of=as_of, county_id=county_id)
    hits += detect_concurrent_builders(session, county_id=county_id)
    hits += detect_townhome_infill(session, as_of=as_of, county_id=county_id)
    hits += detect_land_to_permit(session, county_id=county_id)
    hits += detect_spec_cadence(session, county_id=county_id)
    logger.info(
        "run_builder_detectors: %d hits across 5 patterns (county=%s)",
        len(hits), county_id or "all",
    )
    return hits
