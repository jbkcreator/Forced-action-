"""
WP-8B comparable-sales retrieval adapter.

DB-facing layer that fetches a subject property's attributes and a superset of
candidate DOR sales (joined to their properties for sqft/geo/condition), maps
them into the pure engine's `ARVInput`, and delegates to `compute_arv`.

Retrieval only — all filtering/tiering/adjustment lives in `compute_arv`. This
module deliberately over-fetches (county + property-use + recency window) and
lets the engine apply qual-code exclusion, exact sqft tolerance, locality
tiering, and range math.
"""
from __future__ import annotations

import logging
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from .arv_compute import compute_arv
from .arv_config import ARVConfig
from .arv_models import ARVInput, ARVResult, CandidateSale, SubjectProperty

logger = logging.getLogger(__name__)

_CONDITION_TO_INT: dict[str, int] = {
    "poor": 1,
    "fair": 2,
    "average": 3,
    "good": 4,
    "excellent": 5,
}
_DEFAULT_CONDITION = 3  # Average — neutral when unknown

_SUBJECT_SQL = text(
    """
    SELECT
        p.id                     AS property_id,
        p.heated_sq_ft           AS heated_sq_ft,
        p.sq_ft                  AS sq_ft,
        p.beds                   AS beds,
        p.baths                  AS baths,
        p.property_use_code      AS property_use_code,
        p.building_condition     AS building_condition,
        p.subdivision            AS subdivision,
        p.hcpa_neighborhood_code AS hcpa_neighborhood_code,
        p.zip                    AS zip,
        p.county_id              AS county_id
    FROM properties p
    WHERE p.id = :pid
    """
)

_CANDIDATES_SQL = text(
    """
    SELECT
        d.property_id            AS property_id,
        d.sale_price             AS sale_price,
        d.sale_yr                AS sale_yr,
        d.sale_mo                AS sale_mo,
        d.qual_cd                AS qual_cd,
        p.heated_sq_ft           AS heated_sq_ft,
        p.sq_ft                  AS sq_ft,
        p.beds                   AS beds,
        p.baths                  AS baths,
        p.property_use_code      AS property_use_code,
        p.building_condition     AS building_condition,
        p.subdivision            AS subdivision,
        p.hcpa_neighborhood_code AS hcpa_neighborhood_code,
        p.zip                    AS zip,
        p.county_id              AS county_id
    FROM dor_sales d
    JOIN properties p ON d.property_id = p.id
    WHERE p.county_id = :county
      AND d.property_id != :subject_id
      AND d.sale_price IS NOT NULL
      AND d.sale_price > 0
      AND p.property_use_code = :use_code
      AND ((:as_of_yr * 12 + :as_of_mo) - (d.sale_yr * 12 + d.sale_mo))
          BETWEEN 0 AND :months
    """
)


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------

def condition_to_int(raw: Optional[str]) -> int:
    """Map an HCPA building-condition string to the engine's 1-5 scale.

    Case-insensitive; unknown or missing values fall back to Average (3).
    """
    if raw is None:
        return _DEFAULT_CONDITION
    return _CONDITION_TO_INT.get(str(raw).strip().lower(), _DEFAULT_CONDITION)


def _sqft_of(heated: Any, gross: Any) -> Optional[int]:
    """Prefer heated living area, fall back to gross. None if neither usable."""
    for candidate in (heated, gross):
        if candidate is None:
            continue
        try:
            value = int(round(float(candidate)))
        except (TypeError, ValueError):
            continue
        if value > 0:
            return value
    return None


def _to_decimal(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _to_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(round(float(value)))
    except (TypeError, ValueError):
        return None


def _row_to_candidate(row: Mapping[str, Any]) -> Optional[CandidateSale]:
    """Map a joined dor_sales+properties row to a CandidateSale, or None."""
    sale_price = _to_decimal(row.get("sale_price"))
    if sale_price is None or sale_price <= 0:
        return None
    sqft = _sqft_of(row.get("heated_sq_ft"), row.get("sq_ft"))
    if sqft is None:
        return None
    use_code = row.get("property_use_code")
    if use_code is None:
        return None
    return CandidateSale(
        property_id=int(row["property_id"]),
        sale_price=sale_price,
        sale_yr=int(row["sale_yr"]),
        sale_mo=int(row["sale_mo"]),
        qual_cd=str(row.get("qual_cd") or ""),
        sqft=sqft,
        beds=_to_int(row.get("beds")),
        baths=_to_decimal(row.get("baths")),
        property_use_code=str(use_code),
        building_condition=condition_to_int(row.get("building_condition")),
        subdivision=row.get("subdivision"),
        hcpa_neighborhood_code=row.get("hcpa_neighborhood_code"),
        zip=row.get("zip"),
        county=row.get("county_id"),
    )


def _row_to_subject(
    row: Mapping[str, Any], after_repair_condition: int
) -> Optional[SubjectProperty]:
    """Map a properties row to a SubjectProperty, or None if unusable."""
    sqft = _sqft_of(row.get("heated_sq_ft"), row.get("sq_ft"))
    if sqft is None:
        return None
    use_code = row.get("property_use_code")
    if use_code is None:
        return None
    return SubjectProperty(
        property_id=int(row["property_id"]),
        sqft=sqft,
        beds=_to_int(row.get("beds")),
        baths=_to_decimal(row.get("baths")),
        property_use_code=str(use_code),
        building_condition=condition_to_int(row.get("building_condition")),
        after_repair_condition=after_repair_condition,
        subdivision=row.get("subdivision"),
        hcpa_neighborhood_code=row.get("hcpa_neighborhood_code"),
        zip=row.get("zip"),
        county=row.get("county_id"),
    )


# ---------------------------------------------------------------------------
# DB access
# ---------------------------------------------------------------------------

def load_subject_property(
    session: Session, property_id: int, after_repair_condition: int
) -> Optional[SubjectProperty]:
    try:
        row = session.execute(_SUBJECT_SQL, {"pid": property_id}).mappings().first()
    except Exception:
        logger.warning(
            "arv_repository: subject load failed for property_id=%s", property_id,
            exc_info=True,
        )
        return None
    if row is None:
        return None
    return _row_to_subject(row, after_repair_condition)


def fetch_candidate_sales(
    session: Session,
    *,
    subject_property_id: int,
    county_id: str,
    property_use_code: str,
    as_of_yr: int,
    as_of_mo: int,
    months: int = 24,
) -> list[CandidateSale]:
    params = {
        "county": county_id,
        "subject_id": subject_property_id,
        "use_code": property_use_code,
        "as_of_yr": as_of_yr,
        "as_of_mo": as_of_mo,
        "months": months,
    }
    try:
        rows = session.execute(_CANDIDATES_SQL, params).mappings().all()
    except Exception:
        logger.warning(
            "arv_repository: candidate fetch failed for subject_property_id=%s",
            subject_property_id,
            exc_info=True,
        )
        return []
    candidates = [_row_to_candidate(r) for r in rows]
    return [c for c in candidates if c is not None]


def build_arv_input(
    session: Session,
    *,
    subject_property_id: int,
    as_of_yr: int,
    as_of_mo: int,
    after_repair_condition: int,
    config: Optional[ARVConfig] = None,
) -> Optional[ARVInput]:
    subject = load_subject_property(
        session, subject_property_id, after_repair_condition
    )
    if subject is None:
        return None
    cfg = config or ARVConfig()
    candidates = fetch_candidate_sales(
        session,
        subject_property_id=subject_property_id,
        county_id=subject.county or "",
        property_use_code=subject.property_use_code,
        as_of_yr=as_of_yr,
        as_of_mo=as_of_mo,
        months=cfg.recency_months_extended,
    )
    return ARVInput(
        subject=subject,
        candidate_sales=candidates,
        as_of_yr=as_of_yr,
        as_of_mo=as_of_mo,
        config=cfg,
    )


def compute_arv_for_property(
    session: Session,
    *,
    subject_property_id: int,
    as_of_yr: int,
    as_of_mo: int,
    after_repair_condition: int,
    config: Optional[ARVConfig] = None,
) -> ARVResult:
    inp = build_arv_input(
        session,
        subject_property_id=subject_property_id,
        as_of_yr=as_of_yr,
        as_of_mo=as_of_mo,
        after_repair_condition=after_repair_condition,
        config=config,
    )
    if inp is None:
        return ARVResult(
            arv_unknown=True,
            unknown_reason="subject_unavailable",
            after_repair_condition=after_repair_condition,
        )
    return compute_arv(inp)
