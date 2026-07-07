"""B0-01 — Founder portfolio import.

One-time admin import of the founder's personal deal history into deal_outcomes
as the highest-trust calibration layer (confidence_tier='founder_verified',
outcome_source='founder_import', subscriber_id NULL). Rows resolve to a known
parcel via the loaders' matching cascade (parcel exact, else address >=92);
unmatched rows are reported, never attached (ADR 0026). Idempotent by source_ref.

Pure helpers (parse/validate/map) are unit-tested; the orchestrator needs a DB
session and the loader matcher.
"""
from __future__ import annotations

import csv
import hashlib
import io
import logging
from typing import Optional

from sqlalchemy import text as _sa_text

from config.scoring import VERTICAL_WEIGHTS

logger = logging.getLogger(__name__)

VALID_VERTICALS = frozenset(VERTICAL_WEIGHTS)
_VALID_OUTCOMES = {"won": "closed_won", "lost": "closed_lost"}

# Only attach a founder-verified outcome to a parcel at this confidence or above
# (parcel-id exact = 100). Below this -> reported, not attached (ADR 0026).
MIN_MATCH_CONFIDENCE = 92

_COLUMNS = ("parcel_id", "address", "city", "zip", "deal_date",
            "profit_amount", "outcome", "vertical", "days_to_close", "notes")


def bucket_for_profit(amount: float) -> str:
    """Map a profit amount to the deal_size_bucket enum."""
    if amount < 10_000:
        return "5_10k"
    if amount <= 25_000:
        return "10_25k"
    return "25k_plus"


def stage_for_outcome(outcome: str) -> str:
    """won -> closed_won, lost -> closed_lost. Raises on anything else."""
    key = (outcome or "").strip().lower()
    if key not in _VALID_OUTCOMES:
        raise ValueError(f"outcome must be one of {sorted(_VALID_OUTCOMES)}; got {outcome!r}")
    return _VALID_OUTCOMES[key]


def source_ref(*, parcel_id: Optional[str], address: Optional[str],
               deal_date: str, profit_amount) -> str:
    """Deterministic idempotency key for a founder deal row (md5 hex, 32 chars)."""
    basis = f"{(parcel_id or '').strip()}|{(address or '').strip().lower()}|{deal_date}|{profit_amount}"
    return hashlib.md5(basis.encode("utf-8")).hexdigest()


def parse_rows(csv_text: str) -> tuple[list[dict], list[dict]]:
    """Parse + validate the founder CSV.

    Returns (ok_rows, errors). ok_rows carry mapped fields ready for matching +
    upsert; errors carry the 1-based row number and reason. A row needs a
    parcel_id or address (to match), a numeric profit_amount, a valid
    outcome, a valid vertical, and a deal_date.
    """
    ok: list[dict] = []
    errors: list[dict] = []
    reader = csv.DictReader(io.StringIO(csv_text))
    for i, raw in enumerate(reader, start=1):
        row = {k: (raw.get(k) or "").strip() for k in _COLUMNS}
        if not row["parcel_id"] and not row["address"]:
            errors.append({"row": i, "reason": "missing parcel_id and address"})
            continue
        try:
            amount = float(row["profit_amount"])
            if amount != amount:  # NaN
                raise ValueError("profit_amount is NaN")
        except (ValueError, TypeError):
            errors.append({"row": i, "reason": f"bad profit_amount {row['profit_amount']!r}"})
            continue
        try:
            stage = stage_for_outcome(row["outcome"])
        except ValueError as exc:
            errors.append({"row": i, "reason": str(exc)})
            continue
        vertical = row["vertical"].lower()
        if vertical not in VALID_VERTICALS:
            errors.append({"row": i, "reason": f"unknown vertical {row['vertical']!r}"})
            continue
        if not row["deal_date"]:
            errors.append({"row": i, "reason": "missing deal_date"})
            continue
        try:
            days_to_close = int(row["days_to_close"]) if row["days_to_close"] else None
        except ValueError:
            days_to_close = None
        ok.append({
            "parcel_id":       row["parcel_id"] or None,
            "address":         row["address"] or None,
            "city":            row["city"] or None,
            "zip":             row["zip"] or None,
            "deal_date":       row["deal_date"],
            "deal_amount":     amount,
            "deal_size_bucket": bucket_for_profit(amount),
            "pipeline_stage":  stage,
            "vertical":        vertical,
            "days_to_close":   days_to_close,
            "notes":           row["notes"] or None,
            "source_ref":      source_ref(
                parcel_id=row["parcel_id"], address=row["address"],
                deal_date=row["deal_date"], profit_amount=amount,
            ),
        })
    return ok, errors


def _candidate_counties(session, zip_code: Optional[str]) -> list[str]:
    """Counties to search for a row — those holding its ZIP, else all covered."""
    if zip_code:
        rows = session.execute(
            _sa_text("SELECT DISTINCT county_id FROM properties WHERE zip = :z"),
            {"z": zip_code},
        ).scalars().all()
        if rows:
            return list(rows)
    try:
        from src.utils.county_config import list_counties
        return list_counties() or ["hillsborough"]
    except Exception:
        return ["hillsborough"]


_MATCH_LOADER_CLS = None


def _matcher(session, county: str):
    """A matching-only BaseLoader (BaseLoader is abstract; we need only its
    property-matching cascade, not the load path)."""
    global _MATCH_LOADER_CLS
    if _MATCH_LOADER_CLS is None:
        from src.loaders.base import BaseLoader

        class _MatchLoader(BaseLoader):
            def load_from_dataframe(self, *a, **k):  # matching only — never loads
                raise NotImplementedError("founder-import matcher does not load")

        _MATCH_LOADER_CLS = _MatchLoader
    return _MATCH_LOADER_CLS(session, county)


def resolve_property_id(session, row: dict) -> tuple[Optional[int], Optional[int]]:
    """Resolve a founder row to a parcel via the loaders' cascade.

    Returns (property_id, confidence). property_id is None unless a match meets
    MIN_MATCH_CONFIDENCE (parcel-id exact = 100). confidence is the best score
    seen (for reporting) even when below threshold.
    """
    best_id: Optional[int] = None
    best_conf: int = -1
    for county in _candidate_counties(session, row.get("zip")):
        loader = _matcher(session, county)
        prop, _method, conf = loader.find_property_cascade(
            parcel_id=row.get("parcel_id"),
            address=row.get("address"),
            zip_code=row.get("zip"),
            city=row.get("city"),
        )
        if prop and conf is not None and conf > best_conf:
            best_id, best_conf = prop.id, conf

    if best_id is not None and best_conf >= MIN_MATCH_CONFIDENCE:
        return best_id, best_conf
    return None, (best_conf if best_id is not None else None)


_UPSERT_SQL = _sa_text("""
INSERT INTO deal_outcomes
    (subscriber_id, property_id, deal_size_bucket, deal_amount, deal_date,
     days_to_close, pipeline_stage, county_id, trade_vertical,
     confidence_tier, outcome_source, source_ref, created_at)
VALUES
    (NULL, :pid, :bucket, :amount, CAST(:ddate AS date),
     :days, :stage, (SELECT county_id FROM properties WHERE id = :pid), :vertical,
     'founder_verified', 'founder_import', :sref, NOW())
ON CONFLICT (source_ref) WHERE source_ref IS NOT NULL DO UPDATE SET
    property_id      = EXCLUDED.property_id,
    deal_size_bucket = EXCLUDED.deal_size_bucket,
    deal_amount      = EXCLUDED.deal_amount,
    deal_date        = EXCLUDED.deal_date,
    days_to_close    = EXCLUDED.days_to_close,
    pipeline_stage   = EXCLUDED.pipeline_stage,
    county_id        = EXCLUDED.county_id,
    trade_vertical   = EXCLUDED.trade_vertical
RETURNING (xmax = 0) AS inserted
""")


def import_portfolio(session, csv_text: str) -> dict:
    """Parse, match, and upsert a founder portfolio CSV. Fires no side-effects.

    Returns {imported, updated, matched, unmatched:[...], errors:[...]}.
    Unmatched rows (below MIN_MATCH_CONFIDENCE) are reported, never attached.
    """
    ok, errors = parse_rows(csv_text)
    imported = updated = 0
    unmatched: list[dict] = []

    for r in ok:
        pid, conf = resolve_property_id(session, r)
        if pid is None:
            unmatched.append({
                "identifier": r.get("parcel_id") or r.get("address"),
                "best_confidence": conf,
            })
            continue
        inserted = session.execute(_UPSERT_SQL, {
            "pid":     pid,
            "bucket":  r["deal_size_bucket"],
            "amount":  r["deal_amount"],
            "ddate":   r["deal_date"],
            "days":    r["days_to_close"],
            "stage":   r["pipeline_stage"],
            "vertical": r["vertical"],
            "sref":    r["source_ref"],
        }).scalar()
        if inserted:
            imported += 1
        else:
            updated += 1

    session.commit()
    logger.info(
        "[FounderImport] imported=%d updated=%d unmatched=%d errors=%d",
        imported, updated, len(unmatched), len(errors),
    )
    return {
        "imported":  imported,
        "updated":   updated,
        "matched":   imported + updated,
        "unmatched": unmatched,
        "errors":    errors,
    }
