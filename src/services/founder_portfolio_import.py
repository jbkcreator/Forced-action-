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
from datetime import date
from typing import Optional

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
