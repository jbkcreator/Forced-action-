"""A7: Macro-signal persistence service.

Accepts normalized loader records (dicts matching REQUIRED_KEYS from
src.loaders.macro_signals.normalization) and upserts them into macro_signals.

Upsert key: (source, signal_key, source_series_id, observed_at, geography_scope, geography_id)
On conflict: value and raw_payload are refreshed; created_at is preserved.
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Optional

from sqlalchemy import func, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from src.core.models import MacroSignal

logger = logging.getLogger(__name__)


def _to_row(record: dict) -> dict:
    """Coerce a normalized loader record to a MacroSignal column dict."""
    obs = record["observed_at"]
    if isinstance(obs, str):
        obs = date.fromisoformat(obs[:10])
    elif isinstance(obs, datetime):
        obs = obs.date()

    return {
        "source": record["source"],
        "signal_key": record["signal_key"],
        "source_series_id": record.get("source_series_id") or "",
        "value": record["value"],
        "unit": record["unit"],
        "observed_at": obs,
        "frequency": record["frequency"],
        "geography_scope": record["geography_scope"],
        "geography_id": record["geography_id"],
        "raw_payload": record.get("raw_payload"),
    }


def upsert_macro_signal(session: Session, record: dict) -> dict[str, int]:
    """Upsert a single normalized macro-signal record."""
    return upsert_macro_signals(session, [record])


def upsert_macro_signals(session: Session, records: list[dict]) -> dict[str, int]:
    """Batch upsert normalized macro-signal records.

    Returns {"inserted": N, "updated": N} based on whether each row was
    a fresh insert (xmax=0) or an update (xmax>0) at the Postgres level.
    """
    if not records:
        return {"inserted": 0, "updated": 0}

    rows = [_to_row(r) for r in records]

    stmt = pg_insert(MacroSignal).values(rows)
    upsert_stmt = stmt.on_conflict_do_update(
        constraint="uq_macro_signal_observation",
        set_={
            "value": stmt.excluded.value,
            "raw_payload": stmt.excluded.raw_payload,
            "updated_at": func.now(),
        },
    )
    result = session.execute(
        upsert_stmt.returning(
            MacroSignal.id,
            text("xmax::text::bigint > 0 AS was_update"),
        )
    )
    rows_out = result.fetchall()
    updated = sum(1 for r in rows_out if r[1])
    inserted = len(rows_out) - updated
    logger.debug(
        "[MacroSignalService] upserted %d rows (inserted=%d updated=%d)",
        len(rows_out), inserted, updated,
    )
    return {"inserted": inserted, "updated": updated}


def get_latest_macro_signal(
    session: Session,
    signal_key: str,
    geography_scope: Optional[str] = None,
    geography_id: Optional[str] = None,
) -> Optional[dict]:
    """Return the most recent observation for a signal key, optionally filtered by geography.

    Returns a plain dict (source, signal_key, value, unit, observed_at, geography_scope,
    geography_id, frequency) or None if no matching row exists.
    """
    conditions = ["signal_key = :signal_key"]
    params: dict = {"signal_key": signal_key}
    if geography_scope is not None:
        conditions.append("geography_scope = :geography_scope")
        params["geography_scope"] = geography_scope
    if geography_id is not None:
        conditions.append("geography_id = :geography_id")
        params["geography_id"] = geography_id

    where_clause = " AND ".join(conditions)
    row = session.execute(
        text(f"""
            SELECT source, signal_key, value, unit, observed_at,
                   geography_scope, geography_id, frequency
            FROM macro_signals
            WHERE {where_clause}
            ORDER BY observed_at DESC
            LIMIT 1
        """),
        params,
    ).mappings().first()

    return dict(row) if row else None


def get_latest_macro_signals_by_source(
    session: Session,
    source: str,
) -> list[dict]:
    """Return one row per (signal_key, geography_scope, geography_id) for a source.

    Each row is the latest available observation. Useful for a quick source health check.
    """
    rows = session.execute(
        text("""
            SELECT DISTINCT ON (signal_key, geography_scope, geography_id)
                source, signal_key, value, unit, observed_at,
                geography_scope, geography_id, frequency
            FROM macro_signals
            WHERE source = :source
            ORDER BY signal_key, geography_scope, geography_id, observed_at DESC
        """),
        {"source": source},
    ).mappings().all()

    return [dict(r) for r in rows]
