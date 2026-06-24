"""Heuristic weight loader for A3 Warm-Start Priors.

Provides a 5-minute cached view of scoring_weight_overrides so cds_engine.py
gets fresh deltas without a DB round-trip per property.
"""
from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Optional

from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

_HEURISTICS_PATH = Path(__file__).resolve().parent.parent.parent / "config" / "heuristics.json"
_CACHE_TTL = 300  # seconds

# Module-level cache
_cache: dict[tuple[str, str], float] = {}
_cache_ts: float = 0.0


def _load_heuristics_json() -> dict:
    with open(_HEURISTICS_PATH, encoding="utf-8") as f:
        return json.load(f)


def load_overrides(db: Optional[Session] = None) -> dict[tuple[str, str], float]:
    """Return enabled weight deltas from scoring_weight_overrides, cached for 5 min.

    If db is None, opens its own session via get_db_context().
    Returns an empty dict if the table doesn't exist yet.
    """
    global _cache, _cache_ts
    now = time.monotonic()
    if now - _cache_ts < _CACHE_TTL and _cache is not None:
        return _cache

    def _fetch(session: Session) -> dict[tuple[str, str], float]:
        rows = session.execute(
            sa_text("""
                SELECT vertical, signal_type, delta
                FROM scoring_weight_overrides
                WHERE enabled = TRUE
            """)
        ).fetchall()
        return {(r[0], r[1]): float(r[2]) for r in rows}

    try:
        if db is not None:
            result = _fetch(db)
        else:
            from src.core.database import get_db_context
            with get_db_context() as session:
                result = _fetch(session)
        _cache = result
        _cache_ts = now
        return result
    except Exception:
        logger.warning("[heuristic_loader] failed to load overrides from DB, using cached/empty", exc_info=True)
        return _cache


def invalidate_cache() -> None:
    """Force next load_overrides() call to re-query the DB."""
    global _cache_ts
    _cache_ts = 0.0


def seed_from_json(db: Session, json_path: Optional[str] = None) -> int:
    """Upsert seed rows from heuristics.json into scoring_weight_overrides.

    Only touches source='seed' rows. Feedback rows are left untouched.
    Returns number of rows upserted.
    """
    path = Path(json_path) if json_path else _HEURISTICS_PATH
    data = json.loads(path.read_text(encoding="utf-8"))
    deltas: dict[str, dict[str, float]] = data.get("signal_weight_deltas", {})
    bounds = data.get("delta_bounds", {"min": -15, "max": 15})
    min_d, max_d = bounds["min"], bounds["max"]

    count = 0
    for vertical, signals in deltas.items():
        for signal_type, delta in signals.items():
            clamped = max(min_d, min(max_d, float(delta)))
            db.execute(
                sa_text("""
                    INSERT INTO scoring_weight_overrides
                        (vertical, signal_type, delta, source, reason, enabled)
                    VALUES
                        (:v, :s, :d, 'seed',
                         'Warm-start prior from closing desk (heuristics.json)',
                         TRUE)
                    ON CONFLICT (vertical, signal_type) DO UPDATE
                        SET delta      = EXCLUDED.delta,
                            source     = EXCLUDED.source,
                            reason     = EXCLUDED.reason,
                            enabled    = TRUE,
                            updated_at = NOW()
                    WHERE scoring_weight_overrides.source = 'seed'
                """),
                {"v": vertical, "s": signal_type, "d": clamped},
            )
            count += 1

    invalidate_cache()
    logger.info("[heuristic_loader] seeded %d override rows from %s", count, path.name)
    return count


def reset_feedback_rows(db: Session) -> int:
    """Delete all non-seed override rows (loss_feedback / win_feedback).

    Restores the table to seed-only state without touching expert priors.
    Returns the number of rows deleted.
    """
    result = db.execute(
        sa_text("""
            DELETE FROM scoring_weight_overrides
            WHERE source IN ('loss_feedback', 'win_feedback')
        """)
    )
    deleted = result.rowcount
    invalidate_cache()
    logger.info("[heuristic_loader] reset: deleted %d feedback rows", deleted)
    return deleted
