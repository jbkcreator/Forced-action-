"""A3: Heuristic Tuner — nightly feedback loop for scoring weight overrides.

Reads recent loss autopsy and win autopsy outcomes, computes per-(vertical, signal_type)
loss rates, and nudges scoring_weight_overrides deltas within safety bounds so the CDS
score converges toward actual ROI over time.

Scheduled at 06:45 UTC (before the 07:00 CDS rescore).

Run manually:
    PYTHONPATH=. python tasks/heuristic_tuner.py
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

from src.services.heuristic_loader import invalidate_cache
from src.utils.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

_HEURISTICS_PATH = Path(__file__).resolve().parent.parent / "config" / "heuristics.json"


def run(db: Optional[Session] = None, dry_run: bool = False) -> dict:
    """Execute one tuner pass. Returns a summary dict.

    Args:
        db: SQLAlchemy session (opens its own if None).
        dry_run: If True, compute deltas but do not write to DB.
    """
    data = json.loads(_HEURISTICS_PATH.read_text(encoding="utf-8"))
    cfg = data.get("tuner_config", {})
    bounds = data.get("delta_bounds", {"min": -15, "max": 15})

    loss_window: int = cfg.get("loss_window_days", 30)
    win_window: int = cfg.get("win_window_days", 30)
    min_sample: int = cfg.get("min_sample_size", 5)
    max_per_run: float = cfg.get("max_delta_per_run", 5)
    loss_threshold: float = cfg.get("loss_rate_threshold", 0.70)
    win_threshold: int = cfg.get("win_boost_threshold", 5)
    bound_min: float = bounds["min"]
    bound_max: float = bounds["max"]

    def _execute(session: Session) -> dict:
        # ── 1. Loss signal counts ──────────────────────────────────────────────
        loss_rows = session.execute(
            sa_text("""
                SELECT
                    raw_context->'deal'->>'trade_vertical' AS vertical,
                    jsonb_array_elements_text(raw_context->'property'->'distress_types') AS signal_type,
                    COUNT(*) AS loss_count
                FROM loss_autopsies
                WHERE created_at >= NOW() - INTERVAL ':days days'
                  AND primary_rejection_reason IS NOT NULL
                  AND primary_rejection_reason != 'UNKNOWN'
                  AND raw_context->'deal'->>'trade_vertical' IS NOT NULL
                GROUP BY 1, 2
            """.replace(":days", str(loss_window)))
        ).fetchall()

        loss_map: dict[tuple[str, str], int] = {
            (r[0], r[1]): int(r[2]) for r in loss_rows if r[0] and r[1]
        }

        # ── 2. Win signal counts ───────────────────────────────────────────────
        win_rows = session.execute(
            sa_text("""
                SELECT
                    wins_row->>'trade_vertical'                              AS vertical,
                    jsonb_array_elements_text(wins_row->'distress_signals')  AS signal_type,
                    COUNT(*)                                                 AS win_count
                FROM learning_cards,
                     jsonb_array_elements(data_json->'wins') AS wins_row
                WHERE card_type = 'win_autopsy'
                  AND card_date >= CURRENT_DATE - :days
                GROUP BY 1, 2
            """),
            {"days": win_window},
        ).fetchall()

        win_map: dict[tuple[str, str], int] = {
            (r[0], r[1]): int(r[2]) for r in win_rows if r[0] and r[1]
        }

        # ── 3. Load current deltas ─────────────────────────────────────────────
        existing_rows = session.execute(
            sa_text("""
                SELECT vertical, signal_type, delta
                FROM scoring_weight_overrides
                WHERE enabled = TRUE
            """)
        ).fetchall()
        current_deltas: dict[tuple[str, str], float] = {
            (r[0], r[1]): float(r[2]) for r in existing_rows
        }

        # ── 4. Compute changes ────────────────────────────────────────────────
        all_keys = set(loss_map) | set(win_map)
        updates: list[dict] = []

        for key in all_keys:
            vertical, signal_type = key
            n_loss = loss_map.get(key, 0)
            n_win = win_map.get(key, 0)
            total = n_loss + n_win
            if total < min_sample:
                continue

            loss_rate = n_loss / total
            current = current_deltas.get(key, 0.0)

            if loss_rate > loss_threshold:
                magnitude = min(max_per_run, int((loss_rate - loss_threshold) * 20))
                change = -magnitude
                source = "loss_feedback"
            elif n_win > win_threshold:
                magnitude = min(max_per_run, int(n_win / win_threshold))
                change = magnitude
                source = "win_feedback"
            else:
                continue

            new_delta = max(bound_min, min(bound_max, current + change))
            if new_delta == current:
                continue

            updates.append({
                "vertical":          vertical,
                "signal_type":       signal_type,
                "delta":             new_delta,
                "source":            source,
                "loss_sample_count": n_loss,
                "win_sample_count":  n_win,
            })

        # ── 5. Upsert ─────────────────────────────────────────────────────────
        if not dry_run:
            for u in updates:
                session.execute(
                    sa_text("""
                        INSERT INTO scoring_weight_overrides
                            (vertical, signal_type, delta, source,
                             loss_sample_count, win_sample_count,
                             reason, enabled)
                        VALUES
                            (:v, :s, :d, :src, :lc, :wc,
                             'Tuner update from outcome feedback', TRUE)
                        ON CONFLICT (vertical, signal_type) DO UPDATE
                            SET delta             = EXCLUDED.delta,
                                source            = EXCLUDED.source,
                                loss_sample_count = EXCLUDED.loss_sample_count,
                                win_sample_count  = EXCLUDED.win_sample_count,
                                reason            = EXCLUDED.reason,
                                updated_at        = NOW()
                    """),
                    {
                        "v": u["vertical"], "s": u["signal_type"],
                        "d": u["delta"],    "src": u["source"],
                        "lc": u["loss_sample_count"], "wc": u["win_sample_count"],
                    },
                )
            invalidate_cache()

        summary = {
            "rows_evaluated": len(all_keys),
            "rows_updated":   len(updates),
            "dry_run":        dry_run,
            "updates":        updates[:10],  # top 10 for logging
        }

        top5 = sorted(updates, key=lambda x: abs(x["delta"]), reverse=True)[:5]
        logger.info(
            "[heuristic_tuner] evaluated=%d updated=%d dry_run=%s top_changes=%s",
            len(all_keys), len(updates), dry_run,
            [(u["vertical"], u["signal_type"], u["delta"]) for u in top5],
        )
        return summary

    if db is not None:
        return _execute(db)

    from src.core.database import get_db_context
    with get_db_context() as session:
        return _execute(session)


if __name__ == "__main__":
    import sys
    dry = "--dry-run" in sys.argv
    result = run(dry_run=dry)
    print(f"Tuner complete: {result['rows_updated']} rows updated (dry_run={result['dry_run']})")
