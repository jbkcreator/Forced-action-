"""
Weekly Kill-Switch Scorecard (stages 9-13 reporting layer).

Reads daily platform_daily_stats snapshots + open lifecycle_incident rows and
writes one learning_cards row (card_type='kill_switch_scorecard') per week.

Per feature it reports:
  current_color   — green/yellow/red/unknown (latest daily snapshot)
  observed        — latest observed value
  red_streak      — consecutive pure-red days (int) or None (not snapshotted)
  kill_rec_pending — True if the engine has issued a kill recommendation
                    (source county only — Level 1)
  engine_breach_days — float age of open incident, or None

Footnotes surface data-quality gaps (self-healing disabled, Level-1 limit,
unsnapshotted metrics, channel stub).

The compact summary_text is also read by src/tasks/revenue_pulse.py to
append a one-line alarm to the Monday founder SMS.

Schedule: Monday 08:50 UTC (crontab.txt). No env gate — read-only except
the single learning_cards write.

Usage:
    python -m src.tasks.kill_switch_scorecard
    python -m src.tasks.kill_switch_scorecard --dry-run
"""
from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

from config.lifecycle_guardrails import KILL_SWITCH
from config.settings import get_settings
from src.core.database import get_db_context
from src.services.kill_switch_scorecard_data import (
    SNAPSHOTTED_METRICS,
    consecutive_red_days,
    latest_color,
    open_incident_for,
)
from src.utils.county_config import list_counties

logger = logging.getLogger(__name__)

_WINDOW_DAYS = 7


# ---------------------------------------------------------------------------
# Per-feature row builder
# ---------------------------------------------------------------------------

def _build_feature_row(
    db: Session,
    metric_name: str,
    county_id: str,
    source_county: str,
) -> dict:
    color, observed = latest_color(db, metric_name, county_id)
    streak = consecutive_red_days(db, metric_name, county_id, max_window=_WINDOW_DAYS)

    kill_rec_pending: Optional[bool] = None
    engine_breach_days: Optional[float] = None

    # Kill-rec and engine breach only available for source county (Level 1).
    if county_id == source_county:
        incident = open_incident_for(db, metric_name, county_id)
        if incident is not None:
            kill_rec_pending = incident.action_taken == "feature_killed"
            if incident.breach_started:
                bs = incident.breach_started
                if bs.tzinfo is None:
                    bs = bs.replace(tzinfo=timezone.utc)
                engine_breach_days = round(
                    (datetime.now(timezone.utc) - bs).total_seconds() / 86400, 1
                )
        else:
            kill_rec_pending = False

    return {
        "metric": metric_name,
        "current_color": color,
        "observed": observed,
        "red_streak": streak,
        "kill_rec_pending": kill_rec_pending,
        "engine_breach_days": engine_breach_days,
    }


# ---------------------------------------------------------------------------
# County summary
# ---------------------------------------------------------------------------

def _county_summary(features: list[dict]) -> dict:
    counts = {"red": 0, "yellow": 0, "green": 0, "unknown": 0}
    for f in features:
        c = f["current_color"]
        counts[c] = counts.get(c, 0) + 1
    return counts


# ---------------------------------------------------------------------------
# Compact alarm string (used as summary_text + pulse line)
# ---------------------------------------------------------------------------

def _compact_alarm(counties_data: dict, source_county: str) -> Optional[str]:
    """Build the one-line alarm for the Revenue Pulse SMS.

    Returns None if every county is all-green (line omitted from SMS).
    Format: "KS: hills 🔴1 🟡2 (lock_conversion red 5/7d→kill rec) | pinellas 🟢 all"
    """
    parts = []
    any_problem = False

    for county_id, data in counties_data.items():
        features = data["features"]
        summary = data["summary"]
        red_n = summary.get("red", 0)
        yellow_n = summary.get("yellow", 0)

        if red_n == 0 and yellow_n == 0:
            short = county_id[:6]
            parts.append(f"{short} 🟢 all")
            continue

        any_problem = True
        short = county_id[:6]
        counts_str = ""
        if red_n:
            counts_str += f"🔴{red_n}"
        if yellow_n:
            counts_str += f" 🟡{yellow_n}"
        counts_str = counts_str.strip()

        # Worst feature: red > yellow, longest streak as tiebreak.
        worst = _pick_worst_feature(features)
        worst_str = ""
        if worst:
            streak = worst["red_streak"]
            streak_str = f"{streak}/7d" if streak is not None else "n/a"
            kill_flag = "→kill rec" if worst.get("kill_rec_pending") else ""
            suffix = f" {kill_flag}".rstrip()
            worst_str = f" ({worst['metric']} red {streak_str}{suffix})"

        parts.append(f"{short} {counts_str}{worst_str}")

    if not any_problem:
        return None

    return "KS: " + " | ".join(parts)


def _pick_worst_feature(features: list[dict]) -> Optional[dict]:
    """Red beats yellow; within same color, longest streak wins."""
    reds = [f for f in features if f["current_color"] == "red"]
    yellows = [f for f in features if f["current_color"] == "yellow"]
    candidates = reds or yellows
    if not candidates:
        return None
    return max(candidates, key=lambda f: (f["red_streak"] or 0))


# ---------------------------------------------------------------------------
# Footnotes
# ---------------------------------------------------------------------------

def _build_footnotes(settings, county_ids: list[str], source_county: str) -> list[str]:
    notes = []
    if not settings.lifecycle_self_healing_enabled:
        notes.append(
            "self-healing engine: disabled — kill-rec annotations unavailable"
        )
    non_source = [c for c in county_ids if c != source_county]
    if non_source:
        notes.append(
            f"{', '.join(non_source)}: streaks shown; kill-rec engine source-county-only (Level 1)"
        )
    unsnapshotted = [m for m in KILL_SWITCH if m not in SNAPSHOTTED_METRICS]
    if unsnapshotted:
        notes.append(
            f"streak n/a for: {', '.join(sorted(unsnapshotted))} — not in daily snapshot"
        )
    notes.append(
        "acquisition channels: n/a — no ad-spend ledger (follow-up: wire spend tracking)"
    )
    return notes


# ---------------------------------------------------------------------------
# Learning card write
# ---------------------------------------------------------------------------

def _write_learning_card(db: Session, data_json: dict, summary_text: str) -> None:
    db.execute(sa_text("""
        INSERT INTO learning_cards (card_date, card_type, summary_text, data_json, created_at)
        VALUES (
            (CURRENT_DATE - ((EXTRACT(DOW FROM CURRENT_DATE)::INT + 6) % 7))::date,
            'kill_switch_scorecard',
            :summary,
            CAST(:data AS jsonb),
            NOW()
        )
        ON CONFLICT (card_date, card_type)
        DO UPDATE SET summary_text = EXCLUDED.summary_text,
                      data_json    = EXCLUDED.data_json
    """), {"summary": summary_text, "data": json.dumps(data_json)})


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_weekly_scorecard(dry_run: bool = False) -> dict:
    """Compute the weekly kill-switch scorecard and write a learning_cards row."""
    settings = get_settings()
    source_county = settings.county_launch_source_county or "hillsborough"

    try:
        county_ids = list_counties()
    except Exception:
        county_ids = [source_county]
    if not county_ids:
        county_ids = [source_county]

    counties_data: dict = {}

    with get_db_context() as db:
        for county_id in county_ids:
            features = [
                _build_feature_row(db, metric_name, county_id, source_county)
                for metric_name in KILL_SWITCH
            ]
            counties_data[county_id] = {
                "features": features,
                "summary": _county_summary(features),
            }

        footnotes = _build_footnotes(settings, county_ids, source_county)
        alarm = _compact_alarm(counties_data, source_county)
        summary_text = alarm or "KS: all green"

        card = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "window_days": _WINDOW_DAYS,
            "counties": counties_data,
            "channels": {
                "status": "n/a",
                "reason": "no ad-spend ledger",
            },
            "footnotes": footnotes,
        }

        if not dry_run:
            _write_learning_card(db, card, summary_text)
            logger.info(
                "[KillSwitchScorecard] card written counties=%s alarm=%r",
                list(counties_data.keys()), summary_text,
            )
        else:
            logger.info(
                "[KillSwitchScorecard] dry_run — card=%s",
                json.dumps(card, indent=2, default=str),
            )

    return {"summary": summary_text, "card": card, "dry_run": dry_run}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    dry = "--dry-run" in sys.argv
    result = run_weekly_scorecard(dry_run=dry)
    print(json.dumps(result["card"], indent=2, default=str))
    print("\nAlarm line:", result["summary"])
