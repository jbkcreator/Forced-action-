"""
CDS scoring training-dataset builder (Stage B of the cross-county retune).

Produces one row per (property_id, vertical, score_date) capturing the
feature snapshot the engine saw at score time + the event-proxy outcome
label measured in a fixed window after score_date. Drives the per-vertical
weight refit in src/services/scoring_fit.py (Stage C).

Usage:
    python -m src.services.scoring_training_data
    python -m src.services.scoring_training_data --county hillsborough
    python -m src.services.scoring_training_data --since 2025-01-01
    python -m src.services.scoring_training_data --outcome-window-days 60

Output: data/scoring_training/<run_id>.csv

The outcome label mirrors src/tasks/conversion_report.py — arms-length deed
(sale_price >= $1,000 AND deed_type not in intra-family patterns) OR
foreclosure filing strictly after score_date and within outcome_window_days.
DealOutcome matches are emitted as a second outcome column for future re-fits
once that table accumulates real subscriber-reported volume.

For counties with `missing_signals` (config.scoring.COUNTY_OVERRIDES), the
per-signal columns for missing axes are emitted as NULL — not 0 — so the
Stage C fit can distinguish "unobserved" from "absent" and avoid the
Pinellas-rewards-absence bias the live engine produces today.
"""

from __future__ import annotations

import argparse
import csv
import logging
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Optional

from sqlalchemy import text

from config.scoring import (
    STACKING_WINDOW_DAYS,
    HCPA_AGE_YEARS,
    HCPA_LONG_TERM_YEARS,
    VERTICAL_WEIGHTS,
    for_county,
)
from src.core.database import get_db_context
from src.tasks.conversion_report import _INTRA_FAMILY_DEED_PATTERNS

logger = logging.getLogger(__name__)


# ── Signal axes ──────────────────────────────────────────────────────────────
# The canonical signal taxonomy the fit will use. Names match config/scoring.py
# VERTICAL_WEIGHTS keys so coefficients map directly to existing knobs.
SIGNAL_TYPES: tuple[str, ...] = (
    "foreclosures",
    "tax_delinquencies",
    "code_violations",
    "judgment_liens",
    "irs_tax_liens",
    "hoa_liens",
    "mechanics_liens",
    "code_lien",
    "building_permits",
    "enforcement_permit",
    "probate",
    "evictions",
    "divorce_filings",
    "bankruptcy",
    "deed_transfers",
    "insurance_claim",
    "fire",
    "storm_damage",
    "flood_damage",
)

# Verticals the fit will produce coefficients for. Pulled from the live
# weight table so adding a vertical there automatically extends training output.
VERTICALS: tuple[str, ...] = tuple(VERTICAL_WEIGHTS.keys())

DEFAULT_OUTCOME_WINDOW_DAYS = 90
DEFAULT_LOOKBACK_DAYS = 365
DEFAULT_OUTPUT_DIR = Path("data/scoring_training")


# ── Outcome arms-length deed clause (reused from conversion_report) ──────────
# Build a SQL fragment that excludes intra-family / nominal-transfer deeds.
# Wraps in (NULL OR <patterns>) so deeds with unknown deed_type are kept —
# the LIKE check would return NULL and drop them otherwise.
_DEED_PATTERN_CLAUSE = " AND ".join(
    f"UPPER(d.deed_type) NOT LIKE '%{pat}%'" for pat in _INTRA_FAMILY_DEED_PATTERNS
)
_ARMS_LENGTH_DEED_SQL = (
    "(d.sale_price IS NULL OR d.sale_price >= 1000) "
    f"AND (d.deed_type IS NULL OR ({_DEED_PATTERN_CLAUSE}))"
)


@dataclass(frozen=True)
class BuilderConfig:
    """Knobs for one training-data run."""
    since:                date
    outcome_window_days:  int = DEFAULT_OUTCOME_WINDOW_DAYS
    county_id:            Optional[str] = None         # None → all counties
    output_dir:           Path = DEFAULT_OUTPUT_DIR
    run_id:               Optional[str] = None         # None → uuid4-hex

    @property
    def fully_observed_cutoff(self) -> date:
        """Latest score_date eligible for inclusion.

        Score events newer than this don't have a fully-observed outcome
        window yet — including them would under-count events and bias the
        label toward 0.
        """
        return date.today() - timedelta(days=self.outcome_window_days)

    def resolved_run_id(self) -> str:
        return self.run_id or uuid.uuid4().hex[:12]


# ── SQL ──────────────────────────────────────────────────────────────────────
# Single bulk query that pulls every (property, score_date) snapshot in the
# window plus the joined property/owner/financial context. Per-signal
# evidence + outcomes are layered in via per-property lookups below — kept
# out of this query to keep its row count bounded by score-event count.

_BASE_SCORE_EVENTS_SQL = """
SELECT
    ds.id                       AS score_id,
    ds.property_id              AS property_id,
    ds.score_date               AS score_date,
    ds.final_cds_score          AS final_cds_score,
    ds.vertical_scores          AS vertical_scores,
    ds.lead_tier                AS lead_tier,
    ds.urgency_level            AS urgency_level,
    ds.qualified                AS qualified,
    p.county_id                 AS county_id,
    p.year_built                AS year_built,
    p.parcel_id                 AS parcel_id,
    o.absentee_status           AS absentee_status,
    (o.phone_1 IS NOT NULL
        OR o.phone_2 IS NOT NULL
        OR o.phone_3 IS NOT NULL)            AS has_phone,
    (o.email_1 IS NOT NULL
        OR o.email_2 IS NOT NULL)            AS has_email,
    f.equity_pct                AS equity_pct,
    f.last_sale_date            AS last_sale_date,
    f.value_change_yoy          AS value_change_yoy
FROM distress_scores ds
JOIN properties p ON p.id = ds.property_id
LEFT JOIN owners o      ON o.property_id = p.id
LEFT JOIN financials f  ON f.property_id = p.id
WHERE ds.score_date >= :since
  AND ds.score_date <= :fully_observed_cutoff
  AND (:county_id IS NULL OR p.county_id = :county_id)
ORDER BY ds.property_id, ds.score_date
"""


# Per-signal evidence: for each (property, score_date) snapshot, find the
# latest signal date of each signal type that predates score_date. Built as
# a long UNION over the polymorphic signal tables so a single index scan
# per table covers every property in the window.
_SIGNAL_EVIDENCE_SQL = """
WITH score_events AS (
    SELECT ds.property_id, ds.score_date
    FROM distress_scores ds
    JOIN properties p ON p.id = ds.property_id
    WHERE ds.score_date >= :since
      AND ds.score_date <= :fully_observed_cutoff
      AND (:county_id IS NULL OR p.county_id = :county_id)
),
signal_events AS (
    SELECT property_id, opened_date  AS sig_date, 'code_violations'  AS sig_type
        FROM code_violations
    UNION ALL
    SELECT property_id, record_date  AS sig_date, 'deed_transfers'   AS sig_type
        FROM deeds
        WHERE sale_price IS NULL OR sale_price >= 1000
    UNION ALL
    SELECT property_id, filing_date  AS sig_date, 'foreclosures'     AS sig_type
        FROM foreclosures
    UNION ALL
    SELECT property_id, deed_app_date AS sig_date, 'tax_delinquencies' AS sig_type
        FROM tax_delinquencies
        WHERE total_amount_due IS NOT NULL OR years_delinquent IS NOT NULL
    UNION ALL
    SELECT property_id, filing_date  AS sig_date,
           CASE
               WHEN record_type = 'Judgment' THEN 'judgment_liens'
               WHEN document_type ILIKE '%CODE LIEN%' THEN 'code_lien'
               WHEN document_type ILIKE '%HOA LIEN%'  THEN 'hoa_liens'
               WHEN document_type ILIKE '%MECHANICS%' THEN 'mechanics_liens'
               WHEN document_type ILIKE '%TAX LIEN%'  THEN 'irs_tax_liens'
               ELSE NULL
           END AS sig_type
        FROM legal_and_liens
    UNION ALL
    SELECT property_id, filing_date  AS sig_date,
           CASE record_type
               WHEN 'Probate'    THEN 'probate'
               WHEN 'Eviction'   THEN 'evictions'
               WHEN 'Bankruptcy' THEN 'bankruptcy'
               WHEN 'Divorce'    THEN 'divorce_filings'
               ELSE NULL
           END AS sig_type
        FROM legal_proceedings
    UNION ALL
    SELECT property_id, issue_date   AS sig_date,
           CASE WHEN is_enforcement_permit THEN 'enforcement_permit'
                ELSE 'building_permits'
           END AS sig_type
        FROM building_permits
    UNION ALL
    SELECT property_id, incident_date AS sig_date, LOWER(incident_type) AS sig_type
        FROM incidents
)
SELECT
    se.property_id,
    se.score_date,
    sig.sig_type,
    MAX(sig.sig_date) AS latest_sig_date
FROM score_events se
JOIN signal_events sig
    ON sig.property_id = se.property_id
   AND sig.sig_type IS NOT NULL
   AND sig.sig_date IS NOT NULL
   AND sig.sig_date <= se.score_date
GROUP BY se.property_id, se.score_date, sig.sig_type
"""


# Outcomes: event-proxy (arms-length deed OR foreclosure filing) and deal_outcome
# in (score_date, score_date + outcome_window_days]. The strict > guard kills
# time leakage even if the outcome record predates the score by a day.
_OUTCOMES_SQL = """
WITH score_events AS (
    SELECT ds.property_id, ds.score_date
    FROM distress_scores ds
    JOIN properties p ON p.id = ds.property_id
    WHERE ds.score_date >= :since
      AND ds.score_date <= :fully_observed_cutoff
      AND (:county_id IS NULL OR p.county_id = :county_id)
),
deed_hits AS (
    SELECT se.property_id, se.score_date, MIN(d.record_date) AS event_date
    FROM score_events se
    JOIN deeds d
        ON d.property_id = se.property_id
       AND d.record_date >  se.score_date
       AND d.record_date <= se.score_date + (:outcome_window_days * INTERVAL '1 day')
    WHERE """ + _ARMS_LENGTH_DEED_SQL + """
    GROUP BY se.property_id, se.score_date
),
fc_hits AS (
    SELECT se.property_id, se.score_date, MIN(f.filing_date) AS event_date
    FROM score_events se
    JOIN foreclosures f
        ON f.property_id = se.property_id
       AND f.filing_date >  se.score_date
       AND f.filing_date <= se.score_date + (:outcome_window_days * INTERVAL '1 day')
    GROUP BY se.property_id, se.score_date
),
deal_hits AS (
    -- deal_outcomes is sparse today (no real subscribers yet). Emit the
    -- column so the schema is forward-compatible; later re-fits can use it.
    SELECT se.property_id, se.score_date, MIN(dlo.deal_date) AS event_date
    FROM score_events se
    JOIN deal_outcomes dlo
        ON dlo.property_id = se.property_id
       AND dlo.deal_date IS NOT NULL
       AND dlo.deal_date >  se.score_date
       AND dlo.deal_date <= se.score_date + (:outcome_window_days * INTERVAL '1 day')
    GROUP BY se.property_id, se.score_date
)
SELECT
    se.property_id,
    se.score_date,
    LEAST(dh.event_date, fh.event_date) AS event_date,
    dh.event_date                        AS deed_event_date,
    fh.event_date                        AS fc_event_date,
    dl.event_date                        AS deal_event_date
FROM score_events se
LEFT JOIN deed_hits dh ON dh.property_id = se.property_id AND dh.score_date = se.score_date
LEFT JOIN fc_hits   fh ON fh.property_id = se.property_id AND fh.score_date = se.score_date
LEFT JOIN deal_hits dl ON dl.property_id = se.property_id AND dl.score_date = se.score_date
WHERE dh.event_date IS NOT NULL
   OR fh.event_date IS NOT NULL
   OR dl.event_date IS NOT NULL
"""


# ── Builder ──────────────────────────────────────────────────────────────────


def _equity_bucket(equity_pct: Optional[float]) -> Optional[str]:
    if equity_pct is None:
        return None
    if equity_pct > 50:
        return "high"
    if equity_pct >= 30:
        return "mid"
    return "low"


def _years_between(d: Optional[date], reference: date) -> Optional[float]:
    if d is None:
        return None
    return (reference - d).days / 365.25


def build_training_dataset(session, cfg: BuilderConfig) -> Iterable[dict]:
    """Yield one training-row dict per (property_id, vertical, score_date)."""
    params = {
        "since":                  cfg.since,
        "fully_observed_cutoff":  cfg.fully_observed_cutoff,
        "county_id":              cfg.county_id,
    }

    # 1) Pull score-event base rows.
    score_rows = session.execute(text(_BASE_SCORE_EVENTS_SQL), params).mappings().all()
    logger.info("Fetched %d score events (since=%s, county=%s)",
                len(score_rows), cfg.since, cfg.county_id or "all")

    # 2) Pull signal evidence — index by (property_id, score_date) -> {sig_type: latest_date}.
    sig_rows = session.execute(text(_SIGNAL_EVIDENCE_SQL), params).mappings().all()
    signals_by_key: dict[tuple[int, date], dict[str, date]] = {}
    for r in sig_rows:
        key = (r["property_id"], r["score_date"])
        signals_by_key.setdefault(key, {})[r["sig_type"]] = r["latest_sig_date"]
    logger.info("Fetched signal evidence for %d (property, score_date) keys",
                len(signals_by_key))

    # 3) Pull outcomes — index the same way.
    outcome_params = {**params, "outcome_window_days": cfg.outcome_window_days}
    outcome_rows = session.execute(text(_OUTCOMES_SQL), outcome_params).mappings().all()
    outcomes_by_key: dict[tuple[int, date], dict] = {}
    for r in outcome_rows:
        outcomes_by_key[(r["property_id"], r["score_date"])] = dict(r)
    logger.info("Fetched outcomes for %d (property, score_date) keys",
                len(outcomes_by_key))

    # 4) Compose per-vertical rows.
    stacking_window = timedelta(days=STACKING_WINDOW_DAYS)

    for s in score_rows:
        key = (s["property_id"], s["score_date"])
        per_signal = signals_by_key.get(key, {})
        outcome = outcomes_by_key.get(key)

        # County-aware feature mask: for missing signals the recency/has columns
        # are NULL not 0 so the fit can model them as unobserved.
        county_cfg = for_county(s["county_id"])
        missing = county_cfg.missing_signals

        # Per-signal columns + stacking count.
        score_date = s["score_date"].date() if isinstance(s["score_date"], datetime) else s["score_date"]
        stacking_window_start = score_date - stacking_window
        signal_features: dict[str, Any] = {}
        stacking_count = 0
        for sig in SIGNAL_TYPES:
            if sig in missing:
                signal_features[f"has_{sig}"] = None
                signal_features[f"recency_{sig}_days"] = None
                continue
            latest = per_signal.get(sig)
            signal_features[f"has_{sig}"] = 1 if latest is not None else 0
            if latest is None:
                signal_features[f"recency_{sig}_days"] = None
            else:
                latest_date = latest if isinstance(latest, date) else latest.date()
                signal_features[f"recency_{sig}_days"] = (score_date - latest_date).days
                if latest_date >= stacking_window_start:
                    stacking_count += 1

        # HCPA passive features (always available — derived from Property/Financial).
        property_age = (
            score_date.year - s["year_built"]
            if s["year_built"] else None
        )
        years_since_sale = _years_between(s["last_sale_date"], score_date)

        # Outcome label — strictly POST score event. event_date is the earlier
        # of (deed, foreclosure); deal_event_date is forward-compatibility only.
        outcome_event = 0
        outcome_event_date = None
        outcome_deal = 0
        if outcome is not None:
            if outcome.get("event_date"):
                outcome_event = 1
                outcome_event_date = outcome["event_date"]
            if outcome.get("deal_event_date"):
                outcome_deal = 1

        vertical_scores = s["vertical_scores"] or {}

        for vertical in VERTICALS:
            vscore = vertical_scores.get(vertical) if isinstance(vertical_scores, dict) else None
            yield {
                "property_id":          s["property_id"],
                "parcel_id":            s["parcel_id"],
                "county_id":            s["county_id"],
                "score_id":             s["score_id"],
                "score_date":           score_date.isoformat(),
                "vertical":             vertical,
                "vertical_score":       float(vscore) if vscore is not None else None,
                "final_cds_score":      float(s["final_cds_score"]) if s["final_cds_score"] is not None else None,
                "lead_tier":            s["lead_tier"],
                "urgency_level":        s["urgency_level"],
                "qualified":            bool(s["qualified"]) if s["qualified"] is not None else None,
                # Owner / contact features
                "absentee_status":      s["absentee_status"],
                "has_phone":            bool(s["has_phone"]) if s["has_phone"] is not None else False,
                "has_email":            bool(s["has_email"]) if s["has_email"] is not None else False,
                # Financial features
                "equity_pct":           float(s["equity_pct"]) if s["equity_pct"] is not None else None,
                "equity_bucket":        _equity_bucket(float(s["equity_pct"]) if s["equity_pct"] is not None else None),
                "years_since_sale":     years_since_sale,
                "long_term_owner":      1 if (years_since_sale is not None and years_since_sale >= HCPA_LONG_TERM_YEARS) else 0,
                "value_change_yoy":     float(s["value_change_yoy"]) if s["value_change_yoy"] is not None else None,
                # HCPA passives
                "property_age_years":   property_age,
                "property_age_30plus":  1 if (property_age is not None and property_age >= HCPA_AGE_YEARS) else 0,
                # Stacking
                "stacking_count":       stacking_count,
                # Per-signal features
                **signal_features,
                # Outcome columns
                "outcome_event":        outcome_event,
                "outcome_event_date":   outcome_event_date.isoformat() if outcome_event_date else None,
                "outcome_deal":         outcome_deal,
            }


# ── CSV writer ───────────────────────────────────────────────────────────────


def _row_columns() -> list[str]:
    """Stable column order for the CSV output. Mirrors build_training_dataset."""
    base = [
        "property_id", "parcel_id", "county_id", "score_id", "score_date",
        "vertical", "vertical_score", "final_cds_score", "lead_tier",
        "urgency_level", "qualified",
        "absentee_status", "has_phone", "has_email",
        "equity_pct", "equity_bucket", "years_since_sale", "long_term_owner",
        "value_change_yoy", "property_age_years", "property_age_30plus",
        "stacking_count",
    ]
    for sig in SIGNAL_TYPES:
        base.append(f"has_{sig}")
        base.append(f"recency_{sig}_days")
    base += ["outcome_event", "outcome_event_date", "outcome_deal"]
    return base


def write_csv(rows: Iterable[dict], output_path: Path) -> int:
    """Write rows to output_path. Returns the row count written."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    columns = _row_columns()
    count = 0
    with output_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            writer.writerow(r)
            count += 1
    return count


# ── CLI ──────────────────────────────────────────────────────────────────────


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Build the CDS scoring training dataset (Stage B of retune)."
    )
    p.add_argument("--county", default=None,
                   help="County id (e.g. hillsborough, pinellas). Omit for all counties.")
    p.add_argument("--since", default=None,
                   help=f"ISO date floor for score_date (default: today - {DEFAULT_LOOKBACK_DAYS} days).")
    p.add_argument("--outcome-window-days", type=int, default=DEFAULT_OUTCOME_WINDOW_DAYS,
                   help=f"Days after score_date to look for an outcome event (default {DEFAULT_OUTCOME_WINDOW_DAYS}).")
    p.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR,
                   help=f"Directory to write the CSV into (default {DEFAULT_OUTPUT_DIR}).")
    p.add_argument("--run-id", default=None,
                   help="Override the auto-generated run id (used as CSV stem).")
    return p.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )

    args = _parse_args(argv)
    since = (
        date.fromisoformat(args.since)
        if args.since
        else date.today() - timedelta(days=DEFAULT_LOOKBACK_DAYS)
    )
    cfg = BuilderConfig(
        since=since,
        outcome_window_days=args.outcome_window_days,
        county_id=args.county,
        output_dir=args.output_dir,
        run_id=args.run_id,
    )

    run_id = cfg.resolved_run_id()
    output_path = cfg.output_dir / f"{run_id}.csv"

    with get_db_context() as session:
        rows = build_training_dataset(session, cfg)
        n = write_csv(rows, output_path)

    logger.info("Wrote %d training rows to %s", n, output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
