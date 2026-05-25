"""
Per-signal predictability diagnostic (CDS_RETUNE_STATUS.md item #1).

Answers two questions that the fit can't:

  1. Which individual signals actually predict 30/90/180-day transactions?
     The fit produces coefficients but L2 regularization splits correlated
     signals (foreclosure + judgment_lien + lis_pendens all co-occur on
     distressed properties). A signal that comes out at weight 0 in the
     fit may genuinely predict — just not after the model has already
     absorbed the same predictive power via a correlated feature.

  2. Is `deed_transfers` (weight 75 in the v0 fit) carrying real lift,
     or is it a data-leakage artifact?

For each signal type at each outcome window, the report computes:

    event_rate_with_signal     — Pr(event | has_<sig> = 1)
    event_rate_without_signal  — Pr(event | has_<sig> = 0)
    lift_ratio                 — with / without
    n_with                     — sample size with the signal
    n_without                  — sample size without the signal

Lift > 1.5 = the signal carries real predictive power. Lift ≈ 1 = signal
is neutral. Lift < 1 = signal anti-predicts the outcome.

Reads the same `distress_scores` history the fit uses. Reuses the
arms-length deed filter from conversion_report.py so the outcome
definition matches the fit's training labels exactly.

Usage:
    python -m src.tasks.signal_predictability_report
    python -m src.tasks.signal_predictability_report --windows 30,90,180
    python -m src.tasks.signal_predictability_report --county hillsborough
    python -m src.tasks.signal_predictability_report --json data/diag/<run>.json

Notes:
  - Sample sizes < 30 with-signal hits are marked as "insufficient" rather
    than reporting a noisy lift number.
  - Properties that score multiple times in the window count once (we use
    MIN(score_date) per property to anchor each one to a single outcome
    window) — this matches conversion_report's pattern.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass, asdict, field
from datetime import datetime
from pathlib import Path
from typing import Optional

from sqlalchemy import text

from src.core.database import get_db_context
from src.services.scoring_training_data import SIGNAL_TYPES
from src.tasks.conversion_report import _INTRA_FAMILY_DEED_PATTERNS

logger = logging.getLogger(__name__)


# Reused: arms-length deed filter so the outcome label matches the fit.
_DEED_PATTERN_CLAUSE = " AND ".join(
    f"UPPER(d.deed_type) NOT LIKE '%{pat}%'" for pat in _INTRA_FAMILY_DEED_PATTERNS
)

DEFAULT_WINDOWS = (30, 90, 180)
MIN_WITH_SIGNAL_SAMPLE = 30   # lift below this sample size is noisy


# Map signal_type string → (signal table, date column, where clause, type expression).
# The expression / where pair is how we identify rows of that signal type in the
# polymorphic tables (legal_and_liens, legal_proceedings, incidents, etc.).
_SIGNAL_SOURCES: dict[str, dict] = {
    "foreclosures":       {"table": "foreclosures",      "date_col": "filing_date",   "where": "TRUE"},
    "tax_delinquencies":  {"table": "tax_delinquencies", "date_col": "deed_app_date",
                           "where": "total_amount_due IS NOT NULL OR years_delinquent IS NOT NULL"},
    "code_violations":    {"table": "code_violations",   "date_col": "opened_date",   "where": "TRUE"},
    "deed_transfers":     {"table": "deeds",             "date_col": "record_date",
                           "where": "sale_price IS NULL OR sale_price >= 1000"},
    "judgment_liens":     {"table": "legal_and_liens",   "date_col": "filing_date",
                           "where": "record_type = 'Judgment'"},
    "code_lien":          {"table": "legal_and_liens",   "date_col": "filing_date",
                           "where": "document_type ILIKE '%CODE LIEN%'"},
    "hoa_liens":          {"table": "legal_and_liens",   "date_col": "filing_date",
                           "where": "document_type ILIKE '%HOA LIEN%'"},
    "mechanics_liens":    {"table": "legal_and_liens",   "date_col": "filing_date",
                           "where": "document_type ILIKE '%MECHANICS%'"},
    "irs_tax_liens":      {"table": "legal_and_liens",   "date_col": "filing_date",
                           "where": "document_type ILIKE '%TAX LIEN%'"},
    "probate":            {"table": "legal_proceedings", "date_col": "filing_date",
                           "where": "record_type = 'Probate'"},
    "evictions":          {"table": "legal_proceedings", "date_col": "filing_date",
                           "where": "record_type = 'Eviction'"},
    "bankruptcy":         {"table": "legal_proceedings", "date_col": "filing_date",
                           "where": "record_type = 'Bankruptcy'"},
    "divorce_filings":    {"table": "legal_proceedings", "date_col": "filing_date",
                           "where": "record_type = 'Divorce'"},
    "building_permits":   {"table": "building_permits",  "date_col": "issue_date",
                           "where": "NOT is_enforcement_permit"},
    "enforcement_permit": {"table": "building_permits",  "date_col": "issue_date",
                           "where": "is_enforcement_permit"},
    "insurance_claim":    {"table": "incidents",         "date_col": "incident_date",
                           "where": "LOWER(incident_type) = 'insurance_claim'"},
    "fire":               {"table": "incidents",         "date_col": "incident_date",
                           "where": "LOWER(incident_type) = 'fire'"},
    "storm_damage":       {"table": "incidents",         "date_col": "incident_date",
                           "where": "LOWER(incident_type) = 'storm_damage'"},
    "flood_damage":       {"table": "incidents",         "date_col": "incident_date",
                           "where": "LOWER(incident_type) = 'flood_damage'"},
}


@dataclass
class SignalReport:
    signal_type:               str
    window_days:               int
    n_with:                    int
    n_without:                 int
    transacted_with:           int
    transacted_without:        int
    event_rate_with_pct:       float
    event_rate_without_pct:    float
    lift_ratio:                Optional[float]   # None when sample size too small
    sample_warning:            Optional[str] = None


@dataclass
class FullReport:
    generated_at:  str
    county_id:     Optional[str]
    base_event_rate_by_window: dict[int, float]
    per_signal:    list[SignalReport]

    def to_dict(self) -> dict:
        return {
            "generated_at":               self.generated_at,
            "county_id":                  self.county_id,
            "base_event_rate_by_window":  self.base_event_rate_by_window,
            "per_signal":                 [asdict(s) for s in self.per_signal],
        }


# ── Query builders ───────────────────────────────────────────────────────────

# One row per scored property: their MIN(score_date) anchors the outcome window.
_SCORED_PROPERTIES_SQL = """
WITH scored AS (
    SELECT
        ds.property_id,
        MIN(ds.score_date) AS first_scored_at,
        p.county_id
    FROM distress_scores ds
    JOIN properties p ON p.id = ds.property_id
    WHERE (:county_id IS NULL OR p.county_id = :county_id)
    GROUP BY ds.property_id, p.county_id
)
SELECT * FROM scored
"""


def _per_signal_sql(signal_cfg: dict) -> str:
    """Compose the SQL that joins scored properties with this signal type's
    presence indicator and the post-score outcome.

    Returns one aggregate row: (n_with, n_without, transacted_with, transacted_without).
    """
    sig_table  = signal_cfg["table"]
    sig_date   = signal_cfg["date_col"]
    sig_where  = signal_cfg["where"]

    return f"""
    WITH scored AS (
        SELECT
            ds.property_id,
            MIN(ds.score_date) AS first_scored_at,
            p.county_id
        FROM distress_scores ds
        JOIN properties p ON p.id = ds.property_id
        WHERE (:county_id IS NULL OR p.county_id = :county_id)
        GROUP BY ds.property_id, p.county_id
    ),
    with_signal AS (
        SELECT DISTINCT sc.property_id
        FROM scored sc
        JOIN {sig_table} sig
            ON sig.property_id = sc.property_id
           AND sig.{sig_date}  IS NOT NULL
           AND sig.{sig_date} <= sc.first_scored_at
        WHERE ({sig_where})
    ),
    deed_hits AS (
        SELECT DISTINCT sc.property_id
        FROM scored sc
        JOIN deeds d ON d.property_id = sc.property_id
        WHERE d.record_date >  sc.first_scored_at
          AND d.record_date <= sc.first_scored_at + (:window_days * INTERVAL '1 day')
          AND (d.sale_price IS NULL OR d.sale_price >= 1000)
          AND (d.deed_type IS NULL OR ({_DEED_PATTERN_CLAUSE}))
    ),
    fc_hits AS (
        SELECT DISTINCT sc.property_id
        FROM scored sc
        JOIN foreclosures f ON f.property_id = sc.property_id
        WHERE f.filing_date >  sc.first_scored_at
          AND f.filing_date <= sc.first_scored_at + (:window_days * INTERVAL '1 day')
    ),
    any_hit AS (
        SELECT property_id FROM deed_hits
        UNION
        SELECT property_id FROM fc_hits
    )
    SELECT
        COUNT(*) FILTER (WHERE ws.property_id IS NOT NULL)                              AS n_with,
        COUNT(*) FILTER (WHERE ws.property_id IS NULL)                                  AS n_without,
        COUNT(*) FILTER (WHERE ws.property_id IS NOT NULL AND ah.property_id IS NOT NULL) AS trans_with,
        COUNT(*) FILTER (WHERE ws.property_id IS NULL     AND ah.property_id IS NOT NULL) AS trans_without
    FROM scored sc
    LEFT JOIN with_signal ws ON ws.property_id = sc.property_id
    LEFT JOIN any_hit ah     ON ah.property_id = sc.property_id
    """


# ── Driver ───────────────────────────────────────────────────────────────────


def run_report(windows: list[int], county_id: Optional[str]) -> FullReport:
    per_signal: list[SignalReport] = []
    base_rates: dict[int, float] = {}

    with get_db_context() as session:
        for window in windows:
            for sig_type in SIGNAL_TYPES:
                cfg = _SIGNAL_SOURCES.get(sig_type)
                if cfg is None:
                    logger.warning("No SQL source mapping for signal=%s — skipping.", sig_type)
                    continue

                row = session.execute(
                    text(_per_signal_sql(cfg)),
                    {"county_id": county_id, "window_days": window},
                ).first()
                if row is None:
                    continue

                n_with        = int(row.n_with or 0)
                n_without     = int(row.n_without or 0)
                trans_with    = int(row.trans_with or 0)
                trans_without = int(row.trans_without or 0)

                rate_with = (trans_with / n_with * 100) if n_with else 0.0
                rate_without = (trans_without / n_without * 100) if n_without else 0.0
                lift = (rate_with / rate_without) if rate_without > 0 else None
                if n_with < MIN_WITH_SIGNAL_SAMPLE:
                    lift = None
                    warn = f"n_with={n_with} < {MIN_WITH_SIGNAL_SAMPLE}; lift omitted"
                else:
                    warn = None

                per_signal.append(SignalReport(
                    signal_type=sig_type,
                    window_days=window,
                    n_with=n_with,
                    n_without=n_without,
                    transacted_with=trans_with,
                    transacted_without=trans_without,
                    event_rate_with_pct=round(rate_with, 3),
                    event_rate_without_pct=round(rate_without, 3),
                    lift_ratio=round(lift, 2) if lift is not None else None,
                    sample_warning=warn,
                ))

            total = n_with + n_without
            trans_total = trans_with + trans_without
            base_rate = (trans_total / total * 100) if total else 0.0
            base_rates[window] = round(base_rate, 3)

    return FullReport(
        generated_at=datetime.utcnow().isoformat() + "Z",
        county_id=county_id,
        base_event_rate_by_window=base_rates,
        per_signal=per_signal,
    )


def _print_report(report: FullReport) -> None:
    print()
    print("=" * 92)
    print("  PER-SIGNAL PREDICTABILITY REPORT")
    print(f"  Generated:  {report.generated_at}")
    print(f"  County:     {report.county_id or 'all'}")
    print(f"  Base event rates (all scored properties):")
    for window, rate in report.base_event_rate_by_window.items():
        print(f"    {window:>3}d: {rate:.2f}%")
    print("=" * 92)

    # Group by window for readability.
    by_window: dict[int, list[SignalReport]] = {}
    for r in report.per_signal:
        by_window.setdefault(r.window_days, []).append(r)

    for window, signals in by_window.items():
        print(f"\n  Outcome window: {window} days")
        print(f"  {'signal':<22} {'n_with':>8} {'n_without':>10} "
              f"{'rate_with %':>12} {'rate_without %':>16} {'lift':>8}")
        print(f"  {'-' * 22} {'-' * 8} {'-' * 10} {'-' * 12} {'-' * 16} {'-' * 8}")
        # Sort by lift_ratio descending (None values last).
        signals_sorted = sorted(
            signals,
            key=lambda s: (-s.lift_ratio if s.lift_ratio is not None else 0,),
        )
        for s in signals_sorted:
            lift_str = f"{s.lift_ratio:.2f}" if s.lift_ratio is not None else "n/a"
            print(
                f"  {s.signal_type:<22} {s.n_with:>8} {s.n_without:>10} "
                f"{s.event_rate_with_pct:>12.2f} {s.event_rate_without_pct:>16.2f} "
                f"{lift_str:>8}"
            )
    print()


# ── CLI ──────────────────────────────────────────────────────────────────────


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Per-signal predictability diagnostic. "
                    "Reports event-rate lift for each signal type at each outcome window.",
    )
    p.add_argument(
        "--windows", default=",".join(str(w) for w in DEFAULT_WINDOWS),
        help=f"Comma-separated outcome windows in days (default: {','.join(str(w) for w in DEFAULT_WINDOWS)}).",
    )
    p.add_argument(
        "--county", default=None,
        help="Restrict to a single county (e.g. hillsborough, pinellas). Default: all.",
    )
    p.add_argument(
        "--json", type=Path, default=None, metavar="PATH",
        help="Write the full report (raw counts + rates) to this JSON path.",
    )
    return p.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    args = _parse_args(argv)
    try:
        windows = [int(w.strip()) for w in args.windows.split(",") if w.strip()]
    except ValueError:
        logger.error("Invalid --windows value: %s", args.windows)
        return 2

    if not windows:
        logger.error("--windows produced an empty list.")
        return 2

    report = run_report(windows, args.county)
    _print_report(report)

    if args.json:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(json.dumps(report.to_dict(), indent=2, default=str))
        print(f"Wrote detailed report to {args.json}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
