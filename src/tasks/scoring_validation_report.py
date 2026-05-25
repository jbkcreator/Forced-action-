"""
Scoring validation report (Stage E gate).

Compares shadow-rescored leads (`distress_scores_shadow`) against the live
table (`distress_scores`) to decide whether Stage F's cutover is safe.
Runs three checks and reports each as PASS / WARN / FAIL:

  1. Tier-distribution shift per county.
     Pinellas Ultra-Platinum % should drop from ~94% to single digits if the
     Stage D engine reframe + Stage C fitted weights are working. Hillsborough
     should stay broadly stable.

  2. Event-rate monotonicity per county.
     For each county, UP > Platinum > Gold > Silver > Bronze in 90-day
     event-rate (arms-length deed or foreclosure post-score). Inversion =
     fail; the new scoring isn't predictive of the outcome we care about.

  3. Cross-county Ultra-Platinum event-rate comparability.
     UP event rates across counties should fall inside ±20% relative of
     each other. If Pinellas UP converts at 0.5% while Hillsborough UP
     converts at 5%, the label still doesn't carry consistent meaning.

The report is read-only and safe to re-run. The shadow table can lag the
live table by however long it's been since the last shadow rescore — the
checks compare absolute distributions, not deltas.

Usage:
    python -m src.tasks.scoring_validation_report
    python -m src.tasks.scoring_validation_report --window-days 90
    python -m src.tasks.scoring_validation_report --json data/validation/<run>.json

Exit codes:
    0 — all checks PASS (Stage F cutover safe)
    1 — at least one check WARN (proceed with caution / review)
    2 — at least one check FAIL (do not cut over)
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass, asdict, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

from sqlalchemy import text

from src.core.database import get_db_context
from src.tasks.conversion_report import _INTRA_FAMILY_DEED_PATTERNS

logger = logging.getLogger(__name__)


# ── Configuration ────────────────────────────────────────────────────────────

DEFAULT_OUTCOME_WINDOW_DAYS = 90

TIER_ORDER = ["Ultra Platinum", "Platinum", "Gold", "Silver", "Bronze"]

# A Pinellas UP share above this means the structural fix didn't land.
# Hillsborough's UP share is the de-facto reference (under 10% in healthy
# distributions); 30% is a generous ceiling for a "healthy enough to ship" check.
TIER_DISTRIBUTION_WARN_UP_PCT = 15.0
TIER_DISTRIBUTION_FAIL_UP_PCT = 30.0

# Cross-county UP event-rate parity: relative difference allowed.
CROSS_COUNTY_WARN_REL = 0.20
CROSS_COUNTY_FAIL_REL = 0.50

STATUS_PASS = "PASS"
STATUS_WARN = "WARN"
STATUS_FAIL = "FAIL"


# Reused from conversion_report: arms-length deed exclusion clause.
_DEED_PATTERN_CLAUSE = " AND ".join(
    f"UPPER(d.deed_type) NOT LIKE '%{pat}%'" for pat in _INTRA_FAMILY_DEED_PATTERNS
)


@dataclass
class CheckResult:
    name:   str
    status: str
    detail: str
    data:   dict = field(default_factory=dict)


@dataclass
class ValidationReport:
    generated_at:        str
    window_days:         int
    checks:              list[CheckResult]
    overall_status:      str

    def to_dict(self) -> dict:
        return {
            "generated_at":   self.generated_at,
            "window_days":    self.window_days,
            "overall_status": self.overall_status,
            "checks":         [asdict(c) for c in self.checks],
        }


# ── Tier distribution check ──────────────────────────────────────────────────

def _tier_distribution_per_county(session) -> dict:
    """Return {county_id: {tier: count}} from the shadow table."""
    rows = session.execute(text(
        """
        SELECT s.county_id, s.lead_tier, COUNT(*) AS cnt
        FROM distress_scores_shadow s
        WHERE s.lead_tier IS NOT NULL
        GROUP BY s.county_id, s.lead_tier
        """
    )).fetchall()
    out: dict = {}
    for r in rows:
        out.setdefault(r.county_id, {})[r.lead_tier] = int(r.cnt)
    return out


def check_tier_distribution(session) -> CheckResult:
    """Pinellas UP% must be back in line with the rest of the population."""
    distribution = _tier_distribution_per_county(session)
    if not distribution:
        return CheckResult(
            name="tier_distribution_per_county",
            status=STATUS_WARN,
            detail="distress_scores_shadow is empty — run a shadow rescore first.",
        )

    per_county_up_pct: dict = {}
    failures: list[str] = []
    warnings: list[str] = []
    for cid, tiers in distribution.items():
        total = sum(tiers.values())
        if total == 0:
            continue
        up_pct = tiers.get("Ultra Platinum", 0) / total * 100
        per_county_up_pct[cid] = round(up_pct, 2)
        if up_pct >= TIER_DISTRIBUTION_FAIL_UP_PCT:
            failures.append(f"{cid}: UP={up_pct:.1f}%")
        elif up_pct >= TIER_DISTRIBUTION_WARN_UP_PCT:
            warnings.append(f"{cid}: UP={up_pct:.1f}%")

    if failures:
        status = STATUS_FAIL
        detail = (
            f"At least one county has Ultra-Platinum share > "
            f"{TIER_DISTRIBUTION_FAIL_UP_PCT}% in the shadow distribution: "
            + ", ".join(failures)
        )
    elif warnings:
        status = STATUS_WARN
        detail = (
            f"Ultra-Platinum share above {TIER_DISTRIBUTION_WARN_UP_PCT}% in: "
            + ", ".join(warnings)
        )
    else:
        status = STATUS_PASS
        detail = "Ultra-Platinum share within expected band in every county."

    return CheckResult(
        name="tier_distribution_per_county",
        status=status,
        detail=detail,
        data={"per_county_up_pct": per_county_up_pct, "distribution": distribution},
    )


# ── Event-rate monotonicity check ────────────────────────────────────────────

_EVENT_RATE_SQL = f"""
WITH scored AS (
    SELECT
        s.property_id,
        p.county_id,
        s.lead_tier,
        MIN(s.score_date) AS first_scored_at
    FROM distress_scores_shadow s
    JOIN properties p ON p.id = s.property_id
    WHERE s.lead_tier IS NOT NULL
    GROUP BY s.property_id, p.county_id, s.lead_tier
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
    sc.county_id,
    sc.lead_tier,
    COUNT(DISTINCT sc.property_id) AS total,
    COUNT(DISTINCT ah.property_id) AS transacted,
    ROUND(
        COUNT(DISTINCT ah.property_id)::numeric /
        NULLIF(COUNT(DISTINCT sc.property_id), 0) * 100, 2
    ) AS event_rate_pct
FROM scored sc
LEFT JOIN any_hit ah ON ah.property_id = sc.property_id
GROUP BY sc.county_id, sc.lead_tier
"""


def _event_rates_per_county(session, window_days: int) -> dict:
    rows = session.execute(
        text(_EVENT_RATE_SQL),
        {"window_days": window_days},
    ).fetchall()
    out: dict = {}
    for r in rows:
        out.setdefault(r.county_id, {})[r.lead_tier] = {
            "total":          int(r.total or 0),
            "transacted":     int(r.transacted or 0),
            "event_rate_pct": float(r.event_rate_pct or 0.0),
        }
    return out


def check_event_rate_monotonicity(session, window_days: int) -> CheckResult:
    """Per county, event-rate must descend from UP → Bronze."""
    per_county = _event_rates_per_county(session, window_days)
    if not per_county:
        return CheckResult(
            name="event_rate_monotonicity_per_county",
            status=STATUS_WARN,
            detail="No outcomes joined the shadow scores. Run a shadow rescore "
                   "and wait at least one outcome window before re-checking.",
        )

    inversions: list[str] = []
    per_county_summary: dict = {}
    for cid, tiers in per_county.items():
        rates = []
        for tier in TIER_ORDER:
            tier_data = tiers.get(tier)
            if not tier_data or tier_data["total"] == 0:
                continue
            rates.append((tier, tier_data["event_rate_pct"]))
        per_county_summary[cid] = rates
        # Walk pairwise — every higher tier should have a >= lower tier rate.
        for i in range(len(rates) - 1):
            higher_tier, higher_rate = rates[i]
            lower_tier,  lower_rate  = rates[i + 1]
            if higher_rate < lower_rate:
                inversions.append(
                    f"{cid}: {higher_tier} ({higher_rate:.2f}%) < "
                    f"{lower_tier} ({lower_rate:.2f}%)"
                )

    if inversions:
        status = STATUS_FAIL
        detail = "Event-rate inversion(s) detected: " + "; ".join(inversions)
    else:
        status = STATUS_PASS
        detail = "Every county shows monotonically decreasing event rates across tiers."

    return CheckResult(
        name="event_rate_monotonicity_per_county",
        status=status,
        detail=detail,
        data={"per_county_rates": per_county_summary, "raw": per_county},
    )


# ── Cross-county UP comparability check ──────────────────────────────────────

def check_cross_county_up_parity(session, window_days: int) -> CheckResult:
    """Ultra Platinum should mean the same conversion probability everywhere."""
    per_county = _event_rates_per_county(session, window_days)
    up_rates: dict = {}
    for cid, tiers in per_county.items():
        up = tiers.get("Ultra Platinum")
        if up and up["total"] >= 30:   # below 30 events the rate is noisy
            up_rates[cid] = up["event_rate_pct"]

    if len(up_rates) < 2:
        return CheckResult(
            name="cross_county_up_parity",
            status=STATUS_WARN,
            detail=(
                "Need at least 2 counties with ≥30 Ultra-Platinum leads to "
                f"compare. Currently have {len(up_rates)} eligible: {up_rates}"
            ),
            data={"up_rates": up_rates},
        )

    rates = list(up_rates.values())
    lo, hi = min(rates), max(rates)
    if lo <= 0:
        relative_spread = float("inf")
    else:
        relative_spread = (hi - lo) / lo

    if relative_spread >= CROSS_COUNTY_FAIL_REL:
        status = STATUS_FAIL
    elif relative_spread >= CROSS_COUNTY_WARN_REL:
        status = STATUS_WARN
    else:
        status = STATUS_PASS

    detail = (
        f"Ultra-Platinum event rates per county: {up_rates}. "
        f"Relative spread (max−min)/min = {relative_spread:.1%}."
    )
    return CheckResult(
        name="cross_county_up_parity",
        status=status,
        detail=detail,
        data={"up_rates": up_rates, "relative_spread": round(relative_spread, 3)},
    )


# ── Orchestrator ─────────────────────────────────────────────────────────────


def run_report(window_days: int) -> ValidationReport:
    with get_db_context() as session:
        checks = [
            check_tier_distribution(session),
            check_event_rate_monotonicity(session, window_days),
            check_cross_county_up_parity(session, window_days),
        ]

    if any(c.status == STATUS_FAIL for c in checks):
        overall = STATUS_FAIL
    elif any(c.status == STATUS_WARN for c in checks):
        overall = STATUS_WARN
    else:
        overall = STATUS_PASS

    return ValidationReport(
        generated_at=datetime.utcnow().isoformat() + "Z",
        window_days=window_days,
        checks=checks,
        overall_status=overall,
    )


def _print_report(report: ValidationReport) -> None:
    print()
    print("=" * 70)
    print("  CDS SCORING VALIDATION REPORT (Stage E gate)")
    print(f"  Generated: {report.generated_at}")
    print(f"  Window:    {report.window_days} days")
    print(f"  Overall:   {report.overall_status}")
    print("=" * 70)
    for c in report.checks:
        print(f"\n  [{c.status:4s}] {c.name}")
        print(f"         {c.detail}")
    print()


# ── CLI ──────────────────────────────────────────────────────────────────────

def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Stage E shadow validation report — gates Stage F cutover."
    )
    p.add_argument(
        "--window-days", type=int, default=DEFAULT_OUTCOME_WINDOW_DAYS,
        help=f"Outcome window in days (default {DEFAULT_OUTCOME_WINDOW_DAYS}).",
    )
    p.add_argument(
        "--json", type=Path, default=None, metavar="PATH",
        help="Write the full report (including per-county breakdowns) to this JSON path.",
    )
    return p.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    args = _parse_args(argv)
    report = run_report(args.window_days)
    _print_report(report)

    if args.json:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(json.dumps(report.to_dict(), indent=2, default=str))
        print(f"Wrote detailed report to {args.json}")

    if report.overall_status == STATUS_PASS:
        return 0
    if report.overall_status == STATUS_WARN:
        return 1
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
