"""Task 4.8 — Competitor benchmark flag engine.

Pure classification of scraped competitor rate sheets against Forced Action's
own rate card. A competitor is a high-margin target when its terms are clearly
worse than ours (we can win the deal on price). No DB/network I/O here.

Three advantage axes (each = one way the competitor is worse than us):
  rate advantage  = competitor rate  >= ours + RATE_ADVANTAGE_BPS
  LTV advantage   = competitor maxLTV <= ours - LTV_ADVANTAGE_PTS
  term advantage  = competitor term   <= baseline - TERM_ADVANTAGE_MONTHS
                    (shorter term = worse for the borrower)

  strong target = >= 2 axes,  soft target = 1 axis,  none = 0.

Freshness: rows older than STALE_DAYS are marked stale and sorted below fresh
ones (down-ranked, not dropped). classify_target stays pure; staleness is
applied in compute_benchmark_report against an `as_of` date.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from typing import Optional

from sqlalchemy import text as sa_text

logger = logging.getLogger(__name__)

RATE_ADVANTAGE_BPS = 100      # competitor rate >= ours + 1.00%
LTV_ADVANTAGE_PTS = 5         # competitor max LTV <= ours - 5pts
TERM_BASELINE_MONTHS = 24     # our standard term; shorter competitor term = worse
TERM_ADVANTAGE_MONTHS = 6     # competitor term must be >= 6mo shorter to count
STALE_DAYS = 30              # captured_at older than this = stale, down-ranked


@dataclass
class CompetitorRow:
    lender_name: str
    product: str
    region: Optional[str]
    rate_low: Optional[float]
    max_ltv: Optional[float]
    # enrichment — captured when a source publishes them; engine ignores these.
    rate_high: Optional[float] = None
    points: Optional[float] = None
    min_fico: Optional[int] = None
    min_dscr: Optional[float] = None
    prepay: Optional[str] = None
    term_months: Optional[int] = None   # loan term; shorter = worse for borrower
    hq_location: Optional[str] = None   # lender HQ "City, ST" (≠ lending market)
    captured_at: Optional[object] = None   # datetime when scraped (freshness)


@dataclass
class OurTerms:
    product: str
    rate: float
    max_ltv: float


@dataclass
class TargetFlag:
    row: CompetitorRow
    rate_delta_bps: Optional[float]
    ltv_delta_pts: Optional[float]
    term_delta_months: Optional[float] = None   # baseline - their term; +ve = shorter
    status: str = "none"   # "none" | "soft" | "strong"
    stale: bool = False    # captured_at older than STALE_DAYS (set at report time)


def classify_target(
    row: CompetitorRow,
    ours: Optional[OurTerms],
    *,
    rate_bps: float = RATE_ADVANTAGE_BPS,
    ltv_pts: float = LTV_ADVANTAGE_PTS,
    term_baseline: int = TERM_BASELINE_MONTHS,
    term_margin: int = TERM_ADVANTAGE_MONTHS,
) -> TargetFlag:
    if ours is None:
        return TargetFlag(row, None, None, None, "none")
    rate_delta_bps = (
        round((row.rate_low - ours.rate) * 100, 4) if row.rate_low is not None else None
    )
    ltv_delta_pts = (
        round(ours.max_ltv - row.max_ltv, 4) if row.max_ltv is not None else None
    )
    term_delta_months = (
        term_baseline - row.term_months if row.term_months is not None else None
    )
    rate_adv = rate_delta_bps is not None and rate_delta_bps >= rate_bps
    ltv_adv = ltv_delta_pts is not None and ltv_delta_pts >= ltv_pts
    term_adv = term_delta_months is not None and term_delta_months >= term_margin
    n = sum((rate_adv, ltv_adv, term_adv))
    status = "strong" if n >= 2 else "soft" if n == 1 else "none"
    return TargetFlag(row, rate_delta_bps, ltv_delta_pts, term_delta_months, status)


@dataclass
class BenchmarkReport:
    targets: list[TargetFlag] = field(default_factory=list)   # high-margin only, sorted
    scanned: int = 0


# sort key: strong before soft, then biggest rate edge, then biggest LTV edge
_STATUS_RANK = {"strong": 0, "soft": 1}


def _is_stale(captured_at, as_of: date, stale_days: int) -> bool:
    if captured_at is None or as_of is None:
        return False
    cap = captured_at.date() if hasattr(captured_at, "date") else captured_at
    return (as_of - cap).days > stale_days


def compute_benchmark_report(
    rows: list[CompetitorRow],
    terms: dict[str, OurTerms],
    *,
    rate_bps: float = RATE_ADVANTAGE_BPS,
    ltv_pts: float = LTV_ADVANTAGE_PTS,
    as_of: Optional[date] = None,
    stale_days: int = STALE_DAYS,
) -> BenchmarkReport:
    flags = [
        classify_target(r, terms.get(r.product), rate_bps=rate_bps, ltv_pts=ltv_pts)
        for r in rows
    ]
    for f in flags:
        f.stale = _is_stale(f.row.captured_at, as_of, stale_days)
    targets = [f for f in flags if f.status != "none"]
    # strong before soft; fresh before stale; then biggest rate/LTV/term edge
    targets.sort(key=lambda f: (
        _STATUS_RANK[f.status], f.stale,
        -(f.rate_delta_bps or 0), -(f.ltv_delta_pts or 0), -(f.term_delta_months or 0),
    ))
    return BenchmarkReport(targets=targets, scanned=len(rows))


# ── Sweep (DB I/O glue) ─────────────────────────────────────────────────────

def load_our_terms(session) -> dict[str, OurTerms]:
    rows = session.execute(
        sa_text("SELECT product, rate, max_ltv FROM forced_action_lender_terms")
    ).mappings().all()
    return {
        r["product"]: OurTerms(r["product"], float(r["rate"]), float(r["max_ltv"]))
        for r in rows
    }


def load_competitor_rows(session) -> list[CompetitorRow]:
    """Latest captured row per (lender, product, region)."""
    rows = session.execute(
        sa_text(
            """
            SELECT DISTINCT ON (lender_name, product, region)
                   lender_name, product, region, rate_low, rate_high,
                   max_ltv, min_fico, points, term_months, captured_at
            FROM competitor_rate_sheets
            ORDER BY lender_name, product, region, captured_at DESC
            """
        )
    ).mappings().all()
    out = []
    for r in rows:
        out.append(CompetitorRow(
            lender_name=r["lender_name"],
            product=r["product"],
            region=r["region"],
            rate_low=float(r["rate_low"]) if r["rate_low"] is not None else None,
            max_ltv=float(r["max_ltv"]) if r["max_ltv"] is not None else None,
            rate_high=float(r["rate_high"]) if r["rate_high"] is not None else None,
            points=float(r["points"]) if r["points"] is not None else None,
            min_fico=r["min_fico"],
            term_months=r["term_months"],
            captured_at=r["captured_at"],
        ))
    return out


def _persist_row(session, adapter: dict, row: CompetitorRow) -> None:
    session.execute(
        sa_text(
            """
            INSERT INTO competitor_rate_sheets
                (lender_name, product, region, rate_low, rate_high, max_ltv,
                 min_fico, min_dscr, points, prepay, term_months, hq_location,
                 source_url, source_adapter, confidence, captured_at)
            VALUES
                (:lender_name, :product, :region, :rate_low, :rate_high, :max_ltv,
                 :min_fico, :min_dscr, :points, :prepay, :term_months, :hq_location,
                 :source_url, :source_adapter, :confidence, clock_timestamp())
            ON CONFLICT ON CONSTRAINT uq_competitor_rate_sheet DO NOTHING
            """
        ),
        {
            "lender_name": row.lender_name,
            "product": row.product,
            "region": row.region,
            "rate_low": row.rate_low,
            "rate_high": row.rate_high,
            "max_ltv": row.max_ltv,
            "min_fico": row.min_fico,
            "min_dscr": row.min_dscr,
            "points": row.points,
            "prepay": row.prepay,
            "term_months": row.term_months,
            "hq_location": row.hq_location,
            "source_url": adapter["source_url"],
            "source_adapter": adapter["name"],
            "confidence": adapter["confidence"],
        },
    )


def run_sweep(session, *, dry_run: bool = False, target: Optional[str] = None) -> dict:
    from config.competitor_benchmark import ADAPTERS

    adapters = [a for a in ADAPTERS if target is None or a["name"] == target]
    rows: list[CompetitorRow] = []
    errors: list[dict] = []
    for adapter in adapters:
        try:
            result = adapter["fetch"]()
        except Exception as exc:  # one bad site must not sink the sweep
            logger.warning("adapter %s failed: %s", adapter["name"], exc)
            errors.append({"adapter": adapter["name"], "error": str(exc)})
            continue
        fetched = result if isinstance(result, list) else [result]
        for row in fetched:
            rows.append(row)
            # ponytail: skip rows with no rate AND no ltv — they can never flag.
            if not dry_run and (row.rate_low is not None or row.max_ltv is not None):
                _persist_row(session, adapter, row)

    terms = load_our_terms(session)
    report = compute_benchmark_report(rows, terms)
    return {
        "dry_run": dry_run,
        "scanned": report.scanned,
        "errors": errors,
        "targets": [
            {
                "lender": t.row.lender_name,
                "product": t.row.product,
                "region": t.row.region,
                "status": t.status,
                "rate_delta_bps": t.rate_delta_bps,
                "ltv_delta_pts": t.ltv_delta_pts,
                "term_delta_months": t.term_delta_months,
                "stale": t.stale,
                "competitor_rate": t.row.rate_low,
                "competitor_max_ltv": t.row.max_ltv,
                "competitor_term_months": t.row.term_months,
            }
            for t in report.targets
        ],
    }
