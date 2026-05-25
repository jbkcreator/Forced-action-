"""
Lead Conversion Report — tier hit-rate analysis.

Measures what % of scored leads at each tier (Ultra Platinum → Bronze)
actually transacted after being scored, within a configurable lookback
window. Two proxies are reported side-by-side:

  event_rate  — deed transfer (sale_price >= $1,000, excluding intra-family
                 deed types) OR foreclosure filing. Filters mirror the same
                 exclusions the scoring engine applies at signal-collection
                 time (cds_engine.py:322), so the metric measures arms-length
                 transactions only — not nominal/quit-claim transfers that
                 fire on high-distress properties regardless of subscriber
                 effort.
  deal_rate   — subscriber-reported DealOutcome.property_id match (when
                 present). Ground truth for subscriber-actionable leads but
                 sample is small; presented alongside event_rate so the gap
                 between proxy and truth is visible.

Usage:
    python -m src.tasks.conversion_report                        # full window, all counties
    python -m src.tasks.conversion_report --days 60              # last 60 days only
    python -m src.tasks.conversion_report --county hillsborough
    python -m src.tasks.conversion_report --vertical wholesalers # top-vertical filter
    python -m src.tasks.conversion_report --csv                  # save to reports/conversion/
"""

import argparse
import csv
import logging
from datetime import date, datetime, timedelta
from pathlib import Path

from sqlalchemy import text

from src.core.database import get_db_context
from src.utils.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

TIER_ORDER = ["Ultra Platinum", "Platinum", "Gold", "Silver", "Bronze"]

REPORTS_DIR = Path("reports/conversion")


# ---------------------------------------------------------------------------
# Core query
# ---------------------------------------------------------------------------

# Intra-family / non-arms-length deed types to exclude from the event proxy.
# Matches anywhere in the upper-cased deed_type string. Belt-and-braces on
# top of the sale_price >= 1000 filter — catches nominal-price gifts when
# the price field is missing or zero-stuffed.
_INTRA_FAMILY_DEED_PATTERNS = (
    "QUIT CLAIM", "QUITCLAIM", "QUIT-CLAIM",
    "PERSONAL REPRESENTATIVE",
    "TRUSTEE",
    "LADY BIRD", "ENHANCED LIFE ESTATE",
)


def _run_conversion_query(
    session,
    since: date,
    county_id: str,
    vertical: str | None = None,
) -> list[dict]:
    """
    For each tier, return total leads scored since `since`, how many
    transacted after scoring (event proxy and deal proxy), and both rates.

    Event proxy filters mirror cds_engine.py:322 — same nominal/intra-family
    deeds that scoring excludes from signal collection are excluded here.

    Without this fix, UP leads (more probate / foreclosure activity → more
    quit-claim deeds → more nominal transfers) are double-counted as
    "conversions" unrelated to subscriber action, biasing the metric AGAINST
    high-distress tiers.

    `vertical`: optional top-vertical filter. When set, only properties whose
    highest-scoring vertical matches are included.
    """
    # Build the deed_type exclusion clause. Wrap in (NULL OR <pattern checks>)
    # so deeds with unknown deed_type are kept (don't exclude what we don't know)
    # rather than dropped by the LIKE returning NULL.
    _pattern_checks = " AND ".join(
        f"UPPER(d.deed_type) NOT LIKE '%{pat}%'"
        for pat in _INTRA_FAMILY_DEED_PATTERNS
    )
    intra_family_clause = f" AND (d.deed_type IS NULL OR ({_pattern_checks}))"

    rows = session.execute(text(f"""
        WITH scored AS (
            SELECT
                ds.property_id,
                MIN(ds.score_date)                                       AS first_scored_at,
                (ARRAY_AGG(ds.lead_tier      ORDER BY ds.score_date ASC))[1]  AS tier,
                (ARRAY_AGG(ds.vertical_scores ORDER BY ds.score_date DESC))[1] AS latest_vs
            FROM distress_scores ds
            JOIN properties p ON p.id = ds.property_id
            WHERE ds.score_date >= :since
              AND (:county = 'all' OR p.county_id = :county)
            GROUP BY ds.property_id
        ),
        scored_v AS (
            -- Annotate each property with its top-scoring vertical (from
            -- the most-recent vertical_scores JSONB).
            SELECT
                s.*,
                (
                    SELECT key
                    FROM jsonb_each_text(s.latest_vs)
                    WHERE value ~ '^-?[0-9]+(\\.[0-9]+)?$'
                    ORDER BY value::numeric DESC
                    LIMIT 1
                ) AS top_vertical
            FROM scored s
        ),
        scored_f AS (
            SELECT * FROM scored_v
            WHERE :vertical = 'all' OR top_vertical = :vertical
        ),
        deed_hits AS (
            -- Arms-length deeds only: mirror cds_engine.py:322 sale_price filter,
            -- plus exclude intra-family / non-arms-length deed types.
            SELECT DISTINCT s.property_id
            FROM scored_f s
            JOIN deeds d ON d.property_id = s.property_id
            WHERE d.record_date > s.first_scored_at
              AND (d.sale_price IS NULL OR d.sale_price >= 1000)
              {intra_family_clause}
        ),
        fc_hits AS (
            SELECT DISTINCT s.property_id
            FROM scored_f s
            JOIN foreclosures f ON f.property_id = s.property_id
            WHERE f.filing_date > s.first_scored_at
        ),
        deal_hits AS (
            -- Subscriber-reported ground truth. Sample is small; presented
            -- alongside event_rate so the gap is visible.
            -- NOTE: "do" is reserved in PostgreSQL (PL/pgSQL anonymous blocks),
            -- so alias as `dlo` not `do`.
            SELECT DISTINCT s.property_id
            FROM scored_f s
            JOIN deal_outcomes dlo ON dlo.property_id = s.property_id
            WHERE dlo.deal_date IS NULL OR dlo.deal_date >= s.first_scored_at
        ),
        any_hit AS (
            SELECT property_id FROM deed_hits
            UNION
            SELECT property_id FROM fc_hits
        )
        SELECT
            s.tier,
            COUNT(DISTINCT s.property_id)                              AS total,
            COUNT(DISTINCT h.property_id)                              AS transacted,
            COUNT(DISTINCT dh.property_id)                             AS deed_transfers,
            COUNT(DISTINCT fh.property_id)                             AS foreclosures,
            COUNT(DISTINCT dl.property_id)                             AS deal_outcomes,
            ROUND(
                COUNT(DISTINCT h.property_id)::numeric /
                NULLIF(COUNT(DISTINCT s.property_id), 0) * 100, 2
            )                                                          AS event_rate_pct,
            ROUND(
                COUNT(DISTINCT dl.property_id)::numeric /
                NULLIF(COUNT(DISTINCT s.property_id), 0) * 100, 2
            )                                                          AS deal_rate_pct
        FROM scored_f s
        LEFT JOIN any_hit   h  ON h.property_id  = s.property_id
        LEFT JOIN deed_hits dh ON dh.property_id = s.property_id
        LEFT JOIN fc_hits   fh ON fh.property_id = s.property_id
        LEFT JOIN deal_hits dl ON dl.property_id = s.property_id
        GROUP BY s.tier
    """), {
        "since":    since,
        "county":   county_id,
        "vertical": vertical or "all",
    }).fetchall()

    # Index by tier for ordered output
    by_tier = {r[0]: r for r in rows}
    result = []
    for tier in TIER_ORDER:
        if tier not in by_tier:
            continue
        r = by_tier[tier]
        result.append({
            "tier":           r[0],
            "total":          r[1],
            "transacted":     r[2],
            "deed_transfers": r[3],
            "foreclosures":   r[4],
            "deal_outcomes":  r[5],
            "event_rate_pct": float(r[6]) if r[6] is not None else 0.0,
            "deal_rate_pct":  float(r[7]) if r[7] is not None else 0.0,
            # Back-compat alias for existing callers / CSV consumers
            "hit_rate_pct":   float(r[6]) if r[6] is not None else 0.0,
        })
    return result


# ---------------------------------------------------------------------------
# Display
# ---------------------------------------------------------------------------

def _print_report(
    rows: list[dict], since: date, county_id: str, vertical: str | None = None,
) -> None:
    window_days = (date.today() - since).days
    print()
    print("=" * 88)
    print("  LEAD CONVERSION REPORT — tier hit-rate analysis (arms-length only)")
    print(f"  Window   : {since}  ->  {date.today()}  ({window_days} days)")
    print(f"  County   : {county_id}")
    if vertical:
        print(f"  Vertical : {vertical} (top-scoring vertical filter)")
    print("=" * 88)
    print(
        f"  {'Tier':<18} {'Total':>7} {'Events':>7} {'Deeds':>6} {'FC':>5} "
        f"{'Deals':>5} {'EventRate':>10} {'DealRate':>10}"
    )
    print("  " + "-" * 84)

    for r in rows:
        bar_len = int(r["event_rate_pct"] / 0.5)  # 1 char per 0.5%
        bar = "#" * min(bar_len, 18)
        print(
            f"  {r['tier']:<18} {r['total']:>7,} {r['transacted']:>7,} "
            f"{r['deed_transfers']:>6,} {r['foreclosures']:>5,} "
            f"{r['deal_outcomes']:>5,} "
            f"{r['event_rate_pct']:>9.2f}% {r['deal_rate_pct']:>9.2f}%  {bar}"
        )

    total_leads       = sum(r["total"]         for r in rows)
    total_transacted  = sum(r["transacted"]    for r in rows)
    total_deals       = sum(r["deal_outcomes"] for r in rows)
    overall_event_rate = round(total_transacted / total_leads * 100, 2) if total_leads else 0
    overall_deal_rate  = round(total_deals     / total_leads * 100, 2) if total_leads else 0
    print("  " + "-" * 84)
    print(
        f"  {'ALL TIERS':<18} {total_leads:>7,} {total_transacted:>7,} "
        f"{'':>6} {'':>5} {total_deals:>5,} "
        f"{overall_event_rate:>9.2f}% {overall_deal_rate:>9.2f}%"
    )
    print("=" * 88)
    print()

    # Model lift: compare top tier vs bottom tier on BOTH metrics
    if len(rows) >= 2:
        top = rows[0]
        bot = rows[-1]
        if bot["event_rate_pct"] > 0:
            lift = round(top["event_rate_pct"] / bot["event_rate_pct"], 2)
            print(f"  Event-rate lift : {top['tier']} = {lift}x {bot['tier']}  "
                  f"(values: {top['event_rate_pct']:.2f}% vs {bot['event_rate_pct']:.2f}%)")
        if bot["deal_rate_pct"] > 0:
            lift_d = round(top["deal_rate_pct"] / bot["deal_rate_pct"], 2)
            print(f"  Deal-rate  lift : {top['tier']} = {lift_d}x {bot['tier']}  "
                  f"(values: {top['deal_rate_pct']:.2f}% vs {bot['deal_rate_pct']:.2f}%)")
        if total_deals == 0:
            print("  (Deal-rate is 0 across the board — DealOutcome data is not populated for "
                  "this window. Event-rate is the only signal here; treat with caution.)")
        print()


# ---------------------------------------------------------------------------
# CSV export
# ---------------------------------------------------------------------------

def _save_csv(
    rows: list[dict], since: date, county_id: str, vertical: str | None = None,
) -> Path:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    suffix = f"_{vertical}" if vertical else ""
    fname = REPORTS_DIR / f"conversion_{since}_{date.today()}_{county_id}{suffix}.csv"
    with open(fname, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "tier", "total", "transacted",
            "deed_transfers", "foreclosures", "deal_outcomes",
            "event_rate_pct", "deal_rate_pct", "hit_rate_pct",
        ])
        writer.writeheader()
        writer.writerows(rows)
    logger.info(f"Saved: {fname}")
    return fname


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run_conversion_report(
    days: int = 0,
    county_id: str = "hillsborough",
    save_csv: bool = False,
    vertical: str | None = None,
) -> list[dict]:
    if days:
        since = date.today() - timedelta(days=days)
    else:
        # Default: full available window — earliest score date
        with get_db_context() as s:
            row = s.execute(text("SELECT MIN(score_date) FROM distress_scores")).fetchone()
            since = row[0].date() if row[0] else date.today()

    with get_db_context() as session:
        rows = _run_conversion_query(session, since, county_id, vertical=vertical)

    _print_report(rows, since, county_id, vertical=vertical)

    if save_csv:
        _save_csv(rows, since, county_id, vertical=vertical)

    return rows


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Lead conversion report by tier")
    parser.add_argument("--days",    type=int, default=0,
                        help="Lookback window in days (default: full history)")
    parser.add_argument("--county",  dest="county_id", default="hillsborough")
    parser.add_argument("--vertical", default=None,
                        help="Filter to properties whose top-scoring vertical matches "
                             "(e.g. wholesalers, fix_flip, restoration, roofing, "
                             "public_adjusters, attorneys)")
    parser.add_argument("--csv",     dest="save_csv", action="store_true",
                        help="Save output to reports/conversion/")
    args = parser.parse_args()

    run_conversion_report(
        days=args.days,
        county_id=args.county_id,
        save_csv=args.save_csv,
        vertical=args.vertical,
    )
