"""
Weekly one-pager — executive snapshot of the platform's six core KPIs.

Sent every Monday at 09:30 UTC, after weekly_report. Renders an HTML email
covering:
  1. Total Gold+ inventory (with week-over-week delta)
  2. New Gold+ per vertical (this week vs last week)
  3. Scraper freshness (days since newest record per source)
  4. Match rate (this week vs last week, per-source breakdown)
  5. County coverage (Gold+ counts per county)
  6. A/B variant results (active tests, conversion rates)

Any KPI that has fallen for two consecutive weeks AND is now >10% below
two-weeks-ago triggers a remediation file at reports/remediation/YYYY-MM-DD.md
listing the specific suggested action for that metric.

Usage:
    python -m src.tasks.weekly_one_pager [county_id] [--dry-run]
"""

import argparse
import logging
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from sqlalchemy import func

from config.settings import get_settings
from src.core.database import get_db_context
from src.core.models import (
    DistressScore,
    Property,
    ScraperRunStats,
)
from src.services.email import send_alert
from src.tasks.daily_report import (
    GOLD_PLUS_TIERS,
    SCRAPER_ORDER,
    VERTICAL_DISPLAY,
    _build_signal_freshness,
)

logger = logging.getLogger(__name__)

REPORTS_DIR = Path("reports/remediation")
DOWNTREND_FLOOR = 0.10  # need >10% drop over 2 weeks to flag
GOLD_PLUS_TIER_LIST = list(GOLD_PLUS_TIERS)


# ── Remediation lookup ────────────────────────────────────────────────────────
# Action text shown in both the email and the remediation file when a metric
# trends down 2 weeks. Keep instructions specific and runnable.

REMEDIATION_ACTIONS: Dict[str, str] = {
    "total_gold_plus": (
        "Lower scoring threshold from 57 → 50 in `config/scoring.py` "
        "`LEAD_TIER_THRESHOLDS`, OR schedule the existing SunBiz scraper to "
        "add a new signal source. Run the SunBiz engine once manually to "
        "validate, then add a cron line in `scripts/cron/crontab.txt`."
    ),
    "match_rate": (
        "Lower the rapidfuzz threshold from 85 → 80 in `src/loaders/base.py` "
        "for owner-name fallback. Then run "
        "`python -m src.tasks.rematch_unmatched` to reprocess historical misses."
    ),
    "scraper_freshness": (
        "Check `logs/cron/<source>.log` and the status file at "
        "`logs/cron/status/<source>.status` for the named source. If portal is "
        "blocked, route via the admin upload layer (same pattern as "
        "tax_delinquencies) and disable the cron line."
    ),
    "ab_conversion": (
        "Inspect the losing variant in `ab_assignments` filtered by `outcome`. "
        "If the auto-rollback z-score check has fired, complete the test via "
        "`ab_engine.complete_test(test_name, winner='a')` and seed a new variant."
    ),
    "county_coverage": (
        "Run `python -m src.tasks.load_validator <county_id>` to identify the "
        "missing scraper. If a dormant county was activated, confirm cron lines "
        "pass `--county-id $COUNTY_ID`."
    ),
}


def _vertical_remediation(vertical_key: str) -> str:
    return (
        f"Inspect signal weights for vertical `{vertical_key}` in "
        f"`config/scoring.py`. Tune the top 3 weighted signals up by 5–10 "
        f"points if the upstream scrapers are healthy. Confirm via the daily "
        f"report's freshness section that the vertical's primary signals are "
        f"current."
    )


# ── Time window helpers ───────────────────────────────────────────────────────

def _iso_week_window(today: date, weeks_back: int) -> Tuple[datetime, datetime]:
    """
    Return (start, end) datetime range for the ISO week N weeks before `today`.
    weeks_back=0 = this week (Mon→Sun ending today).
    weeks_back=1 = previous week.
    """
    end_of_week = today - timedelta(days=today.weekday()) - timedelta(days=1)  # last Sunday
    end_of_week = end_of_week - timedelta(weeks=weeks_back - 1) if weeks_back > 0 else today
    start_of_week = end_of_week - timedelta(days=6)
    start_dt = datetime.combine(start_of_week, datetime.min.time(), tzinfo=timezone.utc)
    end_dt = datetime.combine(end_of_week, datetime.max.time(), tzinfo=timezone.utc)
    return start_dt, end_dt


# ── Metric computations ───────────────────────────────────────────────────────

def _total_gold_plus_at(session, county_id: str, snapshot_dt: datetime) -> int:
    """
    Total Gold+ inventory as of a given snapshot datetime — uses DISTINCT ON
    semantics (most recent score per property, filter to Gold+).
    """
    from sqlalchemy import text
    row = session.execute(
        text("""
            SELECT COUNT(*) FROM (
                SELECT DISTINCT ON (property_id) property_id, lead_tier
                FROM distress_scores
                WHERE county_id = :county_id
                  AND score_date <= :snapshot
                ORDER BY property_id, score_date DESC
            ) latest
            WHERE lead_tier = ANY(:tiers)
        """),
        {"county_id": county_id, "snapshot": snapshot_dt, "tiers": GOLD_PLUS_TIER_LIST},
    ).scalar_one_or_none()
    return int(row or 0)


def _new_gold_plus_per_vertical(
    session, county_id: str, week_start: datetime, week_end: datetime,
) -> Dict[str, int]:
    """
    Gold+ leads whose latest score landed within the week, bucketed by primary
    vertical (highest vertical_score). Returns {vertical_key: count}.
    """
    rows = (
        session.query(
            DistressScore.property_id,
            DistressScore.vertical_scores,
        )
        .filter(
            DistressScore.county_id == county_id,
            DistressScore.score_date >= week_start,
            DistressScore.score_date <= week_end,
            DistressScore.lead_tier.in_(GOLD_PLUS_TIER_LIST),
            DistressScore.qualified.is_(True),
        )
        .all()
    )

    # Dedupe to one row per property (latest score within window)
    seen: set = set()
    counts: Dict[str, int] = defaultdict(int)
    for prop_id, vs in rows:
        if prop_id in seen or not vs:
            continue
        seen.add(prop_id)
        best = max(vs, key=lambda k: vs.get(k, 0))
        if best in VERTICAL_DISPLAY:
            counts[best] += 1
    return dict(counts)


def _match_rate_for_week(
    session, county_id: str, week_start: datetime, week_end: datetime,
) -> Dict[str, dict]:
    """
    Per-source match rate aggregated over the week.
    Returns {source_type: {scraped, matched, rate}, "_overall": {...}}.
    """
    rows = (
        session.query(
            ScraperRunStats.source_type,
            func.sum(ScraperRunStats.total_scraped).label("scraped"),
            func.sum(ScraperRunStats.matched).label("matched"),
        )
        .filter(
            ScraperRunStats.county_id == county_id,
            ScraperRunStats.run_date >= week_start.date(),
            ScraperRunStats.run_date <= week_end.date(),
            ScraperRunStats.run_success.is_(True),
        )
        .group_by(ScraperRunStats.source_type)
        .all()
    )

    per_source: Dict[str, dict] = {}
    total_scraped = 0
    total_matched = 0
    for source, scraped, matched in rows:
        scraped = int(scraped or 0)
        matched = int(matched or 0)
        rate = (matched / scraped) if scraped else 0.0
        per_source[source] = {"scraped": scraped, "matched": matched, "rate": rate}
        total_scraped += scraped
        total_matched += matched

    per_source["_overall"] = {
        "scraped": total_scraped,
        "matched": total_matched,
        "rate": (total_matched / total_scraped) if total_scraped else 0.0,
    }
    return per_source


def _county_coverage(session, snapshot_dt: datetime) -> Dict[str, int]:
    """Gold+ count per county_id as of snapshot_dt."""
    from sqlalchemy import text
    rows = session.execute(
        text("""
            SELECT county_id, COUNT(*) FROM (
                SELECT DISTINCT ON (property_id) property_id, county_id, lead_tier
                FROM distress_scores
                WHERE score_date <= :snapshot
                ORDER BY property_id, score_date DESC
            ) latest
            WHERE lead_tier = ANY(:tiers)
            GROUP BY county_id
            ORDER BY COUNT(*) DESC
        """),
        {"snapshot": snapshot_dt, "tiers": GOLD_PLUS_TIER_LIST},
    ).fetchall()
    return {row[0]: int(row[1]) for row in rows}


def _ab_variant_results(session, week_start: datetime, week_end: datetime) -> List[dict]:
    """
    Active A/B tests with this week's per-variant conversion rate.
    Gracefully returns [] when the AbTest/AbAssignment models aren't present
    (writer code lives on phase-2-b/one and merges into dev EOD).
    """
    try:
        from src.core.models import AbAssignment, AbTest
    except ImportError:
        return []

    try:
        active_tests = (
            session.query(AbTest)
            .filter(AbTest.status == "active")
            .all()
        )
    except Exception:
        return []

    out: List[dict] = []
    for test in active_tests:
        assignments = (
            session.query(AbAssignment)
            .filter(
                AbAssignment.test_id == test.id,
                AbAssignment.created_at >= week_start,
                AbAssignment.created_at <= week_end,
            )
            .all()
        )
        a_total = sum(1 for a in assignments if a.variant == "a")
        b_total = sum(1 for a in assignments if a.variant == "b")
        a_conv = sum(1 for a in assignments if a.variant == "a" and a.outcome == "converted")
        b_conv = sum(1 for a in assignments if a.variant == "b" and a.outcome == "converted")
        out.append({
            "test_name": test.test_name,
            "segment": test.segment,
            "a_total": a_total,
            "b_total": b_total,
            "a_rate": (a_conv / a_total) if a_total else 0.0,
            "b_rate": (b_conv / b_total) if b_total else 0.0,
            "winner": (
                "A" if a_total >= 30 and b_total >= 30 and a_conv / a_total > b_conv / b_total
                else "B" if a_total >= 30 and b_total >= 30 and b_conv / b_total > a_conv / a_total
                else "—"
            ),
        })
    return out


# ── Trend detection ───────────────────────────────────────────────────────────

def _is_downtrend(this_week: float, last_week: float, two_weeks_ago: float) -> bool:
    """
    Flag a metric if values monotonically decreased AND the cumulative drop
    exceeds DOWNTREND_FLOOR (default 10%). Skip when the baseline is too small.
    """
    if two_weeks_ago < 5:  # noisy at low volume
        return False
    if not (two_weeks_ago > last_week > this_week):
        return False
    drop = (two_weeks_ago - this_week) / two_weeks_ago
    return drop > DOWNTREND_FLOOR


def _detect_downtrends(
    weeks: Dict[str, dict],   # {"this": payload, "last": payload, "prior": payload}
) -> List[dict]:
    """
    Returns list of {metric, this, last, prior, drop_pct, action} for any
    metric that fell for 2 consecutive weeks beyond the floor.
    """
    flagged: List[dict] = []

    # Total Gold+
    a, b, c = weeks["this"]["total"], weeks["last"]["total"], weeks["prior"]["total"]
    if _is_downtrend(a, b, c):
        flagged.append({
            "metric": "Total Gold+",
            "this_week": a,
            "last_week": b,
            "prior_week": c,
            "drop_pct": (c - a) / c if c else 0,
            "action": REMEDIATION_ACTIONS["total_gold_plus"],
        })

    # Match rate
    a = weeks["this"]["match"]["_overall"]["rate"]
    b = weeks["last"]["match"]["_overall"]["rate"]
    c = weeks["prior"]["match"]["_overall"]["rate"]
    if _is_downtrend(a * 100, b * 100, c * 100):
        flagged.append({
            "metric": "Overall Match Rate",
            "this_week": f"{a*100:.1f}%",
            "last_week": f"{b*100:.1f}%",
            "prior_week": f"{c*100:.1f}%",
            "drop_pct": (c - a) / c if c else 0,
            "action": REMEDIATION_ACTIONS["match_rate"],
        })

    # Per-vertical Gold+
    for vkey, vlabel in VERTICAL_DISPLAY.items():
        a = weeks["this"]["verticals"].get(vkey, 0)
        b = weeks["last"]["verticals"].get(vkey, 0)
        c = weeks["prior"]["verticals"].get(vkey, 0)
        if _is_downtrend(a, b, c):
            flagged.append({
                "metric": f"New Gold+ — {vlabel}",
                "this_week": a,
                "last_week": b,
                "prior_week": c,
                "drop_pct": (c - a) / c if c else 0,
                "action": _vertical_remediation(vkey),
            })

    return flagged


# ── Output: remediation file ──────────────────────────────────────────────────

def _write_remediation_file(trends: List[dict], today: date) -> Optional[Path]:
    if not trends:
        return None
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = REPORTS_DIR / f"{today.isoformat()}.md"

    lines = [
        f"# Remediation Plan — {today.isoformat()}",
        "",
        f"{len(trends)} metric(s) trending down 2+ consecutive weeks. "
        f"Acknowledge and act this week.",
        "",
    ]
    for t in trends:
        lines.extend([
            f"## {t['metric']}",
            "",
            f"- **2 weeks ago:** {t['prior_week']}",
            f"- **Last week:**   {t['last_week']}",
            f"- **This week:**   {t['this_week']}",
            f"- **Drop:**        {t['drop_pct']*100:.1f}%",
            "",
            f"**Suggested action:** {t['action']}",
            "",
            "---",
            "",
        ])
    path.write_text("\n".join(lines), encoding="utf-8")
    logger.warning("[OnePager] Remediation file written: %s", path)
    return path


# ── Output: HTML rendering ────────────────────────────────────────────────────

_BASE_STYLE = (
    "font-family: -apple-system, Segoe UI, Roboto, sans-serif; "
    "color: #111; max-width: 760px; margin: 0 auto; padding: 24px;"
)
_TABLE_STYLE = "border-collapse: collapse; width: 100%; margin: 12px 0; font-size: 14px;"
_TH_STYLE = "text-align: left; padding: 8px; background: #f3f4f6; border-bottom: 2px solid #d1d5db;"
_TD_STYLE = "padding: 8px; border-bottom: 1px solid #e5e7eb;"


def _delta_cell(this_val: float, last_val: float, *, as_pct: bool = False) -> str:
    if last_val == 0:
        return '<td style="' + _TD_STYLE + '">—</td>'
    delta = this_val - last_val
    pct = (delta / last_val) * 100
    color = "#15803d" if delta > 0 else "#b91c1c" if delta < 0 else "#6b7280"
    sign = "+" if delta >= 0 else ""
    if as_pct:
        return f'<td style="{_TD_STYLE} color:{color};">{sign}{pct:.1f}%</td>'
    return f'<td style="{_TD_STYLE} color:{color};">{sign}{int(delta)} ({sign}{pct:.0f}%)</td>'


def _render_html(weeks: Dict[str, dict], freshness: dict, coverage: dict,
                 ab: List[dict], trends: List[dict], today: date,
                 county_id: str, remediation_path: Optional[Path]) -> str:
    this = weeks["this"]
    last = weeks["last"]

    parts: List[str] = [
        f'<div style="{_BASE_STYLE}">',
        f"<h1 style='margin:0 0 4px 0;'>Forced Action — Weekly One-Pager</h1>",
        f"<div style='color:#6b7280; font-size:13px;'>"
        f"County: <b>{county_id}</b> &middot; Generated {today.isoformat()}</div>",
    ]

    # Trend banner
    if trends:
        parts.append(
            f"<div style='margin:16px 0; padding:12px 14px; background:#fef2f2; "
            f"border-left:4px solid #b91c1c; color:#7f1d1d;'>"
            f"<b>{len(trends)} metric(s) trending down 2 weeks.</b> "
            f"Remediation actions for each metric are in the attached "
            f"<code>{today.isoformat()}.md</code>.</div>"
        )
    else:
        parts.append(
            f"<div style='margin:16px 0; padding:12px 14px; background:#f0fdf4; "
            f"border-left:4px solid #15803d; color:#14532d;'>"
            f"All KPIs stable or improving — no remediation required this week.</div>"
        )

    # 1. Total Gold+
    parts.append(
        f"<h2 style='margin-top:24px;'>1. Total Gold+ Inventory</h2>"
        f"<table style='{_TABLE_STYLE}'>"
        f"<tr><th style='{_TH_STYLE}'>This week</th>"
        f"<th style='{_TH_STYLE}'>Last week</th>"
        f"<th style='{_TH_STYLE}'>Δ vs last week</th></tr>"
        f"<tr><td style='{_TD_STYLE} font-size:18px; font-weight:600;'>{this['total']}</td>"
        f"<td style='{_TD_STYLE}'>{last['total']}</td>"
        f"{_delta_cell(this['total'], last['total'])}"
        f"</tr></table>"
    )

    # 2. New Gold+ per vertical
    parts.append(f"<h2>2. New Gold+ This Week — by Vertical</h2><table style='{_TABLE_STYLE}'>")
    parts.append(
        f"<tr><th style='{_TH_STYLE}'>Vertical</th>"
        f"<th style='{_TH_STYLE}'>This week</th>"
        f"<th style='{_TH_STYLE}'>Last week</th>"
        f"<th style='{_TH_STYLE}'>Δ</th></tr>"
    )
    for vkey, vlabel in VERTICAL_DISPLAY.items():
        a = this["verticals"].get(vkey, 0)
        b = last["verticals"].get(vkey, 0)
        parts.append(
            f"<tr><td style='{_TD_STYLE}'>{vlabel}</td>"
            f"<td style='{_TD_STYLE}'>{a}</td>"
            f"<td style='{_TD_STYLE}'>{b}</td>"
            f"{_delta_cell(a, b)}</tr>"
        )
    parts.append("</table>")

    # 3. Scraper freshness
    parts.append(f"<h2>3. Scraper Freshness</h2><table style='{_TABLE_STYLE}'>")
    parts.append(
        f"<tr><th style='{_TH_STYLE}'>Source</th>"
        f"<th style='{_TH_STYLE}'>Days since newest record</th></tr>"
    )
    for source in SCRAPER_ORDER:
        days = freshness.get(source)
        if days is None:
            cell = "<span style='color:#6b7280;'>no data</span>"
        elif days >= 7:
            cell = f"<span style='color:#b91c1c; font-weight:600;'>{days} d</span>"
        elif days >= 3:
            cell = f"<span style='color:#b45309;'>{days} d</span>"
        else:
            cell = f"<span style='color:#15803d;'>{days} d</span>"
        parts.append(f"<tr><td style='{_TD_STYLE}'>{source}</td><td style='{_TD_STYLE}'>{cell}</td></tr>")
    parts.append("</table>")

    # 4. Match rate
    overall_this = this["match"]["_overall"]
    overall_last = last["match"]["_overall"]
    parts.append(
        f"<h2>4. Match Rate</h2>"
        f"<p>Overall: <b>{overall_this['rate']*100:.1f}%</b> "
        f"({overall_this['matched']:,} / {overall_this['scraped']:,}) "
        f"vs last week {overall_last['rate']*100:.1f}%</p>"
        f"<table style='{_TABLE_STYLE}'>"
        f"<tr><th style='{_TH_STYLE}'>Source</th>"
        f"<th style='{_TH_STYLE}'>Scraped</th>"
        f"<th style='{_TH_STYLE}'>Matched</th>"
        f"<th style='{_TH_STYLE}'>Match %</th></tr>"
    )
    per_source = {k: v for k, v in this["match"].items() if k != "_overall"}
    for source in sorted(per_source.keys()):
        s = per_source[source]
        rate_color = "#b91c1c" if s["rate"] < 0.80 else "#b45309" if s["rate"] < 0.90 else "#15803d"
        parts.append(
            f"<tr><td style='{_TD_STYLE}'>{source}</td>"
            f"<td style='{_TD_STYLE}'>{s['scraped']:,}</td>"
            f"<td style='{_TD_STYLE}'>{s['matched']:,}</td>"
            f"<td style='{_TD_STYLE} color:{rate_color};'>{s['rate']*100:.1f}%</td></tr>"
        )
    parts.append("</table>")

    # 5. County coverage
    parts.append(f"<h2>5. County Coverage</h2><table style='{_TABLE_STYLE}'>")
    parts.append(
        f"<tr><th style='{_TH_STYLE}'>County</th>"
        f"<th style='{_TH_STYLE}'>Gold+ Leads</th></tr>"
    )
    if coverage:
        for cname, ccount in coverage.items():
            parts.append(
                f"<tr><td style='{_TD_STYLE}'>{cname}</td>"
                f"<td style='{_TD_STYLE}'>{ccount:,}</td></tr>"
            )
    else:
        parts.append(f"<tr><td colspan='2' style='{_TD_STYLE} color:#6b7280;'>No county data</td></tr>")
    parts.append("</table>")

    # 6. A/B variant results
    parts.append("<h2>6. A/B Variant Results — This Week</h2>")
    if ab:
        parts.append(f"<table style='{_TABLE_STYLE}'>")
        parts.append(
            f"<tr><th style='{_TH_STYLE}'>Test</th>"
            f"<th style='{_TH_STYLE}'>Segment</th>"
            f"<th style='{_TH_STYLE}'>A (n / rate)</th>"
            f"<th style='{_TH_STYLE}'>B (n / rate)</th>"
            f"<th style='{_TH_STYLE}'>Leader</th></tr>"
        )
        for t in ab:
            parts.append(
                f"<tr><td style='{_TD_STYLE}'>{t['test_name']}</td>"
                f"<td style='{_TD_STYLE}'>{t['segment'] or '—'}</td>"
                f"<td style='{_TD_STYLE}'>{t['a_total']} / {t['a_rate']*100:.1f}%</td>"
                f"<td style='{_TD_STYLE}'>{t['b_total']} / {t['b_rate']*100:.1f}%</td>"
                f"<td style='{_TD_STYLE} font-weight:600;'>{t['winner']}</td></tr>"
            )
        parts.append("</table>")
    else:
        parts.append(
            f"<p style='color:#6b7280;'>No active A/B tests this week. "
            f"(MessageOutcome / AbTest writer code lands when phase-2-b/one merges.)</p>"
        )

    # Stage 5 — Top Referrers leaderboard injected from latest snapshot
    parts.append(_render_leaderboard_html())

    parts.append("</div>")
    return "\n".join(parts)


def _render_leaderboard_html() -> str:
    """Render the latest referral leaderboard as a section. Empty on missing data."""
    try:
        from src.tasks.leaderboard import latest_snapshot
    except Exception:
        return ""
    snap = latest_snapshot()
    if not snap or not snap.get("leaderboards"):
        return ""

    parts = ["<h2 style='margin-top:24px;'>Top Referrers — last 7 days</h2>"]
    for board in snap["leaderboards"][:5]:
        title = f"{board['county_id'].title()} · {board['vertical'].replace('_', ' ').title()}"
        parts.append(f"<h3 style='margin:14px 0 6px 0;font-size:14px;color:#374151;'>{title}</h3>")
        parts.append(f"<table style='{_TABLE_STYLE}'>")
        parts.append(
            f"<tr><th style='{_TH_STYLE}'>#</th>"
            f"<th style='{_TH_STYLE}'>Member</th>"
            f"<th style='{_TH_STYLE}'>This week</th>"
            f"<th style='{_TH_STYLE}'>Lifetime</th></tr>"
        )
        for row in board.get("leaderboard", []):
            badge = f" <span style='font-size:11px;color:#6b7280;'>({row['badge']})</span>" if row.get("badge") else ""
            parts.append(
                f"<tr><td style='{_TD_STYLE}'>{row['rank']}</td>"
                f"<td style='{_TD_STYLE}'>{row['handle']}{badge}</td>"
                f"<td style='{_TD_STYLE}'>{row['refs_this_week']}</td>"
                f"<td style='{_TD_STYLE}'>{row['refs_total']}</td></tr>"
            )
        parts.append("</table>")
    return "\n".join(parts)


def _render_text(weeks: Dict[str, dict], today: date, county_id: str,
                 trends: List[dict]) -> str:
    """Plain-text fallback for clients that don't render HTML."""
    this = weeks["this"]
    last = weeks["last"]
    lines = [
        f"Forced Action — Weekly One-Pager — {today.isoformat()} — county={county_id}",
        "",
        f"1. Total Gold+: {this['total']} (last week {last['total']})",
        "",
        "2. New Gold+ this week by vertical:",
    ]
    for vkey, vlabel in VERTICAL_DISPLAY.items():
        lines.append(f"   {vlabel}: {this['verticals'].get(vkey, 0)}  "
                     f"(last week {last['verticals'].get(vkey, 0)})")
    overall = this["match"]["_overall"]
    lines.extend([
        "",
        f"3-4. Match rate: {overall['rate']*100:.1f}% "
        f"({overall['matched']:,}/{overall['scraped']:,})",
        "",
    ])
    if trends:
        lines.append(f"!! {len(trends)} metric(s) trending down 2 weeks — "
                     f"see attached {today.isoformat()}.md")
    else:
        lines.append("All KPIs stable.")
    return "\n".join(lines)


# ── Orchestrator ──────────────────────────────────────────────────────────────

def run_weekly_one_pager(county_id: str = "hillsborough", dry_run: bool = False) -> dict:
    today = date.today()
    settings = get_settings()

    # Compute three weeks of payloads
    weeks: Dict[str, dict] = {}
    with get_db_context() as session:
        for label, weeks_back in (("this", 0), ("last", 1), ("prior", 2)):
            start, end = _iso_week_window(today, weeks_back)
            weeks[label] = {
                "start": start,
                "end": end,
                "total": _total_gold_plus_at(session, county_id, end),
                "verticals": _new_gold_plus_per_vertical(session, county_id, start, end),
                "match": _match_rate_for_week(session, county_id, start, end),
            }

        # Single-snapshot metrics (don't need 3 weeks)
        freshness = _build_signal_freshness(session, county_id)
        coverage = _county_coverage(session, weeks["this"]["end"])
        ab = _ab_variant_results(session, weeks["this"]["start"], weeks["this"]["end"])

    trends = _detect_downtrends(weeks)
    remediation_path = _write_remediation_file(trends, today) if not dry_run else None

    html = _render_html(weeks, freshness, coverage, ab, trends, today, county_id, remediation_path)
    text = _render_text(weeks, today, county_id, trends)

    subject = (
        f"[Forced Action] Weekly One-Pager — {today.isoformat()}"
        + (f" — {len(trends)} metric(s) trending down" if trends else "")
    )

    recipient = settings.report_recipients or settings.alert_email
    if not recipient:
        logger.warning("[OnePager] No REPORT_RECIPIENTS / ALERT_EMAIL configured — skipping send")
        sent = False
    elif dry_run:
        logger.warning("[OnePager] DRY RUN — would send to %s, subject=%s", recipient, subject)
        # Write the rendered HTML to disk for inspection during dry runs
        debug_path = REPORTS_DIR / f"{today.isoformat()}_dryrun.html"
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        debug_path.write_text(html, encoding="utf-8")
        logger.warning("[OnePager] Dry-run HTML written to %s", debug_path)
        sent = False
    else:
        # Single recipient or comma-separated list — send to each
        recipients = [r.strip() for r in recipient.split(",") if r.strip()]
        attachments = [remediation_path] if remediation_path else None
        sent = False
        for r in recipients:
            ok = send_alert(
                subject=subject,
                body=text,
                html_body=html,
                to=r,
                attachments=attachments,
            )
            sent = ok or sent

        # Clean up the remediation file after a successful send. The action
        # text is already attached to the email and the metric values are
        # captured in the email body — keeping the file around just creates
        # uncertainty about whether it's been actioned.
        if sent and remediation_path and remediation_path.exists():
            try:
                remediation_path.unlink()
                logger.warning("[OnePager] Remediation file deleted after send: %s",
                               remediation_path)
            except OSError as exc:
                logger.warning("[OnePager] Could not delete remediation file %s: %s",
                               remediation_path, exc)

    return {
        "date": str(today),
        "county_id": county_id,
        "total_gold_plus": weeks["this"]["total"],
        "trends_flagged": len(trends),
        "remediation_file": str(remediation_path) if remediation_path else None,
        "sent": sent,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Weekly executive one-pager")
    parser.add_argument("county_id", nargs="?", default="hillsborough")
    parser.add_argument("--dry-run", action="store_true",
                        help="Compute and render without sending; write HTML to disk")
    args = parser.parse_args()

    result = run_weekly_one_pager(county_id=args.county_id, dry_run=args.dry_run)
    print(result)
