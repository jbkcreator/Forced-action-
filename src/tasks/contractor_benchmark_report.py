"""
Stage 12 — Contractor Benchmark Report Task.

Generates per-contractor benchmark reports in two formats:
  CSV  — one row per contractor; importable into GHL / AP upsell workflow
  PDF  — printable benchmark card per trade/county group (Jinja2 + Playwright)

Both outputs are written to reports/contractor_benchmark/ with a 30-day
retention window (same pattern as daily_dashboard.py).

Usage:
    python -m src.tasks.contractor_benchmark_report
    python -m src.tasks.contractor_benchmark_report --dry-run
    python -m src.tasks.contractor_benchmark_report --window 60
    python -m src.tasks.contractor_benchmark_report --vertical roofing
    python -m src.tasks.contractor_benchmark_report --county hillsborough
    python -m src.tasks.contractor_benchmark_report --csv-only
    python -m src.tasks.contractor_benchmark_report --pdf-only

Cron: 0 11 * * 1  (Monday 11:00 UTC, after weekly one-pager at 09:30)
"""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from jinja2 import Environment, FileSystemLoader

from config.settings import get_settings
from src.core.database import get_db_context
from src.services.contractor_benchmark import (
    BenchmarkReport,
    ContractorMetrics,
    GroupBenchmark,
    compute_benchmark_report,
)

logger = logging.getLogger(__name__)

REPORT_DIR = Path("reports/contractor_benchmark")
TEMPLATE_DIR = Path("src/templates")
RETENTION_DAYS = 30


# ── CSV export ────────────────────────────────────────────────────────────────

CSV_FIELDNAMES = [
    "subscriber_id",
    "name",
    "email",
    "vertical",
    "county_id",
    "tier",
    "plan_price",
    "revenue_signal_score",
    "total_leads",
    "closed_deals",
    "close_rate_pct",
    "avg_days_to_close",
    "avg_deal_size",
    "total_revenue",
    "sms_reply_rate_pct",
    "benchmark_close_rate_pct",
    "close_rate_vs_benchmark_pct",
    "benchmark_status",
    "ap_upsell_candidate",
    "ap_pro_candidate",
]


def _contractor_to_csv_row(c: ContractorMetrics) -> dict:
    return {
        "subscriber_id":             c.subscriber_id,
        "name":                      c.name,
        "email":                     c.email,
        "vertical":                  c.vertical,
        "county_id":                 c.county_id,
        "tier":                      c.tier,
        "plan_price":                f"{c.plan_price:.2f}",
        "revenue_signal_score":      c.revenue_signal_score,
        "total_leads":               c.total_leads,
        "closed_deals":              c.closed_deals,
        "close_rate_pct":            f"{c.close_rate * 100:.1f}",
        "avg_days_to_close":         f"{c.avg_days_to_close:.1f}",
        "avg_deal_size":             f"{c.avg_deal_size:.2f}",
        "total_revenue":             f"{c.total_revenue:.2f}",
        "sms_reply_rate_pct":        f"{c.sms_reply_rate * 100:.1f}",
        "benchmark_close_rate_pct":  f"{c.benchmark_close_rate * 100:.1f}",
        "close_rate_vs_benchmark_pct": f"{c.close_rate_vs_benchmark * 100:.1f}",
        "benchmark_status":          c.benchmark_status,
        "ap_upsell_candidate":       "yes" if c.ap_upsell_candidate else "no",
        "ap_pro_candidate":          "yes" if c.ap_pro_candidate else "no",
    }


def write_csv(report: BenchmarkReport, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        for c in report.contractors:
            writer.writerow(_contractor_to_csv_row(c))
    logger.info("[benchmark-report] CSV written: %s (%d rows)", output_path, len(report.contractors))
    return output_path


# ── PDF export ────────────────────────────────────────────────────────────────

def _build_pdf_context(report: BenchmarkReport) -> dict:
    """Build the Jinja2 template context from a BenchmarkReport."""
    # Group contractors by (vertical, county_id) for section rendering
    sections = []
    grouped: dict[tuple, list[ContractorMetrics]] = {}
    for c in report.contractors:
        grouped.setdefault((c.vertical, c.county_id), []).append(c)

    benchmark_map = {(b.vertical, b.county_id): b for b in report.benchmarks}

    for (vertical, county_id), members in sorted(grouped.items()):
        benchmark = benchmark_map.get((vertical, county_id))
        sections.append({
            "vertical": vertical,
            "county_id": county_id,
            "benchmark": benchmark,
            "contractors": sorted(members, key=lambda x: x.close_rate, reverse=True),
        })

    return {
        "report_date": report.generated_at.strftime("%B %d, %Y"),
        "window_days": report.window_days,
        "total_contractors": len(report.contractors),
        "total_groups": len(report.benchmarks),
        "below_benchmark_count": len(report.below_benchmark),
        "ap_upsell_count": len(report.ap_upsell_candidates),
        "ap_pro_count": len(report.ap_pro_candidates),
        "sections": sections,
        "ap_upsell_candidates": sorted(
            report.ap_upsell_candidates,
            key=lambda x: (x.vertical, x.close_rate_vs_benchmark),
        ),
        "ap_pro_candidates": sorted(
            report.ap_pro_candidates,
            key=lambda x: x.close_rate,
            reverse=True,
        ),
    }


async def _render_pdf(html: str, output_path: Path) -> None:
    from playwright.async_api import async_playwright
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()
        await page.set_content(html, wait_until="networkidle")
        await page.pdf(
            path=str(output_path),
            format="A4",
            print_background=True,
            margin={"top": "12mm", "bottom": "12mm", "left": "10mm", "right": "10mm"},
        )
        await browser.close()


def write_pdf(report: BenchmarkReport, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    env = Environment(loader=FileSystemLoader(str(TEMPLATE_DIR)), autoescape=True)
    template = env.get_template("contractor_benchmark.html")
    context = _build_pdf_context(report)
    html = template.render(**context)
    asyncio.run(_render_pdf(html, output_path))
    logger.info("[benchmark-report] PDF written: %s", output_path)
    return output_path


# ── Retention cleanup ─────────────────────────────────────────────────────────

def _purge_old_reports() -> None:
    if not REPORT_DIR.exists():
        return
    cutoff = datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)
    for f in REPORT_DIR.glob("*.*"):
        if datetime.fromtimestamp(f.stat().st_mtime, tz=timezone.utc) < cutoff:
            f.unlink()
            logger.info("[benchmark-report] purged stale file: %s", f.name)


# ── Main runner ───────────────────────────────────────────────────────────────

def run_benchmark_report(
    *,
    window_days: int = 90,
    vertical_filter: Optional[str] = None,
    county_filter: Optional[str] = None,
    csv_only: bool = False,
    pdf_only: bool = False,
    dry_run: bool = False,
) -> dict:
    today = date.today().isoformat()
    suffix = f"_{vertical_filter}" if vertical_filter else ""
    csv_path = REPORT_DIR / f"{today}_contractor_benchmark{suffix}.csv"
    pdf_path = REPORT_DIR / f"{today}_contractor_benchmark{suffix}.pdf"

    verticals = (vertical_filter,) if vertical_filter else None

    with get_db_context() as db:
        report = compute_benchmark_report(
            db,
            window_days=window_days,
            vertical_filter=verticals,
            county_filter=county_filter,
        )

    summary = {
        "generated_at": report.generated_at.isoformat(),
        "window_days": window_days,
        "total_contractors": len(report.contractors),
        "total_groups": len(report.benchmarks),
        "below_benchmark": len(report.below_benchmark),
        "ap_upsell_candidates": len(report.ap_upsell_candidates),
        "ap_pro_candidates": len(report.ap_pro_candidates),
        "csv_path": str(csv_path),
        "pdf_path": str(pdf_path),
        "dry_run": dry_run,
    }

    if dry_run:
        logger.info("[benchmark-report] dry-run: %s", json.dumps(summary))
        return summary

    if not pdf_only:
        write_csv(report, csv_path)

    if not csv_only:
        try:
            write_pdf(report, pdf_path)
        except Exception:
            logger.exception("[benchmark-report] PDF generation failed — CSV still written")
            summary["pdf_error"] = "see logs"

    _purge_old_reports()
    logger.info("[benchmark-report] complete: %s", json.dumps(summary))
    return summary


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    import argparse
    parser = argparse.ArgumentParser(description="Generate contractor benchmark reports")
    parser.add_argument("--window", type=int, default=90, help="Look-back days (default 90)")
    parser.add_argument("--vertical", help="Filter to one vertical (e.g. roofing)")
    parser.add_argument("--county", help="Filter to one county_id")
    parser.add_argument("--csv-only", action="store_true")
    parser.add_argument("--pdf-only", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv or sys.argv[1:])

    result = run_benchmark_report(
        window_days=args.window,
        vertical_filter=args.vertical,
        county_filter=args.county,
        csv_only=args.csv_only,
        pdf_only=args.pdf_only,
        dry_run=args.dry_run,
    )
    if args.dry_run:
        print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
