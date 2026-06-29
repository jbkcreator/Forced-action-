"""Task 4.8 — Competitor benchmark & rate-sheet sweep (weekly cron).

Scrapes curated FL DSCR/private lender pages, persists each as a
competitor_rate_sheets row, and computes high-margin targets against Forced
Action's own rate card.

Usage:
    python -m src.tasks.competitor_benchmark_sweep [options]

Options:
    --dry-run         Scrape + classify but do NOT write rows (prints summary)
    --target NAME     Run a single adapter by name (see config ADAPTERS)
"""
from __future__ import annotations

import argparse
import json
import logging
from datetime import date
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from src.core.database import get_db_context
from src.services.competitor_benchmark import load_our_terms, run_sweep
from src.utils.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

REPORT_DIR = Path("reports/competitor_benchmark")
TEMPLATE_DIR = Path("src/templates")


def _write_report(result: dict, rate_card: dict) -> Path:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    env = Environment(loader=FileSystemLoader(str(TEMPLATE_DIR)), autoescape=True)
    html = env.get_template("competitor_benchmark.html").render(
        generated_on=date.today().isoformat(),
        scanned=result["scanned"],
        targets=result["targets"],
        errors=result["errors"],
        rate_card=rate_card,
    )
    out = REPORT_DIR / f"{date.today().isoformat()}.html"
    out.write_text(html, encoding="utf-8")
    logger.info("Report written: %s", out)
    return out


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Competitor benchmark & rate-sheet sweep",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--dry-run", action="store_true", help="Scrape without writing rows")
    parser.add_argument("--target", default=None, help="Run a single adapter by name")
    args = parser.parse_args()

    logger.info("Starting competitor_benchmark_sweep dry_run=%s target=%s", args.dry_run, args.target)

    with get_db_context() as session:
        result = run_sweep(session, dry_run=args.dry_run, target=args.target)
        rate_card = {
            p: {"rate": float(t.rate), "max_ltv": float(t.max_ltv), "points": None, "prepay": None}
            for p, t in load_our_terms(session).items()
        }
        if not args.dry_run:
            session.commit()

    _write_report(result, rate_card)
    logger.info("Sweep complete: scanned=%s targets=%s errors=%s",
                result["scanned"], len(result["targets"]), len(result["errors"]))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
