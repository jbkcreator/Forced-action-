"""
Enrichment queue — M5 (waterfall).

Runs a multi-provider skip trace waterfall: BatchData → IDI → PDL.
Stops per-lead when confidence >= threshold or cost ceiling is reached.

Usage:
  python -m src.tasks.run_enrichment [county_id] [--limit N] [--all-leads]

Cron (daily at 07:30 UTC, after scrapers at 04:00–06:30 and scoring at 07:00):
  30 7 * * * cd /path/to/app && python -m src.tasks.run_enrichment hillsborough
"""

import logging
import sys
from datetime import datetime, timezone

from src.utils.logger import setup_logging
from src.services.email import send_alert

setup_logging()
logger = logging.getLogger(__name__)

_DEFAULT_LIMIT = 200


def run_enrichment_pipeline(
    county_id: str = "hillsborough",
    limit: int = _DEFAULT_LIMIT,
    today_only: bool = True,
    **_kwargs,  # absorb legacy args (batchdata_limit, idi_limit, skip_idi, retrace, etc.)
) -> dict:
    """
    Run the multi-provider skip trace waterfall for a county.

    Returns dict with waterfall stats and combined total_enriched.
    """
    from src.services.skip_trace_waterfall import run_waterfall

    results = {
        "county_id":  county_id,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "errors":     [],
    }

    try:
        wf_stats = run_waterfall(county_id=county_id, limit=limit, today_only=today_only)
        results["waterfall"] = {
            "total_leads":      wf_stats.total_leads,
            "hits":             wf_stats.hits,
            "misses":           wf_stats.misses,
            "total_cost_cents": wf_stats.total_cost_cents,
            "per_provider":     wf_stats.per_provider,
        }
        results["total_enriched"] = wf_stats.hits
        logger.info(
            "[Enrichment] Waterfall done: %d/%d enriched, $%.2f spent",
            wf_stats.hits, wf_stats.total_leads, wf_stats.total_cost_cents / 100,
        )
    except Exception as exc:
        logger.error("[Enrichment] Waterfall failed: %s", exc, exc_info=True)
        results["errors"].append(str(exc))
        results["total_enriched"] = 0
        send_alert(
            subject="[Forced Action] Enrichment waterfall crashed",
            body=(
                f"Skip trace waterfall failed unexpectedly:\n{exc}\n\n"
                f"County: {county_id}\n"
                f"Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                f"Check logs for full traceback."
            ),
        )

    results["finished_at"] = datetime.now(timezone.utc).isoformat()
    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Skip trace waterfall: BatchData → IDI → PDL")
    parser.add_argument("county_id", nargs="?", default="hillsborough")
    parser.add_argument("--limit", type=int, default=_DEFAULT_LIMIT,
                        help="Max leads per run (default: 200)")
    parser.add_argument("--all-leads", dest="all_leads", action="store_true",
                        help="Process all un-traced Gold+ leads, not just today's")
    args = parser.parse_args()

    try:
        stats = run_enrichment_pipeline(
            county_id=args.county_id,
            limit=args.limit,
            today_only=not args.all_leads,
        )
        wf = stats.get("waterfall", {})
        print(f"  Total leads  : {wf.get('total_leads', 0)}")
        print(f"  Enriched     : {stats['total_enriched']}")
        print(f"  Cost         : ${wf.get('total_cost_cents', 0) / 100:.2f}")
        if stats["errors"]:
            print(f"  Errors       : {stats['errors']}")
        sys.exit(0)
    except Exception as e:
        logger.error("Enrichment pipeline failed: %s", e)
        sys.exit(1)
