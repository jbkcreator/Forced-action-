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
    tracerfy_only: bool = False,
    tracerfy_retrace_misses: bool = False,
    individual_only: bool = False,
    entity_only: bool = False,
    **_kwargs,
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
        wf_stats = run_waterfall(
            county_id=county_id,
            limit=limit,
            today_only=today_only,
            tracerfy_only=tracerfy_only,
            tracerfy_retrace_misses=tracerfy_retrace_misses,
            individual_only=individual_only,
            entity_only=entity_only,
        )
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
    parser.add_argument("--tracerfy-only", dest="tracerfy_only", action="store_true",
                        help="Run Tracerfy Tier 1 only — skip BatchData and PDL fallback tiers")
    parser.add_argument("--tracerfy-retrace-misses", dest="tracerfy_retrace_misses", action="store_true",
                        help="Re-submit properties with existing tracerfy miss EC rows, updating in-place on hit")
    parser.add_argument("--individual-only", dest="individual_only", action="store_true",
                        help="Only process Individual owner_type records")
    parser.add_argument("--entity-only", dest="entity_only", action="store_true",
                        help="Only process non-Individual (LLC/Corp/Trust/Estate) records")
    args = parser.parse_args()

    try:
        stats = run_enrichment_pipeline(
            county_id=args.county_id,
            limit=args.limit,
            today_only=not args.all_leads,
            tracerfy_only=args.tracerfy_only,
            tracerfy_retrace_misses=args.tracerfy_retrace_misses,
            individual_only=args.individual_only,
            entity_only=args.entity_only,
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
