"""
Enrichment queue — M5.

Orchestrates the two-stage contact enrichment pipeline:
  Stage 1: BatchSkipTracing (primary)
  Stage 2: IDI fallback (for BatchData misses)

Designed to run daily via cron after scrapers have run and scoring is complete.

Usage:
  python -m src.tasks.run_enrichment [county_id] [--limit N]

Cron (daily at 4am, after scrapers at 2am and scoring at 3am):
  0 4 * * * cd /path/to/app && python -m src.tasks.run_enrichment hillsborough

The "async" in "async enrichment queue" means this task:
  - Runs independently from the web server (no blocking of API requests)
  - Processes in batches with rate-limit-safe delays between batches
  - Reports results back to the monitoring system via scraper_run_stats
  - Sends ops alerts on credential/API failures without human intervention
"""

import logging
import sys
from datetime import datetime, timezone

from src.utils.logger import setup_logging
from src.services.email import send_alert

setup_logging()
logger = logging.getLogger(__name__)

_DEFAULT_BATCHDATA_LIMIT = 200   # owners per daily enrichment run
_DEFAULT_IDI_LIMIT = 100         # BatchData misses to retry via IDI


def run_enrichment_pipeline(
    county_id: str = "hillsborough",
    batchdata_limit: int = _DEFAULT_BATCHDATA_LIMIT,
    idi_limit: int = _DEFAULT_IDI_LIMIT,
    skip_idi: bool = False,
    today_only: bool = True,
) -> dict:
    """
    Run the full enrichment pipeline for a county.

    Returns dict with stage results and combined stats.
    """
    results = {
        "county_id": county_id,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "batchdata": {},
        "idi": {},
        "errors": [],
    }

    # ── Stage 1: BatchSkipTracing ──────────────────────────────────────────
    logger.info("[Enrichment] Stage 1: BatchSkipTracing (limit=%d, county=%s, today_only=%s)",
                batchdata_limit, county_id, today_only)
    try:
        from src.services.skip_trace import run_skip_trace
        bd_stats = run_skip_trace(
            limit=batchdata_limit,
            county_id=county_id,
            today_only=today_only,
        )
        results["batchdata"] = bd_stats
        logger.info(
            "[Enrichment] BatchData complete: %d success / %d failed / %d total",
            bd_stats.get("success", 0),
            bd_stats.get("failed", 0),
            bd_stats.get("total", 0),
        )
    except RuntimeError as e:
        err_msg = str(e)
        logger.error("[Enrichment] BatchData stage failed: %s", err_msg)
        results["errors"].append(f"batchdata: {err_msg}")

        # Credential errors already alerted inside skip_trace — log and continue to IDI
        if "not set" in err_msg.lower():
            logger.warning("[Enrichment] BATCH_SKIP_TRACING_API_KEY not configured — skipping BatchData")
    except Exception as e:
        logger.error("[Enrichment] BatchData stage unexpected error: %s", e, exc_info=True)
        results["errors"].append(f"batchdata: {e}")
        send_alert(
            subject="[Forced Action] Enrichment pipeline: BatchData stage crashed",
            body=(
                f"Unexpected error in BatchData enrichment stage:\n{e}\n\n"
                f"County: {county_id}\n"
                f"Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                f"Check logs for full traceback."
            ),
        )

    # ── Stage 2: IDI fallback ──────────────────────────────────────────────
    if skip_idi:
        logger.info("[Enrichment] IDI fallback skipped (--skip-idi flag)")
        results["idi"] = {"skipped": True}
    else:
        logger.info("[Enrichment] Stage 2: IDI fallback (limit=%d)", idi_limit)
        try:
            from src.services.idi_fallback import run_idi_fallback
            idi_stats = run_idi_fallback(
                limit=idi_limit,
                county_id=county_id,
            )
            results["idi"] = idi_stats
            if not idi_stats.get("skipped"):
                logger.info(
                    "[Enrichment] IDI complete: %d success / %d failed / %d total",
                    idi_stats.get("success", 0),
                    idi_stats.get("failed", 0),
                    idi_stats.get("total", 0),
                )
        except Exception as e:
            logger.error("[Enrichment] IDI stage failed: %s", e, exc_info=True)
            results["errors"].append(f"idi: {e}")

    results["finished_at"] = datetime.now(timezone.utc).isoformat()
    results["total_enriched"] = (
        results["batchdata"].get("success", 0) + results["idi"].get("success", 0)
    )

    logger.info(
        "[Enrichment] Pipeline complete. Total enriched: %d | Errors: %d",
        results["total_enriched"],
        len(results["errors"]),
    )

    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Enrichment pipeline: BatchData + IDI fallback")
    parser.add_argument("county_id", nargs="?", default="hillsborough")
    parser.add_argument("--limit", type=int, default=_DEFAULT_BATCHDATA_LIMIT,
                        help="Max owners for BatchData stage (default: 200)")
    parser.add_argument("--idi-limit", type=int, default=_DEFAULT_IDI_LIMIT,
                        help="Max BatchData misses for IDI stage (default: 100)")
    parser.add_argument("--skip-idi", action="store_true",
                        help="Skip IDI fallback stage")
    parser.add_argument("--all-leads", dest="all_leads", action="store_true",
                        help="Skip-trace all un-traced Gold+ leads, not just today's (use with caution)")
    args = parser.parse_args()

    try:
        stats = run_enrichment_pipeline(
            county_id=args.county_id,
            batchdata_limit=args.limit,
            idi_limit=args.idi_limit,
            skip_idi=args.skip_idi,
            today_only=not args.all_leads,
        )
        print(f"  BatchData enriched : {stats['batchdata'].get('success', 0)}")
        print(f"  IDI enriched       : {stats['idi'].get('success', 0)}")
        print(f"  Total enriched     : {stats['total_enriched']}")
        if stats["errors"]:
            print(f"  Errors             : {stats['errors']}")
        sys.exit(0)
    except Exception as e:
        logger.error("Enrichment pipeline failed: %s", e)
        sys.exit(1)
