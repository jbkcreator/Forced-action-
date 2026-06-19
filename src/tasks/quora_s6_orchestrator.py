"""
Quora S6 daily orchestrator — Mine → Classify+Generate → Store.

Picks ONE keyword per daily run using the admin-managed topic rotation pool
(quora_topics table). Cooldown enforces that the same keyword cannot run again
for cooldown_days days (configured in quora_settings).

Falls back to cycling the QUORA_TARGET_KEYWORDS env var (or _DEFAULT_KEYWORDS)
by day-of-year when no DB topics are configured.

Cron (06:35 UTC daily, after load_validator, before CDS scoring):
    35 6 * * * $PROJECT/scripts/cron/run.sh src.tasks.quora_s6_orchestrator
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as _dt
import logging
import os
import sys
import uuid
from pathlib import Path

from sqlalchemy import text as sa_text

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_PROJECT_ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

_DEFAULT_KEYWORDS = [
    "foreclosures florida",
    "stop foreclosure florida",
    "hard money lenders tampa",
    "lis pendens florida",
    "surplus funds florida",
    "tax lien certificate florida",
    "probate real estate florida",
    "distressed property florida",
]

_MAX_RESULTS_PER_KEYWORD = 20
_POLL_TIMEOUT            = 180   # seconds — longer than miner default; multiple keywords in flight


def _load_keywords() -> list[str]:
    raw = os.environ.get("QUORA_TARGET_KEYWORDS", "").strip()
    if raw:
        return [k.strip() for k in raw.split(",") if k.strip()]
    return _DEFAULT_KEYWORDS


def _pick_db_keyword() -> tuple[int | None, str | None]:
    """
    Select the next available topic from the DB rotation pool.
    A topic is available when last_run_at IS NULL or it has been idle for
    at least cooldown_days days (oldest-first for fair rotation).
    Returns (topic_id, keyword) or (None, None) if no DB topics are configured.
    """
    from src.core.database import get_db_context
    try:
        with get_db_context() as db:
            settings = db.execute(sa_text(
                "SELECT cooldown_days FROM quora_settings WHERE id = 1"
            )).fetchone()
            cooldown = settings.cooldown_days if settings else 1

            row = db.execute(sa_text("""
                SELECT id, keyword
                FROM quora_topics
                WHERE is_active = true
                  AND (
                      last_run_at IS NULL
                      OR last_run_at <= NOW() - make_interval(days => :cooldown)
                  )
                ORDER BY last_run_at ASC NULLS FIRST
                LIMIT 1
            """), {"cooldown": cooldown}).fetchone()

            return (row.id, row.keyword) if row else (None, None)
    except Exception as exc:
        logger.warning("[orchestrator] DB topic lookup failed, using fallback: %s", exc)
        return None, None


def _mark_topic_ran(topic_id: int) -> None:
    from src.core.database import get_db_context
    try:
        with get_db_context() as db:
            db.execute(sa_text(
                "UPDATE quora_topics SET last_run_at = NOW() WHERE id = :id"
            ), {"id": topic_id})
            db.commit()
    except Exception as exc:
        logger.warning("[orchestrator] failed to update last_run_at topic_id=%s: %s", topic_id, exc)


async def run_keyword(
    keyword: str,
    dry_run: bool = False,
) -> dict:
    """
    Run the full pipeline for a single keyword.
    Returns a summary dict: {keyword, scraped, published, classified, drafted, skipped, errors}.
    """
    from src.agents.events.ingestion import publish_cora_event
    from src.scrappers.quora.quora_engine import QuoraResult, scrape_quora
    from src.scrappers.quora.quora_miner import (
        _keyword_slug,
        _poll_cora_results,
        _result_to_candidate_dict,
        _save_classified_to_db,
    )

    logger.info("[orchestrator] keyword=%r  dry_run=%s", keyword, dry_run)

    responses = await scrape_quora([keyword], max_results=_MAX_RESULTS_PER_KEYWORD, dump_raw=False)

    summary = {"keyword": keyword, "scraped": 0, "classified": 0,
               "drafted": 0, "skipped": 0, "errors": 0}

    for resp in responses:
        if resp.error:
            logger.error("[orchestrator] scrape error keyword=%r: %s", keyword, resp.error)
            summary["errors"] += 1
            continue

        summary["scraped"] += len(resp.results)

        if dry_run or not resp.results:
            continue

        kslug = _keyword_slug(keyword)
        decision_id_map: dict[str, QuoraResult] = {}

        for r in resp.results:
            did             = str(uuid.uuid4())
            idempotency_key = f"quora_cla_ans_{r.qid or r.slug or r.position}_{kslug}"
            try:
                publish_cora_event({
                    "event_type":      "quora_candidate_classify",
                    "subscriber_id":   None,
                    "decision_id":     did,
                    "idempotency_key": idempotency_key,
                    "result_channel":  "cora:quora:results",
                    "payload": {
                        "candidate":              _result_to_candidate_dict(r),
                        "matched_keyword":        keyword,
                        "generate_answer_drafts": True,
                    },
                })
                decision_id_map[did] = r
            except Exception as exc:
                logger.error("[orchestrator] publish failed qid=%s: %s", r.qid, exc)
                summary["errors"] += 1

        if not decision_id_map:
            continue

        enriched = await _poll_cora_results(decision_id_map, timeout=_POLL_TIMEOUT)
        summary["classified"] += enriched

        saved = _save_classified_to_db(resp.results, keyword, decision_id_map)

        for r in resp.results:
            if r.cora_classification:
                action = r.cora_classification.get("recommended_action", "skip")
                if action == "generate_answer" and r.cora_answer_draft:
                    summary["drafted"] += 1
                else:
                    summary["skipped"] += 1

    return summary


async def main(dry_run: bool = False) -> None:
    # Prefer DB-driven rotation; fall back to env/default keyword cycling by day
    topic_id, keyword = _pick_db_keyword()

    if keyword:
        logger.info("[orchestrator] DB rotation  keyword=%r  topic_id=%s", keyword, topic_id)
    else:
        fallback = _load_keywords()
        day_idx  = _dt.date.today().timetuple().tm_yday % len(fallback)
        keyword  = fallback[day_idx]
        logger.info("[orchestrator] fallback rotation  keyword=%r  day_index=%d", keyword, day_idx)

    logger.info("[orchestrator] starting  keyword=%r  dry_run=%s", keyword, dry_run)

    try:
        result = await run_keyword(keyword, dry_run=dry_run)
        logger.info(
            "[orchestrator] done  scraped=%d  classified=%d  drafted=%d  skipped=%d  errors=%d",
            result["scraped"], result["classified"],
            result["drafted"], result["skipped"], result["errors"],
        )
        if topic_id and not dry_run:
            _mark_topic_ran(topic_id)
    except Exception as exc:
        logger.error("[orchestrator] unhandled error keyword=%r: %s", keyword, exc)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Quora S6 daily orchestrator")
    ap.add_argument("--dry-run", action="store_true",
                    help="Scrape only — skip Cora publishing and DB writes")
    args = ap.parse_args()

    asyncio.run(main(dry_run=args.dry_run))
    sys.exit(0)
