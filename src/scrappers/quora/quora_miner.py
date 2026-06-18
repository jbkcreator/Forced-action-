"""
Quora search miner — CLI wrapper around quora_engine + Cora Quora graph.

Usage:
    # Scrape + parse + deterministic score only
    python -m src.scrappers.quora.quora_miner --keyword "foreclosures florida" --dump-raw

    # Add Cora classification (publishes to event queue, polls for results)
    python -m src.scrappers.quora.quora_miner --keyword "foreclosures florida" --dump-raw --classify-with-cora

    # Add answer draft generation
    python -m src.scrappers.quora.quora_miner --keyword "foreclosures florida" --dump-raw --classify-with-cora --generate-answer-drafts

--classify-with-cora:
    Publishes one `quora_candidate_classify` event per result to the Cora event
    queue (Redis primary, Postgres fallback). Each event carries a pre-generated
    decision_id. After Cora processes each event it dispatches a result back to
    the `cora:quora:results` Redis channel keyed by decision_id. The miner
    subscribes to that channel and collects results inline, falling back to DB
    polling if Redis is unavailable.

    Requires the Cora agents process to be running:
        python -m src.agents --serve
"""

import argparse
import asyncio
import json
import logging
import re
import time
import uuid
from pathlib import Path

from sqlalchemy import text as sa_text

from src.scrappers.quora.quora_engine import (
    QuoraResult,
    _result_to_dump_dict,
    scrape_quora,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_PROJECT_ROOT   = Path(__file__).resolve().parents[3]
_DEBUG_DIR      = _PROJECT_ROOT / "data" / "debug" / "quora"
_POLL_TIMEOUT   = 120   # seconds to wait for Cora before giving up
_POLL_INTERVAL  = 3.0   # seconds between DB polls


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _result_to_candidate_dict(r: QuoraResult) -> dict:
    """Compact dict sent to Cora — no raw_metadata."""
    return {
        "qid":                   r.qid,
        "title":                 r.title,
        "url":                   r.url,
        "slug":                  r.slug,
        "answer_count":          r.answer_count,
        "follower_count":        r.follower_count,
        "comment_count":         r.comment_count,
        "view_count":            r.view_count,
        "created_time":          r.created_time.isoformat() if r.created_time else None,
        "last_activity_time":    r.last_activity_time.isoformat() if r.last_activity_time else None,
        "is_locked":             r.is_locked,
        "is_deleted":            r.is_deleted,
        "is_sensitive":          r.is_sensitive,
        "viewer_cant_answer":    r.viewer_cant_answer,
        "deterministic_score":   r.deterministic_score,
        "deterministic_reasons": r.deterministic_reasons,
    }


def _keyword_slug(keyword: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", keyword.lower()).strip("_")


async def _poll_cora_results(
    decision_id_map: dict[str, QuoraResult],
    timeout: int = _POLL_TIMEOUT,
    interval: float = _POLL_INTERVAL,
) -> int:
    """
    Subscribe to cora:quora:results Redis channel and collect results keyed by
    decision_id. Falls back to DB polling when Redis is unavailable. Enriches
    each QuoraResult in-place; returns the count successfully enriched.
    """
    from src.core.redis_client import get_redis, redis_available

    if not redis_available():
        logger.warning("[miner] Redis unavailable — falling back to DB poll")
        return await _poll_cora_results_db(decision_id_map, timeout, interval)

    pending = set(decision_id_map.keys())
    loop    = asyncio.get_event_loop()

    def _subscribe_and_collect() -> dict[str, dict]:
        client = get_redis()
        pubsub = client.pubsub(ignore_subscribe_messages=True)
        pubsub.subscribe("cora:quora:results")

        collected: dict[str, dict] = {}
        deadline = time.monotonic() + timeout

        while len(collected) < len(pending) and time.monotonic() < deadline:
            msg = pubsub.get_message(timeout=1.0)
            if msg and msg.get("type") == "message":
                try:
                    data = json.loads(msg["data"])
                    did  = data.get("decision_id")
                    if did and did in pending:
                        collected[did] = data
                        status = data.get("terminal_status", "completed")
                        if status in ("completed", "classify_failed", "answer_failed"):
                            logger.info("[miner] cora result received  decision_id=%s  qid=%s",
                                        did, data.get("qid"))
                        else:
                            logger.warning("[miner] cora dropped  decision_id=%s  reason=%s",
                                           did, status)
                except Exception:
                    pass

        pubsub.unsubscribe()
        pubsub.close()
        return collected

    print(f"  Subscribing to cora:quora:results for {len(pending)} result(s) "
          f"(timeout={timeout}s)…", flush=True)

    collected = await loop.run_in_executor(None, _subscribe_and_collect)

    enriched = 0
    for did, data in collected.items():
        r = decision_id_map.get(did)
        if r:
            r.cora_classification = data.get("cora_classification")
            r.cora_answer_draft   = data.get("cora_answer_draft")
            enriched += 1

    if len(collected) < len(pending):
        logger.warning("[miner] %d/%d result(s) timed out — cora_classification will be null",
                       len(pending) - len(collected), len(decision_id_map))

    return enriched


async def _poll_cora_results_db(
    decision_id_map: dict[str, QuoraResult],
    timeout: int = _POLL_TIMEOUT,
    interval: float = _POLL_INTERVAL,
) -> int:
    """DB-poll fallback — used when Redis is unavailable."""
    from src.core.database import get_db_context

    pending  = set(decision_id_map.keys())
    enriched = 0
    deadline = time.monotonic() + timeout

    print(f"  Polling agent_decisions for {len(pending)} result(s) "
          f"(timeout={timeout}s, interval={interval}s)…", flush=True)

    while pending and time.monotonic() < deadline:
        try:
            with get_db_context() as session:
                rows = session.execute(
                    sa_text("""
                        SELECT decision_id::text, terminal_status, summary
                        FROM agent_decisions
                        WHERE decision_id::text = ANY(:ids)
                          AND terminal_status IS NOT NULL
                    """),
                    {"ids": list(pending)},
                ).fetchall()

            for row in rows:
                did    = str(row[0])
                result = decision_id_map.get(did)
                if result and did in pending:
                    summary = row[2] or {}
                    result.cora_classification = summary.get("cora_classification")
                    result.cora_answer_draft   = summary.get("cora_answer_draft")
                    pending.discard(did)
                    enriched += 1
                    logger.info("[miner] cora result received  decision_id=%s  qid=%s", did, result.qid)

        except Exception as exc:
            logger.error("[miner] poll query failed: %s", exc)

        if pending:
            await asyncio.sleep(interval)

    if pending:
        logger.warning("[miner] %d/%d result(s) timed out — cora_classification will be null",
                       len(pending), len(decision_id_map))

    return enriched


def _save_classified_to_db(
    results: list[QuoraResult],
    keyword: str,
    decision_id_map: dict[str, QuoraResult],
) -> int:
    """
    Upsert all Cora-classified results into quora_questions.
    Uses qid as the conflict key. On conflict, updates classification fields
    and last_classified_at so re-runs reflect the latest Cora decision.
    Returns the number of rows upserted.
    """
    from datetime import timezone as _tz
    from datetime import datetime as _dt
    from src.core.database import get_db_context

    # Build reverse map: QuoraResult → decision_id
    result_to_did = {id(v): k for k, v in decision_id_map.items()}

    classified = [r for r in results if r.cora_classification is not None]
    if not classified:
        return 0

    now = _dt.now(tz=_tz.utc)
    rows = []
    for r in classified:
        cl = r.cora_classification or {}
        action = cl.get("recommended_action", "skip")
        if action == "generate_answer" and r.cora_answer_draft:
            status = "drafted"
        elif action == "generate_answer":
            status = "pending"
        else:
            status = "skipped"

        rows.append({
            "qid":                   r.qid,
            "slug":                  r.slug,
            "url":                   r.url,
            "title":                 r.title,
            "answer_count":          r.answer_count,
            "follower_count":        r.follower_count,
            "view_count":            r.view_count,
            "is_locked":             r.is_locked,
            "is_sensitive":          r.is_sensitive,
            "topics":                r.topics or [],
            "created_time":          r.created_time,
            "deterministic_score":   r.deterministic_score,
            "deterministic_reasons": r.deterministic_reasons or [],
            "matched_keyword":       keyword,
            "cora_decision_id":      result_to_did.get(id(r)),
            "intent_lane":           cl.get("intent_lane"),
            "recommended_action":    action,
            "priority_score":        cl.get("priority_score"),
            "risk_level":            cl.get("risk_level"),
            "cora_classification":   json.dumps(cl),
            "answer_draft":          json.dumps(r.cora_answer_draft) if r.cora_answer_draft else None,
            "answer_status":         status,
            "first_seen_at":         now,
            "last_classified_at":    now,
        })

    saved = 0
    try:
        with get_db_context() as session:
            for row in rows:
                session.execute(
                    sa_text("""
                        INSERT INTO quora_questions (
                            qid, slug, url, title,
                            answer_count, follower_count, view_count,
                            is_locked, is_sensitive, topics, created_time,
                            deterministic_score, deterministic_reasons,
                            matched_keyword, cora_decision_id,
                            intent_lane, recommended_action, priority_score,
                            risk_level, cora_classification,
                            answer_draft, answer_status,
                            first_seen_at, last_classified_at
                        ) VALUES (
                            :qid, :slug, :url, :title,
                            :answer_count, :follower_count, :view_count,
                            :is_locked, :is_sensitive, :topics, :created_time,
                            :deterministic_score, :deterministic_reasons,
                            :matched_keyword, :cora_decision_id,
                            :intent_lane, :recommended_action, :priority_score,
                            :risk_level, :cora_classification::jsonb,
                            :answer_draft::jsonb, :answer_status,
                            :first_seen_at, :last_classified_at
                        )
                        ON CONFLICT (qid) DO UPDATE SET
                            matched_keyword      = EXCLUDED.matched_keyword,
                            cora_decision_id     = EXCLUDED.cora_decision_id,
                            intent_lane          = EXCLUDED.intent_lane,
                            recommended_action   = EXCLUDED.recommended_action,
                            priority_score       = EXCLUDED.priority_score,
                            risk_level           = EXCLUDED.risk_level,
                            cora_classification  = EXCLUDED.cora_classification,
                            answer_draft         = COALESCE(EXCLUDED.answer_draft, quora_questions.answer_draft),
                            answer_status        = CASE
                                WHEN quora_questions.answer_status IN ('published', 'failed')
                                THEN quora_questions.answer_status
                                ELSE EXCLUDED.answer_status
                            END,
                            last_classified_at   = EXCLUDED.last_classified_at,
                            -- refresh scraped signals in case they changed
                            answer_count         = EXCLUDED.answer_count,
                            follower_count       = EXCLUDED.follower_count,
                            view_count           = EXCLUDED.view_count,
                            deterministic_score  = EXCLUDED.deterministic_score
                    """),
                    row,
                )
                saved += 1
    except Exception as exc:
        logger.error("[miner] DB upsert failed: %s", exc)

    logger.info("[miner] upserted %d/%d classified questions to quora_questions", saved, len(classified))
    return saved


def _overwrite_parsed_dump(keyword: str, results: list[QuoraResult]) -> None:
    slug    = keyword[:40].replace(" ", "_").replace("/", "-")
    matches = sorted(_DEBUG_DIR.glob(f"parsed_results_{slug}*.json"), reverse=True)
    if not matches:
        logger.warning("[miner] no parsed dump to overwrite for keyword=%r", keyword)
        return
    path = matches[0]
    path.write_text(json.dumps([_result_to_dump_dict(r) for r in results], indent=2, default=str))
    logger.info("[miner] updated parsed dump with Cora output → %s", path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main(
    keyword: str,
    max_results: int,
    dump_raw: bool,
    classify_with_cora: bool,
    generate_answer_drafts: bool,
) -> None:
    logger.info(
        "Querying Quora: %r  max=%d  dump_raw=%s  classify=%s  answers=%s",
        keyword, max_results, dump_raw, classify_with_cora, generate_answer_drafts,
    )

    responses = await scrape_quora([keyword], max_results=max_results, dump_raw=dump_raw)

    for resp in responses:
        print(f"\n{'='*60}")
        print(f"Query  : {resp.query!r}")
        print(f"Status : {resp.status}")
        if resp.error:
            print(f"Error  : {resp.error}")
            return

        print(f"Results: {len(resp.results)}")

        # ── Cora classification via event queue ───────────────────────────────
        if classify_with_cora and resp.results:
            from src.agents.events.ingestion import publish_cora_event

            kslug          = _keyword_slug(keyword)
            decision_id_map: dict[str, QuoraResult] = {}

            kind = "cla_ans" if generate_answer_drafts else "cla"
            for r in resp.results:
                did             = str(uuid.uuid4())
                idempotency_key = f"quora_{kind}_{r.qid or r.slug or r.position}_{kslug}"
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
                            "generate_answer_drafts": generate_answer_drafts,
                        },
                    })
                    decision_id_map[did] = r
                except Exception as exc:
                    logger.error("[miner] publish failed qid=%s: %s", r.qid, exc)

            print(f"\n  Cora: {len(decision_id_map)}/{len(resp.results)} events published.")

            if decision_id_map:
                enriched = await _poll_cora_results(decision_id_map)
                print(f"  Cora: {enriched}/{len(decision_id_map)} classified.")

                saved = _save_classified_to_db(resp.results, keyword, decision_id_map)
                print(f"  DB:   {saved} question(s) upserted to quora_questions.")

                if dump_raw:
                    _overwrite_parsed_dump(keyword, resp.results)

        # ── Print results ─────────────────────────────────────────────────────
        for r in resp.results:
            print(f"\n  [{r.position:>2}] {r.title}")
            print(f"        {r.url}")

            meta = []
            if r.answer_count:   meta.append(f"answers={r.answer_count}")
            if r.follower_count: meta.append(f"followers={r.follower_count}")
            if r.view_count:     meta.append(f"views={r.view_count}")
            if r.deterministic_score is not None:
                meta.append(f"score={r.deterministic_score}")
            if meta:
                print(f"        {' · '.join(meta)}")

            if r.deterministic_reasons:
                print(f"        reasons: {', '.join(r.deterministic_reasons)}")
            if r.topics:
                print(f"        topics: {', '.join(r.topics)}")
            if r.created_time:
                print(f"        created: {r.created_time.date()}")

            if r.top_answer_author:
                cred = f" — {r.top_answer_author_credentials}" if r.top_answer_author_credentials else ""
                upv  = f"  [{r.top_answer_upvotes} upvotes]" if r.top_answer_upvotes else ""
                print(f"        ↳ {r.top_answer_author}{cred}{upv}")
            if r.top_answer_snippet:
                print(f"          \"{r.top_answer_snippet[:140]}\"")

            if r.cora_classification:
                cl = r.cora_classification
                print(f"        Cora: {cl.get('recommended_action','?')} | "
                      f"priority={cl.get('priority_score','?')} | "
                      f"lane={cl.get('intent_lane','?')} | "
                      f"risk={cl.get('risk_level','?')}")
            if r.cora_answer_draft:
                print(f"        Answer draft: {r.cora_answer_draft.get('answer_status','?')}")

        if dump_raw:
            print(f"\nDumps saved to data/debug/quora/")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Quora search miner")
    ap.add_argument("--keyword",                required=True)
    ap.add_argument("--max-results",            type=int, default=20)
    ap.add_argument("--dump-raw",               action="store_true")
    ap.add_argument("--classify-with-cora",     action="store_true",
                    help="Publish to Cora event queue and poll for classification results")
    ap.add_argument("--generate-answer-drafts", action="store_true",
                    help="Also generate answer drafts for approved candidates")
    args = ap.parse_args()

    asyncio.run(main(
        keyword=args.keyword,
        max_results=args.max_results,
        dump_raw=args.dump_raw,
        classify_with_cora=args.classify_with_cora,
        generate_answer_drafts=args.generate_answer_drafts,
    ))
