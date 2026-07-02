"""Lane description service — nightly Haiku batch generation.

Generates a 1-2 sentence distress summary per lane using Claude Haiku.
Runs after financing intent scoring. Concurrent (max 10 threads).

Staleness rule: description_tier tracks the intent_tier at generation time.
A tier change (e.g. Gold → Platinum) triggers regeneration.
Score fluctuations within the same tier are ignored.
"""
from __future__ import annotations

import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

_MAX_CONCURRENT = 10
_MAX_OUTPUT_TOKENS = 80
_MODEL = "claude-haiku-4-5-20251001"

_SYSTEM = (
    "You write 1–2 sentence lead summaries for real estate distress leads. "
    "Be specific about the distress signals. No filler phrases. "
    "Output only the summary text — no labels, no bullet points."
)

_CANDIDATES_SQL = sa_text("""
    SELECT
        l.lane_id,
        pr.address,
        pr.city,
        pr.state,
        fi.financing_intent_score,
        fi.intent_tier,
        fi.signal_flags,
        ds.vertical_scores
    FROM lanes l
    JOIN properties pr ON pr.id = l.property_id
    JOIN LATERAL (
        SELECT financing_intent_score, intent_tier, signal_flags
        FROM financing_intent_scores
        WHERE property_id = l.property_id
        ORDER BY score_date DESC
        LIMIT 1
    ) fi ON true
    LEFT JOIN LATERAL (
        SELECT vertical_scores
        FROM distress_scores
        WHERE property_id = l.property_id
        ORDER BY score_date DESC
        LIMIT 1
    ) ds ON true
    WHERE l.outcome = 'open'
      AND (
        l.lane_description IS NULL
        OR l.description_tier IS DISTINCT FROM fi.intent_tier
      )
    ORDER BY fi.financing_intent_score DESC
    LIMIT 500
""")


def _top_signals(vertical_scores: Any, n: int = 3) -> str:
    if not vertical_scores:
        return "unknown"
    if isinstance(vertical_scores, str):
        try:
            vertical_scores = json.loads(vertical_scores)
        except Exception:
            return "unknown"
    if not isinstance(vertical_scores, dict):
        return "unknown"
    top = sorted(vertical_scores.items(), key=lambda x: float(x[1] or 0), reverse=True)[:n]
    return ", ".join(k.replace("_", " ") for k, _ in top if k) or "unknown"


def _build_prompt(row: Any) -> str:
    signals = _top_signals(row.vertical_scores)
    flags = ""
    if row.signal_flags:
        sf = row.signal_flags if isinstance(row.signal_flags, dict) else {}
        active = [k.replace("_", " ") for k, v in sf.items() if v]
        if active:
            flags = f"\nIntent signals: {', '.join(active[:4])}"
    score = float(row.financing_intent_score) if row.financing_intent_score is not None else 0
    return (
        f"Property: {row.address}, {row.city}, {row.state}\n"
        f"Financing intent: {row.intent_tier} (score {score:.0f})\n"
        f"Top distress signals: {signals}"
        f"{flags}\n\n"
        "Write a 1–2 sentence lead summary."
    )


def _generate_one(row: Any, api_key: str) -> tuple[str, str, str]:
    """Call Haiku for one lane. Returns (lane_id, description, intent_tier)."""
    import anthropic
    client = anthropic.Anthropic(api_key=api_key)
    msg = client.messages.create(
        model=_MODEL,
        max_tokens=_MAX_OUTPUT_TOKENS,
        system=_SYSTEM,
        messages=[{"role": "user", "content": _build_prompt(row)}],
    )
    return str(row.lane_id), msg.content[0].text.strip(), row.intent_tier


def _write_description(session: Session, lane_id: str, description: str, tier: str) -> None:
    session.execute(
        sa_text("""
            UPDATE lanes
               SET lane_description         = :desc,
                   description_tier         = :tier,
                   description_generated_at = NOW()
             WHERE lane_id = CAST(:lid AS uuid)
        """),
        {"desc": description, "tier": tier, "lid": lane_id},
    )


def run_batch(session: Session) -> dict[str, int]:
    """Generate or refresh descriptions for all lanes that need it.

    Targets open lanes where description is missing or intent_tier has changed.
    Returns {'generated': N, 'failed': N}.
    """
    from config.settings import get_settings
    api_key = get_settings().anthropic_api_key.get_secret_value()

    rows = session.execute(_CANDIDATES_SQL).fetchall()

    if not rows:
        logger.info("[LaneDesc] all descriptions current — nothing to do")
        return {"generated": 0, "failed": 0}

    logger.info("[LaneDesc] generating descriptions for %d lanes", len(rows))
    generated = failed = 0

    with ThreadPoolExecutor(max_workers=_MAX_CONCURRENT) as pool:
        futures = {pool.submit(_generate_one, row, api_key): row for row in rows}
        for future in as_completed(futures):
            row = futures[future]
            try:
                lane_id, description, tier = future.result()
                _write_description(session, lane_id, description, tier)
                generated += 1
            except Exception:
                logger.exception("[LaneDesc] failed lane_id=%s", getattr(row, "lane_id", "?"))
                failed += 1

    session.commit()
    logger.info("[LaneDesc] done — generated=%d failed=%d", generated, failed)
    return {"generated": generated, "failed": failed}
