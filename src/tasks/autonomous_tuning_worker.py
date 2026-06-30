"""Autonomous Inbound Content Loop (Task 5.1) — weekly Quora keyword tuner.

Computes a 30-day per-Seed-Keyword performance score on `quora_topics`, then
deactivates weak keywords and expands strong ones (LLM variations) under a
bounded pool. See docs/adr/0021 for why this reuses `quora_topics` rather than
the spec's `scraper_keyword_metrics`/`thread_clusters` tables.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import text

from src.utils.quora_attribution import campaign_slug

logger = logging.getLogger(__name__)

# ── Config tunables (see docs/adr/0021) ──────────────────────────────────────
# ponytail: module constants for v1; move to config/ when a second tuner needs them.
# AVG_SUBSCRIBER_LTV_USD: estimate, not measured — there is no collected revenue
# or churn data yet (0 paid invoices). Basis: weighted-avg monthly price ≈ $306
# (51 Starter @ $299 + 2 Pro @ $499) × 6-month conservative tenure (the founding-
# rate lock window). It only scales the reported performance_score; the
# deactivate/expand actions key off explicit counts, so the exact value does not
# change which keywords are tuned. Promote to a computed trailing-window mean
# (collected SubscriptionInvoice / paying subscribers) once billing history exists.
AVG_SUBSCRIBER_LTV_USD = Decimal("1840")
MIN_TRIAL_ANSWERS = 5
MIN_SIGNUPS_TO_EXPAND = 1
MAX_ACTIVE_KEYWORDS = 30
WINDOW_DAYS = 30
VARIATIONS_PER_WINNER = 3
ACTIVE_COUNTIES = ("Hillsborough", "Pinellas")


def performance_score(paid_signups, answers_posted, cumulative_spend, avg_ltv) -> Decimal:
    """Value per answer: ((paid_signups × avg_ltv) − spend) / (answers + 1)."""
    revenue = Decimal(paid_signups) * Decimal(avg_ltv)
    return (revenue - Decimal(cumulative_spend)) / (Decimal(answers_posted) + 1)


def decide_action(paid_signups, answers_posted, *, min_trial_answers, min_signups_to_expand) -> str:
    """keep | deactivate | expand, from explicit 30-day counts (not a score threshold)."""
    if paid_signups == 0 and answers_posted >= min_trial_answers:
        return "deactivate"
    if paid_signups >= min_signups_to_expand:
        return "expand"
    return "keep"


def plan_evictions(active_scores, num_new, cap):
    """Replace-worst: ids of active keywords to drop so num_new fit under cap."""
    overflow = len(active_scores) + num_new - cap
    if overflow <= 0:
        return []
    worst_first = sorted(active_scores, key=lambda kv: kv[1])
    return [kv[0] for kv in worst_first[:overflow]]


def _llm_generate_variations(keyword: str, cluster: str | None, count: int) -> list[str]:
    """Generate geo/cluster-constrained Seed Keyword variations via Haiku."""
    from src.services.claude_router import call_claude_with_usage

    counties = " or ".join(ACTIVE_COUNTIES)
    prompt = (
        f"Generate {count} alternative search-query keywords semantically similar to "
        f'"{keyword}". They must stay within the same intent ({cluster or "general"}) '
        f"and target {counties} County, Florida. Return ONLY the keywords, one per line, "
        f"no numbering or commentary."
    )
    try:
        result = call_claude_with_usage(
            task_type="keyword_variations",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=256,
        )
        body = (result.get("text") or "").strip()
        return [ln.strip(" -•\t") for ln in body.splitlines() if ln.strip()][:count]
    except Exception as exc:
        logger.error("[tuning] variation generation failed for %r: %s", keyword, exc)
        return []


def tune_scraper_keywords(
    db,
    *,
    generate_variations=None,
    now: datetime | None = None,
    avg_ltv: Decimal = AVG_SUBSCRIBER_LTV_USD,
    min_trial_answers: int = MIN_TRIAL_ANSWERS,
    min_signups_to_expand: int = MIN_SIGNUPS_TO_EXPAND,
    max_active_keywords: int = MAX_ACTIVE_KEYWORDS,
    window_days: int = WINDOW_DAYS,
) -> dict:
    """Weekly compute-then-act pass over the quora_topics Seed Keyword pool."""
    now = now or datetime.now(timezone.utc)
    cutoff = now - timedelta(days=window_days)
    if generate_variations is None:
        generate_variations = _llm_generate_variations

    active = db.execute(text(
        "SELECT id, keyword, cluster FROM quora_topics WHERE is_active = TRUE"
    )).all()

    # Batched 30-day joins — answers + spend per keyword, paid signups per slug.
    answers_by_kw = {
        r.kw: (r.answers, Decimal(r.spend))
        for r in db.execute(text("""
            SELECT q.matched_keyword AS kw, COUNT(*) AS answers,
                   COALESCE(SUM(d.cost_usd), 0) AS spend
            FROM quora_questions q
            LEFT JOIN agent_decisions d ON d.decision_id = q.cora_decision_id
            WHERE q.posted_at >= :cutoff AND q.matched_keyword IS NOT NULL
            GROUP BY q.matched_keyword
        """), {"cutoff": cutoff}).all()
    }
    signups_by_slug = {
        r.slug: r.n
        for r in db.execute(text("""
            SELECT s.utm_campaign AS slug, COUNT(DISTINCT s.id) AS n
            FROM subscribers s
            WHERE s.utm_campaign IS NOT NULL AND EXISTS (
                SELECT 1 FROM subscription_invoices si
                WHERE si.subscriber_id = s.id AND si.reversed_at IS NULL
                  AND si.paid_at >= :cutoff
            )
            GROUP BY s.utm_campaign
        """), {"cutoff": cutoff}).all()
    }

    to_deactivate: list[int] = []
    to_expand: list[tuple[int, str, str | None]] = []
    for r in active:
        answers, spend = answers_by_kw.get(r.keyword, (0, Decimal("0")))
        paid = signups_by_slug.get(campaign_slug(r.keyword), 0)
        score = performance_score(paid, answers, spend, avg_ltv)
        db.execute(text(
            "UPDATE quora_topics SET signup_count = :sc, cumulative_spend = :cs, "
            "performance_score = :ps WHERE id = :id"
        ), {"sc": paid, "cs": spend, "ps": score, "id": r.id})
        action = decide_action(
            paid, answers,
            min_trial_answers=min_trial_answers,
            min_signups_to_expand=min_signups_to_expand,
        )
        if action == "deactivate":
            to_deactivate.append(r.id)
        elif action == "expand":
            to_expand.append((r.id, r.keyword, r.cluster))

    if to_deactivate:
        db.execute(text("UPDATE quora_topics SET is_active = FALSE WHERE id = ANY(:ids)"),
                   {"ids": to_deactivate})

    inserted = 0
    if to_expand:
        new_keywords: list[tuple[str, str | None]] = []
        for _id, kw, cluster in to_expand:
            for variation in generate_variations(kw, cluster, VARIATIONS_PER_WINNER):
                new_keywords.append((variation, cluster))

        # Replace-worst: evict lowest-scoring active keywords to keep under cap.
        remaining = db.execute(text(
            "SELECT id, performance_score FROM quora_topics "
            "WHERE is_active = TRUE AND id <> ALL(:dead)"
        ), {"dead": to_deactivate or [0]}).all()
        active_scores = [(r.id, r.performance_score or Decimal("0")) for r in remaining]
        evict = plan_evictions(active_scores, len(new_keywords), max_active_keywords)
        if evict:
            db.execute(text("UPDATE quora_topics SET is_active = FALSE WHERE id = ANY(:ids)"),
                       {"ids": evict})

        for kw, cluster in new_keywords:
            # ponytail: ON CONFLICT skip can under-fill vs evictions; fine at v1 volume.
            res = db.execute(text(
                "INSERT INTO quora_topics (keyword, is_active, cluster, last_run_at) "
                "VALUES (:kw, TRUE, :cl, NULL) ON CONFLICT (keyword) DO NOTHING"
            ), {"kw": kw, "cl": cluster})
            inserted += res.rowcount or 0

    summary = {"deactivated": len(to_deactivate), "expanded": len(to_expand),
               "inserted": inserted}
    logger.info("[tuning] %s", summary)
    return summary


def main() -> dict:
    """Cron entry point — one weekly compute-then-act pass, committed once."""
    from src.core.database import get_db_context

    with get_db_context() as db:
        summary = tune_scraper_keywords(db)
        db.commit()
    return summary


if __name__ == "__main__":
    main()
