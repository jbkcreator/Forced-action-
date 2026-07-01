"""Shared Quora attribution helper.

Single source of truth for the keyword→utm_campaign transform used by both the
producer (`src/agents/graphs/quora_channel.py`, which stamps it on posted answer
links) and the consumer (`src/tasks/autonomous_tuning_worker.py`, which joins
paid signups back to the keyword). Keep these in lockstep — see docs/adr/0021.
"""

from __future__ import annotations

import re

from sqlalchemy import text


def campaign_slug(keyword: str) -> str:
    """Build the URL-safe `quora_<slug>` utm_campaign for a Seed Keyword."""
    slug = re.sub(r"[^a-z0-9]+", "_", keyword.lower()).strip("_")[:50]
    return f"quora_{slug}"


def clamp_cooldown(db) -> None:
    """Enforce `cooldown_days <= active_topic_count - 1` after any change to the
    active `quora_topics` pool size (admin deactivate, or the weekly tuning
    worker's deactivate/evict/insert). Must be called by every code path that
    changes `is_active` on quora_topics — otherwise the daily DB rotation
    picker (`quora_s6_orchestrator._pick_db_keyword`) can find zero eligible
    rows and silently fall back to the static default keyword list.
    """
    active_count = db.execute(text(
        "SELECT COUNT(*) FROM quora_topics WHERE is_active = true"
    )).scalar() or 0
    max_cd = max(0, active_count - 1)
    db.execute(text(
        "UPDATE quora_settings SET cooldown_days = LEAST(cooldown_days, :max) WHERE id = 1"
    ), {"max": max_cd})
