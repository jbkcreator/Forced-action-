"""Shared Quora attribution helper.

Single source of truth for the keyword→utm_campaign transform used by both the
producer (`src/agents/graphs/quora_channel.py`, which stamps it on posted answer
links) and the consumer (`src/tasks/autonomous_tuning_worker.py`, which joins
paid signups back to the keyword). Keep these in lockstep — see docs/adr/0021.
"""

from __future__ import annotations

import re


def campaign_slug(keyword: str) -> str:
    """Build the URL-safe `quora_<slug>` utm_campaign for a Seed Keyword."""
    slug = re.sub(r"[^a-z0-9]+", "_", keyword.lower()).strip("_")[:50]
    return f"quora_{slug}"
