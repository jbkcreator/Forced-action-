"""
Shared infrastructure for Vera's standing-job checks.

Extracted from live_state.py once a second check module (revenue_truth.py,
VERA-v2.2 sub-task V3) needed the identical logic — two real call sites is
the point where this earns its own module rather than being duplicated or
imported across a module-private boundary.
"""
from __future__ import annotations


def report_recipients() -> list[str]:
    from config.settings import get_settings
    settings = get_settings()
    raw = settings.report_recipients
    if not raw:
        return []
    return [addr.strip() for addr in raw.split(",") if addr.strip()]
