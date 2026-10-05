"""Retry delivery of website leads that are not yet in GoHighLevel (WP-GL-11).

Picks up leads still pending (GHL was down, or not configured when they arrived) or failed with
attempts left. A lead that exhausts its attempts stays ``failed`` and is logged at ERROR with its
id. Cron: every 5 minutes.

    python -m src.tasks.lending_web_lead_sweep
"""
from __future__ import annotations

import logging

from src.lending.db import lending_session
from src.lending.web_lead_ghl import get_live_sink
from src.lending.web_leads import deliver_pending

logger = logging.getLogger(__name__)


def run() -> int:
    with lending_session() as db:
        delivered = deliver_pending(db, get_live_sink())
    logger.info("[lending-web] sweep delivered=%d", delivered)
    return delivered


if __name__ == "__main__":
    run()
