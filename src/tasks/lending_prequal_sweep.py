"""Retry Minute-5 pre-qualification letters that have not gone out yet (T-07).

Picks up queued letters still pending (GHL or the lender engine was unavailable) or failed, waiting
longer between tries (config.lending_prequal.RETRY_BACKOFF_MINUTES) until a letter is
GIVE_UP_AFTER_HOURS old. Does nothing while LENDING_PREQUAL_PDF_ENABLED is off.

    python -m src.tasks.lending_prequal_sweep
"""
from __future__ import annotations

import logging

from config.settings import get_settings
from src.lending.db import lending_session
from src.lending.prequal_fit import get_fit_evaluator
from src.lending.prequal_ghl import get_live_sink
from src.lending.prequal_letters import send_pending

logger = logging.getLogger(__name__)


def run() -> int:
    settings = get_settings()
    if not settings.lending_prequal_pdf_enabled:
        return 0
    with lending_session() as db:
        sent = send_pending(db, get_live_sink(), get_fit_evaluator(),
                            enabled=True, pct=settings.lending_prequal_range_pct)
    logger.info("[prequal] sweep sent=%d", sent)
    return sent


if __name__ == "__main__":
    run()
