"""Check which call recordings our BatchDialer key can read (cron every 10 minutes).

    python -m src.tasks.lending_recording_check
"""
from __future__ import annotations

import logging

import requests

from config.settings import get_settings
from src.lending.db import lending_session
from src.lending.recordings import check_pending

logger = logging.getLogger(__name__)

TIMEOUT_SECONDS = 20


def run() -> int:
    key = get_settings().batchdialer_api_key
    if key is None:
        logger.error("[lending] recording check skipped: BATCHDIALER_API_KEY is not set")
        return 1
    headers = {"X-ApiKey": key.get_secret_value()}

    def status_of(url: str) -> int:
        # GET without reading the body: a HEAD request may not be supported by this route.
        with requests.get(url, headers=headers, stream=True, timeout=TIMEOUT_SECONDS) as resp:
            return resp.status_code

    with lending_session() as db:
        stats = check_pending(db, status_of)
    logger.info("[lending] recording check: readable=%d forbidden=%d missing=%d skipped=%d",
                stats.readable, stats.forbidden, stats.missing, stats.skipped)
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
