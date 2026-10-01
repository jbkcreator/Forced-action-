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


def make_status_getter(api_key: str):
    headers = {"X-ApiKey": api_key}

    def status_of(url: str) -> int:
        # GET without reading the body (HEAD may be unsupported). Redirects are never followed: the key header
        # must not leave the dialer, and a login page behind a redirect would read as 200.
        with requests.get(url, headers=headers, stream=True, timeout=TIMEOUT_SECONDS, allow_redirects=False) as resp:
            code = resp.status_code
        if 300 <= code < 400:
            logger.warning("[lending] recording check got a redirect (3xx); left unchanged")
            return 0
        return code

    return status_of


def run() -> int:
    key = get_settings().batchdialer_api_key
    if key is None:
        logger.error("[lending] recording check skipped: BATCHDIALER_API_KEY is not set")
        return 1
    status_of = make_status_getter(key.get_secret_value())

    with lending_session() as db:
        stats = check_pending(db, status_of)
    logger.info("[lending] recording check: readable=%d forbidden=%d missing=%d skipped=%d",
                stats.readable, stats.forbidden, stats.missing, stats.skipped)
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
