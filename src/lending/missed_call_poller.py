"""Call-record poller: every finished call becomes an attempt row (3-attempt rail), and each
new no-answer gets its missed-call text decision (WP-GL-9).

Sends only when MISSED_CALL_TEXT_ENABLED=true; otherwise every no-answer is logged as
``dry_run``. One cycle at a time (advisory lock), one transaction per cycle.

Usage:
    python -m src.lending.missed_call_poller            # loop every POLL_SECONDS
    python -m src.lending.missed_call_poller --once
"""
from __future__ import annotations

import argparse
import logging
import time
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import text

from config.lending_missed_call import POLL_LOCK_KEY, POLL_SECONDS
from config.settings import get_settings
from src.lending.call_log import record_call_attempts
from src.lending.compliance import on_attempt_recorded
from src.lending.missed_call_text import consent_gated_sender, parse_cdr, process_missed_calls

logger = logging.getLogger(__name__)


def _records(body: Any) -> list[dict]:
    return list(body.get("items", [])) if isinstance(body, dict) else list(body or [])


def run_cycle(db, *, http, enabled: bool, now: Optional[datetime] = None) -> Optional[dict[str, int]]:
    """None when another poller holds the lock. Does not commit."""
    if not db.execute(text("SELECT pg_try_advisory_xact_lock(:k)"), {"k": POLL_LOCK_KEY}).scalar():
        return None
    records = _records(http("GET", "/cdrs"))
    # Every finished call is an attempt: write it to the call log and run the cap hook,
    # so the 3-attempt rail works even if the dialer never pushes a call event.
    for phone in dict.fromkeys(record_call_attempts(db, records)):
        on_attempt_recorded(db, phone, now=now)
    calls = [c for c in (parse_cdr(r) for r in records) if c is not None]
    return process_missed_calls(db, calls, sender=consent_gated_sender(db), enabled=enabled, now=now)


def main(argv: list[str] | None = None) -> None:
    from src.core.database import get_db_context
    from src.lending.dialer_port import _requests_http

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--once", action="store_true", help="run a single cycle and exit")
    args = parser.parse_args(argv)
    settings = get_settings()
    if settings.batchdialer_api_key is None:
        logger.error("[missed-call-poller] BATCHDIALER_API_KEY is not set")
        return
    http = _requests_http(settings.batchdialer_api_key.get_secret_value())
    while True:
        try:
            with get_db_context() as db:
                counts = run_cycle(db, http=http, enabled=settings.missed_call_text_enabled)
                db.commit()
            if counts:
                logger.info("[missed-call-poller] %s", counts)
        except Exception as exc:
            logger.error("[missed-call-poller] cycle failed (%s); retrying next interval", type(exc).__name__)
            if args.once:
                raise
        if args.once:
            return
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
