"""T1 cold-email creator (daily cron): file one ``send_email`` agent job per eligible cold lead, up to the daily cap.
See src/lending/cold_email_creator.py for which leads and how.

Off until LENDING_COLD_EMAIL_CREATOR_ENABLED=true. LENDING_COLD_EMAIL_DAILY_CAP (default 25) is the most jobs filed
per ET day; raise it as the sending domain warms up. Sends still wait for the Instantly rail to be configured (the
worker blocks cold jobs until the Instantly key, campaign and footer address are set).

    python -m src.tasks.lending_cold_email_creator [--dry-run]
"""
from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone
from typing import Optional, Sequence

logger = logging.getLogger(__name__)

EXIT_OK, EXIT_FAILED, EXIT_REFUSED = 0, 1, 2


def main(argv: Optional[Sequence[str]] = None) -> int:
    from config.settings import get_settings
    from src.core.database import get_db_context
    from src.lending import cold_email_creator as creator
    from src.lending.global_halt import is_globally_halted

    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description="T1 cold-email creator")
    parser.add_argument("--dry-run", action="store_true", help="count eligible leads; create and file nothing")
    args = parser.parse_args(argv)

    settings = get_settings()
    now = datetime.now(timezone.utc)
    with get_db_context() as session:
        if args.dry_run:
            cap = creator.remaining_cap(session, now, settings.lending_cold_email_daily_cap)
            leads = creator.eligible_leads(session, cap)
            logger.info("[cold-email] dry run: %d lead(s) would be filed today (cap left %d)", len(leads), cap)
            return EXIT_OK
        if not settings.lending_cold_email_creator_enabled:
            logger.info("[cold-email] LENDING_COLD_EMAIL_CREATOR_ENABLED is off; nothing filed")
            return EXIT_OK
        sink = creator.ghl_sink()
        if sink is None:
            logger.error("[cold-email] the Next Deal Lending GHL account is not configured")
            return EXIT_REFUSED
        result = creator.run(session, sink=sink, now=now, daily_cap=settings.lending_cold_email_daily_cap,
                             halted=is_globally_halted)
    logger.info("[cold-email] filed=%d failed=%d halted=%s", result.filed, result.failed, result.halted)
    return EXIT_FAILED if result.failed else EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
