"""Morning secondary lender screening: draft packages for declined / unassigned files and post a card for each new
or changed one to the ops channel. Run by the fa-lending-secondary-screen timer at 07:30 ET; off until
LENDING_SECONDARY_SCREEN_ENABLED.

    PYTHONPATH=. python -m src.tasks.secondary_screen_job [--dry-run]
"""
from __future__ import annotations

import argparse
import logging
from datetime import datetime

from sqlalchemy import text

from config.lending_routing import ROUTING_TZ
from config.settings import get_settings
from src.lending.db import lending_session
from src.lending.packaging.package import compile_package, render_deal_facts_pdf
from src.lending.packaging.screen import eligible_files, screen_file, secondary_matrix
from src.lending.packaging.slack_card import cora_client, post_card, refresh_card
from src.lending.slack_channels import CORA_BOT_TOKEN, report_unset

logger = logging.getLogger(__name__)


# Open packages whose card was never delivered (Slack down, or not configured at the time); post_card stores "" for a
# missing ts, so both count.
_UNDELIVERED = text("SELECT id FROM lending.lender_packages "
                    "WHERE status NOT IN ('submitted', 'dismissed') AND COALESCE(card_ts, '') = '' ORDER BY id")
RETIRED_NOTE = ":wastebasket: Retired by the morning screen: this lender no longer fits the file."


def run(*, dry_run: bool = False) -> int:
    """Returns how many packages were drafted or changed."""
    settings = get_settings()
    if not settings.lending_secondary_screen_enabled:
        logger.info("[secondary-screen] LENDING_SECONDARY_SCREEN_ENABLED is off")
        return 0
    channel = settings.lending_ops_channel
    client = None if dry_run else cora_client()
    if not dry_run and not (channel and client):
        report_unset("secondary lender package card", ("LENDING_OPS_CHANNEL", CORA_BOT_TOKEN))
    can_post = not dry_run and bool(channel and client)
    today = datetime.now(ROUTING_TZ).date()
    matrix, version = secondary_matrix()
    drafted = 0
    with lending_session() as db:
        files = eligible_files(db)
        logger.info("[secondary-screen] %d eligible files", len(files))
        changed: list[int] = []
        for ref in files:
            screened = screen_file(db, ref, matrix=matrix, rules_version=version)
            db.commit()
            changed += screened.changed
            for package_id in screened.retired if can_post else ():
                refresh_card(client, db, package_id, RETIRED_NOTE, today)
        seen = set(changed)
        retry = [i for i in db.execute(_UNDELIVERED).scalars() if i not in seen] if can_post else []
        if retry:
            logger.info("[secondary-screen] %d undelivered cards retried", len(retry))
        for package_id in [*changed, *retry]:
            result = compile_package(db, package_id, today=today, renderer=None if dry_run else render_deal_facts_pdf)
            db.commit()
            if result is None:
                continue
            if package_id in seen:
                drafted += 1
            if not can_post:
                logger.info("[secondary-screen] package %s (%s) %s, no card posted", package_id,
                            result.lender_key, result.status)
                continue
            try:
                post_card(db, client, channel, result)
                db.commit()
            except Exception as exc:  # class only: Slack errors can echo the message
                db.rollback()
                logger.error("[secondary-screen] card for package %s failed (%s); retried next run", package_id,
                             type(exc).__name__)
    logger.info("[secondary-screen] %d packages drafted or changed", drafted)
    return drafted

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Draft secondary lender packages and post their cards.")
    parser.add_argument("--dry-run", action="store_true", help="draft and check, render no PDF, post nothing")
    run(dry_run=parser.parse_args().dry_run)
