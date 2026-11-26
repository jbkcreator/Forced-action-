"""Morning secondary lender screening: draft packages for declined / unassigned files and post a card for each new
or changed one to the ops channel. Run by the fa-lending-secondary-screen timer at 07:30 ET; off until
LENDING_SECONDARY_SCREEN_ENABLED.

    PYTHONPATH=. python -m src.tasks.secondary_screen_job [--dry-run]
"""
from __future__ import annotations

import argparse
import logging
from datetime import datetime

from config.lending_routing import ROUTING_TZ
from config.settings import get_settings
from src.lending.db import lending_session
from src.lending.packaging.package import compile_package, render_deal_facts_pdf
from src.lending.packaging.screen import eligible_files, screen_file, secondary_matrix
from src.lending.packaging.slack_card import cora_client, post_card
from src.lending.slack_channels import CORA_BOT_TOKEN, report_unset

logger = logging.getLogger(__name__)


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
    today = datetime.now(ROUTING_TZ).date()
    matrix, version = secondary_matrix()
    drafted = 0
    with lending_session() as db:
        files = eligible_files(db)
        logger.info("[secondary-screen] %d eligible files", len(files))
        for ref in files:
            for package_id in screen_file(db, ref, matrix=matrix, rules_version=version):
                result = compile_package(db, package_id, today=today, renderer=None if dry_run else render_deal_facts_pdf)
                db.commit()
                if result is None:
                    continue
                drafted += 1
                if dry_run or not (channel and client):
                    logger.info("[secondary-screen] package %s (%s) %s, no card posted", package_id,
                                result.lender_key, result.status)
                    continue
                try:
                    post_card(db, client, channel, result)
                    db.commit()
                except Exception as exc:  # class only: Slack errors can echo the message
                    db.rollback()
                    logger.error("[secondary-screen] card for package %s failed (%s)", package_id, type(exc).__name__)
    logger.info("[secondary-screen] %d packages drafted or changed", drafted)
    return drafted


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Draft secondary lender packages and post their cards.")
    parser.add_argument("--dry-run", action="store_true", help="draft and check, render no PDF, post nothing")
    run(dry_run=parser.parse_args().dry_run)
