"""R1 1.7 — Secondary lender screening job (D13, D22, D23).

Run daily at 7:30 ET via systemd timer (no Celery).
Screens eligible files, compiles packages, posts Slack cards.
Gated by LENDING_SECONDARY_SCREEN_ENABLED (default off).

Usage:
    PYTHONPATH=. python -m src.tasks.secondary_screen_job [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import logging
import sys

logger = logging.getLogger(__name__)


def run(*, dry_run: bool = False) -> int:
    """Screen eligible files and post cards. Returns count of packages created/updated."""
    from config.settings import get_settings
    from src.lending.db import get_lending_db_session
    from src.lending.packaging.screen import screen_eligible_files, screen_file, load_eligible_files
    from src.lending.packaging.package import compile_package
    from src.lending.packaging.slack_card import build_card, post_card

    s = get_settings()
    if not s.lending_secondary_screen_enabled:
        logger.info("[secondary-screen] LENDING_SECONDARY_SCREEN_ENABLED=false — skipping")
        return 0

    with get_lending_db_session() as db:
        eligible = load_eligible_files(db)
        logger.info("[secondary-screen] %d eligible files", len(eligible))

        total_packages = 0
        for facts in eligible:
            pkg_ids = screen_file(db, facts)
            db.commit()
            for pkg_id in pkg_ids:
                result = compile_package(db, pkg_id)
                db.commit()
                if result is None:
                    continue
                total_packages += 1
                if dry_run:
                    logger.info("[secondary-screen] dry-run: would post card for pkg %d lender %s",
                                pkg_id, result.lender_key)
                    continue
                # load package row for the card
                from sqlalchemy import text
                row = db.execute(text("""
                    SELECT lender_key, box_confirmed, checklist, validator_verdict, fit_result
                    FROM lending.lender_packages WHERE id = :id
                """), {"id": pkg_id}).mappings().first()
                if row is None:
                    continue
                checklist = row["checklist"] or []
                if isinstance(checklist, str):
                    import json as _json
                    checklist = _json.loads(checklist)
                card = build_card(
                    package_id=pkg_id,
                    lender_key=row["lender_key"],
                    box_confirmed=row["box_confirmed"],
                    checklist=checklist,
                    verdict_json=row["validator_verdict"],
                )
                if s.lending_ops_channel:
                    try:
                        from slack_sdk import WebClient
                        client = WebClient(token=s.slack_bot_token)
                        post_card(card, channel=s.lending_ops_channel, slack_client=client)
                    except Exception as exc:
                        logger.error("[secondary-screen] Slack card failed for pkg %d: %s",
                                     pkg_id, type(exc).__name__)

        logger.info("[secondary-screen] done: %d packages", total_packages)
        return total_packages


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Secondary lender screening job (R1 1.7)")
    parser.add_argument("--dry-run", action="store_true", help="Screen and compile but do not post Slack cards")
    args = parser.parse_args()
    count = run(dry_run=args.dry_run)
    sys.exit(0)
