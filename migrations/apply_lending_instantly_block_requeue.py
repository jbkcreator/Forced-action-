"""One-time data fix (R1 0.11 review): ``instantly_client.block_address`` sent ``{"entries": [...]}`` to Instantly's
single-entry endpoint, which takes ``{"bl_value": ...}``, so block-list adds failed and rows ended ``failed``. This puts
every failed row back to ``pending`` so Cora's sweep sends it again with the fixed body.

Run ONCE, AFTER the fix is deployed (before it, the rows would just fail again). Dry run unless --apply.

Usage:
    PYTHONPATH=. python migrations/apply_lending_instantly_block_requeue.py [--apply]
"""
from __future__ import annotations

import argparse
import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_COUNT = text("SELECT count(*) FROM lending.instantly_block_queue WHERE status = 'failed'")
_REQUEUE = text("""
    UPDATE lending.instantly_block_queue SET status = 'pending', attempts = 0, next_attempt_at = now(), last_error = NULL
     WHERE status = 'failed'
""")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--apply", action="store_true", help="re-queue the failed rows (default: count only)")
    apply = parser.parse_args(argv).apply
    engine = create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        failed = conn.execute(_COUNT).scalar() or 0
        if apply:
            conn.execute(_REQUEUE)
    logger.info("instantly_block_queue failed rows: %d (%s)", failed, "re-queued" if apply else "dry run, nothing changed")


if __name__ == "__main__":
    main()
