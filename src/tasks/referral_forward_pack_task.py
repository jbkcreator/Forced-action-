"""
Weekly referral forward-pack renderer task.

Renders Claude-written per-vertical share copy and caches it in
referral_forward_copy. Idempotent: re-running the same Monday is a no-op.

Usage:
    python -m src.tasks.referral_forward_pack_task

Cron (Monday at 03:00 AM UTC):
    0 3 * * 1 $PROJECT/scripts/cron/run.sh src.tasks.referral_forward_pack_task
"""

import logging

from src.core.database import Database
from src.services.forward_pack_renderer import render_weekly

logger = logging.getLogger(__name__)


def main() -> None:
    db_factory = Database()
    with db_factory.session_scope() as db:
        results = render_weekly(db)
    logger.info("[ForwardPackTask] done. verticals rendered: %s", list(results.keys()))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
