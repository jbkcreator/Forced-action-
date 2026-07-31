"""Mark all existing subscribers as internal/test accounts.

No real paying customers exist yet. This sets is_test=true on every
current subscriber row so Vera's reconciliation excludes them cleanly.
New real customers will have is_test=false (the column default).

Idempotent. Usage:
    PYTHONPATH=. python migrations/apply_mark_existing_subscribers_as_test.py
"""

import logging

from sqlalchemy import text

from src.core.database import Database

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    db = Database()
    with db.session_scope() as s:
        result = s.execute(text("UPDATE subscribers SET is_test = true WHERE is_test = false"))
        logger.info("Marked %d subscriber rows as is_test=true.", result.rowcount)


if __name__ == "__main__":
    main()
