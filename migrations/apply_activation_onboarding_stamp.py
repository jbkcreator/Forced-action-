"""Section 4.10 — add onboarding_completed_time to activation_events.

Fills the one gap in the 5-minute activation funnel (T-B12-05): the only
step between signup and first-leads-shown is the one-time preference form
(PATCH /onboarding/{feed_uuid}), and it wasn't stamped. Without it, "never
finished onboarding" and "finished onboarding, never saw a lead" were
indistinguishable in the funnel data.

Idempotent. Usage:
    PYTHONPATH=. python migrations/apply_activation_onboarding_stamp.py
"""

import logging

from sqlalchemy import text

from src.core.database import Database

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    db = Database()
    with db.session_scope() as s:
        s.execute(text(
            "ALTER TABLE activation_events "
            "ADD COLUMN IF NOT EXISTS onboarding_completed_time TIMESTAMPTZ"
        ))
    logger.info("activation_events.onboarding_completed_time applied.")


if __name__ == "__main__":
    main()
