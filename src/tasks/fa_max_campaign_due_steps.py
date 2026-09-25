"""FA Max WP-T3-4 — due-step sweep (every 15 minutes).

Hands every due, unblocked campaign touch to the right drafting agent's
work queue (investor -> fa_max_outreach / WP-T3-5, partner ->
fa_max_partner_nurture / WP-T3-6). Contains no send code — see
src.services.fa_max_campaigns.selection.process_due_touches for the full
consent/suppression/hold logic (plan Section 6.2, 6.5).

Run every config.fa_max_campaigns.DUE_STEP_SWEEP_MINUTES minutes:

    python -m src.tasks.fa_max_campaign_due_steps
"""
from __future__ import annotations

import logging

from src.core.database import get_db_context
from src.services.fa_max_campaigns.selection import process_due_touches

logger = logging.getLogger(__name__)


def main() -> None:
    with get_db_context() as session:
        summary = process_due_touches(session)

    logger.info(
        "fa_max_campaigns due-step sweep: handed_off=%d held=%d skipped=%d ended_no_channel=%d",
        summary["handed_off"], summary["held"], summary["skipped"], summary["ended_no_channel"],
    )
    print(summary)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
