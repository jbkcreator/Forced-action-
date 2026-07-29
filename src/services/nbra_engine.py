"""
NBRA (Net Business Return per Action) ranking engine — REVINT-I1.

Generalises the human_close_routing candidate-selection pattern for the
full opportunity queue. Produces two lists:
  - ranked queue: non-automated scores sorted by nbra_score DESC → Josh works top-down.
  - automated actions: is_automated=True scores → dispatched to Relay directly.

Called by the REVINT sweep task and any downstream orchestration needing
a prioritised action list.
"""

from __future__ import annotations

import logging
from typing import List

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.core.models import OpportunityScore

logger = logging.getLogger(__name__)

# Hard cap prevents unbounded result sets; callers can pass smaller limits.
DEFAULT_QUEUE_LIMIT = 20
DEFAULT_AUTOMATED_LIMIT = 100


def get_ranked_queue(
    db: Session,
    limit: int = DEFAULT_QUEUE_LIMIT,
) -> List[OpportunityScore]:
    """
    Returns non-automated OpportunityScore rows ordered by nbra_score DESC.
    Rows with nbra_score IS NULL (should not occur for manual actions, but
    guarded) are excluded — only scores with a computable NBRA rank.
    """
    result_rows = db.execute(
        select(OpportunityScore)
        .where(OpportunityScore.is_automated == False)  # noqa: E712
        .where(OpportunityScore.nbra_score.is_not(None))
        .order_by(OpportunityScore.nbra_score.desc())
        .limit(limit)
    ).scalars().all()

    logger.info("nbra_engine: ranked queue size=%d (limit=%d)", len(result_rows), limit)
    return list(result_rows)


def get_automated_actions(
    db: Session,
    limit: int = DEFAULT_AUTOMATED_LIMIT,
) -> List[OpportunityScore]:
    """
    Returns is_automated=True OpportunityScore rows.
    These bypass the NBRA queue and go directly to Relay.
    """
    result_rows = db.execute(
        select(OpportunityScore)
        .where(OpportunityScore.is_automated == True)  # noqa: E712
        .order_by(OpportunityScore.created_at.asc())
        .limit(limit)
    ).scalars().all()

    logger.info("nbra_engine: automated actions size=%d (limit=%d)", len(result_rows), limit)
    return list(result_rows)
