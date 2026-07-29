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

from sqlalchemy import text
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
    rows = db.execute(
        text(
            """
            SELECT id
            FROM opportunity_scores
            WHERE is_automated = FALSE
              AND nbra_score IS NOT NULL
            ORDER BY nbra_score DESC
            LIMIT :lim
            """
        ),
        {"lim": limit},
    ).fetchall()

    ids = [r[0] for r in rows]
    if not ids:
        return []

    scores = [db.get(OpportunityScore, sid) for sid in ids]
    result = [s for s in scores if s is not None]
    logger.info("nbra_engine: ranked queue size=%d (limit=%d)", len(result), limit)
    return result


def get_automated_actions(
    db: Session,
    limit: int = DEFAULT_AUTOMATED_LIMIT,
) -> List[OpportunityScore]:
    """
    Returns is_automated=True OpportunityScore rows.
    These bypass the NBRA queue and go directly to Relay.
    """
    rows = db.execute(
        text(
            """
            SELECT id
            FROM opportunity_scores
            WHERE is_automated = TRUE
            ORDER BY created_at ASC
            LIMIT :lim
            """
        ),
        {"lim": limit},
    ).fetchall()

    ids = [r[0] for r in rows]
    if not ids:
        return []

    scores = [db.get(OpportunityScore, sid) for sid in ids]
    result = [s for s in scores if s is not None]
    logger.info("nbra_engine: automated actions size=%d (limit=%d)", len(result), limit)
    return result
