"""
Pure milestone evaluator for the Referral Core Loop.

evaluate() is side-effect-free — it only reads DB state and returns
the list of milestones that are newly crossed for this referrer.
Callers are responsible for persisting grants.
"""

from enum import Enum
from typing import List

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.core.models import ReferralEvent, ReferralMilestoneAward

MILESTONE_THRESHOLDS = {
    "free_month_3": 3,
    "lock_slot_5": 5,
}


class Milestone(str, Enum):
    FREE_MONTH_3 = "free_month_3"
    LOCK_SLOT_5 = "lock_slot_5"


def evaluate(referrer_subscriber_id: int, db: Session) -> List[Milestone]:
    """
    Return milestones newly crossed by referrer_subscriber_id.

    A milestone is 'newly crossed' when:
    - The referrer's confirmed-or-rewarded referral count meets the threshold, AND
    - No referral_milestone_awards row yet exists for that milestone.

    Does not write anything. Safe to call multiple times (idempotent read).
    """
    confirmed_count = db.execute(
        select(ReferralEvent).where(
            ReferralEvent.referrer_subscriber_id == referrer_subscriber_id,
            ReferralEvent.status.in_(("confirmed", "rewarded")),
        )
    ).scalars().all()
    n = len(confirmed_count)

    awarded = set(
        db.execute(
            select(ReferralMilestoneAward.milestone).where(
                ReferralMilestoneAward.referrer_subscriber_id == referrer_subscriber_id
            )
        ).scalars().all()
    )

    newly_crossed: List[Milestone] = []
    for milestone, threshold in MILESTONE_THRESHOLDS.items():
        if n >= threshold and milestone not in awarded:
            newly_crossed.append(Milestone(milestone))

    return newly_crossed
