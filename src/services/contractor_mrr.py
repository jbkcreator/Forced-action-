"""
Contractor MRR helper.

Contractor MRR = trailing MRR of all paying subscribers in the contractor ICP
(icp_channel_key='contractor'), across all launched counties.

The $50K threshold on this figure is the ICP-only meta-gate; it does NOT
gate county launches.

fa066: icp_channel_key column now exists on Subscriber. Expansion ICP subscribers
are those with icp_channel_key != 'contractor' — excluded from this MRR total.
"""
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from config.icp_channels import DEFAULT_ICP_CHANNEL_KEY
from src.core.models import Subscriber

MRR_ICP_GATE_THRESHOLD = Decimal("50000")

_FREE_TIERS = frozenset(["free", "data_only"])


def global_contractor_mrr(db: Session) -> Decimal:
    """
    Sum plan_price for all active paying contractor-ICP subscribers across all counties.
    Excludes free/data_only tiers and any subscriber attributed to an expansion ICP.

    Returns Decimal so the caller can compare against MRR_ICP_GATE_THRESHOLD
    without float precision risk.
    """
    result = db.execute(
        select(func.coalesce(func.sum(Subscriber.plan_price), 0)).where(
            Subscriber.status == "active",
            Subscriber.tier.notin_(list(_FREE_TIERS)),
            Subscriber.icp_channel_key == DEFAULT_ICP_CHANNEL_KEY,
        )
    ).scalar()
    return Decimal(str(result or 0))
