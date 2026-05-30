"""
Contractor MRR helper.

Contractor MRR = trailing MRR of all paying subscribers in the existing
contractor lead product (all 6 Trade verticals), across all launched
counties, EXCLUDING subscribers attributed to an Expansion ICP Channel.

The $50K threshold on this figure is the ICP-only meta-gate; it does NOT
gate county launches.

ICP attribution seam: no ICP subscribers exist yet.  The exclusion is a
no-op in v1.  When a subscriber.icp_channel_key column is added (future
migration), update _is_icp_subscriber() to filter on it and the gate will
automatically tighten without further changes here.
"""
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from src.core.models import Subscriber

MRR_ICP_GATE_THRESHOLD = Decimal("50000")

_FREE_TIERS = frozenset(["free", "data_only"])


def _is_icp_subscriber_filter():
    """
    Returns a SQLAlchemy filter for ICP-attributed subscribers.

    v1: empty — no icp_channel_key column yet.
    TODO(icp-attribution): replace with
        Subscriber.icp_channel_key.isnot(None)
    once the column is added.
    """
    return None  # no-op filter


def global_contractor_mrr(db: Session) -> Decimal:
    """
    Sum plan_price for all active paying subscribers across all counties,
    excluding free/data_only tiers and ICP-channel subscribers.

    Returns Decimal so the caller can compare against MRR_ICP_GATE_THRESHOLD
    without float precision risk.
    """
    filters = [
        Subscriber.status == "active",
        Subscriber.tier.notin_(list(_FREE_TIERS)),
    ]
    icp_filter = _is_icp_subscriber_filter()
    if icp_filter is not None:
        filters.append(~icp_filter)

    result = db.execute(
        select(func.coalesce(func.sum(Subscriber.plan_price), 0)).where(*filters)
    ).scalar()
    return Decimal(str(result or 0))
