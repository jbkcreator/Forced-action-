"""
Shared lead-delivery filters and ordering.

Single source of truth for two rules used by every path that hands leads to
a subscriber (daily email, feed API, sample-leads, lead pack fulfillment,
GHL CRM sync, proof-moment):

  * has_contact_filter — when settings.debug is False, require at least one
    phone or email on the Owner row. When debug is True, return None so the
    caller can keep contactless leads visible (needed in dev/test where most
    properties haven't been skip-traced).

  * phone_priority_order — sorts phone-bearing leads to the top, then by the
    caller's score column descending. Sort-only (no rows dropped), so it is
    safe to apply unconditionally regardless of the debug flag.
"""

from typing import Optional

from sqlalchemy import case, desc, or_

from src.core.models import Owner


def _any_phone():
    return or_(
        Owner.phone_1.isnot(None),
        Owner.phone_2.isnot(None),
        Owner.phone_3.isnot(None),
    )


def _any_email():
    return or_(
        Owner.email_1.isnot(None),
        Owner.email_2.isnot(None),
    )


def has_contact_filter(settings) -> Optional[object]:
    """
    Return a SQLAlchemy WHERE clause requiring any-phone or any-email on
    Owner, or None when the debug flag is on (caller skips appending).
    """
    if settings.debug:
        return None
    return or_(_any_phone(), _any_email())


def phone_priority_order(score_col) -> list:
    """
    Return a list of order_by expressions putting phone-bearing leads first,
    then ordering by the caller's score column descending.

    Use as: query.order_by(*phone_priority_order(score_col))
    """
    return [
        case((_any_phone(), 0), else_=1),
        desc(score_col),
    ]
