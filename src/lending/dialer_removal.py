"""Take a phone out of the dialer pool (stop-propagation, attempt cap, call window).

Matches the compliance floor's ``DialerRemover`` contract: called with a phone,
returns nothing, raises on failure so the caller keeps the removal pending
and retries it.

The load row is deactivated only after the Aircall side succeeds, so
``active`` never claims a number is out of the dialer while callers can still
dial it. How a contact is made non-dialable in Aircall (remove it from the
dialer campaign, delete it, or another control) is an open client decision;
until it is made the Aircall step raises and nothing changes.
"""
from __future__ import annotations

import logging
from contextlib import AbstractContextManager
from typing import Any, Callable

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.core.database import get_db_context
from src.services.phone_utils import normalize as normalize_phone

logger = logging.getLogger(__name__)

REMOVED_FROM_DIALER = "removed_from_dialer"

DialerRemoval = Callable[[Any], None]


class DialerRemovalUndecided(RuntimeError):
    """The Aircall control that makes a contact non-dialable is not decided yet."""


def undecided_removal(contact_id: str) -> None:
    raise DialerRemovalUndecided(
        "how a contact is made non-dialable in Aircall is not decided; removal stays pending"
    )


def remove_contact_from_pool(
    phone: str,
    *,
    removal: DialerRemoval = undecided_removal,
    db_context: Callable[[], AbstractContextManager[Session]] = get_db_context,
) -> None:
    """Remove the active load row's contact from the dialer, then deactivate the row.

    Idempotent: a phone with no active load row has nothing to remove.
    """
    normalized = normalize_phone(phone)
    if not normalized:
        raise ValueError("cannot remove an invalid phone from the dialer")
    with db_context() as session:
        row = session.execute(
            text(
                "SELECT id, dialer_contact_id FROM lending.dialer_load_records "
                "WHERE active AND phone = :phone FOR UPDATE"
            ),
            {"phone": normalized},
        ).first()
        if row is None:
            return
        if row.dialer_contact_id is not None:
            removal(row.dialer_contact_id)
        session.execute(
            text(
                "UPDATE lending.dialer_load_records SET active = false, deactivated_at = now(), "
                "deactivation_reason = :reason WHERE id = :id"
            ),
            {"reason": REMOVED_FROM_DIALER, "id": row.id},
        )
    logger.info("[dialer-removal] removed load row id=%s from the dialer", row.id)
