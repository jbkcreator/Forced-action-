"""ZIP territory claim — shared by checkout (stripe_webhooks.py) and partner-tier
provisioning (partner_tier.py).

`SELECT ... FOR UPDATE` only locks a row that already exists, so it can't
serialize two transactions racing to claim the *same never-before-locked*
(zip_code, vertical, county_id) — both see no row, both decide to create one,
and only the DB's unique constraint (uq_zip_vertical_county) catches it, as an
IntegrityError on the loser. `claim_zip_territory` closes that window by
attempting an atomic `INSERT ... ON CONFLICT DO NOTHING` first: Postgres
resolves the race itself, with no exception on either side. Only when that
insert doesn't win (the row already existed, from an earlier claim/release
cycle) does it fall back to `SELECT ... FOR UPDATE`, which is where the
available/grace/locked branching genuinely needs a lock on an existing row.
"""
from __future__ import annotations

import logging
from datetime import datetime

from sqlalchemy import select, text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class ZipTerritoryUnavailableError(Exception):
    """Raised when a checkout requested a ZIP that could not be claimed.

    Callers (checkout flow) must treat this as fatal to the whole checkout —
    never activate a subscription/account for a buyer who didn't actually get
    every ZIP they paid for. See stripe_webhooks._on_checkout_completed.
    """


def claim_zip_territory(
    db: Session, *, zip_code: str, vertical: str, county_id: str,
    subscriber_id: int, now: datetime,
) -> bool:
    """Lock (zip_code, vertical, county_id) to subscriber_id.

    Returns True if subscriber_id now holds the territory, False if it is
    already locked to a different subscriber (a real caller MUST check this —
    silently proceeding as if the claim succeeded is the exact TOCTOU bug this
    module exists to close). Safe to call repeatedly for the same subscriber.
    """
    from src.core.models import ZipTerritory

    won = db.execute(text("""
        INSERT INTO zip_territories (zip_code, vertical, county_id, subscriber_id, status, locked_at, updated_at)
        VALUES (:zip, :vertical, :county, :subscriber_id, 'locked', :now, :now)
        ON CONFLICT (zip_code, vertical, county_id) DO NOTHING
        RETURNING id
    """), {
        "zip": zip_code, "vertical": vertical, "county": county_id,
        "subscriber_id": subscriber_id, "now": now,
    }).scalar()

    if won is not None:
        return True  # claimed outright — first-ever lock of this territory, no race possible

    # Row already existed (we lost the insert race, or it's a pre-existing
    # available/grace/locked row from an earlier cycle) — existing-row
    # contention is genuinely serializable via FOR UPDATE, unlike the insert.
    territory = db.execute(
        select(ZipTerritory).where(
            ZipTerritory.zip_code == zip_code,
            ZipTerritory.vertical == vertical,
            ZipTerritory.county_id == county_id,
        ).with_for_update()
    ).scalar_one_or_none()

    if territory is None:
        # Only reachable if the row were deleted between the insert attempt and
        # this re-select — nothing in this codebase deletes zip_territories rows.
        logger.warning(
            "ZIP %s/%s/%s: insert lost the race but no row found on re-select — "
            "skipping", zip_code, vertical, county_id,
        )
        return False

    if territory.subscriber_id == subscriber_id and territory.status == "locked":
        return True  # already held by this same subscriber — idempotent re-call

    if territory.status in ("available", "grace"):
        territory.subscriber_id = subscriber_id
        territory.status = "locked"
        territory.locked_at = now
        territory.grace_expires_at = None
        return True

    logger.warning(
        "ZIP %s/%s/%s already locked by subscriber %s — cannot claim for subscriber %s",
        zip_code, vertical, county_id, territory.subscriber_id, subscriber_id,
    )
    return False
