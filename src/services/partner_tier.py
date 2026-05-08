"""
Partner tier service — Phase B.

Handles eligibility checks, ZIP validation, and subscriber provisioning
for the Partner tier (multi-ZIP lock). Called from /api/upgrade/partner
and from _on_checkout_completed when metadata tier == 'partner'.
"""
import logging
from datetime import datetime, timezone
from typing import List

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.core.models import PartnerSubscription, Subscriber, ZipTerritory

logger = logging.getLogger(__name__)

ELIGIBLE_TIERS = ("annual_lock", "autopilot_lite", "autopilot_pro")
DEFAULT_MAX_ZIPS = 5


def is_eligible(sub: Subscriber) -> tuple[bool, str]:
    if sub.status != "active":
        return False, "subscription_not_active"
    if sub.tier not in ELIGIBLE_TIERS:
        return False, f"tier_not_eligible (current: {sub.tier})"
    return True, "eligible"


def validate_zip_selection(
    db: Session,
    zip_codes: List[str],
    vertical: str,
    county_id: str,
    max_zips: int = DEFAULT_MAX_ZIPS,
) -> dict:
    """Return {"ok": True} or {"ok": False, "reason": ..., "zips": [...]}."""
    if not zip_codes:
        return {"ok": False, "reason": "no_zips_provided"}
    if len(zip_codes) > max_zips:
        return {"ok": False, "reason": "max_zips_exceeded", "limit": max_zips}

    locked = db.execute(
        select(ZipTerritory).where(
            ZipTerritory.zip_code.in_(zip_codes),
            ZipTerritory.vertical == vertical,
            ZipTerritory.county_id == county_id,
            ZipTerritory.status == "locked",
        )
    ).scalars().all()

    if locked:
        return {
            "ok": False,
            "reason": "zips_already_locked",
            "zips": [z.zip_code for z in locked],
        }
    return {"ok": True}


def provision_partner_access(
    db: Session,
    subscriber_id: int,
    zip_codes: List[str],
    vertical: str,
    county_id: str,
) -> None:
    """
    Flip tier to 'partner', create PartnerSubscription row, and lock all ZIPs.
    Called inside the same DB transaction as the checkout webhook.
    """
    sub = db.get(Subscriber, subscriber_id)
    if not sub:
        logger.error("provision_partner_access: subscriber %d not found", subscriber_id)
        return

    now = datetime.now(timezone.utc)
    sub.tier = "partner"

    existing = db.execute(
        select(PartnerSubscription).where(PartnerSubscription.subscriber_id == subscriber_id)
    ).scalar_one_or_none()

    if existing is None:
        db.add(PartnerSubscription(
            subscriber_id=subscriber_id,
            max_zips=len(zip_codes),
            activated_at=now,
        ))
    else:
        existing.max_zips = len(zip_codes)
        existing.deactivated_at = None

    for zc in zip_codes:
        territory = db.execute(
            select(ZipTerritory).where(
                ZipTerritory.zip_code == zc,
                ZipTerritory.vertical == vertical,
                ZipTerritory.county_id == county_id,
            ).with_for_update()
        ).scalar_one_or_none()

        if territory is None:
            territory = ZipTerritory(
                zip_code=zc,
                vertical=vertical,
                county_id=county_id,
                subscriber_id=subscriber_id,
                status="locked",
                locked_at=now,
            )
            db.add(territory)
        elif territory.status in ("available", "grace"):
            territory.subscriber_id = subscriber_id
            territory.status = "locked"
            territory.locked_at = now
            territory.grace_expires_at = None
        else:
            logger.warning(
                "partner_provision: ZIP %s already locked by sub %s — skipping",
                zc, territory.subscriber_id,
            )

    db.flush()
    logger.info(
        "partner_provision: sub=%d zips=%s vertical=%s county=%s",
        subscriber_id, zip_codes, vertical, county_id,
    )
