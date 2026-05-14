"""fa016 — backfill Subscriber.phone for legacy rows.

Sources in priority order:
  1. Owner.phone_1 via a ZipTerritory link (subscriber's locked ZIP → owners
     in that ZIP). This is loose, so we only accept a match when exactly one
     non-null phone is found for the territory — multiple matches are
     ambiguous and skipped.
  2. Stripe PaymentMethod.billing_details.phone (requires stripe_payment_method_id
     and Stripe access).

Idempotent: only fills when phone is NULL. Run as needed; safe to re-run.

Usage:
    python scripts/backfill_subscriber_phone.py [--dry-run] [--source owners|stripe|all]
"""

from __future__ import annotations

import argparse
import logging
import sys

from sqlalchemy import func, select

from src.core.database import get_db_context
from src.core.models import Owner, Property, Subscriber, ZipTerritory

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _normalize(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return s
    if s.startswith("+"):
        return s
    digits = "".join(c for c in s if c.isdigit())
    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    return s


def _from_owners(db, sub: Subscriber) -> str | None:
    zips = db.execute(
        select(ZipTerritory.zip_code).where(
            ZipTerritory.subscriber_id == sub.id,
            ZipTerritory.status.in_(["locked", "grace"]),
        )
    ).scalars().all()
    if not zips:
        return None
    rows = db.execute(
        select(func.count(), func.min(Owner.phone_1)).where(
            Owner.property_id.in_(
                select(Property.id).where(Property.zip.in_(zips))
            ),
            Owner.phone_1.is_not(None),
        )
    ).first()
    count, sample = rows or (0, None)
    if count != 1 or not sample:
        return None
    return _normalize(sample)


def _from_stripe(sub: Subscriber) -> str | None:
    if not sub.stripe_payment_method_id:
        return None
    try:
        from config.settings import settings
        import stripe
        key = settings.active_stripe_secret_key
        if not key:
            return None
        stripe.api_key = key.get_secret_value()
        pm = stripe.PaymentMethod.retrieve(sub.stripe_payment_method_id)
        phone = (pm.get("billing_details") or {}).get("phone")
        if not phone:
            return None
        return _normalize(phone)
    except Exception as exc:
        logger.warning("stripe lookup failed sub=%s: %s", sub.id, exc)
        return None


def run(dry_run: bool, source: str) -> dict:
    counts = {"scanned": 0, "filled_owners": 0, "filled_stripe": 0, "skipped_conflict": 0}
    with get_db_context() as db:
        subs = db.execute(
            select(Subscriber).where(Subscriber.phone.is_(None))
        ).scalars().all()
        counts["scanned"] = len(subs)

        # phones already taken (avoid uniqueness collisions)
        taken = set(db.execute(
            select(Subscriber.phone).where(Subscriber.phone.is_not(None))
        ).scalars().all())

        for sub in subs:
            phone = None
            from_src = None
            if source in ("owners", "all"):
                phone = _from_owners(db, sub)
                from_src = "owners" if phone else None
            if phone is None and source in ("stripe", "all"):
                phone = _from_stripe(sub)
                from_src = "stripe" if phone else None
            if not phone:
                continue
            if phone in taken:
                counts["skipped_conflict"] += 1
                continue
            taken.add(phone)
            logger.info("backfill sub=%s phone=%s source=%s", sub.id, phone, from_src)
            if not dry_run:
                sub.phone = phone
                db.flush()
            if from_src == "owners":
                counts["filled_owners"] += 1
            else:
                counts["filled_stripe"] += 1
    return counts


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--source", choices=("owners", "stripe", "all"), default="all")
    args = parser.parse_args()
    result = run(dry_run=args.dry_run, source=args.source)
    logger.info("[BackfillPhone] %s", result)
    print(result)
