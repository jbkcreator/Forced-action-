"""
Backfill synthflow-eligible / synthflow-suppress tags on existing GHL contacts.

Contacts already synced to GHL before the synthflow tagging logic was added
don't have these tags. This script adds them retroactively.

Usage:
    python scripts/backfill_synthflow_tags.py --dry-run         # preview only
    python scripts/backfill_synthflow_tags.py --limit 50        # first 50
    python scripts/backfill_synthflow_tags.py                   # all synced contacts
"""

import argparse
import logging
import sys
import time

import requests

# ---------------------------------------------------------------------------
# Bootstrap — make imports work when run from repo root
# ---------------------------------------------------------------------------
sys.path.insert(0, ".")

from config.settings import settings
from src.core.database import get_db_context
from src.core.models import Property, DistressScore
from sqlalchemy import select, and_, func

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("backfill_synthflow_tags")

_GHL_BASE = "https://services.leadconnectorhq.com"
_DELAY = 0.5  # seconds between API calls to stay under rate limits


def _headers():
    return {
        "Version": "2021-07-28",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {settings.ghl_api_key.get_secret_value()}",
    }


def _get_contact(contact_id: str) -> dict:
    """Fetch a GHL contact by ID. Returns the contact dict or {}."""
    resp = requests.get(
        f"{_GHL_BASE}/contacts/{contact_id}",
        headers=_headers(),
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json().get("contact", {})


def _update_tags(contact_id: str, tags: list) -> bool:
    """PUT updated tags on a GHL contact. Returns True on success."""
    resp = requests.put(
        f"{_GHL_BASE}/contacts/{contact_id}",
        headers=_headers(),
        json={"tags": tags},
        timeout=15,
    )
    resp.raise_for_status()
    return True


def run(limit: int = 0, dry_run: bool = False):
    if not settings.ghl_api_key or not settings.ghl_location_id:
        logger.error("GHL_API_KEY / GHL_LOCATION_ID not set — aborting")
        return

    # Query synced properties with GHL contact IDs + their latest lead tier
    with get_db_context() as session:
        ds_latest = (
            session.query(
                DistressScore.property_id,
                func.max(DistressScore.score_date).label("max_date"),
            )
            .group_by(DistressScore.property_id)
            .subquery()
        )

        ds_current = (
            session.query(DistressScore.property_id, DistressScore.lead_tier)
            .join(
                ds_latest,
                (DistressScore.property_id == ds_latest.c.property_id)
                & (DistressScore.score_date == ds_latest.c.max_date),
            )
            .subquery()
        )

        q = (
            session.query(Property.id, Property.gohighlevel_contact_id, ds_current.c.lead_tier)
            .join(ds_current, ds_current.c.property_id == Property.id)
            .filter(Property.sync_status == "synced")
            .filter(Property.gohighlevel_contact_id.is_not(None))
        )

        if limit > 0:
            q = q.limit(limit)

        rows = q.all()

    logger.info("Found %d synced contacts to backfill", len(rows))

    stats = {"total": len(rows), "tagged": 0, "skipped": 0, "failed": 0}

    for i, (prop_id, contact_id, lead_tier) in enumerate(rows):
        new_tag = "synthflow-suppress" if lead_tier == "Silver" else "synthflow-eligible"

        if dry_run:
            logger.info("[DRY RUN] property=%d contact=%s → %s", prop_id, contact_id, new_tag)
            stats["tagged"] += 1
            continue

        try:
            # Fetch existing tags to avoid overwriting
            contact = _get_contact(contact_id)
            existing_tags = contact.get("tags", [])

            # Skip if already tagged
            if "synthflow-eligible" in existing_tags or "synthflow-suppress" in existing_tags:
                stats["skipped"] += 1
                if (i + 1) % 50 == 0:
                    logger.info("Progress: %d/%d (skipped=%d)", i + 1, stats["total"], stats["skipped"])
                time.sleep(_DELAY)
                continue

            merged_tags = list(set(existing_tags + [new_tag]))
            _update_tags(contact_id, merged_tags)
            stats["tagged"] += 1

        except requests.exceptions.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 429:
                logger.warning("Rate limited — sleeping 10s before retry")
                time.sleep(10)
                try:
                    contact = _get_contact(contact_id)
                    existing_tags = contact.get("tags", [])
                    merged_tags = list(set(existing_tags + [new_tag]))
                    _update_tags(contact_id, merged_tags)
                    stats["tagged"] += 1
                except Exception:
                    logger.error("Retry failed for contact %s", contact_id, exc_info=True)
                    stats["failed"] += 1
            else:
                logger.error("HTTP error for contact %s: %s", contact_id, exc)
                stats["failed"] += 1
        except Exception as exc:
            logger.error("Error for contact %s: %s", contact_id, exc)
            stats["failed"] += 1

        if (i + 1) % 50 == 0:
            logger.info(
                "Progress: %d/%d — tagged=%d skipped=%d failed=%d",
                i + 1, stats["total"], stats["tagged"], stats["skipped"], stats["failed"],
            )

        time.sleep(_DELAY)

    logger.info("=" * 60)
    logger.info("BACKFILL COMPLETE")
    logger.info("  Total:   %d", stats["total"])
    logger.info("  Tagged:  %d", stats["tagged"])
    logger.info("  Skipped: %d (already had synthflow tag)", stats["skipped"])
    logger.info("  Failed:  %d", stats["failed"])
    logger.info("=" * 60)

    return stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill synthflow tags on existing GHL contacts")
    parser.add_argument("--limit", type=int, default=0, help="Max contacts to process (0 = all)")
    parser.add_argument("--dry-run", action="store_true", help="Preview only, no API calls")
    args = parser.parse_args()

    run(limit=args.limit, dry_run=args.dry_run)
