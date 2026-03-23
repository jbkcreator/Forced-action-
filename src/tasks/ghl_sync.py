"""
Async GHL CRM sync task.

Decoupled from the CDS scoring loop. Processes properties marked
sync_status='pending_sync' and pushes them to GoHighLevel in batches.

Fixes the two problems caused by the old inline-push approach:
  1. Synchronous API calls (4 per lead × 0.5s) no longer block the scoring
     process or sit in an hours-long queue after all 523k properties are scored.
  2. Individual per-lead DB commits on the properties table no longer create
     thousands of long-lived transactions that blocked scraper sessions.

Instead:
  - CDS scoring marks qualifying properties as pending_sync (one batch UPDATE).
  - This task runs independently, calls the GHL API at its own pace, and
    commits DB updates in batches of _DB_BATCH_SIZE rows.

Usage:
    python -m src.tasks.ghl_sync
    python -m src.tasks.ghl_sync --limit 2000 --county-id hillsborough
"""

import logging
import time
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy.orm import joinedload

from src.core.database import get_db_context
from src.core.models import Property
from src.services.ghl_webhook import push_lead_to_ghl, _is_configured, _ghl_request, _headers

logger = logging.getLogger(__name__)

_DB_BATCH_SIZE = 100  # commit this many property updates per transaction
_PUSH_MAX_RETRIES = 3  # per-property retry attempts for transient failures


def _check_ghl_health() -> bool:
    """
    Lightweight GHL API health check — fetches the location record.
    Returns True if reachable and authenticated, False otherwise.
    Prevents kicking off a batch push when GHL is down or keys are wrong.
    """
    from config.settings import get_settings
    settings = get_settings()
    if not settings.ghl_location_id:
        return False
    try:
        url = f"https://services.leadconnectorhq.com/locations/{settings.ghl_location_id}"
        resp = _ghl_request("GET", url, headers=_headers())
        if resp.status_code == 200:
            logger.info("[GHL Sync] Health check passed (HTTP 200)")
            return True
        logger.warning("[GHL Sync] Health check failed — HTTP %d", resp.status_code)
        return False
    except Exception as exc:
        logger.error("[GHL Sync] Health check exception: %s", exc)
        return False


def run_ghl_sync(
    limit: int = 5000,
    county_id: str = "hillsborough",
) -> dict:
    """
    Push pending_sync properties to GHL and mark them synced.

    Loads each property with all signal relationships so that
    MultiVerticalScorer.score_property() can rebuild the full score_data
    (including signal_summaries and vertical_scores) needed by the GHL payload.

    DB updates are committed in batches of _DB_BATCH_SIZE to minimise
    transaction duration on the properties table.

    Returns:
        dict with counts: {total, pushed, failed, skipped}
    """
    if not _is_configured():
        logger.info("[GHL Sync] GHL not configured — nothing to do")
        return {"total": 0, "pushed": 0, "failed": 0, "skipped": 0}

    # Pre-run health check — abort early if GHL is unreachable
    if not _check_ghl_health():
        logger.error(
            "[GHL Sync] Pre-run health check failed — aborting to avoid mass sync_failed marks"
        )
        return {"total": 0, "pushed": 0, "failed": 0, "skipped": 0}

    stats = {"total": 0, "pushed": 0, "failed": 0, "skipped": 0}

    with get_db_context() as session:
        # Import here to avoid circular import at module level
        from src.services.cds_engine import MultiVerticalScorer

        scorer = MultiVerticalScorer(session)

        # Pre-run stats: log how many pending + how many failed last run
        pending_count = session.query(Property).filter(
            Property.sync_status == "pending_sync", Property.county_id == county_id
        ).count()
        failed_count = session.query(Property).filter(
            Property.sync_status == "sync_failed", Property.county_id == county_id
        ).count()
        logger.info(
            "[GHL Sync] Pre-run stats — pending_sync: %d, sync_failed: %d",
            pending_count, failed_count,
        )

        properties = (
            session.query(Property)
            .options(
                joinedload(Property.owner),
                joinedload(Property.financial),
                joinedload(Property.code_violations),
                joinedload(Property.legal_and_liens),
                joinedload(Property.deeds),
                joinedload(Property.legal_proceedings),
                joinedload(Property.tax_delinquencies),
                joinedload(Property.foreclosures),
                joinedload(Property.building_permits),
            )
            .filter(
                Property.sync_status == "pending_sync",
                Property.county_id == county_id,
            )
            .limit(limit)
            .all()
        )

        stats["total"] = len(properties)
        logger.info("[GHL Sync] Processing %d pending_sync properties", stats["total"])

        # Accumulate (property_id, contact_id) pairs for batch DB commits
        pending_db_updates: list = []

        for prop in properties:
            try:
                contact_id = None
                last_exc = None

                # Retry loop — up to _PUSH_MAX_RETRIES attempts for transient failures
                for attempt in range(_PUSH_MAX_RETRIES):
                    try:
                        score_data = scorer.score_property(prop)
                        contact_id = push_lead_to_ghl(score_data)
                        last_exc = None
                        break  # success
                    except Exception as exc:
                        last_exc = exc
                        if attempt < _PUSH_MAX_RETRIES - 1:
                            wait = 2 ** attempt
                            logger.warning(
                                "[GHL Sync] Attempt %d/%d failed for %s — retrying in %ds: %s",
                                attempt + 1, _PUSH_MAX_RETRIES, prop.parcel_id, wait, exc,
                            )
                            time.sleep(wait)

                if last_exc is not None:
                    logger.error(
                        "[GHL Sync] All %d attempts failed for property %s (%s): %s",
                        _PUSH_MAX_RETRIES, prop.id, prop.parcel_id, last_exc,
                    )
                    stats["failed"] += 1
                    pending_db_updates.append((prop.id, prop.gohighlevel_contact_id, "sync_failed"))
                elif not contact_id:
                    # Below score threshold — mark synced so it doesn't re-loop
                    logger.debug("[GHL Sync] Skipped (below threshold): %s", prop.parcel_id)
                    stats["skipped"] += 1
                    pending_db_updates.append((prop.id, prop.gohighlevel_contact_id, "synced"))
                else:
                    stats["pushed"] += 1
                    pending_db_updates.append((prop.id, contact_id, "synced"))

            except Exception as exc:
                # Catch-all: never let a single property kill the entire sync run
                logger.error(
                    "[GHL Sync] Unexpected error for property %s (%s) — skipping: %s",
                    prop.id, prop.parcel_id, exc,
                )
                stats["failed"] += 1
                try:
                    pending_db_updates.append((prop.id, prop.gohighlevel_contact_id, "sync_failed"))
                except Exception:
                    pass  # property object may be in a bad state — just move on

            # Commit in batches to keep transaction duration short
            if len(pending_db_updates) >= _DB_BATCH_SIZE:
                try:
                    _batch_update(session, pending_db_updates)
                except Exception as exc:
                    logger.error("[GHL Sync] Batch commit failed: %s — rolling back", exc)
                    session.rollback()
                pending_db_updates.clear()

        # Final partial batch
        if pending_db_updates:
            try:
                _batch_update(session, pending_db_updates)
            except Exception as exc:
                logger.error("[GHL Sync] Final batch commit failed: %s — rolling back", exc)
                session.rollback()

    logger.info(
        "[GHL Sync] Complete — pushed=%d failed=%d skipped=%d / total=%d",
        stats["pushed"], stats["failed"], stats["skipped"], stats["total"],
    )
    return stats


def _batch_update(session, updates: list) -> None:
    """
    Batch-commit gohighlevel_contact_id + sync_status + last_crm_sync.

    Each call commits one transaction covering up to _DB_BATCH_SIZE rows —
    far cheaper than the old pattern of one transaction per lead.

    Handles duplicate gohighlevel_contact_id gracefully — when GHL returns
    the same contact ID for multiple properties (e.g. same owner, multiple
    parcels), we store the contact_id on the first property and mark the
    rest as synced without the contact_id to avoid unique constraint violations.
    """
    now = datetime.now(timezone.utc)
    for prop_id, contact_id, sync_status in updates:
        values = {"sync_status": sync_status, "last_crm_sync": now, "updated_at": now}
        if contact_id:
            # Check if another property already holds this GHL contact ID
            existing = session.query(Property.id).filter(
                Property.gohighlevel_contact_id == contact_id,
                Property.id != prop_id,
            ).first()
            if existing:
                logger.info(
                    "[GHL Sync] Contact %s already linked to property %d — "
                    "skipping contact_id for property %d (same owner, multiple parcels)",
                    contact_id, existing[0], prop_id,
                )
            else:
                values["gohighlevel_contact_id"] = contact_id
        try:
            session.query(Property).filter(Property.id == prop_id).update(
                values, synchronize_session=False
            )
        except Exception as exc:
            session.rollback()
            logger.warning(
                "[GHL Sync] Update failed for property %d: %s — marking synced without contact_id",
                prop_id, exc,
            )
            session.query(Property).filter(Property.id == prop_id).update(
                {"sync_status": sync_status, "last_crm_sync": now, "updated_at": now},
                synchronize_session=False,
            )
    session.commit()
    logger.debug("[GHL Sync] Batch committed %d property updates", len(updates))


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Push pending_sync properties to GHL CRM")
    parser.add_argument("--limit",      type=int, default=5000, help="Max properties to process")
    parser.add_argument("--county-id",  default="hillsborough",  help="County ID filter")
    args = parser.parse_args()

    result = run_ghl_sync(limit=args.limit, county_id=args.county_id)
    print(result)
