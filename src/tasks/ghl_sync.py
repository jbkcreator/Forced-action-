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

from sqlalchemy import func
from sqlalchemy.orm import joinedload

from src.core.database import get_db_context
from src.core.models import DistressScore, Property
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


def _build_score_data_from_db(prop: Property, ds: DistressScore) -> dict:
    """Build the score_data dict from already-computed DB rows (no rescore)."""
    owner = prop.owner
    fin   = prop.financial
    owner_phone = None
    owner_email = None
    if owner:
        owner_phone = owner.phone_1 or owner.phone_2 or owner.phone_3 or None
        owner_email = owner.email_1 or owner.email_2 or None
    return {
        "property_id":        prop.id,
        "parcel_id":          prop.parcel_id,
        "address":            prop.address,
        "city":               prop.city,
        "state":              prop.state,
        "zip":                prop.zip,
        "sq_ft":              float(prop.sq_ft) if prop.sq_ft else None,
        "beds":               prop.beds,
        "baths":              prop.baths,
        "year_built":         prop.year_built,
        "lot_size":           float(prop.lot_size) if prop.lot_size else None,
        "ghl_contact_id":     prop.gohighlevel_contact_id,
        "owner_name":         owner.owner_name if owner else None,
        "owner_type":         owner.owner_type if owner else None,
        "absentee_status":    owner.absentee_status if owner else None,
        "mailing_address":    owner.mailing_address if owner else None,
        "ownership_years":    owner.ownership_years if owner else None,
        "owner_phone":        owner_phone,
        "owner_email":        owner_email,
        "assessed_value_mkt": float(fin.assessed_value_mkt) if fin and fin.assessed_value_mkt else None,
        "homestead_exempt":   fin.homestead_exempt if fin else None,
        "est_equity":         float(fin.est_equity) if fin and fin.est_equity else None,
        "equity_pct":         float(fin.equity_pct) if fin and fin.equity_pct else None,
        "last_sale_price":    float(fin.last_sale_price) if fin and fin.last_sale_price else None,
        "last_sale_date":     str(fin.last_sale_date) if fin and fin.last_sale_date else None,
        "final_cds_score":    round(float(ds.final_cds_score or 0), 2),
        "vertical_scores":    {k: round(v, 2) for k, v in (ds.vertical_scores or {}).items()},
        "urgency_level":      ds.urgency_level,
        "lead_tier":          ds.lead_tier,
        "qualified":          ds.qualified,
        "signal_count":       len(ds.distress_types or []),
        "distress_types":     list(ds.distress_types or []),
        "signal_summaries":   {},  # not persisted in distress_scores; CRM detail fields left blank
    }


def run_ghl_sync(
    limit: int = 5000,
    county_id: str = "hillsborough",
) -> dict:
    """
    Push pending_sync properties to GHL and mark them synced.

    Reads the already-computed score from distress_scores instead of
    rescoring via MultiVerticalScorer — eliminates the biggest latency
    source (1,019 full rescores for ~60 min → under 10 min).

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
            )
            .filter(
                Property.sync_status == "pending_sync",
                Property.county_id == county_id,
            )
            .limit(limit)
            .all()
        )

        # Batch-load the latest DistressScore per property in one query
        _property_ids = [p.id for p in properties]
        if _property_ids:
            _latest_sq = (
                session.query(
                    DistressScore.property_id,
                    func.max(DistressScore.score_date).label("max_date"),
                )
                .filter(DistressScore.property_id.in_(_property_ids))
                .group_by(DistressScore.property_id)
                .subquery()
            )
            _ds_rows = session.query(DistressScore).join(
                _latest_sq,
                (DistressScore.property_id == _latest_sq.c.property_id)
                & (DistressScore.score_date == _latest_sq.c.max_date),
            ).all()
            score_map = {ds.property_id: ds for ds in _ds_rows}
        else:
            score_map = {}

        stats["total"] = len(properties)
        logger.info("[GHL Sync] Processing %d pending_sync properties", stats["total"])

        # Accumulate (property_id, contact_id) pairs for batch DB commits
        pending_db_updates: list = []

        for prop in properties:
            try:
                # Read pre-computed score — no rescore needed
                ds = score_map.get(prop.id)
                if ds is None:
                    logger.warning(
                        "[GHL Sync] No distress_scores row for %s — skipping", prop.parcel_id
                    )
                    stats["skipped"] += 1
                    pending_db_updates.append((prop.id, prop.gohighlevel_contact_id, "synced"))
                    continue

                score_data = _build_score_data_from_db(prop, ds)
                contact_id = None
                last_exc = None

                # Retry loop — up to _PUSH_MAX_RETRIES attempts for transient GHL API failures
                for attempt in range(_PUSH_MAX_RETRIES):
                    try:
                        contact_id = push_lead_to_ghl(score_data)
                        last_exc = None
                        break  # success
                    except Exception as exc:
                        last_exc = exc
                        if attempt < _PUSH_MAX_RETRIES - 1:
                            logger.warning(
                                "[GHL Sync] Attempt %d/%d failed for %s — retrying in 0.5s: %s",
                                attempt + 1, _PUSH_MAX_RETRIES, prop.parcel_id, exc,
                            )
                            time.sleep(0.5)

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
