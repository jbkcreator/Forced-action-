"""
DBPR → Clay CRM Sync

Pushes certified DBPR contractors (with a scraped DBA company name) to the
Clay webhook. Only rows where clay_synced=False are processed so each
contractor is sent exactly once.

Run:
    python -m src.tasks.clay_sync
    python -m src.tasks.clay_sync --limit 50
    python -m src.tasks.clay_sync --dry-run
"""

import time
import logging
from datetime import datetime, timezone

import requests

from config.settings import get_settings
from src.core.database import get_db_context
from src.core.models import DBPRContact
from src.utils.logger import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)

_DEFAULT_LIMIT = 100
_DELAY_BETWEEN_REQUESTS = 0.5  # seconds — polite to the webhook

# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------

def _fetch_unsynced(limit: int) -> list[DBPRContact]:
    with get_db_context() as db:
        rows = (
            db.query(DBPRContact)
            .filter(
                DBPRContact.data_source == "certified",
                DBPRContact.company_name.isnot(None),
                DBPRContact.company_name != "",
                DBPRContact.company_name_status == "found",
                DBPRContact.clay_synced == False,  # noqa: E712
            )
            .order_by(DBPRContact.updated_at.desc())
            .limit(limit)
            .all()
        )
    return rows


# ---------------------------------------------------------------------------
# Payload builder (mirrors ext_tasks/clay_setup.py shape)
# ---------------------------------------------------------------------------

def _build_payload(contact: DBPRContact) -> dict:
    trade = contact.vertical or "roofing"
    county_id = contact.county_id or "hillsborough"
    return {
        "lead_id": str(contact.id),
        "county_id": county_id,
        "trade": trade,
        "license_type": "certified",
        "license_number": contact.license_number,
        "company_name": contact.company_name,
        "licensee_name": contact.full_name or "",
        "address": contact.address or "",
        "city": contact.city or "",
        "state": contact.state or "FL",
        "zip": contact.zip_code or "",
        "source": "dbpr",
        "sequence_variant": f"{county_id}_{trade}_a",
    }


# ---------------------------------------------------------------------------
# Mark synced
# ---------------------------------------------------------------------------

def _mark_synced(contact_id: int, now: datetime) -> None:
    with get_db_context() as db:
        contact = db.get(DBPRContact, contact_id)
        if contact:
            contact.clay_synced = True
            contact.clay_synced_at = now
            contact.updated_at = now
            db.add(contact)


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------

def run_clay_sync(
    limit: int = _DEFAULT_LIMIT,
    dry_run: bool = False,
) -> dict:
    """
    Push up to `limit` unsynced certified DBPR contractors to the Clay webhook.

    Returns a stats dict with keys: total, sent, failed.
    """
    webhook_url = get_settings().clay_dbpr_webhook_url
    if not webhook_url:
        raise RuntimeError("CLAY_DBPR_WEBHOOK_URL is not configured")

    stats = {"total": 0, "sent": 0, "failed": 0}

    rows = _fetch_unsynced(limit)
    if not rows:
        logger.info("[ClaySyncTask] No unsynced certified contractors found — nothing to do.")
        return stats

    stats["total"] = len(rows)
    logger.info("[ClaySyncTask] Found %d unsynced certified contractor(s) to push (limit=%d).",
                len(rows), limit)

    if dry_run:
        for c in rows[:5]:
            logger.info("[ClaySyncTask DRY RUN] Would push: %s | %s | %s",
                        c.license_number, c.company_name, c.vertical)
        logger.info("[ClaySyncTask DRY RUN] Would push %d record(s) — no HTTP calls or DB writes.",
                    len(rows))
        return stats

    now = datetime.now(timezone.utc)

    for i, contact in enumerate(rows, start=1):
        payload = _build_payload(contact)
        try:
            resp = requests.post(webhook_url, json=payload, timeout=30)
            resp.raise_for_status()
            _mark_synced(contact.id, now)
            stats["sent"] += 1
            logger.info("[ClaySyncTask] [%d/%d] OK %d — %s (%s)",
                        i, len(rows), resp.status_code, payload["license_number"], payload["company_name"])
        except requests.exceptions.RequestException as exc:
            stats["failed"] += 1
            logger.error("[ClaySyncTask] [%d/%d] FAILED %s: %s",
                         i, len(rows), payload["license_number"], exc)

        if i < len(rows):
            time.sleep(_DELAY_BETWEEN_REQUESTS)

    logger.info("[ClaySyncTask] Done — sent=%d failed=%d / total=%d",
                stats["sent"], stats["failed"], stats["total"])
    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="Push unsynced DBPR certified contractors to Clay")
    parser.add_argument(
        "--limit", type=int, default=_DEFAULT_LIMIT,
        help=f"Max contractors to push per run (default: {_DEFAULT_LIMIT})",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Log what would be pushed without making HTTP calls or marking rows synced",
    )
    args = parser.parse_args()

    try:
        result = run_clay_sync(limit=args.limit, dry_run=args.dry_run)
        print(f"  Total candidates : {result['total']}")
        print(f"  Sent             : {result['sent']}")
        print(f"  Failed           : {result['failed']}")
        sys.exit(1 if result["failed"] and not result["sent"] else 0)
    except Exception as e:
        logger.error("[ClaySyncTask] Unhandled error: %s", e, exc_info=True)
        sys.exit(1)
