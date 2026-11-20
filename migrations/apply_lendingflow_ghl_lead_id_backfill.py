"""Write each LendingFlow lead's ``lead_uuid`` into the GHL contact field "lead_id" (LENDING_GHL_CF_LEAD_ID).
Dry run unless ``--apply``.

New leads get the field on push (``lendingflow_ghl._upsert_contact``); this fills the contacts pushed before that.
Only leads with a stored ``ghl_contact_id`` are touched, one PUT /contacts/{id} each, custom field only (no tags, name,
DND or other field is sent). Idempotent: re-running writes the same value. ``--limit 1`` tries one contact first.

Usage:
    PYTHONPATH=. python migrations/apply_lendingflow_ghl_lead_id_backfill.py                    # dry run
    PYTHONPATH=. python migrations/apply_lendingflow_ghl_lead_id_backfill.py --apply --limit 1
    PYTHONPATH=. python migrations/apply_lendingflow_ghl_lead_id_backfill.py --apply
"""
from __future__ import annotations

import argparse
import logging
from typing import Optional

from sqlalchemy import create_engine, text

from config.settings import get_settings
from src.lending.ghl_account import ghl_headers, lending_ghl_account
from src.services import ghl_webhook

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_LEADS = text(
    "SELECT id, lead_uuid, ghl_contact_id FROM lending.lendingflow_leads "
    "WHERE ghl_contact_id IS NOT NULL AND NOT is_test ORDER BY id LIMIT :limit")


def run(*, apply: bool, limit: Optional[int]) -> int:
    settings = get_settings()
    field_id = settings.lending_ghl_cf_lead_id
    if not field_id:
        raise SystemExit("LENDING_GHL_CF_LEAD_ID is not set")
    account = lending_ghl_account()
    if account is None:
        raise SystemExit("the lending GHL account is not configured")
    with create_engine(settings.database_url).connect() as conn:
        rows = conn.execute(_LEADS, {"limit": limit or 100000}).all()
    logger.info("%d LendingFlow contacts to %s", len(rows), "update" if apply else "update (dry run)")
    failed = 0
    for row in rows:
        if not apply:
            logger.info("would set lead=%s contact=%s", row.id, row.ghl_contact_id)
            continue
        try:
            response = ghl_webhook._ghl_request(
                "PUT", f"{ghl_webhook._GHL_BASE}/contacts/{row.ghl_contact_id}",
                headers=ghl_headers(account.api_key),
                json={"customFields": [{"id": field_id, "value": str(row.lead_uuid)}]})
        except Exception as exc:
            failed += 1
            logger.error("lead=%s contact=%s failed: %s", row.id, row.ghl_contact_id, type(exc).__name__)
            continue
        if response.status_code >= 400:
            failed += 1
            logger.error("lead=%s contact=%s HTTP %s", row.id, row.ghl_contact_id, response.status_code)
        else:
            logger.info("set lead=%s contact=%s", row.id, row.ghl_contact_id)
    logger.info("done: %d failed", failed)
    return 1 if failed else 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()
    raise SystemExit(run(apply=args.apply, limit=args.limit))
