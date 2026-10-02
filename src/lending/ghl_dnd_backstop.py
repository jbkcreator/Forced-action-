"""Backstop for the GHL opt-out webhook: every contact marked do-not-disturb in GHL is
suppressed in lending, so a STOP is never missed while the workflow is missing or down.

Usage (cron, every 15 minutes):
    python -m src.lending.ghl_dnd_backstop
"""
from __future__ import annotations

import logging
from typing import Any, Callable

from sqlalchemy import text

from config.lending_compliance import GHL_BACKSTOP_MAX_PAGES, GHL_BACKSTOP_PAGE_SIZE, OptOutChannel
from config.settings import get_settings
from src.lending.compliance import propagate_opt_out

logger = logging.getLogger(__name__)

FetchPage = Callable[[int], list[dict[str, Any]]]
_EXISTING = text("SELECT 1 FROM lending.opt_out_events WHERE channel = 'ghl' AND source_ref = :ref LIMIT 1")


def run_backstop(db, *, fetch_dnd_page: FetchPage) -> int:
    """Record each DND contact not yet seen (idempotent per GHL contact). Returns how many
    were newly recorded. Does not commit."""
    recorded = 0
    for page in range(1, GHL_BACKSTOP_MAX_PAGES + 1):
        contacts = fetch_dnd_page(page)
        if not contacts:
            break
        for contact in contacts:
            phone, email, contact_id = contact.get("phone"), contact.get("email"), contact.get("id")
            if not contact_id or not (phone or email):
                continue
            source_ref = f"ghl:{contact_id}"
            before = db.execute(_EXISTING, {"ref": source_ref}).scalar()
            propagate_opt_out(db, phone=phone, email=email, source_ref=source_ref, channel=OptOutChannel.GHL)
            recorded += 0 if before else 1
    return recorded


def _ghl_fetch_dnd_page(page: int) -> list[dict[str, Any]]:
    """One page of GHL contacts with DND on. Filter shape per the GHL v2 contact search;
    confirmed against the live sub-account on first run."""
    from src.services import ghl_webhook

    response = ghl_webhook._ghl_request(
        "POST", f"{ghl_webhook._GHL_BASE}/contacts/search", headers=ghl_webhook._headers(),
        json={"locationId": get_settings().ghl_location_id, "page": page, "pageLimit": GHL_BACKSTOP_PAGE_SIZE,
              "filters": [{"field": "dnd", "operator": "eq", "value": True}]},
    )
    response.raise_for_status()
    return list(response.json().get("contacts") or [])


def main() -> None:
    from src.core.database import get_db_context

    settings = get_settings()
    if settings.ghl_api_key is None or not settings.ghl_location_id:
        logger.error("[lending-ghl-backstop] GHL_API_KEY / GHL_LOCATION_ID not set")
        return
    try:
        with get_db_context() as db:
            recorded = run_backstop(db, fetch_dnd_page=_ghl_fetch_dnd_page)
            db.commit()
        logger.info("[lending-ghl-backstop] new_dnd_opt_outs=%d", recorded)
    except Exception as exc:
        logger.error("[lending-ghl-backstop] run failed (%s)", type(exc).__name__)
        raise


if __name__ == "__main__":
    main()
