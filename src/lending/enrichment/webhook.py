"""``POST /webhooks/lending/lendingflow-deal-facts`` (T-12), served by ``lending-api``.

The booking form (target close date, Josh F3) and the after-call Slack form (address, Josh B5) report
what the borrower or caller supplied for a LendingFlow lead, matched by phone. The facts are stored,
the card and routing are rebuilt in the background, and nothing is sent to the borrower.

Auth: ``X-Webhook-Secret`` = ``LENDING_GHL_WEBHOOK_SECRET``, like the other lending GHL webhooks.
Closed (503) while ``LENDING_ENRICHMENT_ENABLED`` is false. The payload field names are our own
contract, not yet wired to a GHL workflow or the Slack form: see docs/lending/lendingflow-enrichment.md.
"""
from __future__ import annotations

import hmac
import logging
from datetime import date
from typing import Any, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException
from sqlalchemy.orm import Session

from config.settings import get_settings
from src.lending.db import get_lending_db
from src.lending.enrichment.service import enabled, enrich_lead, record_deal_facts
from src.lending.payload_shape import log_shape

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/webhooks/lending", tags=["lending"])

ALLOWED_SOURCES = frozenset({"booking_form", "slack_form"})


def _verify_secret(received: Optional[str]) -> None:
    settings = get_settings()
    secret = settings.lending_ghl_webhook_secret
    if not enabled() or secret is None:
        raise HTTPException(status_code=503, detail="Lead enrichment is disabled")
    if not received or not hmac.compare_digest(received.encode(), secret.get_secret_value().encode()):
        raise HTTPException(status_code=401, detail="Invalid webhook secret")


def _close_date(raw: Any) -> Optional[date]:
    if raw in (None, ""):
        return None
    try:
        return date.fromisoformat(str(raw)[:10])
    except ValueError:
        raise HTTPException(status_code=422, detail="target_close_date must be an ISO date (YYYY-MM-DD)") from None


def _enrich_in_background(lead_id: int) -> None:
    from src.lending.db import lending_session

    try:
        with lending_session() as db:
            enrich_lead(db, lead_id)
    except Exception as exc:
        logger.error("[enrichment] background run crashed lead=%s: %s", lead_id, type(exc).__name__)


@router.post("/lendingflow-deal-facts")
def lendingflow_deal_facts(
    body: dict[str, Any],
    background_tasks: BackgroundTasks,
    x_webhook_secret: Optional[str] = Header(default=None),
    db: Session = Depends(get_lending_db),
) -> dict[str, Any]:
    _verify_secret(x_webhook_secret)
    log_shape("lendingflow-deal-facts", body)
    phone = body.get("phone")
    source = body.get("source")
    address = body.get("property_address")
    close = _close_date(body.get("target_close_date"))
    if not isinstance(phone, str) or not phone.strip() or source not in ALLOWED_SOURCES:
        raise HTTPException(status_code=422, detail="phone and a known source are required")
    if not (isinstance(address, str) and address.strip()) and close is None:
        raise HTTPException(status_code=422, detail="property_address or target_close_date is required")
    try:
        lead_id = record_deal_facts(db, phone=phone, address=address if isinstance(address, str) else None,
                                    target_close_date=close, source=source)
        db.commit()
    except Exception as exc:
        db.rollback()
        logger.error("[enrichment] deal-facts save failed: %s", type(exc).__name__)
        raise HTTPException(status_code=500, detail="Could not store the deal facts") from None
    if lead_id is None:
        raise HTTPException(status_code=404, detail="No LendingFlow lead for that contact")
    background_tasks.add_task(_enrich_in_background, lead_id)
    return {"status": "accepted", "lead_id": lead_id}
