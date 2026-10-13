"""Hand the soft approval form's address and target close date to the T-12 enrichment card.

The after-call form is where a caller types the property address and the close date, which are the facts the
enrichment card routes on (``FULL_MACHINE`` / ``NURTURE``). This runs after the soft approval has been processed
and its status reply posted. It is best-effort: a failure is logged by call id and exception class only, and never
changes the soft approval outcome. It does nothing while ``LENDING_ENRICHMENT_ENABLED`` is off.

The lead is resolved the same way the soft approval resolves it: the most recent LendingFlow lead for the phone,
and never past a suppressed one. When that latest lead is suppressed the soft approval refuses it, so the facts are
not attached to an older lead either.
"""
from __future__ import annotations

import logging
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.lending.db import lending_session
from src.lending.enrichment.service import enabled as enrichment_enabled, enrich_lead, record_deal_facts
from src.lending.soft_approval.facts import SoftApprovalFacts
from src.services.phone_utils import normalize

logger = logging.getLogger(__name__)


def _latest_lead_is_usable(db: Session, phone: str) -> bool:
    """True only when the phone's most recent LendingFlow lead exists and is not suppressed."""
    normalized = normalize(phone)
    if not normalized:
        return False
    suppressed: Optional[bool] = db.execute(
        text("SELECT suppressed FROM lending.lendingflow_leads WHERE phone = :phone ORDER BY received_at DESC LIMIT 1"),
        {"phone": normalized},
    ).scalar_one_or_none()
    return suppressed is False


def share_facts_with_enrichment(call_id: str, phone: str, facts: SoftApprovalFacts) -> None:
    if not enrichment_enabled():
        return
    try:
        with lending_session() as db:
            if not _latest_lead_is_usable(db, phone):
                logger.info("[soft-approval] call %s: no usable LendingFlow lead, enrichment facts not shared", call_id)
                return
            lead_id = record_deal_facts(
                db, phone=phone, address=facts.property_address, target_close_date=facts.target_close_date,
                source="slack_form")
            db.commit()
            if lead_id is not None:
                enrich_lead(db, lead_id)
    except Exception as exc:  # class only: SQL errors embed bound params (phones)
        logger.error("[soft-approval] call %s enrichment hand-off failed: %s", call_id, type(exc).__name__)
