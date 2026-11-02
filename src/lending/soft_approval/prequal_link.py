"""Queue the T-07 pre-qual letter once the caller has entered the deal figures (client list Nov 2, Q2).

The letter is no longer queued at LendingFlow intake: it waits for the first call. A Call-One form submission
whose call figures fit a lender (soft approval status ``generated``) hands the borrower's LendingFlow lead to
``prequal_letters.queue_and_send_in_background``; any other outcome (no lender fits, no lead, out of scope) sends
no letter. The letter itself still shows only the range from the lead's four core fields, clamped to the fitting
lenders, and is still off while ``LENDING_PREQUAL_PDF_ENABLED`` is off. One letter per lead (unique lead ref), so a
re-submitted form never sends a second one.

Best-effort, like the enrichment hand-off: a failure is logged by call id and exception class only and never
changes the soft approval outcome. The lead is resolved as the soft approval resolves it: the phone's most recent
LendingFlow lead, never past a suppressed one.
"""
from __future__ import annotations

import logging
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.lending_lendingflow import LEAD_SOURCE
from config.settings import get_settings
from src.lending.db import lending_session
from src.lending.prequal import PrequalLead
from src.services.phone_utils import normalize

logger = logging.getLogger(__name__)

_LATEST_LEAD = text(
    "SELECT vendor_lead_id, ghl_contact_id, credit_band, loan_amount, property_state, loan_type, suppressed "
    "FROM lending.lendingflow_leads WHERE phone = :phone ORDER BY received_at DESC LIMIT 1"
)


def _latest_lead(db: Session, phone: str) -> Optional[tuple[str, str, PrequalLead]]:
    """(lead ref, GHL contact id, letter inputs) of the phone's latest usable LendingFlow lead, or None."""
    normalized = normalize(phone)
    if not normalized:
        return None
    row = db.execute(_LATEST_LEAD, {"phone": normalized}).first()
    if row is None or row.suppressed or not row.ghl_contact_id:
        return None
    lead = PrequalLead(credit_band=row.credit_band, loan_amount=row.loan_amount,
                       property_state=row.property_state, loan_type=row.loan_type)
    return row.vendor_lead_id, row.ghl_contact_id, lead


def send_prequal_after_call(call_id: str, phone: str) -> None:
    """Queue and send the letter for the call's lead. Call only after a ``generated`` soft approval."""
    if not get_settings().lending_prequal_pdf_enabled:
        return
    from src.lending.prequal_letters import queue_and_send_in_background

    try:
        with lending_session() as db:
            found = _latest_lead(db, phone)
        if found is None:
            logger.info("[soft-approval] call %s: no usable LendingFlow lead, no pre-qual letter", call_id)
            return
        lead_ref, contact_id, lead = found
        queue_and_send_in_background(lead_source=LEAD_SOURCE, lead_ref=lead_ref, contact_id=contact_id, lead=lead)
    except Exception as exc:  # class only: SQL errors embed bound params (phones)
        logger.error("[soft-approval] call %s pre-qual hand-off failed: %s", call_id, type(exc).__name__)
