"""Queue the T-07 pre-qual letter once the caller has entered the deal figures (client list Nov 2, Q2).

The letter is no longer queued at LendingFlow intake: it waits for the first call. A Call-One form submission
whose call figures fit a lender (soft approval status ``generated``) hands the borrower's LendingFlow lead to
``prequal_letters.queue_and_send_in_background``; any other outcome (no lender fits, no lead, out of scope) sends
no letter. The letter itself still shows only the range from the lead's four core fields, clamped to the fitting
lenders, and is still off while ``LENDING_PREQUAL_PDF_ENABLED`` is off. One letter per lead (unique lead ref), so a
re-submitted form never sends a second one.

The hand-off right after the submission is best-effort: a failure is logged by call id and exception class only
and never changes the soft approval outcome. It is never lost: the committed ``generated`` soft approval is the
durable marker, and :func:`queue_missed_letters` (run by the pre-qual sweep, every 5 minutes) queues the letter
for any such approval of the last ``GIVE_UP_AFTER_HOURS`` whose lead still has none. Both send the letter only for the
lead the call assessed (``soft_approvals.lead_ref``, stored with the approval), never for a newer lead on the same phone
that no call has assessed, and never past a suppressed lead.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.lending_lendingflow import LEAD_SOURCE
from config.lending_prequal import GIVE_UP_AFTER_HOURS, SWEEP_BATCH_SIZE
from config.settings import get_settings
from src.lending.db import lending_session
from src.lending.prequal import PrequalLead
from src.services.phone_utils import normalize

logger = logging.getLogger(__name__)

# The assessed lead is usable while it and every later lead on its phone are unsuppressed and it is in GHL.
_USABLE = """
    NOT l.suppressed AND l.ghl_contact_id IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM lending.lendingflow_leads later WHERE later.phone = l.phone
                     AND later.received_at > l.received_at AND later.suppressed)
"""

_ASSESSED_LEAD = text(
    "SELECT l.vendor_lead_id, l.ghl_contact_id, l.credit_band, l.loan_amount, l.property_state, l.loan_type "
    "FROM lending.lendingflow_leads l WHERE l.phone = :phone AND CAST(l.lead_uuid AS text) = :lead_ref AND "
    + _USABLE
)


def _assessed_lead(db: Session, phone: str, lead_ref: str) -> Optional[tuple[str, str, PrequalLead]]:
    """(lead ref, GHL contact id, letter inputs) of the assessed LendingFlow lead while usable, or None."""
    normalized = normalize(phone)
    if not normalized or not lead_ref:
        return None
    row = db.execute(_ASSESSED_LEAD, {"phone": normalized, "lead_ref": lead_ref}).first()
    if row is None:
        return None
    lead = PrequalLead(credit_band=row.credit_band, loan_amount=row.loan_amount,
                       property_state=row.property_state, loan_type=row.loan_type)
    return row.vendor_lead_id, row.ghl_contact_id, lead


def send_prequal_after_call(call_id: str, phone: str, lead_ref: Optional[str]) -> None:
    """Queue and send the letter for the lead the call assessed (``SoftApprovalOutcome.lead_ref``). Call only after
    a ``generated`` soft approval."""
    if not get_settings().lending_prequal_pdf_enabled:
        return
    from src.lending.prequal_letters import queue_and_send_in_background

    try:
        with lending_session() as db:
            found = _assessed_lead(db, phone, lead_ref)
        if found is None:
            logger.info("[soft-approval] call %s: no usable LendingFlow lead, no pre-qual letter", call_id)
            return
        vendor_lead_id, contact_id, lead = found
        queue_and_send_in_background(lead_source=LEAD_SOURCE, lead_ref=vendor_lead_id, contact_id=contact_id,
                                     lead=lead)
    except Exception as exc:  # class only: SQL errors embed bound params (phones)
        logger.error("[soft-approval] call %s pre-qual hand-off failed: %s", call_id, type(exc).__name__)


# ``generated`` soft approvals whose assessed lead is usable and has no letter row yet. An approval without a
# ``lead_ref`` (saved before it was stored) is skipped: it names no assessed lead.
_MISSED = text("""
    SELECT l.vendor_lead_id, l.ghl_contact_id, l.credit_band, l.loan_amount, l.property_state, l.loan_type
      FROM lending.soft_approvals s
      JOIN lending.lendingflow_leads l ON l.phone = s.phone AND CAST(l.lead_uuid AS text) = s.lead_ref
     WHERE s.status = 'generated' AND s.updated_at >= :since AND""" + _USABLE + """
       AND NOT EXISTS (SELECT 1 FROM lending.prequal_letters p WHERE p.lead_source = :src AND p.lead_ref = l.vendor_lead_id)
     ORDER BY s.updated_at LIMIT :limit
""")


def queue_missed_letters(db: Session, now: datetime) -> int:
    """Queue the letter for every recent ``generated`` soft approval whose hand-off did not land. Returns the number
    queued. Does not commit; idempotent (one letter per lead)."""
    from src.lending.prequal_letters import enqueue

    rows = db.execute(_MISSED, {"since": now - timedelta(hours=GIVE_UP_AFTER_HOURS), "src": LEAD_SOURCE,
                                "limit": SWEEP_BATCH_SIZE}).all()
    queued = 0
    for row in rows:
        lead = PrequalLead(credit_band=row.credit_band, loan_amount=row.loan_amount,
                           property_state=row.property_state, loan_type=row.loan_type)
        if enqueue(db, lead_source=LEAD_SOURCE, lead_ref=row.vendor_lead_id, contact_id=row.ghl_contact_id,
                   lead=lead) is not None:
            queued += 1
    if queued:
        logger.warning("[prequal] queued %d post-call letter(s) whose hand-off had not landed", queued)
    return queued
