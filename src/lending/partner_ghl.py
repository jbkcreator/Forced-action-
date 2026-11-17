"""Partner Door (P-1): the pre-qual letter to the partner, sent through the Next Deal Lending GoHighLevel account.

The letter goes to the partner, never the borrower (the borrower gave no consent of their own). The partner's
contact is upserted by email and the PDF is attached the same way as T-07 (upload, then an Email message with the
file URL).
"""
from __future__ import annotations

import logging
from typing import Optional

from config.lending_partners import PARTNER_LETTER_HTML, PARTNER_LETTER_SUBJECT
from config.lending_prequal import BORROWER_CALLBACK_PHONE, BORROWER_SIGNER
from config.settings import get_settings
from src.lending.ghl_account import lending_ghl_account
from src.lending.prequal_ghl import GhlPrequalAttachmentSink
from src.lending.web_leads import DeliveryError

logger = logging.getLogger(__name__)


class PartnerLetterSink(GhlPrequalAttachmentSink):
    def send_letter(self, partner_email: str, partner_name: str, place: str, file_id: int, pdf: bytes) -> None:
        """Email the pre-qual PDF to the partner. Raises SendOutcomeUnknown when the send may have gone out."""
        data = self._call("partner upsert", "POST", "/contacts/upsert",
                          json={"locationId": self._account.location_id, "email": partner_email,
                                "firstName": partner_name.split()[0]})
        contact_id = (data.get("contact") or {}).get("id")
        if not contact_id:
            raise DeliveryError("partner upsert: no contact id in response")
        url = self._upload(str(contact_id), pdf)
        body = {"type": "Email", "contactId": str(contact_id),
                "subject": PARTNER_LETTER_SUBJECT.format(place=place, file_id=file_id),
                "html": PARTNER_LETTER_HTML.format(callback=BORROWER_CALLBACK_PHONE, signer=BORROWER_SIGNER),
                "attachments": [url]}
        email_from = get_settings().lending_prequal_email_from
        if email_from:
            body["emailFrom"] = email_from
        self._send_email(body)
        logger.info("[partner-door] pre-qual letter sent file=%s", file_id)


def get_partner_letter_sink() -> Optional[PartnerLetterSink]:
    """The sink, or None when the Next Deal Lending GHL account is not configured."""
    account = lending_ghl_account()
    return PartnerLetterSink(account) if account is not None else None
