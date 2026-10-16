"""Partner Door (P-1) in the Next Deal Lending GoHighLevel sub-account.

A partner file becomes a borrower contact with an opportunity in the "Partner - New" stage, so Josh works it
in the same pipeline as LendingFlow leads. Same rules as ``lendingflow_ghl``: the upsert never carries ``dnd``,
``tags`` or ``source``, tags go through the add-tags call, and a card is created only when the contact has no
card in the pipeline yet. The borrower gave no consent of their own, so the contact is tagged
``partner-no-borrower-consent`` and never gets an SMS-trigger tag. The note carries the file id only.

The pre-qual letter goes to the partner (never the borrower): the partner's contact is upserted by email and
the PDF is attached the same way as T-07 (upload, then an Email message with the file URL).
"""
from __future__ import annotations

import logging
from typing import Any, Mapping, Optional

from config.lending_partners import (
    GHL_SOURCE_PARTNER,
    GHL_TAG_NO_BORROWER_CONSENT,
    GHL_TAG_PARTNER_FILE,
    PARTNER_LETTER_HTML,
    PARTNER_LETTER_SUBJECT,
)
from config.lending_prequal import BORROWER_CALLBACK_PHONE, BORROWER_SIGNER
from config.settings import get_settings
from src.lending.ghl_account import lending_ghl_account
from src.lending.prequal_ghl import GhlPrequalAttachmentSink
from src.lending.web_leads import DeliveryError

logger = logging.getLogger(__name__)


class PartnerGhlSink(GhlPrequalAttachmentSink):
    def push_file(self, file: Mapping[str, Any], partner_name: str, stage_id: str) -> str:
        """Upsert the borrower contact, tag it, add the card and a note. Returns the GHL contact id."""
        contact_id = self._upsert_borrower(file)
        self._call("add tags", "POST", f"/contacts/{contact_id}/tags",
                   json={"tags": [GHL_TAG_PARTNER_FILE, GHL_TAG_NO_BORROWER_CONSENT]})
        self._ensure_card(file, contact_id, stage_id)
        self._note(file, partner_name, contact_id)
        return contact_id

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

    def _upsert_borrower(self, file: Mapping[str, Any]) -> str:
        if not (file.get("borrower_phone") or file.get("borrower_email")):
            raise DeliveryError("borrower upsert: no phone or email on the file")
        body: dict[str, Any] = {"locationId": self._account.location_id,
                                "firstName": file["borrower_first_name"], "lastName": file["borrower_last_name"]}
        if file.get("borrower_phone"):
            body["phone"] = file["borrower_phone"]
        if file.get("borrower_email"):
            body["email"] = file["borrower_email"]
        data = self._call("borrower upsert", "POST", "/contacts/upsert", json=body)
        contact_id = (data.get("contact") or {}).get("id")
        if not contact_id:
            raise DeliveryError("borrower upsert: no contact id in response")
        return str(contact_id)

    def _ensure_card(self, file: Mapping[str, Any], contact_id: str, stage_id: str) -> None:
        pipeline_id = get_settings().lending_ghl_pipeline_id
        found = self._call("opportunity search", "GET", "/opportunities/search",
                           params={"location_id": self._account.location_id, "contact_id": contact_id,
                                   "pipeline_id": pipeline_id})
        if found.get("opportunities"):
            return
        name = f"{file['borrower_first_name']} {file['borrower_last_name']}".strip()
        self._call("opportunity create", "POST", "/opportunities/", json={
            "pipelineId": pipeline_id, "locationId": self._account.location_id, "name": f"Partner - {name}",
            "pipelineStageId": stage_id, "contactId": contact_id, "status": "open",
            "monetaryValue": int(file["loan_amount"]), "source": GHL_SOURCE_PARTNER,
        })

    def _note(self, file: Mapping[str, Any], partner_name: str, contact_id: str) -> None:
        """Best effort: the record of truth is lending.loan_files + partner_submissions."""
        body = (f"Partner file #{file['id']} from {partner_name}. Borrower not contacted: partner consent only, "
                "no borrower consent on file.")
        try:
            self._call("note", "POST", f"/contacts/{contact_id}/notes", json={"body": body})
        except DeliveryError as exc:
            logger.warning("[partner-door] GHL note failed file=%s: %s", file["id"], exc)


def get_partner_ghl_sink() -> Optional[PartnerGhlSink]:
    """The sink, or None when the Next Deal Lending GHL account is not configured."""
    account = lending_ghl_account()
    return PartnerGhlSink(account) if account is not None else None


def partner_stage_id() -> Optional[str]:
    """The "Partner - New" stage, or None when the pipeline or stage is not configured (no GHL card)."""
    settings = get_settings()
    if not (settings.lending_ghl_pipeline_id and settings.lending_ghl_stage_partner_new):
        return None
    return settings.lending_ghl_stage_partner_new
