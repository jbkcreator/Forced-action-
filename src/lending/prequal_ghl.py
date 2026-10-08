"""Sends the pre-qualification PDF to a Next Deal Lending GoHighLevel contact as an email attachment.

Two calls: upload the PDF to the contact's conversation (GHL hosts the file and returns its URL),
then send an Email message carrying that URL in ``attachments``. Verified live Oct 2026 with API
Version 2021-07-28: upload 201, send 201, delivered with the PDF attached. Nothing is stored on our side.
The email goes out under ``LENDING_PREQUAL_EMAIL_FROM``; unset, GHL uses its shared sending address,
so set it to a verified Next Deal Lending mailbox before the flag is turned on.
"""
from __future__ import annotations

import logging
from typing import Optional

from config.lending_web import GHL_CONFIG_ERROR_STATUS_CODES
from config.settings import get_settings
from src.lending.ghl_account import ghl_headers, lending_ghl_account
from src.lending.prequal import PrequalSink
from src.lending.web_lead_ghl import GhlLeadSink
from src.lending.web_leads import DeliveryError

logger = logging.getLogger(__name__)

PDF_FILENAME = "Pre-Qualification-Estimate.pdf"
EMAIL_SUBJECT = "Your pre-qualification estimate from Next Deal Lending"
# PLACEHOLDER: final wording pending Josh (Q11, Q12) and compliance review. No rates or terms.
EMAIL_HTML = (
    "<p>Thanks for sending your request. Your pre-qualification estimate is attached.</p>"
    "<p>This is a non-binding estimate for informational purposes only. It is not a commitment "
    "to lend or an offer of specific terms.</p>"
    "<p>Questions? Call or text (727) 436-9951, or reply to this email.</p>"
    "<p>Next Deal Lending</p>"
)


class GhlPrequalAttachmentSink(GhlLeadSink):
    """Reuses GhlLeadSink's JSON request and error handling; the multipart upload is new."""

    def deliver(self, lead_id: int, contact_id: str, pdf: bytes) -> None:
        url = self._upload(contact_id, pdf)
        body = {"type": "Email", "contactId": contact_id, "subject": EMAIL_SUBJECT,
                "html": EMAIL_HTML, "attachments": [url]}
        email_from = get_settings().lending_prequal_email_from
        if email_from:
            body["emailFrom"] = email_from
        self._call("prequal email send", "POST", "/conversations/messages", json=body)
        logger.info("[prequal] email sent lead_id=%s", lead_id)

    def _upload(self, contact_id: str, pdf: bytes) -> str:
        from src.services import ghl_webhook

        headers = ghl_headers(self._account.api_key)
        headers.pop("Content-Type")  # requests sets the multipart boundary
        try:
            response = ghl_webhook._ghl_request(
                "POST", f"{ghl_webhook._GHL_BASE}/conversations/messages/upload", headers=headers,
                files={"fileAttachment": (PDF_FILENAME, pdf, "application/pdf")}, data={"contactId": contact_id},
            )
        except Exception as exc:
            raise DeliveryError(f"prequal upload: {type(exc).__name__}") from exc
        if response.status_code >= 400:
            raise DeliveryError(f"prequal upload: HTTP {response.status_code}",
                                config_error=response.status_code in GHL_CONFIG_ERROR_STATUS_CODES)
        try:
            files = response.json().get("uploadedFiles") or {}
        except ValueError:
            files = {}
        url = next(iter(files.values()), None)
        if not url:
            raise DeliveryError("prequal upload: no file url in response")
        return str(url)


def get_live_sink() -> Optional[PrequalSink]:
    """The GHL attachment sink, or None when the Next Deal Lending account is not configured."""
    account = lending_ghl_account()
    return GhlPrequalAttachmentSink(account) if account is not None else None
