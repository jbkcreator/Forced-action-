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
from requests.exceptions import ConnectTimeout, RequestException

from src.lending.ghl_account import ghl_headers, ghl_multipart_headers, lending_ghl_account
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


class SendOutcomeUnknown(DeliveryError):
    """The email request may or may not have reached GHL (timeout, dropped connection, 5xx).
    The letter must not be retried automatically: that could send the borrower a second email."""


class GhlPrequalAttachmentSink(GhlLeadSink):
    """Upload is idempotent and keeps the normal retries; the email send is a single attempt."""

    def deliver(self, lead_id: int, contact_id: str, pdf: bytes) -> None:
        url = self._upload(contact_id, pdf)
        body = {"type": "Email", "contactId": contact_id, "subject": EMAIL_SUBJECT,
                "html": EMAIL_HTML, "attachments": [url]}
        email_from = get_settings().lending_prequal_email_from
        if email_from:
            body["emailFrom"] = email_from
        self._send_email(body)
        logger.info("[prequal] email sent lead_id=%s", lead_id)

    def _send_email(self, body: dict) -> None:
        from src.services.ghl_webhook import ghl_post_once

        try:
            response = ghl_post_once("/conversations/messages", headers=ghl_headers(self._account.api_key), json=body)
        except ConnectTimeout as exc:  # never connected: nothing was sent, safe to retry later
            raise DeliveryError("prequal email send: ConnectTimeout") from exc
        except RequestException as exc:
            raise SendOutcomeUnknown(f"prequal email send: {type(exc).__name__}") from exc
        if response.status_code >= 500:
            raise SendOutcomeUnknown(f"prequal email send: HTTP {response.status_code}")
        if response.status_code >= 400:  # rejected (incl. 429): not sent
            raise DeliveryError(f"prequal email send: HTTP {response.status_code}",
                                config_error=response.status_code in GHL_CONFIG_ERROR_STATUS_CODES)

    def _upload(self, contact_id: str, pdf: bytes) -> str:
        from src.services.ghl_webhook import ghl_post_multipart

        try:
            response = ghl_post_multipart(
                "/conversations/messages/upload", headers=ghl_multipart_headers(self._account.api_key),
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
