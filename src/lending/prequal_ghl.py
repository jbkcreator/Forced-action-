"""Publishes a pre-qualification PDF link to the Next Deal Lending GoHighLevel contact.

Writes the link to the "Prequal PDF Link" contact custom field, then adds the ``prequal-ready`` tag.
A GHL workflow (built in GHL, Tag added = prequal-ready) sends the email and removes the tag.
GHL workflows only start on a tag being *added*, so a stale tag is removed first (best effort).
Never touches ``dnd``, other tags or other fields.
"""
from __future__ import annotations

import logging
from typing import Optional, Protocol

from config.settings import get_settings
from src.lending.ghl_account import lending_ghl_account
from src.lending.web_lead_ghl import GhlLeadSink
from src.lending.web_leads import DeliveryError

logger = logging.getLogger(__name__)

GHL_TAG_PREQUAL_READY = "prequal-ready"


class LinkPublisher(Protocol):
    def publish(self, contact_id: str, pdf_url: str) -> None: ...


class GhlPrequalLinkPublisher(GhlLeadSink):
    """Reuses GhlLeadSink's request and error handling; only the calls below are new."""

    def publish(self, contact_id: str, pdf_url: str) -> None:
        field_id = get_settings().lending_ghl_cf_prequal_pdf_link
        if not field_id:
            raise DeliveryError("prequal link: LENDING_GHL_CF_PREQUAL_PDF_LINK not set", config_error=True)
        self._call("prequal link field", "PUT", f"/contacts/{contact_id}",
                   json={"customFields": [{"id": field_id, "value": pdf_url}]})
        try:
            self._call("prequal tag reset", "DELETE", f"/contacts/{contact_id}/tags",
                       json={"tags": [GHL_TAG_PREQUAL_READY]})
        except DeliveryError as exc:
            logger.warning("[prequal] stale tag reset failed contact=%s: %s", contact_id, exc)
        self._call("prequal tag add", "POST", f"/contacts/{contact_id}/tags",
                   json={"tags": [GHL_TAG_PREQUAL_READY]})


def get_live_publisher() -> Optional[LinkPublisher]:
    """The GHL publisher, or None when the Next Deal Lending account is not configured."""
    account = lending_ghl_account()
    return GhlPrequalLinkPublisher(account) if account is not None else None
