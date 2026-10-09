"""Live delivery of a LendingFlow lead into the Next Deal Lending GoHighLevel sub-account.

Same rules as ``web_lead_ghl``: the upsert never carries ``dnd``, ``tags`` or ``source`` (GHL's
upsert REPLACES tags), tags go through the add-tags call, and a pipeline card is created only when
a LendingFlow stage is configured and the contact has no card yet (never moved). The note carries
the vendor lead id only, no phone or email.
"""
from __future__ import annotations

import logging
from typing import Any, Mapping, Optional

from config.lending_lendingflow import (
    GHL_CONFIG_ERROR_STATUS_CODES,
    GHL_SOURCE,
    GHL_TAG_CONSENT_MISSING,
    GHL_TAG_LENDINGFLOW,
)
from config.settings import get_settings
from src.lending.ghl_account import GhlAccount, ghl_headers, lending_ghl_account
from src.lending.web_leads import DeliveryError, PushResult

logger = logging.getLogger(__name__)


def _tags(lead: Mapping[str, Any]) -> list[str]:
    tags = [GHL_TAG_LENDINGFLOW]
    if lead["consent_status"] != "present":
        tags.append(GHL_TAG_CONSENT_MISSING)
    return tags


class LendingFlowGhlSink:
    def __init__(self, account: GhlAccount) -> None:
        self._account = account

    def _call(self, step: str, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        from src.services import ghl_webhook

        try:
            response = ghl_webhook._ghl_request(
                method, f"{ghl_webhook._GHL_BASE}{path}", headers=ghl_headers(self._account.api_key), **kwargs,
            )
        except Exception as exc:
            raise DeliveryError(f"{step}: {type(exc).__name__}") from exc
        if response.status_code >= 400:
            raise DeliveryError(f"{step}: HTTP {response.status_code}",
                                config_error=response.status_code in GHL_CONFIG_ERROR_STATUS_CODES)
        try:
            return response.json()
        except ValueError:
            return {}

    def push(self, lead: Mapping[str, Any]) -> PushResult:
        contact_id = self._upsert_contact(lead)
        self._call("add tags", "POST", f"/contacts/{contact_id}/tags", json={"tags": _tags(lead)})
        pipeline_card = self._ensure_pipeline_card(lead, contact_id)
        self._add_note(lead, contact_id)
        return PushResult(contact_id=contact_id, pipeline_card=pipeline_card)

    def _upsert_contact(self, lead: Mapping[str, Any]) -> str:
        body: dict[str, Any] = {"locationId": self._account.location_id, "phone": lead["phone"]}
        if lead.get("first_name"):
            body["firstName"] = lead["first_name"]
        if lead.get("last_name"):
            body["lastName"] = lead["last_name"]
        if lead.get("email"):
            body["email"] = lead["email"]
        data = self._call("contact upsert", "POST", "/contacts/upsert", json=body)
        contact_id = (data.get("contact") or {}).get("id")
        if not contact_id:
            raise DeliveryError("contact upsert: no contact id in response")
        return str(contact_id)

    def _ensure_pipeline_card(self, lead: Mapping[str, Any], contact_id: str) -> bool:
        settings = get_settings()
        pipeline_id, stage_id = settings.lending_ghl_pipeline_id, settings.lending_ghl_stage_lendingflow_new
        if not (pipeline_id and stage_id):
            return False
        found = self._call(
            "opportunity search", "GET", "/opportunities/search",
            params={"location_id": self._account.location_id, "contact_id": contact_id, "pipeline_id": pipeline_id},
        )
        if found.get("opportunities"):
            return True
        name = " ".join(p for p in (lead.get("first_name"), lead.get("last_name")) if p) or "LendingFlow lead"
        self._call("opportunity create", "POST", "/opportunities/", json={
            "pipelineId": pipeline_id, "locationId": self._account.location_id, "name": f"LendingFlow - {name}",
            "pipelineStageId": stage_id, "contactId": contact_id, "status": "open", "source": GHL_SOURCE,
        })
        return True

    def _add_note(self, lead: Mapping[str, Any], contact_id: str) -> None:
        """Best effort: the evidence of record is lending.lendingflow_leads + lead_consent_certificates."""
        body = (f"LendingFlow lead {lead['vendor_lead_id']} received "
                f"{lead['received_at'].strftime('%Y-%m-%d %H:%M:%S UTC')}. Consent certificate: {lead['consent_status']}.")
        try:
            self._call("note", "POST", f"/contacts/{contact_id}/notes", json={"body": body})
        except DeliveryError as exc:
            logger.warning("[lendingflow] GHL note failed lead=%s: %s", lead["id"], exc)


def get_live_sink() -> Optional[LendingFlowGhlSink]:
    """The GHL sink, or None when the Next Deal Lending account is not configured."""
    account = lending_ghl_account()
    return LendingFlowGhlSink(account) if account is not None else None
