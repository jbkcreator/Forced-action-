"""Live delivery of a website lead into the Next Deal Lending GoHighLevel sub-account.

Contact: upsert by phone/email (an existing contact is updated, never duplicated). The upsert
body never carries ``dnd``, so a contact an earlier STOP marked do-not-disturb stays blocked, and
never carries ``tags`` or ``source``: GHL's upsert REPLACES tags (verified live, Oct 2026), which
would wipe an earlier consent tag, the opt-out tag or the dialer's tags. Tags are added through
the separate add-tags call, which cannot remove any. Positive consent tags (text, call) are added whenever
ticked; a "no" tag only on a brand-new contact, so a later unticked form never contradicts an
earlier "yes". Custom fields (when their ids are configured) are written only for "yes" or a value.
Deal Drop is the weekly email only: its tag and field say nothing about texts.
Pipeline: a card is created only when none exists for the contact in the Booked Calls pipeline
and a new-lead stage is configured. A contact that already has a card is returning (Chunk 1
Addendum 1.2.2): it is tagged, and a closed card (a reopen stage, or status lost/abandoned) is
moved back to the new-lead stage; an active one (e.g. Booked) or a won one is never moved.
Without that stage the lead is contact-only. Each submission also adds a timestamped note.
"""
from __future__ import annotations

import logging
from datetime import timezone
from typing import Any, Mapping, Optional

from config.lending_web import (
    GHL_CONFIG_ERROR_STATUS_CODES,
    GHL_CUSTOM_FIELD_YES,
    GHL_REOPEN_STATUSES,
    GHL_SOURCE,
    GHL_TAG_CALL_CONSENT_NO,
    GHL_TAG_CALL_CONSENT_YES,
    GHL_TAG_DEAL_DROP_OPTIN,
    GHL_TAG_RETURNING,
    GHL_TAG_SMS_CONSENT_NO,
    GHL_TAG_SMS_CONSENT_YES,
    GHL_TAG_WEB_LEAD,
)
from config.settings import get_settings
from src.lending.ghl_account import GhlAccount, ghl_headers, lending_ghl_account
from src.lending.web_leads import DeliveryError, LeadSink, PushResult

logger = logging.getLogger(__name__)


def _consent_effective(lead: Mapping[str, Any]) -> bool:
    return bool(lead["sms_consent"]) and not lead["suppressed"]


def _call_consent_effective(lead: Mapping[str, Any]) -> bool:
    return bool(lead["call_consent"]) and not lead["suppressed"]


def _deal_drop_effective(lead: Mapping[str, Any]) -> bool:
    return bool(lead["deal_drop_optin"]) and not lead["suppressed"]


def _tags_to_add(lead: Mapping[str, Any], is_new_contact: bool) -> list[str]:
    tags = [GHL_TAG_WEB_LEAD]
    if _consent_effective(lead):
        tags.append(GHL_TAG_SMS_CONSENT_YES)
    elif is_new_contact:
        tags.append(GHL_TAG_SMS_CONSENT_NO)
    if _call_consent_effective(lead):
        tags.append(GHL_TAG_CALL_CONSENT_YES)
    elif is_new_contact:
        tags.append(GHL_TAG_CALL_CONSENT_NO)
    if _deal_drop_effective(lead):
        tags.append(GHL_TAG_DEAL_DROP_OPTIN)
    return tags


def _custom_fields(lead: Mapping[str, Any]) -> list[dict[str, str]]:
    settings = get_settings()
    fields = []
    if settings.lending_ghl_cf_sms_consent and _consent_effective(lead):
        fields.append({"id": settings.lending_ghl_cf_sms_consent, "value": GHL_CUSTOM_FIELD_YES})
    if settings.lending_ghl_cf_call_consent and _call_consent_effective(lead):
        fields.append({"id": settings.lending_ghl_cf_call_consent, "value": GHL_CUSTOM_FIELD_YES})
    if settings.lending_ghl_cf_deal_drop_optin and _deal_drop_effective(lead):
        fields.append({"id": settings.lending_ghl_cf_deal_drop_optin, "value": GHL_CUSTOM_FIELD_YES})
    if settings.lending_ghl_cf_loan_amount and lead["loan_amount"]:
        fields.append({"id": settings.lending_ghl_cf_loan_amount, "value": str(lead["loan_amount"])})
    if settings.lending_ghl_cf_credit_band and lead["credit_band"]:
        fields.append({"id": settings.lending_ghl_cf_credit_band, "value": lead["credit_band"]})
    if settings.lending_ghl_cf_property_address and lead["property_address"]:
        fields.append({"id": settings.lending_ghl_cf_property_address, "value": lead["property_address"]})
    return fields


def _reopen_stage_ids() -> frozenset[str]:
    settings = get_settings()
    configured = {s.strip() for s in settings.lending_ghl_reopen_stage_ids.split(",") if s.strip()}
    if not configured and settings.lending_ghl_stage_nurture:
        configured = {settings.lending_ghl_stage_nurture}
    return frozenset(configured)


def _is_closed(opportunity: Mapping[str, Any]) -> bool:
    status = str(opportunity.get("status") or "").lower()
    if status == "won":
        return False
    return status in GHL_REOPEN_STATUSES or opportunity.get("pipelineStageId") in _reopen_stage_ids()


def _note(lead: Mapping[str, Any]) -> str:
    received = lead["received_at"].astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    lines = [
        f"Website form submitted {received}.",
        f"Text message consent box: {'ticked' if lead['sms_consent'] else 'not ticked'}"
        + (" (number was already suppressed: no consent applied)" if lead["suppressed"] and lead["sms_consent"] else "") + ".",
        f"Call consent box: {'ticked' if lead['call_consent'] else 'not ticked'}"
        + (" (number was already suppressed: no consent applied)" if lead["suppressed"] and lead["call_consent"] else "") + ".",
        f"Deal Drop (weekly email) opt-in box: {'ticked' if lead['deal_drop_optin'] else 'not ticked'}.",
        f"Doing: {lead['deal_type'] or 'n/a'}. Projects finished, last 3 years: {lead['completed_projects_3y'] or 'n/a'}. "
        f"Property city: {lead['property_city'] or 'n/a'}.",
        f"Loan amount: {'${:,}'.format(lead['loan_amount']) if lead['loan_amount'] else 'n/a'}. Credit band: {lead['credit_band'] or 'n/a'}. "
        f"Property: {lead['property_address'] or 'n/a'}, state {lead['property_state'] or 'n/a'}.",
        f"Lead id {lead['id']}.",
    ]
    return "\n".join(lines)


class GhlLeadSink:
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
        contact_id, is_new = self._upsert_contact(lead)
        self._call("add tags", "POST", f"/contacts/{contact_id}/tags", json={"tags": _tags_to_add(lead, is_new)})
        stale = ([GHL_TAG_SMS_CONSENT_NO] if _consent_effective(lead) else []) + (
            [GHL_TAG_CALL_CONSENT_NO] if _call_consent_effective(lead) else [])
        if stale:
            # only our own earlier "no" tags are ever removed, so "no" and "yes" never sit side by side
            self._call("remove stale tag", "DELETE", f"/contacts/{contact_id}/tags", json={"tags": stale})
        pipeline_card, existing = self._ensure_pipeline_card(lead, contact_id)
        moved = existing is not None and self._reopen_if_closed(existing)
        if existing is not None:
            self._call("add returning tag", "POST", f"/contacts/{contact_id}/tags", json={"tags": [GHL_TAG_RETURNING]})
        self._add_note(lead, contact_id)
        return PushResult(contact_id=contact_id, pipeline_card=pipeline_card,
                          returning=existing is not None, moved_to_new_lead=moved)

    def _upsert_contact(self, lead: Mapping[str, Any]) -> tuple[str, bool]:
        first, _, last = str(lead["name"]).partition(" ")
        body: dict[str, Any] = {
            "locationId": self._account.location_id,
            "firstName": first,
            "lastName": last.strip(),
            "phone": lead["phone"],
        }
        if lead["email"]:
            body["email"] = lead["email"]
        custom = _custom_fields(lead)
        if custom:
            body["customFields"] = custom
        data = self._call("contact upsert", "POST", "/contacts/upsert", json=body)
        contact_id = (data.get("contact") or {}).get("id")
        if not contact_id:
            raise DeliveryError("contact upsert: no contact id in response")
        return str(contact_id), bool(data.get("new"))

    def _ensure_pipeline_card(self, lead: Mapping[str, Any], contact_id: str) -> tuple[bool, Optional[Mapping[str, Any]]]:
        """(has a card, the contact's existing card or None when this submission created it)."""
        settings = get_settings()
        pipeline_id, stage_id = settings.lending_ghl_pipeline_id, settings.lending_ghl_stage_new_lead
        if not (pipeline_id and stage_id):
            return False, None
        found = self._call(
            "opportunity search", "GET", "/opportunities/search",
            params={"location_id": self._account.location_id, "contact_id": contact_id, "pipeline_id": pipeline_id},
        )
        cards = found.get("opportunities") or []
        if cards:
            # an active card wins: a contact mid-deal (e.g. Booked) is never pulled back to New Lead
            return True, next((c for c in cards if not _is_closed(c)), cards[0])
        self._call("opportunity create", "POST", "/opportunities/", json={
            "pipelineId": pipeline_id,
            "locationId": self._account.location_id,
            "name": f"Web lead - {lead['name']}",
            "pipelineStageId": stage_id,
            "contactId": contact_id,
            "status": "open",
            "source": GHL_SOURCE,
        })
        return True, None

    def _reopen_if_closed(self, opportunity: Mapping[str, Any]) -> bool:
        if not _is_closed(opportunity):
            return False
        self._call("opportunity reopen", "PUT", f"/opportunities/{opportunity['id']}",
                   json={"pipelineStageId": get_settings().lending_ghl_stage_new_lead, "status": "open"})
        return True

    def _add_note(self, lead: Mapping[str, Any], contact_id: str) -> None:
        """Best effort: the consent evidence of record is the lending.web_leads row."""
        try:
            self._call("note", "POST", f"/contacts/{contact_id}/notes", json={"body": _note(lead)})
        except DeliveryError as exc:
            logger.warning("[lending-web] GHL note failed lead=%s: %s", lead["id"], exc)


def get_live_sink() -> Optional[LeadSink]:
    """The GHL sink, or None when the Next Deal Lending account is not configured."""
    account = lending_ghl_account()
    return GhlLeadSink(account) if account is not None else None
