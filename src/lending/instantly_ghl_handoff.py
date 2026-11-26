"""P1.3: a cold lead that replies with interest ("Deal") is handed to the Next Deal Lending GoHighLevel account.

Run from Cora's reply sweep (``instantly_replies.ReplyProcessor``) for a Deal reply only, behind
``LENDING_INSTANTLY_GHL_HANDOFF_ENABLED`` (default off). It does four things, each safe to repeat:

1. Find the GHL contact by email (a contact search, exact email match); only when there is none, create it from the email
   alone (the upsert body carries no ``dnd``, ``tags``, ``source`` or phone, so nothing already on a contact is
   overwritten). A phone the lead gave in the reply ("gives a phone number") is put on the contact afterwards, and only
   when no other GHL contact owns that number and this contact has none, so a shared or mistyped number can never
   select another contact. That number is not consent to text.
2. Add the tag ``ndl-instantly-interested``. ``ndl-cold`` is removed (so the agents' later emails go through GHL) only
   when GHL email is open for the contact; a cold-imported contact carries Email do-not-disturb, which the GHL rail
   refuses, so it keeps ``ndl-cold`` and its Instantly route (nothing lifts the do-not-disturb here). The first answer is
   not affected either way: it is a Cora draft sent inside the Instantly thread, chosen by its channel and not by the tag.
3. Create a pipeline card at the new-lead stage when the contact has no card in the pipeline (an existing card, in any
   stage, is never moved). Without ``LENDING_GHL_PIPELINE_ID`` and ``LENDING_GHL_STAGE_NEW_LEAD`` the lead is
   contact-only.
4. Add a note saying where the contact came from.

This never lifts a do-not-disturb setting: texts still need consent (``cold_leads.reconcile_warm_contact``), and an
address that opted out is not handed over (the caller checks).

Known limits (documented in docs/lending/instantly-replies.md): the contact is matched by email only, so a cold
contact loaded into GHL by phone with no email on file is not found and a second contact is created; a failure part-way
leaves a partly built contact and raises an inbox item (the reply is not retried). Refused while the lending emergency
halt is on.
"""
from __future__ import annotations

import logging
import re
from typing import Any, Callable, Mapping, Optional

from config.lending_compliance import GHL_OPT_OUT_TAG, GHL_TAG_COLD_LEAD, GHL_TAG_INSTANTLY_INTERESTED
from config.settings import get_settings
from src.lending.ghl_account import GhlAccount, lending_ghl_account
from src.lending.ghl_crm import GhlContact, _contact_from
from src.lending.global_halt import is_globally_halted
from config.lending_fallbacks import CHANNEL_GHL_CRM
from src.lending.dependency_health import outbound_paused
from src.lending.web_lead_ghl import GhlLeadSink
from src.lending.instantly_replies import new_reply_text
from src.lending.web_leads import DeliveryError
from src.services.phone_utils import normalize

logger = logging.getLogger(__name__)

HandOff = Callable[[Mapping[str, Any]], Optional[str]]
SOURCE = "instantly"
_PHONE = re.compile(r"(?<!\d)(?:\+?1[\s.\-]?)?\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4}(?!\d)")


def phone_in_reply(reply: Optional[str]) -> Optional[str]:
    """The first US phone number in the lead's own words (quoted earlier mail left out), normalised, else None."""
    for match in _PHONE.findall(new_reply_text(reply or "")):
        if phone := normalize(match):
            return phone
    return None


class InstantlyGhlHandoff(GhlLeadSink):
    """Call it with an ``lending.instantly_replies`` row; it returns the GHL contact id."""

    def __call__(self, row: Mapping[str, Any]) -> str:
        if is_globally_halted():  # a tag or card can start a GHL workflow that messages the lead
            raise DeliveryError("lending emergency halt is on; nothing changed in GHL")
        if outbound_paused(CHANNEL_GHL_CRM):
            raise DeliveryError("opt-outs can't be verified (a feed is stale); nothing changed in GHL")
        email = str(row["lead_email"]).strip().lower()
        contact_id, duplicates = self._contact_for_email(row, email)
        contact = self._read_contact(contact_id)
        if GHL_OPT_OUT_TAG in contact.tags or "Call" in contact.dnd_channels:  # an opt-out kept in GHL itself
            logger.info("[instantly-handoff] reply %s not handed over: the GHL contact is opted out", row["id"])
            return contact_id
        notes = [f"Replied with interest to NDL cold email (Instantly campaign: {row.get('campaign_name') or 'n/a'}, "
                 f"reply id {row['id']}). Handed over by Cora; the first answer goes in the Instantly thread."]
        if duplicates:
            notes.append(f"{duplicates + 1} GHL contacts share this email: this one (the lowest id) was used; check for a duplicate.")
        notes += self._attach_phone(row, contact_id, contact)
        self._call("add tags", "POST", f"/contacts/{contact_id}/tags", json={"tags": [GHL_TAG_INSTANTLY_INTERESTED]})
        notes += self._release_cold_route(contact_id, contact)
        self._ensure_card(row, contact_id)
        self._note(row, contact_id, notes)
        return contact_id

    def _search(self, query: str) -> list[dict[str, Any]]:
        data = self._call("contact search", "POST", "/contacts/search", json={
            "locationId": self._account.location_id, "page": 1, "pageLimit": 20, "query": query})
        return [c for c in data.get("contacts") or [] if isinstance(c, dict) and c.get("id")]

    def _contact_for_email(self, row: Mapping[str, Any], email: str) -> tuple[str, int]:
        """The contact that owns this email, and how many other contacts share it: found by email first, so a phone
        number the lead typed can never select another contact. With duplicates the lowest id is used, so a retry picks
        the same record. A new contact is created from the email alone (the phone is attached separately, if safe)."""
        matches = sorted(str(c["id"]) for c in self._search(email) if (c.get("email") or "").strip().lower() == email)
        if matches:
            return matches[0], len(matches) - 1
        body: dict[str, Any] = {"locationId": self._account.location_id, "email": email}
        if row.get("first_name"):
            body["firstName"] = row["first_name"]
        data = self._call("contact upsert", "POST", "/contacts/upsert", json=body)
        contact_id = (data.get("contact") or {}).get("id")
        if not contact_id:
            raise DeliveryError("contact upsert: no contact id in response")
        return str(contact_id), 0

    def _read_contact(self, contact_id: str) -> GhlContact:
        data = self._call("contact read", "GET", f"/contacts/{contact_id}")
        return _contact_from({**(data.get("contact") or data), "id": contact_id})

    def _attach_phone(self, row: Mapping[str, Any], contact_id: str, contact: GhlContact) -> list[str]:
        """Put the phone the lead gave in the reply on the contact, only when nothing else owns it and the contact
        has no phone of its own. Returns note lines saying what was left alone and why."""
        phone = phone_in_reply(row.get("reply_text"))
        if not phone or contact.phone == phone:
            return []
        owners = [c for c in self._search(phone) if normalize(c.get("phone")) == phone and str(c["id"]) != contact_id]
        if owners:
            return ["The phone number in the reply is already on another GHL contact: not added to this one."]
        if contact.phone:
            return ["The reply gave a different phone number than the one on this contact: left unchanged."]
        try:
            self._call("contact phone", "PUT", f"/contacts/{contact_id}", json={"phone": phone})
        except DeliveryError as exc:  # e.g. the location rejects a duplicate phone: the rest of the hand-over still runs
            logger.warning("[instantly-handoff] phone not added reply=%s: %s", row["id"], exc)
            return ["The phone number in the reply could not be added to this contact."]
        return []

    def _release_cold_route(self, contact_id: str, contact: GhlContact) -> list[str]:
        """Remove ``ndl-cold`` (so later agent emails go through GHL) only when GHL email is open for this contact. A
        cold import sets Email do-not-disturb, and the GHL rail refuses a contact with it: removing the tag then would
        cut off every follow-up. Such a contact keeps ``ndl-cold`` and its Instantly route, and its do-not-disturb is
        never lifted here."""
        if GHL_TAG_COLD_LEAD not in contact.tags:
            return []
        if "Email" in contact.dnd_channels:
            return ["Cold email do-not-disturb is on in GHL: ndl-cold kept, follow-up emails stay on Instantly."]
        self._call("remove cold tag", "DELETE", f"/contacts/{contact_id}/tags", json={"tags": [GHL_TAG_COLD_LEAD]})
        return []

    def _ensure_card(self, row: Mapping[str, Any], contact_id: str) -> None:
        settings = get_settings()
        pipeline_id, stage_id = settings.lending_ghl_pipeline_id, settings.lending_ghl_stage_new_lead
        if not (pipeline_id and stage_id):
            return
        found = self._call("opportunity search", "GET", "/opportunities/search", params={
            "location_id": self._account.location_id, "contact_id": contact_id, "pipeline_id": pipeline_id})
        if found.get("opportunities"):
            return
        who = row.get("first_name") or "Instantly reply"
        self._call("opportunity create", "POST", "/opportunities/", json={
            "pipelineId": pipeline_id, "locationId": self._account.location_id, "name": f"Instantly reply - {who}",
            "pipelineStageId": stage_id, "contactId": contact_id, "status": "open", "source": SOURCE})

    def _note(self, row: Mapping[str, Any], contact_id: str, lines: list[str]) -> None:
        """Best effort: the reply itself is on lending.instantly_replies."""
        try:
            self._call("note", "POST", f"/contacts/{contact_id}/notes", json={"body": "\n".join(lines)})
        except DeliveryError as exc:
            logger.warning("[instantly-handoff] note failed reply=%s: %s", row["id"], exc)


def get_handoff() -> Optional[HandOff]:
    """The hand-off, or None while it is switched off or the Next Deal Lending GHL account is not configured."""
    if not get_settings().lending_instantly_ghl_handoff_enabled:
        return None
    account: Optional[GhlAccount] = lending_ghl_account()
    return InstantlyGhlHandoff(account) if account is not None else None
