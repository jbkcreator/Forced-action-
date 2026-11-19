"""P1.3: a cold lead that replies with interest ("Deal") is handed to the Next Deal Lending GoHighLevel account.

Run from Cora's reply sweep (``instantly_replies.ReplyProcessor``) for a Deal reply only, behind
``LENDING_INSTANTLY_GHL_HANDOFF_ENABLED`` (default off). It does four things, each safe to repeat:

1. Upsert the GHL contact by email (an existing contact is updated, never duplicated). The upsert body carries no
   ``dnd``, ``tags`` or ``source``, so a do-not-disturb setting or an earlier tag is never overwritten (GHL's upsert
   REPLACES tags). It carries a phone only when the lead gave one in the reply ("gives a phone number"), so the
   conversation can move to GHL calls; that number is not consent to text.
2. Add the tag ``ndl-instantly-interested`` and remove ``ndl-cold`` (add-tags and remove-tags calls). From then on the
   agents' emails to this contact go through GHL, not Instantly. The first answer to the reply is not affected: it
   is a Cora draft sent inside the Instantly thread, chosen by its channel and not by the tag.
3. Create a pipeline card at the new-lead stage when the contact has no card in the pipeline (an existing card, in any
   stage, is never moved). Without ``LENDING_GHL_PIPELINE_ID`` and ``LENDING_GHL_STAGE_NEW_LEAD`` the lead is
   contact-only.
4. Add a note saying where the contact came from.

This never lifts a do-not-disturb setting: texts still need consent (``cold_leads.reconcile_warm_contact``), and an
address that opted out is not handed over (the caller checks).

Known limits (documented in docs/lending/instantly-replies.md): the contact is matched by email only, so a cold
contact loaded into GHL by phone with no email on file is not found and a second contact is created; a failure part-way
leaves a partly built contact and raises an inbox item (the reply is not retried); removing ``ndl-cold`` moves the
agents' later emails to this contact from Instantly to GHL, while the cold-push do-not-disturb stays until consent.
"""
from __future__ import annotations

import logging
import re
from typing import Any, Callable, Mapping, Optional

from config.lending_compliance import GHL_TAG_COLD_LEAD, GHL_TAG_INSTANTLY_INTERESTED
from config.settings import get_settings
from src.lending.ghl_account import GhlAccount, lending_ghl_account
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
        email = str(row["lead_email"]).strip().lower()
        contact_id = self._upsert(row, email)
        self._call("add tags", "POST", f"/contacts/{contact_id}/tags", json={"tags": [GHL_TAG_INSTANTLY_INTERESTED]})
        self._call("remove cold tag", "DELETE", f"/contacts/{contact_id}/tags", json={"tags": [GHL_TAG_COLD_LEAD]})
        self._ensure_card(row, contact_id)
        self._note(row, contact_id)
        return contact_id

    def _upsert(self, row: Mapping[str, Any], email: str) -> str:
        body: dict[str, Any] = {"locationId": self._account.location_id, "email": email}
        if row.get("first_name"):
            body["firstName"] = row["first_name"]
        if phone := phone_in_reply(row.get("reply_text")):
            body["phone"] = phone
        data = self._call("contact upsert", "POST", "/contacts/upsert", json=body)
        contact_id = (data.get("contact") or {}).get("id")
        if not contact_id:
            raise DeliveryError("contact upsert: no contact id in response")
        return str(contact_id)

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

    def _note(self, row: Mapping[str, Any], contact_id: str) -> None:
        """Best effort: the reply itself is on lending.instantly_replies."""
        campaign = row.get("campaign_name") or "n/a"
        body = (f"Replied with interest to NDL cold email (Instantly campaign: {campaign}, reply id {row['id']}). "
                "Handed over by Cora; the first answer goes in the Instantly thread.")
        try:
            self._call("note", "POST", f"/contacts/{contact_id}/notes", json={"body": body})
        except DeliveryError as exc:
            logger.warning("[instantly-handoff] note failed reply=%s: %s", row["id"], exc)


def get_handoff() -> Optional[HandOff]:
    """The hand-off, or None while it is switched off or the Next Deal Lending GHL account is not configured."""
    if not get_settings().lending_instantly_ghl_handoff_enabled:
        return None
    account: Optional[GhlAccount] = lending_ghl_account()
    return InstantlyGhlHandoff(account) if account is not None else None
