"""The GHL tag on a contact in an open night conversation, so GHL Conversation AI does not answer them as well.

While a conversation is open, its GHL contact carries ``GHL_TAG_NIGHT_CONVERSATION``; the GHL Conversation AI
workflow skips contacts with that tag (set in GHL, docs/lending/ghl-conversation-ai.md). The tag comes off once the
conversation is booked, closed or expired, unless the same contact has another conversation still open: the tag is
one per contact, so it stays for that one. :func:`sync_night_tags` runs at the start of every night responder pass,
before its messages go out.

A conversation with no GHL contact id is looked up by phone; one not found in GHL is tried again next pass. Each
add or removal is recorded on the row (``ghl_tagged_at`` / ``ghl_untagged_at``), so it is made once; a failed GHL
call changes nothing and is retried next pass. A tag write never messages the lead, so it also runs while sends
wait for the 8am-8pm window. Logs carry conversation ids only.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Mapping, Optional, Protocol

from sqlalchemy import text

from config.lending_after_hours_conversation import GHL_TAG_NIGHT_CONVERSATION, OPEN_STATES, TAG_SYNC_BATCH_SIZE

logger = logging.getLogger(__name__)


class NightTagClient(Protocol):
    def find_contact_id(self, phone: str) -> Optional[str]: ...
    def add_tag(self, contact_id: str, tag: str) -> None: ...
    def remove_tag(self, contact_id: str, tag: str) -> None: ...


class GhlTagError(RuntimeError):
    """A GHL tag call failed; the message holds the step and HTTP status only."""


class LendingGhlTagClient:
    """The Next Deal Lending GHL sub-account (``ghl_account.lending_ghl_account``)."""

    def __init__(self, account: Any) -> None:
        self._account = account

    def _call(self, step: str, method: str, path: str, body: Mapping[str, Any]) -> dict[str, Any]:
        from src.lending.ghl_account import ghl_headers
        from src.services import ghl_webhook

        try:
            response = ghl_webhook._ghl_request(method, f"{ghl_webhook._GHL_BASE}{path}",
                                                headers=ghl_headers(self._account.api_key), json=dict(body))
        except Exception as exc:  # class only: the message can carry request detail
            raise GhlTagError(f"{step}: {type(exc).__name__}") from exc
        if response.status_code >= 400:
            raise GhlTagError(f"{step}: HTTP {response.status_code}")
        try:
            return dict(response.json() or {})
        except ValueError:
            return {}

    def find_contact_id(self, phone: str) -> Optional[str]:
        found = self._call("search contact", "POST", "/contacts/search", {
            "locationId": self._account.location_id, "page": 1, "pageLimit": 1,
            "filters": [{"field": "phone", "operator": "eq", "value": phone}],
        })
        contacts = found.get("contacts") or []
        return str(contacts[0]["id"]) if contacts and contacts[0].get("id") else None

    def add_tag(self, contact_id: str, tag: str) -> None:
        self._call("add tag", "POST", f"/contacts/{contact_id}/tags", {"tags": [tag]})

    def remove_tag(self, contact_id: str, tag: str) -> None:
        self._call("remove tag", "DELETE", f"/contacts/{contact_id}/tags", {"tags": [tag]})


def night_tag_client() -> Optional[NightTagClient]:
    from src.lending.ghl_account import lending_ghl_account

    account = lending_ghl_account()
    return LendingGhlTagClient(account) if account is not None else None


_TO_TAG = text("""
    SELECT id, phone, ghl_contact_id FROM lending.after_hours_conversations
     WHERE state = ANY(:open_states) AND expires_at > :now AND ghl_tagged_at IS NULL
       AND (ghl_contact_id IS NOT NULL OR phone IS NOT NULL)
     ORDER BY opened_at LIMIT :limit
""")
# The tag is per GHL contact, shared by all of that contact's conversations: a finished conversation whose contact
# has another one still open keeps it (still_needed), and the open one takes over the tag.
_TO_UNTAG = text("""
    SELECT c.id, c.ghl_contact_id,
           EXISTS (SELECT 1 FROM lending.after_hours_conversations o
                    WHERE o.id <> c.id AND o.state = ANY(:open_states) AND o.expires_at > :now
                      AND (o.ghl_contact_id = c.ghl_contact_id OR (o.phone IS NOT NULL AND o.phone = c.phone)))
               AS still_needed
      FROM lending.after_hours_conversations c
     WHERE c.ghl_tagged_at IS NOT NULL AND c.ghl_untagged_at IS NULL
       AND (c.state <> ALL(:open_states) OR c.expires_at <= :now)
     ORDER BY c.updated_at LIMIT :limit
""")
_MARK_TAGGED = text("""
    UPDATE lending.after_hours_conversations
       SET ghl_tagged_at = :now, ghl_contact_id = COALESCE(ghl_contact_id, :contact_id)
     WHERE id = :id AND ghl_tagged_at IS NULL
""")
_MARK_UNTAGGED = text("UPDATE lending.after_hours_conversations SET ghl_untagged_at = :now WHERE id = :id")


def sync_night_tags(db, client: NightTagClient, now: datetime) -> dict[str, int]:
    """Tag the GHL contacts of open conversations and untag those of finished ones. Commits after each write, so
    a later failure never undoes a tag GHL already holds."""
    counts = {"tagged": 0, "untagged": 0, "kept_for_open": 0, "not_in_ghl": 0, "failed": 0}
    params = {"open_states": list(OPEN_STATES), "now": now, "limit": TAG_SYNC_BATCH_SIZE}
    for row in db.execute(_TO_TAG, params).mappings().all():
        try:
            contact_id = row["ghl_contact_id"] or client.find_contact_id(row["phone"])
            if contact_id is None:
                counts["not_in_ghl"] += 1
                continue
            client.add_tag(contact_id, GHL_TAG_NIGHT_CONVERSATION)
        except GhlTagError as exc:
            counts["failed"] += 1
            logger.warning("[night-tag] conversation %s not tagged (%s); retrying next pass", row["id"], exc)
            continue
        db.execute(_MARK_TAGGED, {"id": row["id"], "now": now, "contact_id": contact_id})
        db.commit()
        counts["tagged"] += 1
    for row in db.execute(_TO_UNTAG, params).mappings().all():
        if row["still_needed"]:
            db.execute(_MARK_UNTAGGED, {"id": row["id"], "now": now})
            db.commit()
            counts["kept_for_open"] += 1
            continue
        try:
            client.remove_tag(row["ghl_contact_id"], GHL_TAG_NIGHT_CONVERSATION)
        except GhlTagError as exc:
            counts["failed"] += 1
            logger.warning("[night-tag] conversation %s not untagged (%s); retrying next pass", row["id"], exc)
            continue
        db.execute(_MARK_UNTAGGED, {"id": row["id"], "now": now})
        db.commit()
        counts["untagged"] += 1
    return {key: value for key, value in counts.items() if value}
