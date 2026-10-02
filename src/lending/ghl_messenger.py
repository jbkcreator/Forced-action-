"""WP-GL-10: GHL messenger port — Fake and Live implementations.

All GL-10 outbound texts go through this port. The Live adapter calls the
GHL Conversations API under the Next Deal Lending sub-account (G4 confirmed:
"every automated text goes from GoHighLevel under the Next Deal Lending
number"). The Fake adapter records sends in memory and writes nothing to GHL.

LIVE IS NOT WIRED UNTIL GHL ADMIN ACCESS IS GRANTED. The Live implementation
reflects the GHL v2 API field names from the public docs, but has NOT been
confirmed against a real round-trip. Review field names against a live call
before trusting them in production.

Controlled by env vars:
  LENDING_TEXT_ENABLED=false  (default) — fail-closed until 10DLC approved
  LENDING_GHL_MESSENGER_MODE=fake|live  — fake is the default

Only LENDING_TEXT_ENABLED=true AND LENDING_GHL_MESSENGER_MODE=live together
produce real sends.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional, Protocol

from config.settings import get_settings

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MessageResult:
    sent: bool
    message_id: Optional[str] = None
    skip_reason: Optional[str] = None  # logged, never exposed externally


class GHLMessenger(Protocol):
    """Send one text to a contact in the GHL Next Deal Lending sub-account."""

    def send_text(
        self,
        *,
        contact_phone: str,
        body: str,
        ghl_contact_id: Optional[str] = None,
    ) -> MessageResult:
        ...


# ── Fake ─────────────────────────────────────────────────────────────────────

class FakeGHLMessenger:
    """In-memory recorder used in all tests and in fake mode.

    Never makes a network call. Sends accumulate in self.sent, skips in
    self.skipped. Cleared between test cases by passing a fresh instance.
    """

    def __init__(self) -> None:
        self.sent: list[dict] = []
        self.skipped: list[dict] = []

    def send_text(
        self,
        *,
        contact_phone: str,
        body: str,
        ghl_contact_id: Optional[str] = None,
    ) -> MessageResult:
        record = {
            "contact_phone": contact_phone,
            "body": body,
            "ghl_contact_id": ghl_contact_id,
        }
        self.sent.append(record)
        logger.info(
            "[ghl-messenger.fake] would-send to phone_hash=%.12s body_len=%d",
            _phone_hash(contact_phone),
            len(body),
        )
        return MessageResult(sent=True, message_id=f"fake-{len(self.sent)}")


# ── Live ─────────────────────────────────────────────────────────────────────

class LiveGHLMessenger:
    """Sends texts through GHL Conversations API under the NDL sub-account.

    UNVERIFIED against a real call: GHL field names come from the v2 public
    docs. Verify the exact payload shape by testing with a real GHL contact
    before trusting in production.

    This adapter will raise GHLMessengerError on any HTTP error rather than
    returning sent=False, so the reminder worker can record the failure and
    retry on the next cycle.
    """

    _GHL_BASE = "https://services.leadconnectorhq.com"
    _API_VERSION = "2021-04-15"
    _TIMEOUT = 15

    def __init__(self, *, api_key: str, location_id: str) -> None:
        self._api_key = api_key
        self._location_id = location_id

    @classmethod
    def from_settings(cls) -> "LiveGHLMessenger":
        from config.settings import get_settings
        s = get_settings()
        if not s.ghl_api_key:
            raise GHLMessengerError("GHL_API_KEY is not set")
        if not s.ghl_location_id:
            raise GHLMessengerError("GHL_LOCATION_ID is not set")
        return cls(
            api_key=s.ghl_api_key.get_secret_value(),
            location_id=s.ghl_location_id,
        )

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self._api_key}",
            "Version": self._API_VERSION,
            "Content-Type": "application/json",
        }

    def send_text(
        self,
        *,
        contact_phone: str,
        body: str,
        ghl_contact_id: Optional[str] = None,
    ) -> MessageResult:
        import requests

        # GHL Conversations API requires a contactId. If not supplied we look
        # up by phone. If still not found we cannot send — return skip.
        cid = ghl_contact_id or self._lookup_contact_id(contact_phone)
        if not cid:
            logger.warning(
                "[ghl-messenger.live] no GHL contact found phone_hash=%.12s — skipping",
                _phone_hash(contact_phone),
            )
            return MessageResult(sent=False, skip_reason="no_ghl_contact")

        payload = {
            "type": "SMS",
            "message": body,
            "contactId": cid,
            "locationId": self._location_id,
        }
        try:
            resp = requests.post(
                f"{self._GHL_BASE}/conversations/messages",
                headers=self._headers(),
                json=payload,
                timeout=self._TIMEOUT,
            )
        except Exception as exc:
            raise GHLMessengerError(f"network error sending text: {exc}") from exc

        if not resp.ok:
            raise GHLMessengerError(
                f"GHL send-text failed HTTP {resp.status_code}: {resp.text[:300]}"
            )

        data = resp.json()
        msg_id = (
            data.get("message", {}).get("id")
            or data.get("id")
            or data.get("messageId")
        )
        logger.info(
            "[ghl-messenger.live] sent msg_id=%s phone_hash=%.12s",
            msg_id, _phone_hash(contact_phone),
        )
        return MessageResult(sent=True, message_id=str(msg_id) if msg_id else None)

    def _lookup_contact_id(self, phone: str) -> Optional[str]:
        """Search GHL for a contact by phone. Returns contactId or None.

        UNVERIFIED field names — confirm against GHL v2 contacts/search docs.
        """
        import requests
        try:
            resp = requests.get(
                f"{self._GHL_BASE}/contacts/",
                headers=self._headers(),
                params={"phone": phone, "locationId": self._location_id},
                timeout=self._TIMEOUT,
            )
        except Exception as exc:
            logger.warning("[ghl-messenger.live] contact lookup failed: %s", exc)
            return None
        if not resp.ok:
            return None
        contacts = resp.json().get("contacts", [])
        return contacts[0]["id"] if contacts else None


class GHLMessengerError(Exception):
    """Raised by LiveGHLMessenger on a recoverable send failure."""


# ── Factory ───────────────────────────────────────────────────────────────────

def get_messenger() -> GHLMessenger:
    """Return the appropriate messenger based on env config.

    Only LENDING_TEXT_ENABLED=true AND LENDING_GHL_MESSENGER_MODE=live
    produces a Live adapter. Anything else falls back to Fake, which is
    the correct safe default: the reminder worker calls get_messenger() at
    runtime and the returned adapter determines whether a real network call
    is made.
    """
    s = get_settings()
    text_enabled = getattr(s, "lending_text_enabled", False)
    mode = getattr(s, "lending_ghl_messenger_mode", "fake")
    if text_enabled and mode == "live":
        return LiveGHLMessenger.from_settings()
    if text_enabled and mode != "fake":
        logger.warning(
            "[ghl-messenger] unrecognised LENDING_GHL_MESSENGER_MODE=%r — using fake", mode
        )
    if not text_enabled:
        logger.debug("[ghl-messenger] LENDING_TEXT_ENABLED is false — using fake messenger")
    return FakeGHLMessenger()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _phone_hash(phone: str) -> str:
    import hashlib
    return hashlib.sha256(phone.encode()).hexdigest()
