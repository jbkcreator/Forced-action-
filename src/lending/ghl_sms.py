"""GoHighLevel SMS sender for the Next Deal Lending sub-account (WP-GL-9).

Credentials: LENDING_GHL_API_KEY + LENDING_GHL_LOCATION_ID (the Next Deal Lending sub-account,
client answer B1) when both are set, else the shared GHL_API_KEY / GHL_LOCATION_ID, which today
point at the Bay Street Capital sub-account. The fallback is an interim, team-lead-approved
decision until the client provides the Next Deal Lending sub-account; switching is two env vars.

Request shapes follow the public GHL v2 API reference (contacts/upsert, conversations/messages)
and are NOT yet confirmed against a live round trip. Run ``--send-test`` once with real
credentials before enabling texting, and correct field names here if the live call disagrees.
Error text carries the HTTP status only: GHL response bodies can echo phone numbers.

Usage:
    python -m src.lending.ghl_sms --send-test +1813XXXXXXX   # sends one real text to YOUR phone
"""
from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from typing import Any, Callable, Optional

from config.settings import get_settings
from src.services import ghl_webhook
from src.services.phone_utils import normalize

logger = logging.getLogger(__name__)

Request = Callable[..., Any]


class GhlSmsError(RuntimeError):
    """A text could not be handed to GHL. The message never contains a phone or response body."""


@dataclass(frozen=True)
class GhlAccount:
    api_key: str
    location_id: str


_warned_fallback = False


def lending_ghl_account() -> Optional[GhlAccount]:
    """Next Deal Lending credentials when both are set, else the shared (Bay Street) ones, else None."""
    global _warned_fallback
    s = get_settings()
    if s.lending_ghl_api_key is not None and s.lending_ghl_location_id:
        return GhlAccount(s.lending_ghl_api_key.get_secret_value(), s.lending_ghl_location_id)
    if s.ghl_api_key is None or not s.ghl_location_id:
        return None
    if not _warned_fallback:
        logger.warning("[lending-ghl] LENDING_GHL_* not set: using the shared GHL_* account (interim, Bay Street Capital)")
        _warned_fallback = True
    return GhlAccount(s.ghl_api_key.get_secret_value(), s.ghl_location_id)


def ghl_headers(api_key: str, version: str = "2021-07-28") -> dict[str, str]:
    return {"Authorization": f"Bearer {api_key}", "Version": version,
            "Content-Type": "application/json", "Accept": "application/json"}


def texting_number() -> Optional[str]:
    """The one number used for calling and texting (E.164), or None when not configured."""
    raw = get_settings().lending_ghl_sms_from_number
    return (normalize(raw) or raw) if raw else None


class GhlSmsSender:
    def __init__(self, account: GhlAccount, from_number: str, request: Request = ghl_webhook._ghl_request) -> None:
        self._account, self._from, self._request = account, from_number, request

    def _post(self, path: str, body: dict, what: str, version: str = "2021-07-28") -> dict:
        try:
            response = self._request("POST", f"{ghl_webhook._GHL_BASE}{path}",
                                     headers=ghl_headers(self._account.api_key, version), json=body)
        except Exception as exc:  # class only: the message can carry request detail
            logger.warning("[lending-ghl-sms] %s request error: %s", what, type(exc).__name__)
            raise GhlSmsError(f"GHL {what} request error ({type(exc).__name__})") from None
        if response.status_code >= 400:
            logger.warning("[lending-ghl-sms] %s failed: HTTP %s", what, response.status_code)
            raise GhlSmsError(f"GHL {what} failed: HTTP {response.status_code}")
        try:
            return response.json() or {}
        except ValueError:
            return {}

    def __call__(self, phone: str, body: str, first_name: Optional[str] = None) -> str:
        contact = {"locationId": self._account.location_id, "phone": phone}
        if first_name:
            contact["firstName"] = first_name
        contact_id = (self._post("/contacts/upsert", contact, "contact upsert").get("contact") or {}).get("id")
        if not contact_id:
            raise GhlSmsError("GHL contact upsert returned no contact id")
        sent = self._post("/conversations/messages",
                          {"type": "SMS", "contactId": contact_id, "message": body, "fromNumber": self._from},
                          "message send", version="2021-04-15")  # conversations endpoints use this version
        message_id = sent.get("messageId") or sent.get("id")
        if not message_id:
            raise GhlSmsError("GHL message send returned no message id")
        return str(message_id)


def get_sender() -> Optional[GhlSmsSender]:
    """None until a GHL account and the texting number are both configured."""
    account, number = lending_ghl_account(), texting_number()
    if account is None or not number:
        return None
    return GhlSmsSender(account, number)


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--send-test", metavar="PHONE", required=True, help="send ONE real text to this (your own) phone")
    phone = normalize(parser.parse_args(argv).send_test)
    sender = get_sender()
    if sender is None or not phone:
        logger.error("[lending-ghl-sms] GHL account / LENDING_GHL_SMS_FROM_NUMBER not configured or phone invalid; nothing sent")
        return 2
    message_id = sender(phone, "Next Deal Lending test message. Reply STOP to opt out.", None)
    logger.info("[lending-ghl-sms] test text accepted by GHL (message id %s)", message_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
