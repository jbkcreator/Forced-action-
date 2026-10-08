"""GoHighLevel SMS sender for the Next Deal Lending sub-account (WP-GL-9).

Credentials: LENDING_GHL_API_KEY + LENDING_GHL_LOCATION_ID, the Next Deal Lending sub-account, which is the
only GHL account lending uses (see src.lending.ghl_account: no fallback to the platform's GHL_* account,
and one of the two set without the other fails closed, with no account).

Both the contact upsert and the message send make exactly ONE attempt. The send must never repeat: after
a read timeout GHL may already have accepted the text, and a retry would double-text a borrower. The
upsert is single too because the text has to leave within 60 s of the call, so a retrying helper (which
can run for minutes during a GHL outage) would stall the whole batch; a failed upsert means nothing was
sent and the caller records it failed. A failed send is reported to the caller and never retried here.

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
from datetime import datetime, timezone
from typing import Any, Callable, Optional

import requests

from config.settings import get_settings
from src.lending.ghl_account import GhlAccount, ghl_headers, lending_ghl_account
from src.services import ghl_webhook
from src.services.phone_utils import normalize

logger = logging.getLogger(__name__)

Request = Callable[..., Any]

_SEND_TIMEOUT = 15  # seconds, same as the shared GHL helper


class GhlSmsError(RuntimeError):
    """A text could not be handed to GHL. The message never contains a phone or response body.

    ``ambiguous`` is True when GHL may nonetheless have accepted the text (send timeout or connection
    error, HTTP 5xx, or a 2xx with no message id): the caller must treat the outcome as unknown and
    never resend. False means nothing was sent (upsert failures, 4xx rejections).
    """

    def __init__(self, message: str, *, ambiguous: bool = False) -> None:
        super().__init__(message)
        self.ambiguous = ambiguous


def texting_number() -> Optional[str]:
    """The one number used for calling and texting (E.164), or None when not configured."""
    raw = get_settings().lending_ghl_sms_from_number
    if not raw:
        return None
    number = normalize(raw)
    if number is None:
        logger.warning("[lending-ghl-sms] LENDING_GHL_SMS_FROM_NUMBER is not a valid US number; texting disabled")
    return number


def _single_attempt(method: str, url: str, **kwargs: Any) -> requests.Response:
    """One HTTP attempt, no retry: a repeated send can double-text a borrower."""
    return requests.request(method, url, timeout=_SEND_TIMEOUT, **kwargs)


class GhlSmsSender:
    def __init__(self, account: GhlAccount, from_number: str, request: Optional[Request] = None) -> None:
        """``request`` (tests) replaces both calls; unset, both are single attempts."""
        self._account, self._from, self._request = account, from_number, request

    def _http(self) -> Request:
        return self._request or _single_attempt

    def _post(self, request: Request, path: str, body: dict, what: str, version: str = "2021-07-28",
              is_send: bool = False) -> dict:
        try:
            response = request("POST", f"{ghl_webhook._GHL_BASE}{path}",
                                     headers=ghl_headers(self._account.api_key, version), json=body)
        except Exception as exc:  # class only: the message can carry request detail
            logger.warning("[lending-ghl-sms] %s request error: %s", what, type(exc).__name__)
            raise GhlSmsError(f"GHL {what} request error ({type(exc).__name__})", ambiguous=is_send) from None
        if response.status_code >= 400:
            logger.warning("[lending-ghl-sms] %s failed: HTTP %s", what, response.status_code)
            raise GhlSmsError(f"GHL {what} failed: HTTP {response.status_code}",
                              ambiguous=is_send and response.status_code >= 500)
        try:
            return response.json() or {}
        except ValueError:
            return {}

    def __call__(self, phone: str, body: str, first_name: Optional[str] = None, *,
                 deadline: Optional[datetime] = None) -> str:
        """Upsert the contact, then send. ``deadline`` (UTC) is the latest moment a send may still start;
        past it nothing is sent and a non-ambiguous GhlSmsError is raised."""
        contact = {"locationId": self._account.location_id, "phone": phone}
        if first_name:
            contact["firstName"] = first_name
        contact_id = (self._post(self._http(), "/contacts/upsert", contact, "contact upsert").get("contact") or {}).get("id")
        if not contact_id:
            raise GhlSmsError("GHL contact upsert returned no contact id")
        if deadline is not None and datetime.now(timezone.utc) > deadline:
            raise GhlSmsError("deadline passed before send")
        sent = self._post(self._http(), "/conversations/messages",
                          {"type": "SMS", "contactId": contact_id, "message": body, "fromNumber": self._from},
                          "message send", version="2021-04-15", is_send=True)  # conversations endpoints use this version
        message_id = sent.get("messageId") or sent.get("id")
        if not message_id:
            raise GhlSmsError("GHL message send returned no message id", ambiguous=True)
        return str(message_id)


def get_sender() -> Optional[GhlSmsSender]:
    """None until a GHL account and the texting number are both configured."""
    account, number = lending_ghl_account(), texting_number()
    if account is None or not number:
        return None
    return GhlSmsSender(account, number)


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--send-test", metavar="PHONE", required=True, help="send ONE real text to this (your own) phone")
    phone = normalize(parser.parse_args(argv).send_test)
    sender = get_sender()
    if sender is None or not phone:
        logger.error("[lending-ghl-sms] GHL account / LENDING_GHL_SMS_FROM_NUMBER not configured or phone invalid; nothing sent")
        return 2
    try:
        message_id = sender(phone, "Next Deal Lending test message. Reply STOP to opt out.", None)
    except GhlSmsError as exc:
        logger.error("[lending-ghl-sms] test text failed: %s", exc)
        return 1
    logger.info("[lending-ghl-sms] test text accepted by GHL (message id %s)", message_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
