"""GoHighLevel email sender for booking reminders to contacts without text consent.

Same account resolution, single-attempt rule and error semantics as the GHL text sender
(``ghl_sms``): one contact upsert, then one send, never retried here, and error text carries the
HTTP status only. The sending address is ``LENDING_GHL_EMAIL_FROM`` (hello@nextdeallending.com);
it must be a verified sender on the sub-account's email domain, so the sender stays unconfigured
until both the address and a GHL account are set.

The message shape (conversations/messages, type Email, html + subject + emailFrom) follows the
public GHL v2 API reference and is NOT yet confirmed against a live round trip. Run
``--send-test`` once with real credentials before enabling ``BOOKING_REMINDER_EMAIL_ENABLED``.

Usage:
    python -m src.lending.ghl_email --send-test you@example.com   # sends one real email to YOUR inbox
"""
from __future__ import annotations

import argparse
import html
import logging
from datetime import datetime, timezone
from typing import Optional

from config.settings import get_settings
from src.lending.ghl_sms import GhlAccount, GhlSmsError, GhlSmsSender, Request, lending_ghl_account

logger = logging.getLogger(__name__)


def email_from_address() -> Optional[str]:
    """The verified sending address, or None when not configured."""
    address = (get_settings().lending_ghl_email_from or "").strip()
    return address or None


def _as_html(body: str) -> str:
    return "<br>".join(html.escape(line) for line in body.splitlines())


class GhlEmailSender(GhlSmsSender):
    _log_prefix = "[lending-ghl-email]"

    def __init__(self, account: GhlAccount, from_address: str, request: Optional[Request] = None) -> None:
        super().__init__(account, from_address, request)

    def __call__(self, to: str, subject: str, body: str, *, phone: Optional[str] = None,
                 first_name: Optional[str] = None, deadline: Optional[datetime] = None) -> str:
        """Upsert the contact by email (and phone when known, so it stays one GHL contact), then send.
        Past ``deadline`` (UTC) nothing is sent and a non-ambiguous GhlSmsError is raised."""
        contact = {"locationId": self._account.location_id, "email": to}
        if phone:
            contact["phone"] = phone
        if first_name:
            contact["firstName"] = first_name
        contact_id = (self._post(self._http(), "/contacts/upsert", contact, "contact upsert").get("contact") or {}).get("id")
        if not contact_id:
            raise GhlSmsError("GHL contact upsert returned no contact id")
        if deadline is not None and datetime.now(timezone.utc) > deadline:
            raise GhlSmsError("deadline passed before send")
        sent = self._post(self._http(), "/conversations/messages",
                          {"type": "Email", "contactId": contact_id, "subject": subject, "html": _as_html(body),
                           "emailFrom": self._from},
                          "email send", version="2021-04-15", is_send=True)
        message_id = sent.get("messageId") or sent.get("emailMessageId") or sent.get("id")
        if not message_id:
            raise GhlSmsError("GHL email send returned no message id", ambiguous=True)
        return str(message_id)


def get_email_sender() -> Optional[GhlEmailSender]:
    """None until a GHL account and the sending address are both configured."""
    account, address = lending_ghl_account(), email_from_address()
    if account is None or not address:
        return None
    return GhlEmailSender(account, address)


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--send-test", metavar="EMAIL", required=True, help="send ONE real email to this (your own) address")
    to = parser.parse_args(argv).send_test.strip()
    sender = get_email_sender()
    if sender is None or "@" not in to:
        logger.error("[lending-ghl-email] GHL account / LENDING_GHL_EMAIL_FROM not configured or address invalid; nothing sent")
        return 2
    try:
        message_id = sender(to, "Next Deal Lending test email", "This is a test email from Next Deal Lending.")
    except GhlSmsError as exc:
        logger.error("[lending-ghl-email] test email failed: %s", exc)
        return 1
    logger.info("[lending-ghl-email] test email accepted by GHL (message id %s)", message_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
