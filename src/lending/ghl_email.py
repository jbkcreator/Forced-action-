"""GoHighLevel email sender for booking reminders to contacts without text consent.

Same account resolution, single-attempt rule and error semantics as the GHL text sender
(``ghl_sms``): one contact upsert, then one send, never retried here, and error text carries the
HTTP status only. Every email is From ``LENDING_EMAIL_FROM`` (David) with Josh in ``emailBcc``
(``LENDING_EMAIL_BCC``), never CC (src/lending/email_identity.py). The address must be a verified sender on
the sub-account's email domain, so the sender stays unconfigured until the identity and a GHL account are set.

The message shape (conversations/messages, type Email, html + subject + emailFrom) was accepted by GHL in a
live send-test; ``emailBcc`` (a list of addresses) follows the public GHL v2 API reference and is NOT yet
confirmed against a live round trip. Run
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

from src.lending.cold_leads import SendChannel
from src.lending.email_identity import LendingEmailIdentity, lending_email_identity
from src.lending.ghl_sms import GhlAccount, GhlSmsError, GhlSmsSender, Request, lending_ghl_account

logger = logging.getLogger(__name__)


def _as_html(body: str) -> str:
    return "<br>".join(html.escape(line) for line in body.splitlines())


class GhlEmailSender(GhlSmsSender):
    _log_prefix = "[lending-ghl-email]"

    def __init__(self, account: GhlAccount, identity: LendingEmailIdentity, request: Optional[Request] = None) -> None:
        super().__init__(account, identity.from_address, request)
        self._bcc = list(identity.bcc)

    def __call__(self, to: str, subject: str, body: str, *, phone: Optional[str] = None,
                 first_name: Optional[str] = None, deadline: Optional[datetime] = None) -> str:
        """Upsert the contact by email (and phone when known, so it stays one GHL contact), then send.
        Past ``deadline`` (UTC) nothing is sent and a non-ambiguous GhlSmsError is raised."""
        decision = self._cold_lead_check(phone, SendChannel.EMAIL, email=to)
        contact: dict = {"locationId": self._account.location_id, "email": to}
        if phone:
            contact["phone"] = phone
        if first_name:
            contact["firstName"] = first_name
        contact_id = (self._post(self._http(), "/contacts/upsert", contact, "contact upsert").get("contact") or {}).get("id")
        if not contact_id:
            raise GhlSmsError("GHL contact upsert returned no contact id")
        if decision.lift_dnd:  # a warm lead first reached cold: lift only the email do-not-disturb the cold push set
            identity = {"locationId": self._account.location_id, "email": to, **({"phone": phone} if phone else {})}
            self._lift_cold_dnd(contact_id, identity, decision.lift_dnd)
        if deadline is not None and datetime.now(timezone.utc) > deadline:
            raise GhlSmsError("deadline passed before send")
        sent = self._post(self._http(), "/conversations/messages",
                          {"type": "Email", "contactId": contact_id, "subject": subject, "html": _as_html(body),
                           "emailFrom": self._from, "emailBcc": self._bcc},
                          "email send", version="2021-04-15", is_send=True)
        message_id = sent.get("messageId") or sent.get("emailMessageId") or sent.get("id")
        if not message_id:
            raise GhlSmsError("GHL email send returned no message id", ambiguous=True)
        return str(message_id)


def get_email_sender() -> Optional[GhlEmailSender]:
    """None until a GHL account and the lending email identity (David as sender, Josh in BCC) are configured."""
    account, identity = lending_ghl_account(), lending_email_identity()
    if account is None or identity is None:
        return None
    return GhlEmailSender(account, identity)


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--send-test", metavar="EMAIL", required=True, help="send ONE real email to this (your own) address")
    to = parser.parse_args(argv).send_test.strip()
    sender = get_email_sender()
    if sender is None or "@" not in to:
        logger.error("[lending-ghl-email] GHL account / LENDING_EMAIL_FROM / LENDING_EMAIL_BCC not configured or address invalid; nothing sent")
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
