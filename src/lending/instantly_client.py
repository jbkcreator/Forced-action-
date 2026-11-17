"""NDL's Instantly workspace (P1.3): reply in an email thread and block an address. Separate from FA's
``src.services.instantly_service`` (its own key, ``LENDING_INSTANTLY_API_KEY``), so the two lanes never share an
account.

UNVERIFIED: the request shapes follow Instantly's public API v2 reference (``POST /api/v2/emails/reply`` with
``eaccount``, ``reply_to_uuid``, ``subject`` and ``body``; ``POST /api/v2/block-lists-entries``), not a captured
call. Check them with one test reply before relying on them. Every reply is sent from David's inbox
(``LENDING_EMAIL_FROM``) with Josh in ``bcc_address_email_list`` (``LENDING_EMAIL_BCC``), never CC; the BCC field
name and its comma-separated string form match Instantly's OpenAPI spec (checked 2026-11-17), not a live call.

Never retried: a reply that timed out may already have been sent, and the relay records it as failed instead of
sending twice.
"""
from __future__ import annotations

import html
import logging
from dataclasses import dataclass
from typing import Optional

import requests

from config.settings import get_settings

logger = logging.getLogger(__name__)

TIMEOUT_SECONDS = 20


class InstantlyError(RuntimeError):
    """Instantly refused or could not be reached. The message never carries the address or the text."""


@dataclass(frozen=True)
class InstantlyAccount:
    api_key: str
    base_url: str


def lending_instantly_account() -> Optional[InstantlyAccount]:
    """The NDL Instantly account, or None while ``LENDING_INSTANTLY_API_KEY`` is unset."""
    settings = get_settings()
    if not settings.lending_instantly_api_key:
        return None
    return InstantlyAccount(settings.lending_instantly_api_key.get_secret_value(),
                            settings.lending_instantly_base_url.rstrip("/"))


def _html_body(body: str) -> str:
    return "<br>".join(html.escape(line) for line in body.splitlines())


def _post(account: InstantlyAccount, path: str, payload: dict) -> dict:
    try:
        response = requests.post(f"{account.base_url}{path}", json=payload, timeout=TIMEOUT_SECONDS,
                                 headers={"Authorization": f"Bearer {account.api_key}"})
    except requests.RequestException as exc:
        raise InstantlyError(f"{path} unreachable ({type(exc).__name__})") from None
    if response.status_code >= 400:
        raise InstantlyError(f"{path} returned HTTP {response.status_code}")
    try:
        return response.json() or {}
    except ValueError:
        return {}


def send_reply(account: InstantlyAccount, *, reply_to_uuid: str, subject: str, body: str) -> Optional[str]:
    """Reply inside the lead's thread from David's inbox with Josh in BCC. Returns Instantly's email id, if it gives
    one. Refused while the lending-wide emergency halt is set (an unreadable flag counts as set), like the GHL
    senders, and while the lending email identity is not configured."""
    from src.lending.email_identity import lending_email_identity
    from src.lending.global_halt import is_globally_halted

    if is_globally_halted():
        raise InstantlyError("lending emergency halt is on; nothing was sent")
    identity = lending_email_identity()
    if identity is None:
        raise InstantlyError("lending email sender (LENDING_EMAIL_FROM / LENDING_EMAIL_BCC) not configured; nothing was sent")
    result = _post(account, "/api/v2/emails/reply", {
        "eaccount": identity.from_address, "reply_to_uuid": reply_to_uuid, "subject": subject,
        "body": {"text": body, "html": _html_body(body)},
        "bcc_address_email_list": ",".join(identity.bcc),
    })
    email_id = result.get("id")
    return str(email_id) if email_id else None


def block_address(account: InstantlyAccount, email: str) -> bool:
    """Add ``email`` to the workspace block list so no NDL campaign emails it again. Best effort: False (logged)
    when Instantly refuses; the opt-out is already recorded in our stores either way."""
    try:
        _post(account, "/api/v2/block-lists-entries", {"entries": [email]})
    except InstantlyError as exc:
        logger.warning("[instantly] block list entry not added (%s)", exc)
        return False
    return True
