"""Phones behind an email that lending's own tables do not hold (P1.3).

A cold (Instantly) contact is often only in GHL and in the dialer: its email is not on a lending contact, lead or
partner row. An email-only STOP from it must still reach the dialer and GHL, so ``compliance._with_linked_phones``
asks here for the contact's phones: the GHL contact found by that email (read only, the Next Deal Lending sub-account
only), and every dialer load row of that GHL contact id (``lending.dialer_load_records.ghl_contact_id``).

One attempt per email (4 s timeout, 30 s for the batch, at most 10 emails), never raises: a GHL outage only costs the phone link (the email is
suppressed regardless), and the failure is logged by class so the operator can record the phone by hand.
"""
from __future__ import annotations

import logging
import time
from collections import defaultdict
from typing import Any, Iterable, Optional

import requests
from sqlalchemy import text

from src.lending.ghl_account import ghl_headers, lending_ghl_account
from src.services import ghl_webhook
from src.services.phone_utils import normalize

logger = logging.getLogger(__name__)

MAX_LOOKUPS_PER_BATCH = 10
_TIMEOUT = 4  # seconds per request
_BUDGET = 30.0  # seconds for the whole batch: it runs inside the webhook's 60-second stop budget

_LOAD_PHONES = text(
    "SELECT ghl_contact_id, phone FROM lending.dialer_load_records "
    "WHERE ghl_contact_id = ANY(:ids) AND phone IS NOT NULL"
)


def _search(account: Any, email: str) -> list[dict[str, Any]]:
    response = requests.request(
        "POST", f"{ghl_webhook._GHL_BASE}/contacts/search", headers=ghl_headers(account.api_key), timeout=_TIMEOUT,
        json={"locationId": account.location_id, "page": 1, "pageLimit": 20, "query": email})
    response.raise_for_status()
    return [c for c in (response.json().get("contacts") or []) if (c.get("email") or "").strip().lower() == email]


def phones_for_emails(db, emails: Iterable[str], failed: Optional[list[str]] = None) -> dict[str, set[str]]:
    """Email -> normalised phones of its GHL contact(s) and their dialer loads. Empty when GHL is not configured.

    ``failed`` collects the emails whose GHL search failed, was cut by the batch cap or ran out of time, so the caller
    can raise them to a person."""
    account = lending_ghl_account()
    wanted = sorted(set(emails))
    if account is None or not wanted:
        return {}
    if len(wanted) > MAX_LOOKUPS_PER_BATCH:
        logger.warning("[ghl-email-lookup] %d emails to look up, only the first %d are", len(wanted),
                       MAX_LOOKUPS_PER_BATCH)
        if failed is not None:
            failed.extend(wanted[MAX_LOOKUPS_PER_BATCH:])
        wanted = wanted[:MAX_LOOKUPS_PER_BATCH]
    phones: dict[str, set[str]] = defaultdict(set)
    ids: dict[str, set[str]] = defaultdict(set)
    deadline = time.monotonic() + _BUDGET
    for email in wanted:
        if time.monotonic() > deadline:
            logger.error("[ghl-email-lookup] time budget used up; the remaining emails' phones are not linked")
            if failed is not None:
                failed.append(email)
            continue
        try:
            contacts = _search(account, email)
        except Exception as exc:  # class only: the request carries the address
            logger.error("[ghl-email-lookup] GHL search failed (%s); this email's phones are not linked",
                         type(exc).__name__)
            if failed is not None:
                failed.append(email)
            continue
        for contact in contacts:
            if contact.get("id"):
                ids[email].add(str(contact["id"]))
            if contact.get("phone") and (phone := normalize(contact["phone"])):
                phones[email].add(phone)
    all_ids = sorted({i for found in ids.values() for i in found})
    if all_ids:
        by_id: dict[str, set[str]] = defaultdict(set)
        for contact_id, raw in db.execute(_LOAD_PHONES, {"ids": all_ids}).fetchall():
            if phone := normalize(raw):
                by_id[contact_id].add(phone)
        for email, found in ids.items():
            for contact_id in found:
                phones[email] |= by_id.get(contact_id, set())
    return dict(phones)
