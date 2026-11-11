"""Cold email for Next Deal Lending through NDL's own Instantly workspace (P1.1, decisions D16 to D22).

Same "passthrough" method Forced Action's Relay uses (src/services/relay/channels_email.py): one long-lived
campaign, "NDL Cora Passthrough", whose only step is subject ``{{ra_subject}}`` / body ``{{ra_body}}``. Sending an
approved email = adding the recipient to that campaign as a lead carrying the approved text, with the CAN-SPAM
footer (NDL brand, postal address, phone, one-click unsubscribe) added here at send time. Instantly emails it in
the campaign's next slot; its schedule is 09:00-20:00 America/Detroit (Eastern), which keeps every send inside
8am-8pm for Eastern and Central recipients.

Credentials are LENDING_INSTANTLY_API_KEY / LENDING_INSTANTLY_CAMPAIGN_ID only, never Forced Action's
INSTANTLY_API_KEY: like GHL (src/lending/ghl_account.py), lending never touches FA's accounts. The sender is off
(``get_ndl_instantly_sender()`` is None) until the key, the campaign and the footer postal address are all set.

Instantly skips a lead already in the campaign, so a repeat email to the same address first removes the earlier
lead (the 3-day cold-email cap means that email already went out). The lookup sends ``search`` with the address and
also matches the exact address in the response, so a wrong lead is never removed; ``search`` is not verified against
Instantly's docs (developer.instantly.ai returned 404 on 2026-11-11). If it is ignored, a repeat send fails as a
duplicate and the receipt says ``error``.

Every send is one HTTP attempt per call, never retried: a duplicate email is worse than a missing one. Errors carry
the HTTP status only, never an address. Every send and campaign change is refused while the lending emergency halt is
on; the halt also pauses the campaign (src/lending/global_halt.py).

Usage (once, after Muhammad provides the key and the sending inbox):
    PYTHONPATH=. python -m src.lending.instantly_email --setup-campaign
"""
from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Callable, Optional

import requests

from config.settings import get_settings
from src.lending.global_halt import is_globally_halted

logger = logging.getLogger(__name__)

CAMPAIGN_NAME = "NDL Cora Passthrough"
SCHEDULE_TIMEZONE = "America/Detroit"  # Instantly's timezone enum takes Detroit for Eastern time
SCHEDULE_FROM, SCHEDULE_TO = "09:00", "20:00"
_TIMEOUT = 15  # seconds

Request = Callable[..., Any]


class InstantlySendError(RuntimeError):
    """The email was not handed to Instantly (or the outcome is unknown). Never holds an address."""


@dataclass(frozen=True)
class NdlInstantly:
    api_key: str
    base_url: str
    campaign_id: str
    sender_email: str
    footer_brand: str
    footer_postal_address: str
    footer_phone: str


def _single_attempt(method: str, url: str, **kwargs: Any) -> requests.Response:
    return requests.request(method, url, timeout=_TIMEOUT, **kwargs)


def _headers(api_key: str, *, has_body: bool) -> dict[str, str]:
    # Instantly (Fastify) rejects a JSON content-type with an empty body (FST_ERR_CTP_EMPTY_JSON_BODY).
    headers = {"Authorization": f"Bearer {api_key}"}
    if has_body:
        headers["Content-Type"] = "application/json"
    return headers


_warned_missing: Optional[tuple[str, ...]] = None


def ndl_instantly() -> Optional[NdlInstantly]:
    """NDL's Instantly settings, or None (logged once) unless key, campaign and footer postal address are set."""
    global _warned_missing
    s = get_settings()
    api_key = s.lending_instantly_api_key.get_secret_value() if s.lending_instantly_api_key else ""
    missing = tuple(name for name, value in (("LENDING_INSTANTLY_API_KEY", api_key),
                                             ("LENDING_INSTANTLY_CAMPAIGN_ID", s.lending_instantly_campaign_id),
                                             ("LENDING_EMAIL_FOOTER_POSTAL_ADDRESS",
                                              s.lending_email_footer_postal_address)) if not value)
    if missing:
        if missing != _warned_missing:
            logger.warning("[lending-instantly] cold email is off; missing settings: %s", ", ".join(missing))
            _warned_missing = missing
        return None
    return NdlInstantly(api_key=api_key, base_url=s.lending_instantly_base_url.rstrip("/"),
                        campaign_id=s.lending_instantly_campaign_id, sender_email=s.lending_instantly_sender_email,
                        footer_brand=s.lending_email_footer_brand,
                        footer_postal_address=s.lending_email_footer_postal_address,
                        footer_phone=s.lending_email_footer_phone)


class NdlInstantlySender:
    _log_prefix = "[lending-instantly]"

    def __init__(self, account: NdlInstantly, request: Optional[Request] = None) -> None:
        self._account = account
        self._request = request or _single_attempt

    def _call(self, what: str, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        try:
            response = self._request(method, f"{self._account.base_url}{path}",
                                     headers=_headers(self._account.api_key, has_body="json" in kwargs), **kwargs)
        except Exception as exc:  # class only: the message can carry request detail
            logger.warning("%s %s request error: %s", self._log_prefix, what, type(exc).__name__)
            raise InstantlySendError(f"Instantly {what} request error ({type(exc).__name__})") from None
        if response.status_code >= 400:
            logger.warning("%s %s failed: HTTP %s", self._log_prefix, what, response.status_code)
            raise InstantlySendError(f"Instantly {what} failed: HTTP {response.status_code}")
        try:
            return response.json() or {}
        except ValueError:
            return {}

    def _refuse_if_halted(self, what: str) -> None:
        if is_globally_halted():
            logger.warning("%s %s refused: lending emergency halt is on", self._log_prefix, what)
            raise InstantlySendError("lending emergency halt is on; nothing sent")

    def footer_body(self, body: str, recipient: str) -> str:
        """The approved text as the passthrough campaign's HTML, with NDL's CAN-SPAM footer and unsubscribe link."""
        from src.services.relay.channels_email import build_passthrough_body

        venture = SimpleNamespace(brand_name=self._account.footer_brand,
                                  postal_address=self._account.footer_postal_address,
                                  outbound_contact_phone=self._account.footer_phone, outbound_disclaimer=None)
        return build_passthrough_body(body, recipient, venture)

    def _remove_previous_lead(self, email: str) -> None:
        data = self._call("lead lookup", "POST", "/api/v2/leads/list",
                          json={"campaign": self._account.campaign_id, "search": email, "limit": 50})
        for lead in data.get("items") or data.get("leads") or []:
            if (lead.get("email") or "").strip().lower() == email and lead.get("id"):
                self._call("previous lead removal", "DELETE", f"/api/v2/leads/{lead['id']}")
                logger.info("%s removed the previous lead %s before a repeat send", self._log_prefix, lead["id"])

    def send(self, to_email: str, subject: str, body: str) -> str:
        """Hand one approved email to Instantly; returns the Instantly lead id (or ``instantly:<campaign>``)."""
        self._refuse_if_halted("send")
        email = to_email.strip().lower()
        self._remove_previous_lead(email)
        self._refuse_if_halted("send")  # again: a halt can be set while the lookup runs
        result = self._call("send", "POST", "/api/v2/leads/add", json={
            "campaign_id": self._account.campaign_id,
            "leads": [{"email": email, "custom_variables": {"ra_subject": subject,
                                                            "ra_body": self.footer_body(body, email)}}],
        })
        if result.get("duplicated_leads") or result.get("leads_skipped"):
            raise InstantlySendError("Instantly skipped the email: the address is already in the NDL campaign")
        if not (result.get("leads_uploaded") or result.get("leads_created")):
            raise InstantlySendError("Instantly reported no lead created")
        created = result.get("created_leads") or []
        lead_id = created[0].get("id") if created and isinstance(created[0], dict) else None
        logger.info("%s email handed to Instantly campaign %s", self._log_prefix, self._account.campaign_id)
        return str(lead_id) if lead_id else f"instantly:{self._account.campaign_id}"

    def _campaign_action(self, action: str) -> bool:
        try:
            self._call(f"campaign {action}", "POST", f"/api/v2/campaigns/{self._account.campaign_id}/{action}")
        except InstantlySendError:
            return False
        logger.info("%s campaign %s: %s", self._log_prefix, self._account.campaign_id, action)
        return True

    def pause_campaign(self) -> bool:
        return self._campaign_action("pause")

    def resume_campaign(self) -> bool:
        return self._campaign_action("activate")


def get_ndl_instantly_sender() -> Optional[NdlInstantlySender]:
    account = ndl_instantly()
    return NdlInstantlySender(account) if account is not None else None


def setup_campaign(api_key: str, base_url: str, sender_email: str, request: Optional[Request] = None) -> str:
    """Find or create the "NDL Cora Passthrough" campaign, make sure it is active, and return its id."""
    sender = NdlInstantlySender(NdlInstantly(api_key, base_url.rstrip("/"), "", sender_email, "", "", ""), request)
    listed = sender._call("campaign list", "GET", "/api/v2/campaigns")
    campaigns = listed if isinstance(listed, list) else listed.get("items") or listed.get("data") or []
    existing = next((c for c in campaigns if c.get("name") == CAMPAIGN_NAME), None)
    if existing:
        # A run that died between create and activate left it inactive, and an inactive campaign sends nothing.
        sender._call("campaign activate", "POST", f"/api/v2/campaigns/{existing['id']}/activate")
        return str(existing["id"])
    created = sender._call("campaign create", "POST", "/api/v2/campaigns", json={
        "name": CAMPAIGN_NAME,
        "campaign_schedule": {"schedules": [{
            "name": "ndl-8-to-8", "timing": {"from": SCHEDULE_FROM, "to": SCHEDULE_TO},
            "days": {str(day): True for day in range(7)}, "timezone": SCHEDULE_TIMEZONE}]},
        "sequences": [{"steps": [{"type": "email", "delay": 0,
                                  "variants": [{"subject": "{{ra_subject}}", "body": "{{ra_body}}"}]}]}],
        "email_list": [sender_email],
    })
    campaign_id = str(created["id"])
    sender._call("campaign activate", "POST", f"/api/v2/campaigns/{campaign_id}/activate")
    return campaign_id


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="NDL Instantly cold-email channel")
    parser.add_argument("--setup-campaign", action="store_true", help="find or create the NDL passthrough campaign")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    if not args.setup_campaign:
        parser.print_help()
        return 2
    s = get_settings()
    if not (s.lending_instantly_api_key and s.lending_instantly_sender_email):
        logger.error("set LENDING_INSTANTLY_API_KEY and LENDING_INSTANTLY_SENDER_EMAIL first")
        return 2
    campaign_id = setup_campaign(s.lending_instantly_api_key.get_secret_value(), s.lending_instantly_base_url,
                                 s.lending_instantly_sender_email)
    logger.info("Set this in .env, then restart fa-lending-cora:\n  LENDING_INSTANTLY_CAMPAIGN_ID=%s", campaign_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
