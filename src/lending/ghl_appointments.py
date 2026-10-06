"""Release a GoHighLevel calendar slot after a failed caller check (WP-GL-10).

An AI-booked call holds a slot on Josh's calendar before any caller has run the check. When the check fails the
contact is moved to the Nurture stage (see ``booking_messages.handle_nurture_entry``) and Josh approved releasing the
slot ("approved as written"): this cancels the GHL appointment so the time is bookable again.

Cancelling a live appointment is switched off by default (``LENDING_GHL_RELEASE_SLOT_ENABLED=false``): turn it on
after the end-to-end test. A failure here never raises: the message and reminder handling has already been committed,
and the caller of this module reports the unreleased slot instead.
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Optional, Protocol

import requests

from config.settings import get_settings
from src.lending.ghl_sms import GhlAccount, ghl_headers, lending_ghl_account
from src.services import ghl_webhook

logger = logging.getLogger(__name__)

_TIMEOUT_SECONDS = 15
_VERSION = "2021-04-15"

Request = Callable[..., Any]


class AppointmentCanceller(Protocol):
    """The port: ``GhlAppointmentCanceller`` in production, ``FakeAppointmentCanceller`` in tests."""

    def __call__(self, appointment_id: str) -> bool: ...


class FakeAppointmentCanceller:
    """Records the appointment ids it was asked to cancel. Never touches the network."""

    def __init__(self, succeed: bool = True) -> None:
        self.cancelled: list[str] = []
        self._succeed = succeed

    def __call__(self, appointment_id: str) -> bool:
        self.cancelled.append(appointment_id)
        return self._succeed


class GhlAppointmentCanceller:
    def __init__(self, account: GhlAccount, request: Optional[Request] = None) -> None:
        self._account = account
        self._request = request or (lambda method, url, **kwargs: requests.request(method, url, timeout=_TIMEOUT_SECONDS, **kwargs))

    def __call__(self, appointment_id: str) -> bool:
        """True when GHL accepted the cancellation. One attempt, never raises, logs the status only."""
        url = f"{ghl_webhook._GHL_BASE}/calendars/events/appointments/{appointment_id}"
        try:
            response = self._request("PUT", url, headers=ghl_headers(self._account.api_key, _VERSION),
                                     json={"appointmentStatus": "cancelled"})
        except Exception as exc:  # class only: the message can carry request detail
            logger.error("[ghl-appointments] cancel request error for appointment %s (%s)", appointment_id, type(exc).__name__)
            return False
        if response.status_code >= 400:
            logger.error("[ghl-appointments] GHL refused to cancel appointment %s (HTTP %s)", appointment_id, response.status_code)
            return False
        logger.info("[ghl-appointments] released GHL slot for appointment %s", appointment_id)
        return True


def get_appointment_canceller() -> Optional[AppointmentCanceller]:
    """The live canceller when the flag is on and the lending GHL account is configured; None otherwise."""
    if not get_settings().lending_ghl_release_slot_enabled:
        return None
    account = lending_ghl_account()
    if account is None:
        logger.error("[ghl-appointments] slot release is on but the lending GHL account is not configured")
        return None
    return GhlAppointmentCanceller(account)
