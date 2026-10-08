"""GHL (GoHighLevel) calendar behind the CalendarClient Protocol.

Per the client's confirmed answer to Q23 ("GHL calendar synced to my Google
Calendar"), GHL is the calendar of record for this venture — bookings are
created here, and GHL's own two-way Google Calendar sync (configured by the
client inside GHL, connecting his Google login to this calendar) reflects
them onto Google and blocks GHL slots from his existing Google busy times.
This client therefore never talks to Google directly; it only talks to GHL.

GHL field names and exact response shapes below follow the public v2 API
reference (https://highlevel.stoplight.io/docs/integrations/) but are NOT
yet confirmed against a live round-trip call the way BatchDialer's contact
shape was confirmed during the Wave 0 compliance work. Treat this as the
documented, best-effort shape — run one real booking before trusting it in
production, and correct field names here if the live call disagrees.

Auth uses LENDING_GHL_API_KEY / LENDING_GHL_LOCATION_ID — Next Deal
Lending's own GHL sub-account, not the generic GHL_API_KEY / GHL_LOCATION_ID
src/services/ghl_webhook.py uses for Bay Street Capital's contact/pipeline
pushes. LENDING_GHL_CALENDAR_ID is a separate setting from
GHL_AP_PRO_CALENDAR_ID, which is an unrelated calendar for a different
venture.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Optional

import requests
from requests.exceptions import ConnectionError, ConnectTimeout, RequestException, Timeout

from src.services.calendar.availability import BusyBlock
from src.services.calendar.client import CalendarEvent, CalendarOutcomeUnknown, CalendarUnavailable

logger = logging.getLogger(__name__)

_GHL_BASE = "https://services.leadconnectorhq.com"
_GHL_API_VERSION = "2021-04-15"  # calendars/appointments endpoints use this version
_DEFAULT_TIMEOUT = 15


class GHLCalendarClient:
    """Reads and writes one GHL calendar's appointments.

    The requests session is injected rather than built here so the request
    and response shapes can be exercised without real credentials, the same
    pattern GoogleCalendarClient uses.
    """

    def __init__(self, *, api_key: str, location_id: str) -> None:
        self._api_key = api_key
        self._location_id = location_id

    @classmethod
    def from_settings(cls) -> "GHLCalendarClient":
        """Uses LENDING_GHL_API_KEY/LENDING_GHL_LOCATION_ID — Next Deal
        Lending's own GHL sub-account, shared with WP-GL-10's confirmation/
        reminder texts (same sub-account, one credential pair). NOT the
        generic GHL_API_KEY/GHL_LOCATION_ID — those are Bay Street Capital's
        and already depended on elsewhere (ghl_webhook.py's lead push);
        pointing those at a different sub-account would silently break Bay
        Street's existing GHL usage.
        """
        from config.settings import get_settings

        settings = get_settings()
        if settings.lending_ghl_api_key is None:
            raise ValueError("LENDING_GHL_API_KEY is not set")
        if not settings.lending_ghl_location_id:
            raise ValueError("LENDING_GHL_LOCATION_ID is not set")
        logger.info(
            "calendar.ghl: authenticated for location %s", settings.lending_ghl_location_id
        )
        return cls(
            api_key=settings.lending_ghl_api_key.get_secret_value(),
            location_id=settings.lending_ghl_location_id,
        )

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self._api_key}",
            "Version": _GHL_API_VERSION,
            "Content-Type": "application/json",
        }

    def _request(self, method: str, path: str, *, idempotent: bool = True, **kwargs) -> requests.Response:
        """Same retry-on-429 pattern as src/services/ghl_webhook.py::_ghl_request.

        Never hangs indefinitely and never silently swallows a failure — a
        calendar call that cannot be confirmed must raise, not report free.
        """
        kwargs.setdefault("timeout", _DEFAULT_TIMEOUT)
        url = f"{_GHL_BASE}{path}"
        last_exc: Optional[Exception] = None
        resp: Optional[requests.Response] = None

        for attempt in range(4):
            try:
                resp = requests.request(method, url, headers=self._headers(), **kwargs)
            except (ConnectionError, Timeout) as exc:
                if not idempotent and not isinstance(exc, ConnectTimeout):
                    logger.error("calendar.ghl: %s %s sent but no answer — not retrying", method, path)
                    raise CalendarOutcomeUnknown(
                        f"GHL {method} {path} outcome unknown after a network error"
                    ) from exc
                last_exc = exc
                wait = 2**attempt
                logger.warning(
                    "calendar.ghl: network error on %s %s (attempt %d/4) — retrying in %ds",
                    method, path, attempt + 1, wait,
                )
                time.sleep(wait)
                continue

            if resp.status_code != 429:
                return resp

            wait = 2**attempt
            logger.debug("calendar.ghl: 429 rate limit — retrying in %ds (attempt %d/4)", wait, attempt + 1)
            time.sleep(wait)

        if last_exc:
            raise CalendarUnavailable(
                f"GHL calendar request failed after 4 attempts: {method} {path}"
            ) from last_exc
        return resp

    # ── reads ────────────────────────────────────────────────────────────

    def get_busy(
        self, *, calendar_id: str, start: datetime, end: datetime
    ) -> list[BusyBlock]:
        """Committed spans from this GHL calendar's booked events.

        GHL's own Google sync means Google's busy times already show up as
        GHL events here, so this one call reflects both calendars — no
        separate Google freebusy read is needed once mode is "ghl".
        """
        resp = self._request(
            "GET",
            "/calendars/events",
            params={
                "locationId": self._location_id,
                "calendarId": calendar_id,
                "startTime": _to_epoch_ms(start),
                "endTime": _to_epoch_ms(end),
            },
        )
        if not resp.ok:
            raise CalendarUnavailable(
                f"GHL calendar/events failed for {calendar_id!r}: "
                f"HTTP {resp.status_code} {resp.text[:300]}"
            )

        payload = resp.json()
        events = payload.get("events", [])
        return [
            BusyBlock(
                start=_parse_timestamp(ev["startTime"]),
                end=_parse_timestamp(ev["endTime"]),
            )
            for ev in events
            if ev.get("appointmentStatus") not in ("cancelled", "showed_cancelled")
        ]

    def get_event(self, *, calendar_id: str, event_id: str) -> Optional[CalendarEvent]:
        resp = self._request("GET", f"/calendars/events/appointments/{event_id}")
        if resp.status_code == 404:
            return None
        if not resp.ok:
            raise CalendarUnavailable(
                f"GHL get appointment failed for {event_id!r}: "
                f"HTTP {resp.status_code} {resp.text[:300]}"
            )
        return self._to_event(resp.json().get("appointment") or resp.json())

    # ── writes ───────────────────────────────────────────────────────────

    def create_event(
        self,
        *,
        calendar_id: str,
        start: datetime,
        end: datetime,
        summary: str,
        attendee_email: str,
        description: str = "",
    ) -> CalendarEvent:
        resp = self._request(
            "POST",
            "/calendars/events/appointments",
            idempotent=False,
            json={
                "locationId": self._location_id,
                "calendarId": calendar_id,
                "title": summary,
                "startTime": start.isoformat(),
                "endTime": end.isoformat(),
                "appointmentStatus": "confirmed",
                # GHL resolves/creates the contact by email on this endpoint
                # per the v2 docs; confirm against a live call before trusting
                # this creates vs. requires an existing contactId.
                "contactEmail": attendee_email,
                "notes": description,
            },
        )
        if not resp.ok:
            raise CalendarUnavailable(
                f"GHL create appointment failed: HTTP {resp.status_code} {resp.text[:300]}"
            )
        raw = resp.json()
        return self._to_event(raw.get("appointment") or raw)

    def cancel_event(self, *, calendar_id: str, event_id: str) -> None:
        resp = self._request(
            "PUT",
            f"/calendars/events/appointments/{event_id}",
            json={"appointmentStatus": "cancelled"},
        )
        if resp.status_code == 404:
            logger.info("calendar.ghl: appointment %s already absent on cancel", event_id)
            return
        if not resp.ok:
            raise CalendarUnavailable(
                f"GHL cancel appointment failed for {event_id!r}: "
                f"HTTP {resp.status_code} {resp.text[:300]}"
            )

    # ── shaping ──────────────────────────────────────────────────────────

    def _to_event(self, raw: dict) -> CalendarEvent:
        return CalendarEvent(
            event_id=raw["id"],
            start=_parse_timestamp(raw["startTime"]),
            end=_parse_timestamp(raw["endTime"]),
            summary=raw.get("title", ""),
            attendee_email=raw.get("contactEmail") or raw.get("email"),
            html_link=None,
            status="cancelled" if raw.get("appointmentStatus") == "cancelled" else "confirmed",
        )


def _to_epoch_ms(value: datetime) -> int:
    if value.tzinfo is None:
        raise ValueError(f"calendar query requires an aware datetime, got {value!r}")
    return int(value.astimezone(timezone.utc).timestamp() * 1000)


def _parse_timestamp(value: Any) -> datetime:
    """GHL returns epoch milliseconds or an ISO string depending on endpoint;
    accept either rather than guessing one and breaking on the other."""
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / 1000, tz=timezone.utc)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"GHL calendar returned a timestamp with no offset: {value!r}")
    return parsed
