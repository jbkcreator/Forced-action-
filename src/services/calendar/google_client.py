"""Google Calendar behind the CalendarClient Protocol.

Availability is read through freebusy rather than by listing events: it
returns only the spans the client is committed for, so meeting titles,
attendees and descriptions never enter this system. Google also applies the
rules we would otherwise have to reimplement — an event marked "free" does
not appear, nor does one the client declined, and an all-day event occupies
the whole day.

Authentication reuses the existing service account with domain-wide
delegation, impersonating the calendar's owner. The same pattern as
src/agents/cora/ingestion/reply_mailbox_poller.py, with calendar scopes.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, time, timedelta
from typing import Any, Optional
from zoneinfo import ZoneInfo

from config.calendar import CALENDAR_TIMEZONE
from src.services.calendar.availability import BusyBlock
from src.services.calendar.client import CalendarEvent, CalendarUnavailable

logger = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/calendar.freebusy",
    "https://www.googleapis.com/auth/calendar.events",
]

# Google refuses a freebusy window wider than three months. Our booking
# horizon is far shorter, so this only guards a caller passing something odd.
MAX_FREEBUSY_WINDOW = timedelta(days=90)


class GoogleCalendarClient:
    """Reads and writes one Google calendar.

    The service object is injected rather than built here so the request and
    response shapes can be exercised without credentials.
    """

    def __init__(self, *, service: Any, timezone_name: str = CALENDAR_TIMEZONE) -> None:
        self._service = service
        self._timezone_name = timezone_name
        self._tz = ZoneInfo(timezone_name)

    @classmethod
    def from_settings(cls) -> "GoogleCalendarClient":
        from google.oauth2 import service_account
        from googleapiclient.discovery import build

        from config.settings import get_settings

        settings = get_settings()
        key_path = settings.cora_gmail_service_account_key_path
        subject = settings.fa_max_calendar_subject
        if not key_path:
            raise ValueError("CORA_GMAIL_SERVICE_ACCOUNT_KEY_PATH is not set")
        if not subject:
            raise ValueError("FA_MAX_CALENDAR_SUBJECT is not set")

        credentials = service_account.Credentials.from_service_account_file(
            key_path, scopes=SCOPES
        ).with_subject(subject)
        service = build("calendar", "v3", credentials=credentials, cache_discovery=False)
        logger.info("calendar.google: authenticated as %s", subject)
        return cls(service=service)

    # ── reads ────────────────────────────────────────────────────────────

    def get_busy(
        self, *, calendar_id: str, start: datetime, end: datetime
    ) -> list[BusyBlock]:
        if end - start > MAX_FREEBUSY_WINDOW:
            raise ValueError(
                f"freebusy window of {end - start} exceeds Google's {MAX_FREEBUSY_WINDOW} limit"
            )

        response = self._service.freebusy().query(
            body={
                "timeMin": start.isoformat(),
                "timeMax": end.isoformat(),
                "items": [{"id": calendar_id}],
            }
        ).execute()

        payload = self._single_calendar(response, calendar_id)
        errors = payload.get("errors")
        if errors:
            raise CalendarUnavailable(
                f"freebusy returned errors for {calendar_id!r}: {errors}"
            )

        return [
            BusyBlock(start=_parse_timestamp(span["start"]), end=_parse_timestamp(span["end"]))
            for span in payload.get("busy", [])
        ]

    @staticmethod
    def _single_calendar(response: dict, calendar_id: str) -> dict:
        """Pull this calendar's entry out of a freebusy response.

        Google usually echoes the id that was asked for, but an alias such as
        "primary" can come back resolved to the owner's address. When exactly
        one calendar was requested, the sole entry is unambiguously it.
        """
        calendars = response.get("calendars") or {}
        if calendar_id in calendars:
            return calendars[calendar_id]
        if len(calendars) == 1:
            return next(iter(calendars.values()))
        raise CalendarUnavailable(
            f"freebusy response contained no entry for {calendar_id!r} "
            f"(got {sorted(calendars)})"
        )

    def get_event(self, *, calendar_id: str, event_id: str) -> Optional[CalendarEvent]:
        from googleapiclient.errors import HttpError

        try:
            raw = self._service.events().get(
                calendarId=calendar_id, eventId=event_id
            ).execute()
        except HttpError as exc:
            if exc.resp.status == 404:
                return None
            raise
        return self._to_event(raw)

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
        raw = self._service.events().insert(
            calendarId=calendar_id,
            body={
                "summary": summary,
                "description": description,
                "start": {"dateTime": start.isoformat(), "timeZone": self._timezone_name},
                "end": {"dateTime": end.isoformat(), "timeZone": self._timezone_name},
                "attendees": [{"email": attendee_email}],
            },
            # The invitation is the point of the booking: without this Google
            # creates the event silently and the borrower is never told.
            sendUpdates="all",
        ).execute()
        return self._to_event(raw)

    def cancel_event(self, *, calendar_id: str, event_id: str) -> None:
        from googleapiclient.errors import HttpError

        try:
            self._service.events().delete(
                calendarId=calendar_id, eventId=event_id, sendUpdates="all",
            ).execute()
        except HttpError as exc:
            # 404 is already gone and 410 is already cancelled; both are the
            # state the caller asked for.
            if exc.resp.status in (404, 410):
                logger.info(
                    "calendar.google: event %s already absent on cancel", event_id
                )
                return
            raise

    # ── shaping ──────────────────────────────────────────────────────────

    def _to_event(self, raw: dict) -> CalendarEvent:
        attendees = raw.get("attendees") or []
        return CalendarEvent(
            event_id=raw["id"],
            start=self._parse_endpoint(raw["start"]),
            end=self._parse_endpoint(raw["end"]),
            summary=raw.get("summary", ""),
            attendee_email=attendees[0].get("email") if attendees else None,
            html_link=raw.get("htmlLink"),
            status="cancelled" if raw.get("status") == "cancelled" else "confirmed",
        )

    def _parse_endpoint(self, endpoint: dict) -> datetime:
        """Read an event boundary, which is timed or all-day but never both."""
        if "dateTime" in endpoint:
            return _parse_timestamp(endpoint["dateTime"])
        return datetime.combine(
            date.fromisoformat(endpoint["date"]),
            time(0, 0),
            tzinfo=ZoneInfo(endpoint.get("timeZone") or self._timezone_name),
        )


def _parse_timestamp(value: str) -> datetime:
    """RFC-3339 to an aware datetime."""
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError(f"calendar returned a timestamp with no offset: {value!r}")
    return parsed
