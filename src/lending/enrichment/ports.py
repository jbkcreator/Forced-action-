"""Ports for the card's external lookups and the deal thread, each with a Fake and a Live side.

Nothing here sends to a borrower. The deal thread is internal (default approved 2026-10-09: a Slack
thread in ``LENDING_DIAL_TASKS_CHANNEL``; Josh has not named the location). Street View has no live
key yet, so the production default is the unavailable port until ``GOOGLE_MAPS_API_KEY`` is set.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional, Protocol
from urllib.parse import quote

from sqlalchemy.orm import Session

from config.lending_enrichment import COMPS_AFTER_REPAIR_CONDITION, COMPS_SHOWN
from config.settings import get_settings
from src.utils.http_helpers import requests_get_with_retry

logger = logging.getLogger(__name__)

STREETVIEW_METADATA_URL = "https://maps.googleapis.com/maps/api/streetview/metadata"
STREETVIEW_LINK = "https://www.google.com/maps/@?api=1&map_action=pano&pano={pano_id}"


# --------------------------------------------------------------------------- Street View

class StreetViewPort(Protocol):
    def link(self, address: str) -> Optional[str]:
        """A keyless link to the address's Street View panorama, or None when there is none."""


class UnavailableStreetView:
    def link(self, address: str) -> Optional[str]:
        return None


class FakeStreetView:
    def __init__(self, links: Optional[dict[str, str]] = None, error: Optional[Exception] = None) -> None:
        self._links, self._error, self.calls = dict(links or {}), error, []

    def link(self, address: str) -> Optional[str]:
        self.calls.append(address)
        if self._error:
            raise self._error
        return self._links.get(address)


class GoogleStreetView:
    """Street View Static API metadata lookup. The returned link carries the pano id, never the key."""

    def __init__(self, api_key: str, timeout: int = 8) -> None:
        self._key, self._timeout = api_key, timeout

    def link(self, address: str) -> Optional[str]:
        try:
            response = requests_get_with_retry(
                STREETVIEW_METADATA_URL, max_retries=2, retry_delay=1,
                params={"location": address, "key": self._key}, timeout=self._timeout,
            )
            body = response.json()
        except Exception as exc:  # class only: the request carries the borrower's address and our key
            logger.error("[enrichment] street view lookup failed: %s", type(exc).__name__)
            return None
        if body.get("status") != "OK" or not body.get("pano_id"):
            return None
        return STREETVIEW_LINK.format(pano_id=quote(str(body["pano_id"]), safe=""))


def default_street_view() -> StreetViewPort:
    key = get_settings().google_maps_api_key
    return GoogleStreetView(key.get_secret_value()) if key else UnavailableStreetView()


# --------------------------------------------------------------------------- comps

@dataclass(frozen=True)
class CompsView:
    available: bool
    reason: Optional[str] = None
    low: Optional[Decimal] = None
    point: Optional[Decimal] = None
    high: Optional[Decimal] = None
    confidence: Optional[str] = None
    comp_count: int = 0
    comps: list[dict] = field(default_factory=list)

    def to_json(self) -> dict:
        return {
            "available": self.available, "reason": self.reason, "low": str(self.low) if self.low is not None else None,
            "point": str(self.point) if self.point is not None else None,
            "high": str(self.high) if self.high is not None else None, "confidence": self.confidence,
            "comp_count": self.comp_count, "comps": self.comps,
        }


class CompsPort(Protocol):
    def comps(self, db: Session, property_id: int) -> CompsView: ...


class FakeComps:
    def __init__(self, view: Optional[CompsView] = None) -> None:
        self._view, self.calls = view or CompsView(available=False, reason="fake"), []

    def comps(self, db: Session, property_id: int) -> CompsView:
        self.calls.append(property_id)
        return self._view


class ForcedActionComps:
    """Forced Action comps from the WP-8B ARV engine (DOR qualified sales). Internal estimate only."""

    def comps(self, db: Session, property_id: int) -> CompsView:
        from src.services.quote_ready.arv_repository import compute_arv_for_property

        now = datetime.now(timezone.utc)
        try:
            result = compute_arv_for_property(
                db, subject_property_id=property_id, as_of_yr=now.year, as_of_mo=now.month,
                after_repair_condition=COMPS_AFTER_REPAIR_CONDITION,
            )
        except Exception as exc:
            logger.error("[enrichment] comps failed property=%s: %s", property_id, type(exc).__name__)
            return CompsView(available=False, reason="comps_error")
        if result.arv_unknown:
            return CompsView(available=False, reason=result.unknown_reason or "arv_unknown")
        shown = [
            {"sale_price": str(c.sale_price), "sale": f"{c.sale_yr}-{c.sale_mo:02d}", "sqft": c.sqft}
            for c in result.selected_comps[:COMPS_SHOWN]
        ]
        return CompsView(True, None, result.low, result.point, result.high,
                         str(result.confidence) if result.confidence else None, result.comp_count, shown)


# --------------------------------------------------------------------------- deal thread

class DealThreadPort(Protocol):
    def post(self, text: str, *, thread_ts: Optional[str] = None) -> Optional[str]:
        """Post to the lead's deal thread (a new thread when ``thread_ts`` is None). Returns the thread id, or None."""


class UnavailableDealThread:
    def post(self, text: str, *, thread_ts: Optional[str] = None) -> Optional[str]:
        return None


class FakeDealThread:
    def __init__(self, error: Optional[Exception] = None) -> None:
        self.posts: list[tuple[Optional[str], str]] = []
        self._error = error

    def post(self, text: str, *, thread_ts: Optional[str] = None) -> Optional[str]:
        if self._error:
            raise self._error
        self.posts.append((thread_ts, text))
        return thread_ts or f"fake-thread-{len(self.posts)}"


class SlackDealThread:
    def __init__(self, client: Any, channel: str) -> None:
        self._client, self._channel = client, channel

    def post(self, text: str, *, thread_ts: Optional[str] = None) -> Optional[str]:
        kwargs: dict[str, Any] = {"channel": self._channel, "text": text}
        if thread_ts:
            kwargs["thread_ts"] = thread_ts
        try:
            response = self._client.chat_postMessage(**kwargs)
        except Exception as exc:
            logger.error("[enrichment] deal thread post failed: %s", type(exc).__name__)
            return None
        return thread_ts or response.get("ts")


def default_deal_thread() -> DealThreadPort:
    settings = get_settings()
    if not (settings.lending_slack_bot_token and settings.lending_dial_tasks_channel):
        return UnavailableDealThread()
    from src.lending.disposition_delivery import _slack_client

    return SlackDealThread(_slack_client(), settings.lending_dial_tasks_channel)
