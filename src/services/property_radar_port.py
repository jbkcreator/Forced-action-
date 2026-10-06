"""PropertyRadar API adapter — Protocol, Fake, and Live implementations.

All business logic (campaign criteria, normalisation, budget guard, dedup)
lives in callers; this module only knows how to talk to the API.

API billing:
  Purchase=0  — count-only; returns totalResultCount, consumes zero exports.
  Purchase=1  — paged export; each returned record costs 1 export credit
                (Solo plan: 10,000/month). Always call count() first and
                check remaining_allowance() before any purchase call.

Live implementation reuses requests_get_with_retry conventions but sends
POST requests; retry logic mirrors the team pack's pr_client.py findings
(RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}).
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Iterator, Optional, Protocol

import requests

from config.settings import settings

logger = logging.getLogger(__name__)

_RETRYABLE = {429, 500, 502, 503, 504}
_BASE_URL = "https://api.propertyradar.com"
_EXPORT_FIELDS = [
    "RadarID",
    "APN",
    "County",
    "State",
    "Address",
    "City",
    "ZipFive",
    "PType",
    "Owner",
    "OwnershipType",
    "OwnerAddress",
    "OwnerCity",
    "OwnerState",
    "OwnerZipFive",
    "isSameMailing",
    "FirstLenderOriginal",
    "FirstDate",
    "FirstAmount",
    "FirstTermInYears",
    "Persons",
    "PhoneAvailability",
    "EmailAvailability",
    "isListedForSale",
    "inForeclosure",
    "AVM",
    "EquityPercent",
]
_PAGE_SIZE = 500


# ---------------------------------------------------------------------------
# Shared types
# ---------------------------------------------------------------------------

@dataclass
class PropertyRadarRecord:
    """Raw record as returned by the API — no normalisation applied here."""
    radar_id: str
    raw: dict


@dataclass
class AllowanceInfo:
    quantity_free_remaining: int
    quantity_purchased_remaining: int
    # False when the real balance could not be observed (see LivePropertyRadarPort.allowance()
    # docstring — PropertyRadar has no free pre-purchase quota endpoint). Callers must not
    # treat an unverified total_remaining as a real number; it exists only so the dataclass
    # shape is uniform between Fake (always verified) and Live (never verified pre-purchase).
    verified: bool = True

    @property
    def total_remaining(self) -> int:
        return self.quantity_free_remaining + self.quantity_purchased_remaining


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------

class PropertyRadarPort(Protocol):
    def count(self, criteria: list[dict]) -> int:
        """Return the total matching count using Purchase=0 (free, never billed)."""
        ...

    def allowance(self) -> AllowanceInfo:
        """Return the current monthly export allowance."""
        ...

    def purchase(
        self, criteria: list[dict], max_records: Optional[int] = None
    ) -> Iterator[PropertyRadarRecord]:
        """Yield matching records, paged, at most ``max_records`` when given.
        Each record costs 1 export credit."""
        ...


# ---------------------------------------------------------------------------
# Fake — for tests and local dev
# ---------------------------------------------------------------------------

@dataclass
class FakePropertyRadarPort:
    """Deterministic test double; no network calls.

    Populate ``canned_records`` before calling ``purchase()``; the fake
    returns exactly those records in insertion order.  ``count()`` returns
    ``len(canned_records)``.  ``allowance()`` returns ``canned_allowance``.

    ``purchase_calls`` records every list of criteria passed to ``purchase()``
    so tests can assert the right campaign criteria were sent.
    """

    canned_records: list[dict] = field(default_factory=list)
    canned_allowance: AllowanceInfo = field(
        default_factory=lambda: AllowanceInfo(
            quantity_free_remaining=10000,
            quantity_purchased_remaining=0,
        )
    )
    purchase_calls: list[list[dict]] = field(default_factory=list)
    count_calls: list[list[dict]] = field(default_factory=list)

    def count(self, criteria: list[dict]) -> int:
        self.count_calls.append(criteria)
        return len(self.canned_records)

    def allowance(self) -> AllowanceInfo:
        return self.canned_allowance

    def purchase(
        self, criteria: list[dict], max_records: Optional[int] = None
    ) -> Iterator[PropertyRadarRecord]:
        self.purchase_calls.append(criteria)
        for raw in self.canned_records[:max_records]:
            yield PropertyRadarRecord(radar_id=raw["RadarID"], raw=raw)


# ---------------------------------------------------------------------------
# Live — production
# ---------------------------------------------------------------------------

class LivePropertyRadarPort:
    """Real PropertyRadar API client.

    Instantiated by get_property_radar_port() when property_radar_mode="live".
    The API key is pulled from settings at construction time; it is never
    stored as a plain string.
    """

    def __init__(self) -> None:
        if not settings.property_radar_api_key:
            raise RuntimeError(
                "PROPERTY_RADAR_API_KEY is not set — cannot instantiate LivePropertyRadarPort"
            )
        key = settings.property_radar_api_key.get_secret_value()
        self._session = requests.Session()
        self._session.headers.update(
            {"Authorization": f"Bearer {key}", "Accept": "application/json"}
        )

    def _post(self, params: dict, body: dict, max_retries: int = 4) -> dict:
        for attempt in range(max_retries):
            resp = self._session.post(
                f"{_BASE_URL}/v1/properties",
                params=params,
                json=body,
                timeout=90,
            )
            if resp.status_code not in _RETRYABLE:
                break
            wait = int(resp.headers.get("Retry-After", 3 * (attempt + 1)))
            logger.warning(
                "PropertyRadar HTTP %s on attempt %d/%d — waiting %ds",
                resp.status_code, attempt + 1, max_retries, wait,
            )
            time.sleep(wait)
        resp.raise_for_status()
        return resp.json()

    def count(self, criteria: list[dict]) -> int:
        body = self._post(
            params={"Purchase": 0, "Limit": 1, "Fields": "RadarID"},
            body={"Criteria": criteria},
        )
        return int(body.get("totalResultCount") or body.get("resultCount") or 0)

    def allowance(self) -> AllowanceInfo:
        """Return an UNVERIFIED placeholder — PropertyRadar has no free
        pre-purchase quota/allowance endpoint.

        Confirmed empirically (2026-09-28) against the real API: there is no
        quota/allowance/usage path anywhere in the OpenAPI spec, and a
        Purchase=0 count() response does not include quantityFreeRemaining
        either (the team pack's docs describe that field appearing in
        Purchase=1 purchase responses, which by definition already means
        credits were spent to observe it). There is therefore no way to check
        the real remaining balance before spending anything.

        Callers must not compare total_remaining against a real budget here —
        _check_budget() checks AllowanceInfo.verified and skips the
        over-allowance comparison when False, relying on PROPERTY_RADAR_PER_RUN_CAP
        as the only pre-flight guard, plus PropertyRadar's own mid-purchase
        error (which propagates as an HTTPError and fails the run cleanly)
        as the real backstop against running out of credits.
        """
        return AllowanceInfo(
            quantity_free_remaining=0,
            quantity_purchased_remaining=0,
            verified=False,
        )

    def purchase(
        self, criteria: list[dict], max_records: Optional[int] = None
    ) -> Iterator[PropertyRadarRecord]:
        # Every returned record is billed, so a cap must shrink the page
        # request itself rather than stop reading a page already bought.
        start = 0
        while True:
            limit = _PAGE_SIZE if max_records is None else min(_PAGE_SIZE, max_records - start)
            if limit <= 0:
                break
            body = self._post(
                params={
                    "Purchase": 1,
                    "Limit": limit,
                    "Start": start,
                    "Fields": ",".join(_EXPORT_FIELDS),
                },
                body={"Criteria": criteria},
            )
            records = body.get("results", [])
            if not records:
                break
            for raw in records:
                radar_id = raw.get("RadarID")
                if not radar_id:
                    logger.warning("PropertyRadar record missing RadarID — skipped: %r", raw)
                    continue
                yield PropertyRadarRecord(radar_id=radar_id, raw=raw)
            start += len(records)
            if start >= int(body.get("totalResultCount", 0)):
                break


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def get_property_radar_port() -> PropertyRadarPort:
    """Return the correct port implementation based on settings.

    "fake" (default) — safe for tests, CI, and local dev; no network calls.
    "live"           — requires PROPERTY_RADAR_API_KEY and
                       PROPERTY_RADAR_ENABLED=true in env.
    """
    mode = settings.property_radar_mode
    if mode == "live":
        if not settings.property_radar_enabled:
            raise RuntimeError(
                "property_radar_mode=live but PROPERTY_RADAR_ENABLED is false — "
                "set PROPERTY_RADAR_ENABLED=true to allow live API calls"
            )
        return LivePropertyRadarPort()
    return FakePropertyRadarPort()
