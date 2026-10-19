"""Split T-11: parse FL wholesaler pulls into Partners pool records and tag them in GHL.

Scrubbing (National/State DNC via Tracerfy, internal suppression) and the dialer load
belong to ``run_dialer_load``; this module only builds the records and pushes the numbers
that load actually reached to GHL, tagged by source and state.
"""
from __future__ import annotations

import csv
import io
import logging
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Mapping, Optional, Protocol, Sequence

from config.lending_wholesalers import (
    GHL_TAG_SOURCE_PREFIX,
    GHL_TAG_STATE_PREFIX,
    GHL_TAG_WHOLESALER,
    REQUIRED_COLUMNS,
    WHOLESALER_SOURCE_TAGS,
    WHOLESALER_STATES,
)
from src.lending.ghl_account import GhlAccount, ghl_headers, lending_ghl_account
from src.lending.queues import assign_queue
from src.services.phone_utils import normalize as normalize_phone

logger = logging.getLogger(__name__)


@dataclass
class ParsedList:
    records: list[dict[str, Any]] = field(default_factory=list)
    dropped: Counter = field(default_factory=Counter)


def _cell(row: Mapping[str, Optional[str]], key: str) -> str:
    return (row.get(key) or "").strip()


def parse_wholesaler_csv(text: str) -> ParsedList:
    """FL rows from known sources with a valid phone, one record per phone."""
    reader = csv.DictReader(io.StringIO(text))
    columns = {c.strip().lower() for c in reader.fieldnames or []}
    missing = REQUIRED_COLUMNS - columns
    if missing:
        raise ValueError(f"wholesaler CSV is missing column(s): {', '.join(sorted(missing))}")
    parsed = ParsedList()
    seen: set[str] = set()
    for raw in reader:
        row = {(k or "").strip().lower(): v for k, v in raw.items()}
        source = _cell(row, "source").lower()
        tag = WHOLESALER_SOURCE_TAGS.get(source)
        state = _cell(row, "state").upper()
        phone = normalize_phone(_cell(row, "phone"))
        if tag is None:
            parsed.dropped["UNKNOWN_SOURCE"] += 1
        elif state not in WHOLESALER_STATES:
            parsed.dropped["NOT_FL"] += 1
        elif not phone:
            parsed.dropped["NO_PHONE"] += 1
        elif phone in seen:
            parsed.dropped["DUPLICATE"] += 1
        else:
            seen.add(phone)
            parsed.records.append(assign_queue({
                "source_record_ref": f"wholesaler:{source}:{phone}",
                "source_tag": tag,
                "wholesaler_source": source,
                "phone": phone,
                "state": state,
                "borrower_name": _cell(row, "name") or None,
                "entity_name": _cell(row, "entity_name") or None,
                "email": _cell(row, "email") or None,
            }))
    return parsed


def ghl_tags(record: Mapping[str, Any]) -> list[str]:
    return [GHL_TAG_WHOLESALER,
            f"{GHL_TAG_SOURCE_PREFIX}{record['wholesaler_source']}",
            f"{GHL_TAG_STATE_PREFIX}{str(record['state']).lower()}"]


class GhlTagSink(Protocol):
    def push(self, record: Mapping[str, Any], tags: list[str]) -> None: ...


@dataclass
class GhlPushResult:
    pushed: int = 0
    failed: list[str] = field(default_factory=list)


def push_to_ghl(records: Sequence[Mapping[str, Any]], sink: GhlTagSink) -> GhlPushResult:
    """Upsert + tag each record; a failure is logged (source only, no phone) and the rest still go."""
    result = GhlPushResult()
    for record in records:
        try:
            sink.push(record, ghl_tags(record))
        except Exception as exc:
            logger.warning("[wholesaler-ghl] push failed for a %s record: %s",
                           record.get("wholesaler_source"), type(exc).__name__)
            result.failed.append(str(record["source_record_ref"]))
        else:
            result.pushed += 1
    return result


class GhlWholesalerSink:
    """Next Deal Lending GHL sub-account: contact upsert (never tags), then add-tags."""

    def __init__(self, account: GhlAccount) -> None:
        self._account = account

    def _call(self, step: str, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        from src.services import ghl_webhook

        response = ghl_webhook._ghl_request(
            method, f"{ghl_webhook._GHL_BASE}{path}", headers=ghl_headers(self._account.api_key), **kwargs,
        )
        if response.status_code >= 400:
            raise RuntimeError(f"{step}: HTTP {response.status_code}")
        try:
            return response.json()
        except ValueError:
            return {}

    def push(self, record: Mapping[str, Any], tags: list[str]) -> None:
        first, _, last = str(record.get("borrower_name") or "").partition(" ")
        body: dict[str, Any] = {"locationId": self._account.location_id, "phone": record["phone"],
                                "firstName": first, "lastName": last.strip()}
        if record.get("entity_name"):
            body["companyName"] = record["entity_name"]
        if record.get("email"):
            body["email"] = record["email"]
        data = self._call("contact upsert", "POST", "/contacts/upsert", json=body)
        contact_id = (data.get("contact") or {}).get("id")
        if not contact_id:
            raise RuntimeError("contact upsert: no contact id in response")
        self._call("add tags", "POST", f"/contacts/{contact_id}/tags", json={"tags": tags})


def ghl_sink() -> Optional[GhlWholesalerSink]:
    account = lending_ghl_account()
    return GhlWholesalerSink(account) if account else None
