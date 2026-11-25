"""R1 1.3 D9: Clay lookups for morning-list items with a gap, through Clay Routines (works on the Launch plan).

Two routines, built in Clay's UI on the Forced Action workspace:
- Work email (``LENDING_CLAY_EMAIL_ROUTINE``): inputs ``full_name`` + ``company_name``; for an item with no email that
  has both a person and a company. Result: ``result["Work Email"]``.
- LLC owner (``LENDING_CLAY_LLC_ROUTINE``, off by default: 0 of 12 live matches on 2026-11-25): inputs ``llc_name``,
  ``state``, ``property_address``; for an item with a company and an address but no person. Kept only when Clay says
  ``ownership_verified``.

API (checked live 2026-11-25): ``POST {base}/routines/workflow:<id>/run`` with ``{"items": [{"id", "inputs"}]}`` ->
``{"routine_run_id"}``; ``GET {base}/routines/run/{run_id}/results`` -> ``{"status", "data": [{"id", "result"}]}``.

``enrich_gaps`` is one pass: collect results for pending runs, then submit new gaps. ``lending.clay_enrichments``
holds one row per (lead, field), so a lead is never paid for twice inside ``ttl_days``, and every row requested on
the current ET day counts toward ``daily_cap`` (0 = off). Any Clay failure is logged and the pass ends; the list
never waits for Clay.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta, timezone
from typing import Optional, Protocol, Sequence
from zoneinfo import ZoneInfo

import httpx
from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session

from src.lending.morning_list.items import ListItem

logger = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")
EMAIL = "email"
LLC_OWNER = "llc_owner"
DEFAULT_STATE = "FL"
_STATE = re.compile(r"\b([A-Z]{2})\s+\d{5}\b")

_PENDING = text("SELECT lead_ref, field, run_id FROM lending.clay_enrichments WHERE status = 'pending'")
_KNOWN = text("""
    SELECT lead_ref, field FROM lending.clay_enrichments WHERE lead_ref IN :refs AND requested_at >= :fresh
""").bindparams(bindparam("refs", expanding=True))
_TODAY = text("SELECT count(*) FROM lending.clay_enrichments WHERE requested_at >= :start")
_FORGET = text("DELETE FROM lending.clay_enrichments WHERE lead_ref = :ref AND field = :field")
_REQUEST = text("""
    INSERT INTO lending.clay_enrichments (lead_ref, field, run_id, status, requested_at)
    VALUES (:ref, :field, :run_id, 'pending', :now)
""")
_SETTLE = text("""
    UPDATE lending.clay_enrichments SET status = :status, value = :value, enriched_at = :now
     WHERE lead_ref = :ref AND field = :field AND status = 'pending'
""")


class ClayError(Exception):
    """Clay refused, timed out or answered in a shape we do not know."""


@dataclass(frozen=True)
class RunResults:
    complete: bool
    rows: dict[str, dict]  # item id -> that item's ``result``


class ClayPort(Protocol):
    def run(self, routine: str, items: list[dict]) -> str: ...
    def results(self, run_id: str) -> RunResults: ...


@dataclass(frozen=True)
class ClaySettings:
    email_routine: str
    llc_routine: str
    email_enabled: bool
    llc_enabled: bool
    daily_cap: int
    ttl_days: int


@dataclass
class ClayStats:
    submitted: int = 0
    settled: int = 0
    failed: bool = False


class LiveClayRoutines:
    """Clay's public API (``clay-api-key`` header). Never logs the key, an input or a result value."""

    def __init__(self, api_key: str, base: str, timeout: float = 20.0) -> None:
        self._headers = {"clay-api-key": api_key}
        self._base = base.rstrip("/")
        self._timeout = timeout

    def run(self, routine: str, items: list[dict]) -> str:
        try:
            response = httpx.post(f"{self._base}/routines/{routine}/run", headers=self._headers,
                                  json={"items": items}, timeout=self._timeout)
            response.raise_for_status()
            return str(response.json()["routine_run_id"])
        except (httpx.HTTPError, KeyError, ValueError) as exc:
            raise ClayError(f"run refused ({type(exc).__name__})") from exc

    def results(self, run_id: str) -> RunResults:
        try:
            response = httpx.get(f"{self._base}/routines/run/{run_id}/results", headers=self._headers,
                                 timeout=self._timeout)
            response.raise_for_status()
            body = response.json()
            rows = {str(row["id"]): row.get("result") or {} for row in body.get("data") or []
                    if row.get("status") == "complete"}
            return RunResults(complete=body.get("status") == "complete", rows=rows)
        except (httpx.HTTPError, KeyError, ValueError, AttributeError) as exc:
            raise ClayError(f"results refused ({type(exc).__name__})") from exc


@dataclass
class FakeClay:
    """Records what would be sent; ``answers`` maps an item id to its ``result`` (unknown ids get no match)."""
    answers: dict[str, dict] = field(default_factory=dict)
    fail: bool = False
    submitted: list[tuple[str, list[dict]]] = field(default_factory=list)
    _runs: dict[str, list[str]] = field(default_factory=dict)

    def run(self, routine: str, items: list[dict]) -> str:
        if self.fail:
            raise ClayError("fake failure")
        self.submitted.append((routine, items))
        run_id = f"run_{len(self._runs) + 1}"
        self._runs[run_id] = [item["id"] for item in items]
        return run_id

    def results(self, run_id: str) -> RunResults:
        if self.fail:
            raise ClayError("fake failure")
        ids = self._runs.get(run_id, [])
        return RunResults(complete=True, rows={i: self.answers.get(i, {"success": False}) for i in ids})


def _email_value(result: dict) -> Optional[str]:
    inner = result.get("result") if isinstance(result.get("result"), dict) else {}
    value = inner.get("Work Email") if inner else None
    return str(value).strip().lower() if value and "@" in str(value) else None


def _llc_value(result: dict) -> Optional[str]:
    outputs = result.get("structuredOutputs") if isinstance(result.get("structuredOutputs"), dict) else result
    if not outputs.get("ownership_verified"):
        return None
    candidates = outputs.get("contact_candidates") or []
    first = candidates[0] if candidates and isinstance(candidates[0], dict) else {}
    name = first.get("name") or first.get("full_name")
    return str(name).strip() if name else None


def _email_input(item: ListItem) -> Optional[dict]:
    if item.email or not item.name or not item.entity_name:
        return None
    return {"full_name": item.name, "company_name": item.entity_name}


def _llc_input(item: ListItem) -> Optional[dict]:
    if item.name or not item.entity_name or not item.property_address:
        return None
    found = _STATE.search(item.property_address.upper())
    return {"llc_name": item.entity_name, "state": found.group(1) if found else DEFAULT_STATE,
            "property_address": item.property_address}


def _collect(db: Session, port: ClayPort, now: datetime, stats: ClayStats) -> None:
    by_run: dict[str, list[tuple[str, str]]] = {}
    for row in db.execute(_PENDING).mappings():
        by_run.setdefault(row["run_id"], []).append((row["lead_ref"], row["field"]))
    for run_id, pending in by_run.items():
        results = port.results(run_id)
        for ref, kind in pending:
            if ref not in results.rows:
                continue
            value = (_email_value if kind == EMAIL else _llc_value)(results.rows[ref])
            db.execute(_SETTLE, {"status": "found" if value else "not_found", "value": value, "now": now,
                                 "ref": ref, "field": kind})
            stats.settled += 1


def _submit(db: Session, port: ClayPort, items: Sequence[ListItem], settings: ClaySettings, now: datetime,
            stats: ClayStats) -> None:
    start = datetime.combine(now.astimezone(ET).date(), time.min, ET).astimezone(timezone.utc)
    room = settings.daily_cap - int(db.execute(_TODAY, {"start": start}).scalar() or 0)
    refs = [i.ref for i in items]
    known = {(r["lead_ref"], r["field"]) for r in db.execute(
        _KNOWN, {"refs": refs, "fresh": now - timedelta(days=settings.ttl_days)}).mappings()} if refs else set()
    batches: dict[str, tuple[str, list[dict]]] = {}
    for item in items:  # list order: real work first, padding last
        for kind, enabled, routine, make in ((EMAIL, settings.email_enabled, settings.email_routine, _email_input),
                                             (LLC_OWNER, settings.llc_enabled, settings.llc_routine, _llc_input)):
            inputs = make(item) if enabled and routine else None
            if inputs is None or (item.ref, kind) in known or room <= 0:
                continue
            batches.setdefault(kind, (routine, []))[1].append({"id": item.ref, "inputs": inputs})
            known.add((item.ref, kind))
            room -= 1
    for kind, (routine, batch) in batches.items():
        run_id = port.run(routine, batch)
        for entry in batch:
            db.execute(_FORGET, {"ref": entry["id"], "field": kind})  # a stale row past its TTL
            db.execute(_REQUEST, {"ref": entry["id"], "field": kind, "run_id": run_id, "now": now})
        stats.submitted += len(batch)


def enrich_gaps(db: Session, items: Sequence[ListItem], port: ClayPort, settings: ClaySettings, *,
                now: datetime) -> ClayStats:
    """One pass: settle finished runs, then submit the list's new gaps within the daily cap."""
    stats = ClayStats()
    try:
        _collect(db, port, now, stats)
        if settings.daily_cap > 0:
            _submit(db, port, items, settings, now, stats)
    except ClayError as exc:
        logger.warning("[morning-list] Clay pass stopped: %s", exc)
        stats.failed = True
    logger.info("[morning-list] Clay pass: %s submitted, %s settled", stats.submitted, stats.settled)
    return stats


def clay_port(api_key: Optional[str], base: str) -> Optional[ClayPort]:
    return LiveClayRoutines(api_key, base) if api_key else None
