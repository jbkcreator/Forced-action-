"""R1 0.5 T8 (grill D13): heal LendingFlow leads that reached nginx but never got stored.

nginx writes the raw body of every ``POST /api/lending/lendingflow`` to its own log
(``deploy/nginx/lending-api.conf.example``: format ``lendingflow_body``, one JSON object per line with ``t``,
``status`` and ``body``; no headers). The 2 AM reconcile (``src.tasks.lending_reconciliation``) reads the current file
and the rotated ``.1`` file, keeps the bodies of the window whose lead is not in ``lending.lendingflow_leads`` (vendor
id or phone+email hash, the same match ``save_lead`` uses) and replays each through the journal
(``replay_lendingflow_body``), so a lead is never created twice. It heals a lending-api / database outage, a killed
worker or a 5xx. It cannot heal a delivery that never reached nginx. Logs carry counts and error classes only.
"""
from __future__ import annotations

import json
import logging
from collections.abc import Callable, Iterable, Iterator
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import text

from src.lending.lendingflow import ParsedLendingFlowLead, ParseError, dedupe_hash, parse_lendingflow

logger = logging.getLogger(__name__)

ROTATED_SUFFIX = ".1"
HEALED_OUTCOMES = frozenset({"created", "duplicate"})

ExistingLookup = Callable[[list[ParsedLendingFlowLead]], set[str]]
Replay = Callable[[dict[str, Any]], str]

_EXISTING = text(
    "SELECT vendor_lead_id, dedupe_hash FROM lending.lendingflow_leads "
    "WHERE vendor_lead_id = ANY(:ids) OR dedupe_hash = ANY(:hashes)"
)


def existing_vendor_ids(leads: list[ParsedLendingFlowLead]) -> set[str]:
    """Vendor ids of ``leads`` already stored, matched by vendor id or phone+email hash (one query)."""
    if not leads:
        return set()
    from src.lending.db import lending_session

    hashes = {lead.vendor_lead_id: dedupe_hash(lead.phone, lead.email) for lead in leads}
    with lending_session() as db:
        rows = db.execute(_EXISTING, {"ids": list(hashes), "hashes": list(hashes.values())}).all()
    stored_ids, stored_hashes = {r[0] for r in rows}, {r[1] for r in rows}
    return {vid for vid, digest in hashes.items() if vid in stored_ids or digest in stored_hashes}


def _replay(raw: dict[str, Any]) -> str:
    from src.lending.inbound.lendingflow import replay_lendingflow_body

    return replay_lendingflow_body(raw)


def _read_lines(path: Path) -> Iterator[str]:
    for candidate in (Path(str(path) + ROTATED_SUFFIX), path):
        if candidate.is_file():
            with candidate.open(encoding="utf-8", errors="replace") as handle:
                yield from handle


def _logged_body(line: str, since: datetime) -> dict[str, Any] | None:
    """The request body of one log line inside the window, or None (outside the window or unreadable)."""
    try:
        entry = json.loads(line)
        if datetime.fromisoformat(entry["t"]) < since:
            return None
        body = json.loads(entry["body"])
    except (ValueError, KeyError, TypeError):
        return None
    return body if isinstance(body, dict) else None


class LendingFlowEdgeLogSlot:
    """Reconcile slot (``src.tasks.lending_reconciliation.Slot``) over the nginx LendingFlow body log."""

    name = "LendingFlow leads (nginx edge log)"

    def __init__(self, path: str, *, existing: ExistingLookup = existing_vendor_ids, replay: Replay = _replay):
        self._path = Path(path) if path else None
        self._existing = existing
        self._replay = replay

    def find_missing(self, since: datetime) -> list[dict[str, Any]]:
        from src.tasks.lending_reconciliation import SlotWarning

        if self._path is None:
            raise SlotWarning("LENDING_LENDINGFLOW_BODY_LOG is not set")
        if not self._path.is_file() and not Path(str(self._path) + ROTATED_SUFFIX).is_file():
            raise SlotWarning("the LendingFlow edge log is not there (nginx step not applied?)")
        bodies, unreadable = self._bodies(_read_lines(self._path), since)
        if unreadable:
            logger.warning("[lending-reconcile] LendingFlow edge log: %d unreadable line(s) skipped", unreadable)
        stored = self._existing([lead for lead, _ in bodies])
        return [raw for lead, raw in bodies if lead.vendor_lead_id not in stored]

    @staticmethod
    def _bodies(lines: Iterable[str], since: datetime) -> tuple[list[tuple[ParsedLendingFlowLead, dict]], int]:
        """Parsed leads of the window, one per vendor id (the last body logged wins), and the unreadable count."""
        by_vendor: dict[str, tuple[ParsedLendingFlowLead, dict[str, Any]]] = {}
        unreadable = 0
        for line in lines:
            if not line.strip():
                continue
            raw = _logged_body(line, since)
            if raw is None:
                unreadable += 1
                continue
            try:
                lead = parse_lendingflow(raw)
            except ParseError:
                unreadable += 1
                continue
            by_vendor[lead.vendor_lead_id] = (lead, raw)
        return list(by_vendor.values()), unreadable

    def heal(self, payload: dict[str, Any]) -> None:
        from src.tasks.lending_reconciliation import ReconcileError

        outcome = self._replay(payload)
        if outcome not in HEALED_OUTCOMES:
            raise ReconcileError(f"LendingFlow replay {outcome}")
