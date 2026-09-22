"""Backflip campaign feed adapter — WP-T2-3.

Interface for importing the Backflip active-campaign contact snapshot into
fa_max_backflip_campaign_contacts. The current implementation is CSV-based
(operator-driven; see scripts/import_backflip_suppression_csv.py). A future
live-sync implementation (pending Bailey's API format confirmation — SOT.md
open clarification #6) slots in without changing the suppression predicate or
the health monitor.

Every adapter must:
  1. Replace the full active-contact snapshot atomically (deactivate-all + upsert-new).
  2. Stamp fa_max_backflip_campaign_feed.last_success_at on a successful import.
  3. Return the count of identifiers successfully imported.
  4. Raise on any failure so the caller can decide whether to alert.

The Port/Fake/Csv/NotImplemented split follows the same pattern as
backflip_port.py (WP-7 WI-6) — one interface, several bodies, ship the one
that doesn't depend on an answer.

Feed ordering note: the CSV import uses SELECT FOR UPDATE on the singleton
feed row, which serialises concurrent imports but does NOT enforce timestamp
ordering — if an operator runs two imports in quick succession, whichever
transaction commits last wins. This is acceptable: imports are operator-ordered
by definition, and the singleton lock prevents snapshot interleaving (partial
deactivate from run A racing with partial upsert from run B). Do not add
version-based ordering logic until a live-sync source exists to originate a
real version/timestamp; any such field added today would be invented data.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Protocol
from sqlalchemy import text

from src.core.database import get_db_context

logger = logging.getLogger(__name__)


def replace_backflip_snapshot(
    identifiers: set[tuple[str, str]], *, allow_empty: bool = False,
) -> int:
    """Validate and atomically replace the active snapshot for every adapter."""
    from src.services.phone_utils import normalize
    normalized: set[tuple[str, str]] = set()
    for kind, value in identifiers:
        if kind == "email":
            canonical = value.strip().lower()
            if canonical.count("@") != 1 or not canonical.split("@", 1)[0] or "." not in canonical.split("@", 1)[1]:
                raise ValueError("invalid campaign email")
        elif kind == "phone":
            canonical = normalize(value)
            if canonical is None:
                raise ValueError("invalid campaign phone")
        else:
            raise ValueError("invalid campaign identifier kind")
        normalized.add((kind, canonical))
    if not normalized and not allow_empty:
        raise ValueError("empty campaign snapshot; pass --allow-empty after verifying the export")
    with get_db_context() as session:
        session.execute(text("INSERT INTO fa_max_backflip_campaign_feed (id, last_success_at) "
                             "VALUES (1, to_timestamp(0)) ON CONFLICT (id) DO NOTHING"))
        session.execute(text("SELECT id FROM fa_max_backflip_campaign_feed WHERE id = 1 FOR UPDATE"))
        session.execute(text("UPDATE fa_max_backflip_campaign_contacts SET active = false WHERE active"))
        for kind, value in normalized:
            session.execute(text(
                "INSERT INTO fa_max_backflip_campaign_contacts "
                "(identifier_kind, identifier_value, active, imported_at) "
                "VALUES (:kind, :value, true, now()) "
                "ON CONFLICT (identifier_kind, identifier_value) DO UPDATE SET "
                "active = true, imported_at = now()"
            ), {"kind": kind, "value": value})
        session.execute(text(
            "UPDATE fa_max_backflip_campaign_feed SET last_success_at = now() WHERE id = 1"
        ))
    return len(normalized)


class BackflipFeedPort(Protocol):
    """One import cycle replaces the entire active snapshot."""

    def import_snapshot(self) -> int:
        """Return the number of contact identifiers imported."""
        ...


class FakeBackflipFeedPort:
    """Tests and local dev. Records calls and returns a deterministic count."""

    def __init__(self, identifiers: list[tuple[str, str]] | None = None) -> None:
        self.identifiers = identifiers or []
        self.calls: list[list[tuple[str, str]]] = []

    def import_snapshot(self) -> int:
        self.calls.append(self.identifiers)
        return len(self.identifiers)


class CsvBackflipFeedPort:
    """Production v1: imports from an operator-supplied CSV export.

    Wraps scripts/import_backflip_suppression_csv.py's run() function so
    the adapter contract is satisfied without duplicating the import logic.
    The CSV path is supplied at construction time (from a settings key or a
    cron job argument); allow_empty controls the safety gate against an
    accidentally empty export clearing the protection.
    """

    def __init__(self, csv_path: Path, *, allow_empty: bool = False) -> None:
        self.csv_path = csv_path
        self.allow_empty = allow_empty

    def import_snapshot(self) -> int:
        from scripts.import_backflip_suppression_csv import run
        count = run(self.csv_path, dry_run=False, allow_empty=self.allow_empty)
        logger.info("[BackflipFeed] CsvBackflipFeedPort: imported %d identifiers from %s", count, self.csv_path)
        return count


class NotImplementedBackflipFeedPort:
    """Placeholder for a future live API. Raises rather than pretending to
    succeed — there is no real API shape to implement yet (SOT.md open
    clarification #6: suppression feed format pending Bailey's response).
    Select CsvBackflipFeedPort until Bailey answers.
    """

    def import_snapshot(self) -> int:
        raise NotImplementedError(
            "NotImplementedBackflipFeedPort: Backflip's live campaign-feed API "
            "format is unknown (SOT.md open clarification #6 — pending Bailey). "
            "Use CsvBackflipFeedPort until the API shape is confirmed."
        )


def get_backflip_feed_port(csv_path: Path | None = None) -> BackflipFeedPort:
    """Settings-driven selector. Defaults to CsvBackflipFeedPort when a path
    is supplied; falls back to NotImplementedBackflipFeedPort when no csv_path
    is provided and the adapter is not 'fake'.

    Set FA_MAX_BACKFLIP_FEED_ADAPTER=fake in .env to get FakeBackflipFeedPort
    (tests / local dev). Any other value (including the default) uses Csv when
    csv_path is supplied, NotImplemented otherwise — a misconfigured production
    environment fails loudly on the next import attempt rather than silently
    clearing the snapshot.
    """
    from config.settings import get_settings
    adapter = (getattr(get_settings(), "fa_max_backflip_feed_adapter", None) or "csv").lower()
    if adapter == "fake":
        return FakeBackflipFeedPort()
    if csv_path is not None:
        return CsvBackflipFeedPort(csv_path)
    return NotImplementedBackflipFeedPort()
