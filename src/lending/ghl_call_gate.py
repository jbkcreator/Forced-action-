"""Calling rules on Josh's GHL call lists (G5): attempt limit, calling hours, DNC result and the emergency stop.

Josh calls from eight GHL smart lists, one per lead-type tag (GHL_CALL_LIST_TAGS), each excluding
GHL_DO_NOT_CALL_NOW_TAG. Every run reads the listed contacts, decides which cannot be called now, and adds or removes
that tag only where the answer changed: an unchanged contact costs no GHL call, and GHL's tag is the only state.
Lead-type tags are never touched (removing and re-adding "lendingflow" re-fires GHL workflows).

A listed contact cannot be called now when:
* the emergency stop is on (an unreadable flag counts as on): every listed contact;
* its number is suppressed (opt-out, STOP, litigator);
* ``dial_blocks``: the 24-hour attempt limit, the 10-business-day attempt history or the calling window (8am-8pm
  recipient local and 9:00-19:15 ET); for a number that is not a LendingFlow lead, also a missing or stale DNC scrub;
* a non-LendingFlow number's newest scrub found it on a DNC or litigator list.
LendingFlow numbers need no scrub: every LendingFlow lead counts as having agreed to calls and texts (client decision).

Fails safe: a run that cannot read the rules writes nothing, so a blocked contact never comes back early; a contact
with no usable phone is left as it is. The tag only empties the smart lists. It cannot stop Josh dialling a number
from a contact page, nor the calls a GHL workflow places itself (those workflows are paused by hand in an emergency).

GHL contact search by tag (POST /contacts/search, ``tags`` ``contains``) follows the DND backstop's filter shape;
checked read-only against the live sub-account on 2026-11-03 (each list returned only contacts with its tag; counts
matched the smart lists).

Usage (cron, every minute; idle until LENDING_GHL_CALL_GATE_ENABLED=true):
    python -m src.lending.ghl_call_gate
    python -m src.lending.ghl_call_gate --dry-run   # blocked counts per list and reason; writes nothing
    python -m src.lending.ghl_call_gate --clear     # remove the tag from every contact (after switching off)
"""
from __future__ import annotations

import argparse
import logging
import time
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Protocol

from sqlalchemy import text

from config.lending_compliance import (
    GHL_CALL_GATE_ALERT_AFTER_MINUTES,
    GHL_CALL_GATE_LOCK_KEY,
    GHL_CALL_GATE_MAX_PAGES,
    GHL_CALL_GATE_PAGE_SIZE,
    GHL_CALL_LIST_TAGS,
    GHL_DO_NOT_CALL_NOW_TAG,
    ReasonCode,
)
from config.settings import get_settings
from src.lending.compliance import (
    dial_blocks,
    lendingflow_lead_phones,
    recipient_timezone,
    scrub_verdict_blocks,
    suppressed_phones,
)
from src.lending.ghl_account import GhlAccount, ghl_headers, lending_ghl_account
from src.services.phone_utils import normalize

logger = logging.getLogger(__name__)

EMERGENCY_STOP = "EMERGENCY_STOP"
EXIT_FAILED = 1


class GhlListError(RuntimeError):
    """A GHL read or tag write failed. The message carries the step and HTTP status or error class only."""


class CallListClient(Protocol):
    def contacts_with_tag(self, tag: str) -> list[dict[str, Any]]: ...
    def add_tag(self, contact_id: str, tag: str) -> None: ...
    def remove_tag(self, contact_id: str, tag: str) -> None: ...


class GhlCallLists:
    """The Next Deal Lending sub-account's contacts, read and tagged through the shared GHL request helper."""

    def __init__(self, account: GhlAccount) -> None:
        self._account = account

    def _call(self, step: str, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        from src.services import ghl_webhook

        try:
            response = ghl_webhook._ghl_request(
                method, f"{ghl_webhook._GHL_BASE}{path}", headers=ghl_headers(self._account.api_key), **kwargs,
            )
        except Exception as exc:
            raise GhlListError(f"{step}: {type(exc).__name__}") from exc
        if response.status_code >= 400:
            raise GhlListError(f"{step}: HTTP {response.status_code}")
        try:
            return response.json()
        except ValueError:
            return {}

    def contacts_with_tag(self, tag: str) -> list[dict[str, Any]]:
        contacts: list[dict[str, Any]] = []
        for page in range(1, GHL_CALL_GATE_MAX_PAGES + 1):
            body = self._call("search contacts", "POST", "/contacts/search", json={
                "locationId": self._account.location_id, "page": page, "pageLimit": GHL_CALL_GATE_PAGE_SIZE,
                "filters": [{"field": "tags", "operator": "contains", "value": tag}],
            })
            batch = list(body.get("contacts") or [])
            contacts.extend(batch)
            if len(batch) < GHL_CALL_GATE_PAGE_SIZE:
                return contacts
        logger.warning("[ghl-call-gate] list %s has more than %d contacts; the rest were not read this run",
                       tag, GHL_CALL_GATE_MAX_PAGES * GHL_CALL_GATE_PAGE_SIZE)
        return contacts

    def add_tag(self, contact_id: str, tag: str) -> None:
        self._call("add tag", "POST", f"/contacts/{contact_id}/tags", json={"tags": [tag]})

    def remove_tag(self, contact_id: str, tag: str) -> None:
        self._call("remove tag", "DELETE", f"/contacts/{contact_id}/tags", json={"tags": [tag]})


@dataclass(frozen=True)
class ListedContact:
    contact_id: str
    phone: Optional[str]  # phone_utils.normalize, None when unusable
    tags: frozenset[str]  # lower case, as GHL stores them

    @property
    def gated(self) -> bool:
        return GHL_DO_NOT_CALL_NOW_TAG in self.tags

    @property
    def lists(self) -> frozenset[str]:
        return self.tags & frozenset(GHL_CALL_LIST_TAGS)


@dataclass(frozen=True)
class GateChanges:
    to_add: list[ListedContact]
    to_remove: list[ListedContact]


@dataclass
class GateRunResult:
    listed: int = 0
    no_phone: int = 0
    no_timezone: int = 0
    blocked: int = 0
    added: int = 0
    removed: int = 0
    failed_lists: list[str] = field(default_factory=list)
    write_failures: int = 0

    @property
    def ok(self) -> bool:
        return not self.failed_lists and not self.write_failures


def _listed_contact(raw: Mapping[str, Any]) -> Optional[ListedContact]:
    contact_id = str(raw.get("id") or "").strip()
    if not contact_id:
        return None
    tags = frozenset(str(t).strip().lower() for t in (raw.get("tags") or []) if str(t).strip())
    return ListedContact(contact_id=contact_id, phone=normalize(raw.get("phone")) if raw.get("phone") else None, tags=tags)


def read_listed_contacts(client: CallListClient,
                         tags: Sequence[str] = GHL_CALL_LIST_TAGS) -> tuple[list[ListedContact], list[str]]:
    """Every contact on any of the lists, once each, and the lists that could not be read."""
    by_id: dict[str, ListedContact] = {}
    failed: list[str] = []
    for tag in tags:
        try:
            raw_contacts = client.contacts_with_tag(tag)
        except GhlListError as exc:
            logger.error("[ghl-call-gate] list %s not read: %s", tag, exc)
            failed.append(tag)
            continue
        for raw in raw_contacts:
            contact = _listed_contact(raw)
            if contact is not None:
                by_id.setdefault(contact.contact_id, contact)
    return list(by_id.values()), failed


def blocked_reasons(db, phones: Sequence[str], *, now: datetime, halted: bool) -> dict[str, str]:
    """phone -> why it cannot be called now, for each blocked phone (see the module docstring). Three batched
    reads plus ``dial_blocks``; never one query per phone. While halted, no database read at all."""
    phones = sorted(set(phones))
    if halted:
        return dict.fromkeys(phones, EMERGENCY_STOP)
    reasons: dict[str, str] = dict.fromkeys(suppressed_phones(db, phones), ReasonCode.SUPPRESSED.value)
    open_phones = [p for p in phones if p not in reasons]
    lendingflow = lendingflow_lead_phones(db, open_phones)
    scrubbed = [p for p in open_phones if p not in lendingflow]
    blocks = {**dial_blocks(db, scrubbed, now=now),
              **dial_blocks(db, sorted(lendingflow), now=now, check_scrub=False)}
    reasons.update({p: r.value for p, r in blocks.items()})
    verdicts = scrub_verdict_blocks(db, [p for p in scrubbed if p not in reasons])
    reasons.update({p: r.value for p, r in verdicts.items()})
    return reasons


def decide(contacts: Iterable[ListedContact], reasons: Mapping[str, str]) -> GateChanges:
    """Tag the blocked contacts that lack the tag; untag the allowed ones that have it. No phone = unchanged."""
    to_add, to_remove = [], []
    for contact in contacts:
        if contact.phone is None:
            continue
        blocked = contact.phone in reasons
        if blocked and not contact.gated:
            to_add.append(contact)
        elif not blocked and contact.gated:
            to_remove.append(contact)
    return GateChanges(to_add=to_add, to_remove=to_remove)


def apply_changes(client: CallListClient, changes: GateChanges) -> tuple[int, int, int]:
    """Adds first (a blocked lead leaves the list before anyone returns), then removes. Returns
    (added, removed, failed); a failed write is retried by the next run, which reads the tags again."""
    added = removed = failed = 0
    for contact in changes.to_add:
        try:
            client.add_tag(contact.contact_id, GHL_DO_NOT_CALL_NOW_TAG)
            added += 1
        except GhlListError as exc:
            failed += 1
            logger.error("[ghl-call-gate] contact %s not tagged: %s", contact.contact_id, exc)
    for contact in changes.to_remove:
        try:
            client.remove_tag(contact.contact_id, GHL_DO_NOT_CALL_NOW_TAG)
            removed += 1
        except GhlListError as exc:
            failed += 1
            logger.error("[ghl-call-gate] contact %s not untagged: %s", contact.contact_id, exc)
    return added, removed, failed


def _log_report(contacts: Sequence[ListedContact], reasons: Mapping[str, str], changes: GateChanges) -> None:
    by_list: Counter[tuple[str, str]] = Counter()
    for contact in contacts:
        reason = reasons.get(contact.phone) if contact.phone else "NO_PHONE"
        for tag in sorted(contact.lists) or ["(none)"]:
            by_list[(tag, reason or "CALLABLE")] += 1
    for (tag, reason), count in sorted(by_list.items()):
        logger.info("[ghl-call-gate] dry run: list=%s %s=%d", tag, reason, count)
    logger.info("[ghl-call-gate] dry run: would tag %d and untag %d contact(s)",
                len(changes.to_add), len(changes.to_remove))


def run_gate(db, client: CallListClient, *, halted: bool, now: Optional[datetime] = None,
             dry_run: bool = False) -> GateRunResult:
    """One pass over the lists. Raises when the rules cannot be read (nothing is written then). Does not commit."""
    now = now or datetime.now(timezone.utc)
    contacts, failed_lists = read_listed_contacts(client)
    result = GateRunResult(listed=len(contacts), failed_lists=failed_lists)
    phones = sorted({c.phone for c in contacts if c.phone})
    result.no_phone = sum(1 for c in contacts if c.phone is None)
    result.no_timezone = sum(1 for p in phones if recipient_timezone(p) is None)
    reasons = blocked_reasons(db, phones, now=now, halted=halted)
    result.blocked = len(reasons)
    changes = decide(contacts, reasons)
    if dry_run:
        _log_report(contacts, reasons, changes)
        return result
    result.added, result.removed, result.write_failures = apply_changes(client, changes)
    return result


def clear_tags(client: CallListClient) -> tuple[int, int]:
    """Remove the tag from every contact that has it, listed or not. Returns (removed, failed)."""
    contacts = [c for c in (_listed_contact(raw) for raw in client.contacts_with_tag(GHL_DO_NOT_CALL_NOW_TAG)) if c]
    _, removed, failed = apply_changes(client, GateChanges(to_add=[], to_remove=contacts))
    return removed, failed


_RECORD_OUTCOME = text("""
    INSERT INTO lending.ghl_call_gate_state AS s (id, last_success_at, failing_since)
    VALUES (1, :success, :failure)
    ON CONFLICT (id) DO UPDATE SET
        last_success_at = COALESCE(EXCLUDED.last_success_at, s.last_success_at),
        failing_since = CASE WHEN EXCLUDED.last_success_at IS NOT NULL THEN NULL
                             ELSE COALESCE(s.failing_since, EXCLUDED.failing_since) END,
        alerted_at = CASE WHEN EXCLUDED.last_success_at IS NOT NULL THEN NULL ELSE s.alerted_at END
    RETURNING failing_since, alerted_at
""")
_CLAIM_ALERT = text("UPDATE lending.ghl_call_gate_state SET alerted_at = :now WHERE id = 1 AND alerted_at IS NULL")


def record_outcome(db, *, ok: bool, now: datetime) -> bool:
    """Store this run's outcome. True exactly once per failing spell, when it has lasted
    GHL_CALL_GATE_ALERT_AFTER_MINUTES: the caller then posts the ops alert. Does not commit."""
    failing_since, alerted_at = db.execute(
        _RECORD_OUTCOME, {"success": now if ok else None, "failure": None if ok else now},
    ).one()
    if ok or alerted_at is not None or failing_since is None:
        return False
    if now - failing_since < timedelta(minutes=GHL_CALL_GATE_ALERT_AFTER_MINUTES):
        return False
    return db.execute(_CLAIM_ALERT, {"now": now}).rowcount == 1


def _run_once(client: CallListClient) -> int:
    from src.lending.db import lending_session
    from src.lending.global_halt import is_globally_halted
    from src.lending.ops_alerts import post_ops_alert

    started = time.monotonic()
    now = datetime.now(timezone.utc)
    with lending_session() as db:
        if not db.execute(text("SELECT pg_try_advisory_xact_lock(:k)"), {"k": GHL_CALL_GATE_LOCK_KEY}).scalar():
            logger.info("[ghl-call-gate] another run holds the lock; skipped")
            return 0
        halted = is_globally_halted()
        try:
            with db.begin_nested():
                result = run_gate(db, client, halted=halted, now=now)
            ok = result.ok
        except Exception as exc:  # class only: SQL errors embed bound parameters (phones)
            logger.error("[ghl-call-gate] run failed, nothing changed: %s", type(exc).__name__)
            result, ok = None, False
        alert = record_outcome(db, ok=ok, now=now)
    if result is not None:
        logger.info("[ghl-call-gate] halted=%s listed=%d blocked=%d added=%d removed=%d write_failures=%d "
                    "failed_lists=%d no_phone=%d no_timezone=%d seconds=%.1f",
                    halted, result.listed, result.blocked, result.added, result.removed, result.write_failures,
                    len(result.failed_lists), result.no_phone, result.no_timezone, time.monotonic() - started)
    if alert:
        post_ops_alert(f"GHL call-list gate has not completed a run for {GHL_CALL_GATE_ALERT_AFTER_MINUTES} minutes. "
                       "Josh's GHL call lists may show leads outside calling hours or over the attempt limit. "
                       "Check the ghl_call_gate cron log.")
    return 0 if ok else EXIT_FAILED


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Apply the calling rules to Josh's GHL call lists.")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="report blocked counts per list; write nothing")
    mode.add_argument("--clear", action="store_true", help=f"remove {GHL_DO_NOT_CALL_NOW_TAG} from every contact")
    args = parser.parse_args(argv)

    account = lending_ghl_account()
    if account is None:
        logger.error("[ghl-call-gate] not run: the Next Deal Lending GHL account is not configured")
        return EXIT_FAILED
    client = GhlCallLists(account)

    if args.clear:
        try:
            removed, failed = clear_tags(client)
        except GhlListError as exc:
            logger.error("[ghl-call-gate] clear failed: %s", exc)
            return EXIT_FAILED
        logger.info("[ghl-call-gate] clear: removed=%d failed=%d", removed, failed)
        return EXIT_FAILED if failed else 0

    if args.dry_run:
        from src.lending.db import lending_session
        from src.lending.global_halt import is_globally_halted

        with lending_session() as db:
            result = run_gate(db, client, halted=is_globally_halted(), dry_run=True)
        logger.info("[ghl-call-gate] dry run: listed=%d blocked=%d no_phone=%d no_timezone=%d failed_lists=%s",
                    result.listed, result.blocked, result.no_phone, result.no_timezone, result.failed_lists)
        return 0

    if not get_settings().lending_ghl_call_gate_enabled:
        logger.info("[ghl-call-gate] idle: LENDING_GHL_CALL_GATE_ENABLED is off")
        return 0
    return _run_once(client)


if __name__ == "__main__":
    raise SystemExit(main())
