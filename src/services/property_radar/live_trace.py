"""Live Tracerfy trace of staged PropertyRadar leads (replaces the ``--trace-results`` CSV).

Every address is gated by ``skip_trace_ledger`` so it is never paid for twice, a
hard ``RunSpendCap`` stops the run before a batch that would exceed it, and each
paid attempt is written back to ``enrichment_usage_logs`` (the shared ledger).
Vendor I/O is injected: nothing here spends credits unless the caller passes the
real submit function.

Each batch's contacts are handed to ``write_contacts`` before its ledger rows go to
``write_ledger``; the caller commits both together so a paid hit is never ledgered
(and so never re-traced) without its contacts being stored.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Optional, Sequence

from src.services.property_radar.staging import normalize_owner
from src.services.property_radar.trace_contacts import LeadContacts, parse_contacts
from src.services.skip_trace_ledger import BillingModel, NORMAL, RunSpendCap, should_submit, trace_key

logger = logging.getLogger(__name__)

CENTS_PER_HIT = 2          # Tracerfy: 1 credit per hit = $0.02, a miss is free
DEFAULT_BATCH_SIZE = 250

Submit = Callable[[list[dict]], list[dict]]
WriteLedger = Callable[[list[dict]], None]
WriteContacts = Callable[[dict[str, LeadContacts]], None]


@dataclass
class TraceOutcome:
    contacts: dict[str, LeadContacts] = field(default_factory=dict)
    submitted: int = 0
    skipped_already_traced: int = 0
    skipped_unkeyable: int = 0
    skipped_cap: int = 0
    skipped_no_result: int = 0


def _core(key: str) -> str:
    return key.rsplit("|", 1)[0]


def _owner(lead: Any) -> str:
    return normalize_owner(lead.principal_name or lead.owner_name)


def _split_name(name: Optional[str]) -> tuple[str, str]:
    parts = (name or "").split()
    return (parts[0], " ".join(parts[1:])) if parts else ("", "")


def _contacts_from_row(row: dict) -> LeadContacts:
    from src.services.tracerfy_batch import _parse_trace_row

    parsed = _parse_trace_row(row)
    phones = [p for p in (parsed["mobile_phone"], parsed["landline"]) if p]
    return parse_contacts([parsed["email"]] if parsed["email"] else [], phones)


def trace_staged_leads(
    leads: Iterable[Any],
    *,
    ledger: dict[str, set[str]],
    submit: Submit,
    write_ledger: WriteLedger,
    cap: RunSpendCap,
    write_contacts: WriteContacts = lambda contacts: None,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> TraceOutcome:
    outcome = TraceOutcome()
    by_key: dict[str, list[Any]] = {}
    for lead in leads:
        key = trace_key(lead.property_address, lead.zip)
        if not key:
            outcome.skipped_unkeyable += 1
        elif not should_submit(key, NORMAL, ledger, BillingModel.PER_HIT):
            outcome.skipped_already_traced += 1
        else:
            by_key.setdefault(key, []).append(lead)

    keys = list(by_key)
    for start in range(0, len(keys), batch_size):
        batch_keys = keys[start:start + batch_size]
        if cap.would_exceed(len(batch_keys) * CENTS_PER_HIT):
            outcome.skipped_cap += sum(len(by_key[k]) for k in keys[start:])
            logger.warning("[pr-live-trace] spend cap reached; %d address(es) left untraced", len(keys) - start)
            break
        cap.add(len(batch_keys) * CENTS_PER_HIT)
        rows = [_submit_row(by_key[k][0]) for k in batch_keys]
        results = submit(rows)
        outcome.submitted += len(batch_keys)
        if not results:
            # An empty queue is a timeout or an all-miss batch; either way we cannot tell, so
            # nothing is ledgered (a miss is free to retry) and the run stops.
            outcome.skipped_no_result += sum(len(by_key[k]) for k in keys[start:])
            logger.error("[pr-live-trace] Tracerfy returned no rows for %d address(es); not ledgered, "
                         "run stopped", len(batch_keys))
            break
        hits = {_core(trace_key(r.get("address"), "")): r for r in results if r.get("address")}
        entries, traced = [], {}
        for key in batch_keys:
            row = hits.get(_core(key))
            contacts = _contacts_from_row(row) if row else LeadContacts()
            # The ledger key is building-level and the row sent names the first lead's owner,
            # so only leads with that owner get the contacts (other units' owners stay untraced).
            owner = _owner(by_key[key][0])
            for lead in by_key[key]:
                if not contacts.is_empty and _owner(lead) == owner:
                    traced[lead.radar_id] = contacts
            entries.append(_ledger_entry(key, not contacts.is_empty))
        write_contacts(traced)
        write_ledger(entries)
        outcome.contacts.update(traced)
    return outcome


def _submit_row(lead: Any) -> dict:
    first, last = _split_name(lead.principal_name or lead.owner_name)
    return {"address": lead.property_address, "city": lead.city, "state": lead.state, "zip": lead.zip,
            "first_name": first, "last_name": last, "label": lead.radar_id}


def _ledger_entry(key: str, success: bool) -> dict:
    return {"vendor": "tracerfy", "purpose": "skip_trace", "success": success,
            "cost_cents": CENTS_PER_HIT if success else 0, "property_id": None,
            "target_address": key, "request_ref": None, "created_at": datetime.now(timezone.utc)}
