"""Live Tracerfy trace of staged PropertyRadar leads (replaces the ``--trace-results`` CSV).

Every address is gated by ``skip_trace_ledger`` so it is never paid for twice, a
hard ``RunSpendCap`` stops the run before a batch that would exceed it, and each
paid attempt is written back to ``enrichment_usage_logs`` (the shared ledger).
Vendor I/O is injected: nothing here spends credits unless the caller passes the
real submit function.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Optional, Sequence

from src.services.property_radar.trace_contacts import LeadContacts, parse_contacts
from src.services.skip_trace_ledger import BillingModel, NORMAL, RunSpendCap, should_submit, trace_key

logger = logging.getLogger(__name__)

CENTS_PER_HIT = 2          # Tracerfy: 1 credit per hit = $0.02, a miss is free
DEFAULT_BATCH_SIZE = 250

Submit = Callable[[list[dict]], list[dict]]
WriteLedger = Callable[[list[dict]], None]
ReadContacts = Callable[[list[str]], dict[str, LeadContacts]]
WriteContacts = Callable[[dict[str, LeadContacts]], None]


class TraceBilledError(Exception):
    """The batch was accepted (and billed) by Tracerfy but its results could not be fetched.
    Any other exception from ``submit`` means the batch was never billed."""

    def __init__(self, queue_id: str):
        super().__init__(f"queue_id={queue_id}")
        self.queue_id = queue_id


@dataclass
class TraceOutcome:
    contacts: dict[str, LeadContacts] = field(default_factory=dict)
    aborted: bool = False
    submitted: int = 0
    skipped_already_traced: int = 0
    skipped_unkeyable: int = 0
    skipped_cap: int = 0


def _core(key: str) -> str:
    return key.rsplit("|", 1)[0]


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
    batch_size: int = DEFAULT_BATCH_SIZE,
    read_contacts: Optional[ReadContacts] = None,
    write_contacts: Optional[WriteContacts] = None,
) -> TraceOutcome:
    """``read_contacts``/``write_contacts`` persist hits keyed by address, so a lead
    already billed for (``ledger`` says so) but not used by *this* run's handoff still
    has its paid-for contacts available on a later run, instead of being re-reported as
    ``no_contact_data`` forever."""
    outcome = TraceOutcome()
    by_key: dict[str, list[Any]] = {}
    already_traced_leads: list[Any] = []
    for lead in leads:
        key = trace_key(lead.property_address, lead.zip)
        if not key:
            outcome.skipped_unkeyable += 1
        elif not should_submit(key, NORMAL, ledger, BillingModel.PER_HIT):
            outcome.skipped_already_traced += 1
            already_traced_leads.append(lead)
        else:
            by_key.setdefault(key, []).append(lead)

    if already_traced_leads and read_contacts:
        persisted = read_contacts([trace_key(lead.property_address, lead.zip) for lead in already_traced_leads])
        for lead in already_traced_leads:
            contacts = persisted.get(trace_key(lead.property_address, lead.zip))
            if contacts and not contacts.is_empty:
                outcome.contacts[lead.radar_id] = contacts

    keys = list(by_key)
    for start in range(0, len(keys), batch_size):
        batch_keys = keys[start:start + batch_size]
        if cap.would_exceed(len(batch_keys) * CENTS_PER_HIT):
            outcome.skipped_cap += sum(len(by_key[k]) for k in keys[start:])
            logger.warning("[pr-live-trace] spend cap reached; %d address(es) left untraced", len(keys) - start)
            break
        rows = [_submit_row(by_key[k][0]) for k in batch_keys]
        try:
            results = submit(rows)
        except TraceBilledError as exc:
            # Tracerfy billed the batch but the result poll failed: ledger the keys as
            # consumed (a zero-cost miss, queue_id kept for manual recovery) so a retry
            # can't double-charge. Earlier batches' contacts are kept.
            logger.error("[pr-live-trace] results poll failed after billing for %d address(es), "
                         "queue_id=%s; ledgered as consumed", len(batch_keys), exc.queue_id)
            cap.add(len(batch_keys) * CENTS_PER_HIT)
            write_ledger([_ledger_entry(key, False, request_ref=exc.queue_id) for key in batch_keys])
            outcome.submitted += len(batch_keys)
            continue
        except Exception as exc:
            # The submit step failed before billing (bad key, no credits, 429, 5xx,
            # timeout): nothing was spent, so ledger nothing and stop rather than keep
            # hitting a broken vendor. These addresses are retried on the next run.
            logger.error("[pr-live-trace] batch submit failed before billing for %d address(es); "
                         "stopping the run, nothing ledgered: %s", len(batch_keys), type(exc).__name__)
            outcome.aborted = True
            break
        cap.add(len(batch_keys) * CENTS_PER_HIT)
        outcome.submitted += len(batch_keys)
        hits = {_core(trace_key(r.get("address"), "")): r for r in results if r.get("address")}
        entries = []
        new_contacts: dict[str, LeadContacts] = {}
        for key in batch_keys:
            row = hits.get(_core(key))
            contacts = _contacts_from_row(row) if row else LeadContacts()
            for lead in by_key[key]:
                if not contacts.is_empty:
                    outcome.contacts[lead.radar_id] = contacts
            if not contacts.is_empty:
                new_contacts[key] = contacts
            entries.append(_ledger_entry(key, not contacts.is_empty))
        write_ledger(entries)
        if new_contacts and write_contacts:
            write_contacts(new_contacts)
    return outcome


def _submit_row(lead: Any) -> dict:
    first, last = _split_name(lead.principal_name or lead.owner_name)
    return {"address": lead.property_address, "city": lead.city, "state": lead.state, "zip": lead.zip,
            "first_name": first, "last_name": last, "label": lead.radar_id}


def _ledger_entry(key: str, success: bool, *, request_ref: Optional[str] = None) -> dict:
    return {"vendor": "tracerfy", "purpose": "skip_trace", "success": success,
            "cost_cents": CENTS_PER_HIT if success else 0, "property_id": None,
            "target_address": key, "request_ref": request_ref, "created_at": datetime.now(timezone.utc)}
