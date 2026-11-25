"""R1 0.5 T4: LendingFlow leads through the inbound journal (source ``lendingflow_lead``).

The endpoint (``src/lending/lendingflow_webhook.py``) keeps its 401 / 413 / 400 checks, then calls ``ingest_lead``:
the raw body is saved first, ``save_lead`` runs in the journal's transaction, and a failure goes to
``webhook_failures`` (retried every 5 minutes through the same handler) while the endpoint still answers 200.

Dedup key = ``vendor_lead_id|dedupe_hash`` (a lead without a LendingFlow id already carries the hash in its id).
An exact re-send is a journal duplicate; a re-send that changed the phone/email or the lead id gets its own row and
reaches ``save_lead``, which records it against the existing lead and raises the "possible duplicate" inbox item.
"""
from __future__ import annotations

from typing import Any, Literal

from config.lending_lendingflow import is_missing_lead_id
from src.lending.inbound import journal
from src.lending.lendingflow import ParsedLendingFlowLead, SaveResult, dedupe_hash, parse_lendingflow, save_lead

SOURCE = "lendingflow_lead"
LABEL = "LendingFlow lead"
RESEND_HINT = "ask LendingFlow to resend the lead"

ReplayOutcome = Literal["created", "duplicate", "failed"]


def dedup_key(parsed: ParsedLendingFlowLead) -> str:
    if is_missing_lead_id(parsed.vendor_lead_id):
        return parsed.vendor_lead_id
    return f"{parsed.vendor_lead_id}|{dedupe_hash(parsed.phone, parsed.email)}"


def handle_lead(db, raw: dict[str, Any]) -> SaveResult:
    """Journal handler (also the 5-minute replay): idempotent, ``save_lead`` dedupes on vendor id / phone+email."""
    return save_lead(db, parse_lendingflow(raw), raw)


journal.register_source(SOURCE, handle_lead, label=LABEL, resend_hint=RESEND_HINT)


def ingest_lead(parsed: ParsedLendingFlowLead, raw: dict[str, Any]) -> journal.IngestResult:
    """Save ``raw`` and store the lead once. Raises only when the raw body could not be saved."""
    return journal.ingest(SOURCE, dedup_key(parsed), raw)


def outcome_of(result: journal.IngestResult) -> ReplayOutcome:
    if result.outcome == "processed":
        return "created" if result.value.created else "duplicate"
    return result.outcome
