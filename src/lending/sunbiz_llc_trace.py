"""Who to look up for each new Florida real estate LLC (src/tasks/sunbiz_llc_trace.py).

The state file has no phone or email, so Tracerfy is asked for one person per LLC: the first person-type officer, else
a person registered agent. The state writes names ``LAST FIRST [MIDDLE]``; the lookup reads ``LAST, FIRST``
(``live_trace._split_name``), so the name is reordered here. A subject whose address is shared by more than
MAX_LLCS_PER_ADDRESS of the day's LLCs is skipped (a formation service or agent, not that LLC's owner).

shortcut: a surname of two words ("DE LA CRUZ") reads as last name "DE" and misses (a miss is free); LLCs that list the same
person at the same address on one run share one lookup, but a later LLC by a person already looked up on an earlier run
gets no row of its own (the earlier LLC's row holds the contact), and a person already traced through PropertyRadar is
paid for again here (2 cents). Upgrade when the pilot shows how often any of these happens.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Iterable, Optional

from config.sunbiz_re_llc import MAX_LLCS_PER_ADDRESS, PERSON_TYPE
from src.scrappers.sunbiz.daily_filings import Filing
from src.services.skip_trace_ledger import trace_key


@dataclass(frozen=True)
class TraceLead:
    """The shape ``live_trace.trace_staged_leads`` reads. ``radar_id`` carries the LLC's document number, the key the
    contacts come back under."""

    radar_id: str
    owner_name: str
    property_address: str
    city: str
    state: str
    zip: str
    role: str
    subject_name: str
    entity_name: str
    principal_name: Optional[str] = None


@dataclass
class Plan:
    leads: list[TraceLead]
    skipped: Counter  # no_person | shared_address


def ledger_key(lead: TraceLead) -> str:
    """The key this lookup is ledgered under: address + ZIP + person. PropertyRadar ledgers a plain address + ZIP, so a
    lookup here never suppresses a different person's lookup there (or the other way round), and two people at one
    address are each looked up."""
    return f"{trace_key(lead.property_address, lead.zip)}|{lead.owner_name.upper()}"


def trace_name(raw: str) -> str:
    """``SMITH JOHN M`` -> ``SMITH, JOHN``; empty when there is no first name."""
    parts = raw.replace(",", " ").split()
    return f"{parts[0]}, {parts[1]}" if len(parts) >= 2 else ""


def _subject(filing: Filing) -> Optional[TraceLead]:
    candidates = [("officer", o.name, o.address, o.city, o.state, o.zip) for o in filing.officers if o.type == PERSON_TYPE]
    if filing.agent_type == PERSON_TYPE:
        candidates.append(("agent", filing.agent_name, filing.agent_address, filing.agent_city, filing.agent_state,
                           filing.agent_zip))
    for role, name, address, city, state, zip_code in candidates:
        owner = trace_name(name)
        if owner and address and trace_key(address, zip_code):
            return TraceLead(radar_id=filing.document_number, owner_name=owner, property_address=address, city=city,
                             state=state, zip=zip_code, role=role, subject_name=name, entity_name=filing.name)
    return None


def plan(filings: Iterable[Filing]) -> Plan:
    chosen, skipped = [], Counter()
    for filing in filings:
        lead = _subject(filing)
        if lead is None:
            skipped["no_person"] += 1
        else:
            chosen.append(lead)
    use = Counter(trace_key(lead.property_address, lead.zip) for lead in chosen)
    leads = []
    for lead in chosen:
        if use[trace_key(lead.property_address, lead.zip)] > MAX_LLCS_PER_ADDRESS:
            skipped["shared_address"] += 1
        else:
            leads.append(lead)
    return Plan(leads=leads, skipped=skipped)
