"""R1 1.1 contact card: one person's phones (last 4), emails, LLCs, systems and every touch, newest first.

``load_card`` does every database read; ``finish_card`` adds the live GHL and Instantly touches with no session open
(Cora's tool closes its read-only transaction in between). CLI for the Done-when check:
    python -m src.lending.spine.card --query <phone|email|name|lead_ref>
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Optional, Sequence

from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session

from config.lending_contact_spine import CARD_MATCH_LIMIT, EMAIL, ENTITY, PHONE, TIMELINE_LIMIT
from src.lending.spine.contact_spine import canonical_id
from src.lending.spine.identity import email_identifier, phone_identifier
from src.lending.spine.timeline import merge_touches
from src.lending.spine.touches import ContactKeys, Touch, TouchSource, contact_keys, local_touches

logger = logging.getLogger(__name__)

Flags = Callable[[Session, list[str]], Mapping[str, Mapping[str, Any]]]

# lead_ref prefix (the form find_lead and the draft tools use) <-> contact_sources.source
_REF_SOURCES = {"lendingflow_lead": "lendingflow_leads", "web_lead": "web_leads", "contact": "contacts",
                "loan_file": "loan_files", "dialer_load_record": "dialer_load_records",
                "after_hours_conversation": "after_hours_conversations", "instantly_reply": "instantly_replies",
                "sunbiz_llc_contact": "sunbiz_llc_contacts", "call": "call_dispositions"}
_SOURCE_REFS = {source: prefix for prefix, source in _REF_SOURCES.items()}
_LEAD_REF = re.compile(r"^([a-z_]+):(\S+)$")
_FLAG_KEYS = ("text_consent", "suppressed", "do_not_contact")


def _last4(phone: str) -> str:
    return f"…{phone[-4:]}"


def find_contacts(db: Session, query: str) -> list[int]:
    """Live contact ids matching a phone, an email, a lead_ref or (otherwise) part of a name; at most
    ``CARD_MATCH_LIMIT``."""
    query = query.strip()
    ref = _LEAD_REF.match(query)
    phone = phone_identifier(query) if len(re.sub(r"\D", "", query)) >= 7 and not ref else None
    email = email_identifier(query)
    if phone or email:
        identifier = phone or email
        rows = db.execute(text("SELECT contact_id FROM lending.contact_identifiers WHERE kind = :k AND value = :v"),
                          {"k": identifier.kind, "v": identifier.value}).scalars().all()
    elif ref and ref.group(1) in _REF_SOURCES:
        rows = db.execute(text("SELECT contact_id FROM lending.contact_sources WHERE source = :s AND source_ref = :r"),
                          {"s": _REF_SOURCES[ref.group(1)], "r": ref.group(2)}).scalars().all()
    else:
        rows = db.execute(text("SELECT id FROM lending.contact_spine WHERE merged_into IS NULL "
                               "AND lower(display_name) LIKE :q ORDER BY id LIMIT :n"),
                          {"q": f"%{query.lower()}%", "n": CARD_MATCH_LIMIT}).scalars().all()
    found: list[int] = []
    for contact_id in rows:
        live = canonical_id(db, contact_id)
        if live is not None and live not in found:
            found.append(live)
    return found[:CARD_MATCH_LIMIT]


@dataclass(frozen=True)
class LoadedCard:
    contact_id: int
    display_name: Optional[str]
    keys: ContactKeys
    phones: list[dict[str, Any]]
    emails: list[str]
    entities: list[str]
    sources: list[str]
    local: list[Touch]


def load_card(db: Session, contact_id: int, *, flags: Flags) -> Optional[LoadedCard]:
    live = canonical_id(db, contact_id)
    if live is None:
        return None
    display_name = db.execute(text("SELECT display_name FROM lending.contact_spine WHERE id = :i"), {"i": live}).scalar()
    keys = contact_keys(db, live)
    entities = db.execute(text("SELECT value FROM lending.contact_identifiers WHERE contact_id = :c AND kind = :k "
                               "ORDER BY value"), {"c": live, "k": ENTITY}).scalars().all()
    sources = [f"{_SOURCE_REFS.get(s, s)}:{r}" for s, r in db.execute(text(
        "SELECT source, source_ref FROM lending.contact_sources WHERE contact_id = :c ORDER BY source, source_ref"),
        {"c": live})]
    phones = list(keys.phones)
    line_types: dict[str, Any] = {}
    phone_flags: Mapping[str, Mapping[str, Any]] = {}
    if phones:
        line_types = dict(db.execute(text("SELECT phone, line_type FROM lending.contacts WHERE phone IN :p")
                                     .bindparams(bindparam("p", expanding=True)), {"p": phones}).all())
        phone_flags = flags(db, phones)
    return LoadedCard(
        contact_id=live, display_name=display_name, keys=keys,
        phones=[{"last4": _last4(p), "line_type": line_types.get(p),
                 **{k: bool(phone_flags.get(p, {}).get(k, False)) for k in _FLAG_KEYS}} for p in phones],
        emails=list(keys.emails), entities=list(entities), sources=sources, local=local_touches(db, keys),
    )


def finish_card(loaded: LoadedCard, *, remote: Sequence[TouchSource], limit: int = TIMELINE_LIMIT) -> dict[str, Any]:
    result = merge_touches(loaded.local, loaded.keys, remote=remote, limit=limit)
    every = list(loaded.local) + list(result.touches)
    calls = [s for s in loaded.sources if s.startswith("call:")]
    systems = {
        "ghl": bool(loaded.keys.ghl_contact_ids) or any(t.system == "ghl" for t in every)
               or any(s.startswith("call:ghl:") for s in calls),
        "batchdialer": bool(loaded.keys.dialer_contact_ids) or any(t.system == "batchdialer" for t in every)
                       or any(not s.startswith("call:ghl:") for s in calls),
        "instantly": any(t.system == "instantly" for t in every)
                     or any(s.startswith("instantly_reply:") for s in loaded.sources),
    }
    return {
        "contact_id": loaded.contact_id,
        "display_name": loaded.display_name,
        "phones": loaded.phones,
        "emails": loaded.emails,
        "entities": loaded.entities,
        "systems": systems,
        "sources": loaded.sources,
        "timeline": [{"at": t.at.isoformat() if t.at else None, "system": t.system, "kind": t.kind,
                      "direction": t.direction, "outcome": t.outcome} for t in result.touches],
        "unavailable": list(result.unavailable),
    }


def contact_card(db: Session, contact_id: int, *, remote: Sequence[TouchSource], flags: Flags,
                 limit: int = TIMELINE_LIMIT) -> Optional[dict[str, Any]]:
    loaded = load_card(db, contact_id, flags=flags)
    return finish_card(loaded, remote=remote, limit=limit) if loaded else None


def live_sources() -> list[TouchSource]:
    """GHL and Instantly for the NDL accounts; an unset account is reported unavailable, never called."""
    from src.lending.ghl_account import lending_ghl_account
    from src.lending.instantly_client import lending_instantly_account
    from src.lending.spine.touches import GhlTouches, InstantlyTouches

    return [GhlTouches(lending_ghl_account()), InstantlyTouches(lending_instantly_account())]


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="R1 1.1: one person's unified contact card")
    parser.add_argument("--query", required=True, help="phone, email, name or lead_ref")
    parser.add_argument("--limit", type=int, default=TIMELINE_LIMIT)
    args = parser.parse_args(argv)
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(message)s")
    from src.lending.cora_tools import phone_flags
    from src.lending.db import lending_session

    with lending_session() as db:
        ids = find_contacts(db, args.query)
        loaded = load_card(db, ids[0], flags=phone_flags) if len(ids) == 1 else None
    if loaded is None:
        logger.info("%d contacts matched (ids %s); narrow the query", len(ids), ids)
        return 1
    # the card holds emails and LLC names: stdout for the operator, never the log
    sys.stdout.write(json.dumps(finish_card(loaded, remote=live_sources(), limit=args.limit), indent=2, default=str) + "\n")
    logger.info("contact card %s written", loaded.contact_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
