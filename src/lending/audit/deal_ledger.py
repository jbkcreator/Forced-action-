"""R1 item 0.8 — the deal-level immutable audit ledger.

``lending.deal_audit_events`` holds one append-only row for everything that happens to a loan file: it is created, moves
a stage, is closed, gets a lender submission, an underwriting quote, a field edit or a payment, and Cora drafts a message
for it, a person edits that draft, or a person approves it.  Each row says who did it (``cora``, ``human:<id>`` or
``system:<source>``) and which lender rule version (``lender_rule_snapshots.version_no``) was in force, so a file's whole
life can be read back in order (``lifecycle``).

Two ways in:

* In the same transaction as the change (``record``): stage moves, payments, field edits, submissions, closings and file
  creation in ``state_machine``.  If the ledger write fails the change fails with it.  The older partner flow, which was
  live before the ledger, uses ``record_safely`` so a ledger problem never blocks a partner notice.
* A catch-up pass (``sync_card_events``) for Cora's cards, run by her minute sweep: drafts, approvals and edits live in
  ``packages/agent_core``'s own tables, which cannot call into ``src``.  Each event has a ``dedupe_key``, so the pass can
  run any number of times.  A card is tied to a loan file through its ``deal_ref`` (``loan_file:<id>`` or
  ``doc_chase:<id>``) or, failing that, the borrower's phone (only when exactly one file has that number); a card about nothing in
  the pipeline is not ledgered.

    python -m src.lending.audit.deal_ledger --loan-file 12 [--json]
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from datetime import datetime
from typing import Any, Iterable, Mapping, Optional, Sequence

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.lending_standing_sends import AUTO_GRADUATED_ACTOR

logger = logging.getLogger(__name__)

EVENT_TYPES = frozenset({
    "file_created", "stage_transition", "stage_rejected", "closed", "status_change", "lender_submission",
    "quote_generated", "field_update", "payment_recorded", "cora_draft", "human_edit", "card_approved", "card_rejected",
    "doc_quarantined", "doc_released", "doc_type_tagged", "doc_block_override", "package_blocked", "package_submitted",
    "package_dismissed",
})

ACTOR_CORA = "cora"
_ACTOR = re.compile(r"^(cora|human:\S+|system:\S+)$")
_HUMAN_SOURCES = frozenset({"api", "manual", "slack"})
_UNSET: Any = object()


# ── Actors ─────────────────────────────────────────────────────────────────────

def human(identity: str) -> str:
    return f"human:{identity.strip()}"


def system(source: str) -> str:
    return f"system:{source.strip() or 'unknown'}"


def tag_actor(actor: Optional[str], *, source: str) -> str:
    """The ledger actor for a caller's loose ``actor`` string: already tagged stays; ``slack:<id>`` is a person; a name
    given through the API or by hand is a person; anything else is the system source that did it."""
    cleaned = re.sub(r"\s+", "_", (actor or "").strip())
    if _ACTOR.match(cleaned):
        return cleaned
    if cleaned in ("human:", "system:", "slack:"):  # a reserved prefix with nothing after it
        cleaned = ""
    if cleaned.startswith("slack:"):
        return human(cleaned[6:])
    if cleaned and source in _HUMAN_SOURCES:
        return human(cleaned)
    return system(source)


# ── Pure helpers ───────────────────────────────────────────────────────────────

def diff_fields(before: Mapping[str, Any], after: Mapping[str, Any], fields: Optional[Iterable[str]] = None) -> dict[str, dict]:
    """``{field: {"before": x, "after": y}}`` for each field whose value changed (a field missing on one side is None)."""
    names = list(fields) if fields is not None else sorted(set(before) | set(after))
    return {name: {"before": before.get(name), "after": after.get(name)}
            for name in names if before.get(name) != after.get(name)}


def describe(event: Mapping[str, Any]) -> str:
    """One readable line for an event row."""
    when = event["occurred_at"]
    stamp = f"{when:%Y-%m-%d %H:%M}" if isinstance(when, datetime) else str(when)
    version = f"rules v{event['rule_version']}" if event.get("rule_version") is not None else "rules -"
    changes = "; ".join(f"{name}: {_short(c.get('before'))} -> {_short(c.get('after'))}"
                        for name, c in (event.get("diff") or {}).items())
    return f"{stamp}  {event['event_type']:<17} {event['actor']:<24} {version:<11} {changes}".rstrip()


def _short(value: Any, limit: int = 60) -> str:
    shown = "None" if value is None else str(value).replace("\n", " ")
    return shown if len(shown) <= limit else shown[:limit - 1] + "…"


# ── Writing ────────────────────────────────────────────────────────────────────

def rule_version_at(db: Session, at: Optional[datetime] = None) -> Optional[int]:
    """The lender rule version in force at ``at`` (now by default); None before snapshots exist or if they are unreadable."""
    try:
        with db.begin_nested():
            return db.execute(text("SELECT max(version_no) FROM lending.lender_rule_snapshots WHERE created_at <= COALESCE(:at, now())"),
                              {"at": at}).scalar_one()
    except Exception as exc:  # class only: the snapshots migration may not have run
        logger.warning("[deal-ledger] rule version unavailable (%s)", type(exc).__name__)
        return None


_INSERT = text("""
    INSERT INTO lending.deal_audit_events
        (loan_file_id, event_type, actor, rule_version, diff, detail, ref_table, ref_id, dedupe_key, occurred_at)
    VALUES (:file, :type, :actor, :version, CAST(:diff AS jsonb), CAST(:detail AS jsonb), :ref_table, :ref_id, :dedupe,
            COALESCE(:at, now()))
    ON CONFLICT (dedupe_key) WHERE dedupe_key IS NOT NULL DO NOTHING
    RETURNING id
""")


def record(db: Session, loan_file_id: int, event_type: str, *, actor: str, diff: Optional[Mapping[str, Any]] = None,
           detail: Optional[Mapping[str, Any]] = None, ref_table: Optional[str] = None, ref_id: Any = None,
           dedupe_key: Optional[str] = None, occurred_at: Optional[datetime] = None,
           rule_version: Any = _UNSET) -> Optional[int]:
    """Append one event in the caller's transaction; returns its id, or None when ``dedupe_key`` was already recorded.
    Raises ``ValueError`` for an unknown event type or an untagged actor, and the database error if the write fails."""
    if event_type not in EVENT_TYPES:
        raise ValueError(f"unknown ledger event type {event_type!r}")
    if not _ACTOR.match(actor or ""):
        raise ValueError("ledger actor must be 'cora', 'human:<id>' or 'system:<source>'")
    version = rule_version_at(db, occurred_at) if rule_version is _UNSET else rule_version
    return db.execute(_INSERT, {
        "file": loan_file_id, "type": event_type, "actor": actor, "version": version,
        "diff": json.dumps(dict(diff or {}), default=str), "detail": json.dumps(dict(detail or {}), default=str),
        "ref_table": ref_table, "ref_id": None if ref_id is None else str(ref_id), "dedupe": dedupe_key, "at": occurred_at,
    }).scalar_one_or_none()


def record_safely(db: Session, loan_file_id: int, event_type: str, **fields: Any) -> Optional[int]:
    """``record`` in a savepoint: a failure is logged (class only) and rolled back alone, the caller's work is kept.
    For code that was live before the ledger, where an audit problem must not block the business step."""
    try:
        with db.begin_nested():
            return record(db, loan_file_id, event_type, **fields)
    except Exception as exc:  # class only: SQL errors embed bound parameters
        logger.error("[deal-ledger] %s for loan file %s not recorded (%s)", event_type, loan_file_id, type(exc).__name__)
        return None


_QUOTE = text("SELECT status, reason, result, created_at FROM lending.instant_quotes WHERE id = CAST(:q AS uuid)")


def record_quote(db: Session, loan_file_id: int, quote_id: str, *, actor: str = ACTOR_CORA) -> Optional[int]:
    """An underwriting quote that now belongs to this file.  Stamped with the moment the quote was made, so it sits in
    order, and with the rule version in force then.  Once per (file, quote)."""
    try:
        with db.begin_nested():
            quote = db.execute(_QUOTE, {"q": quote_id}).mappings().first()
    except Exception as exc:  # class only: a quote that cannot be read is simply not ledgered
        logger.error("[deal-ledger] quote for loan file %s not read (%s)", loan_file_id, type(exc).__name__)
        return None
    if quote is None:
        return None
    result = quote["result"] or {}
    return record_safely(
        db, loan_file_id, "quote_generated", actor=actor, occurred_at=quote["created_at"],
        detail={"quote_id": quote_id, "status": quote["status"], "reason": quote["reason"],
                "loan_amount": result.get("loan_amount"), "rehab_funding": result.get("rehab_funding"),
                "purchase_advance": result.get("purchase_advance"), "unconfirmed": result.get("unconfirmed")},
        ref_table="instant_quotes", ref_id=quote_id, dedupe_key=f"quote:{loan_file_id}:{quote_id}")


# ── Reading ────────────────────────────────────────────────────────────────────

def lifecycle(db: Session, loan_file_id: int) -> list[dict]:
    """Every event of a loan file, oldest first."""
    rows = db.execute(text(
        "SELECT id, event_type, actor, rule_version, diff, detail, ref_table, ref_id, occurred_at "
        "FROM lending.deal_audit_events WHERE loan_file_id = :id ORDER BY occurred_at, id"), {"id": loan_file_id}).mappings().all()
    return [dict(row) for row in rows]


# ── Cora's cards (catch-up pass) ───────────────────────────────────────────────

_RESOLVED_CARDS = """
WITH cards AS (
    SELECT a.action_id, a.tool_name, a.channel, a.summary, a.status, a.requested_by, a.decided_by, a.decided_at,
           a.created_at, a.payload ->> 'template_version' AS template_version, a.deal_ref, a.recipient_phone
      FROM lending.pending_actions a
     WHERE a.updated_at > now() - make_interval(days => :days)
),
resolved AS (
    SELECT c.*, CASE WHEN starts_with(c.deal_ref, 'ghl_opportunity:') THEN
        (SELECT min(f.id) FROM lending.loan_files f WHERE f.ghl_opportunity_id = substr(c.deal_ref, 17)
          HAVING count(*) = 1)
        ELSE COALESCE(
        CASE WHEN c.deal_ref ~ '^loan_file:[0-9]+$' THEN substr(c.deal_ref, 11)::bigint END,
        (SELECT d.loan_file_id FROM lending.doc_chases d
          WHERE c.deal_ref ~ '^doc_chase:[0-9]+$' AND d.id = substr(c.deal_ref, 11)::bigint),
        (SELECT min(f.id) FROM lending.loan_files f WHERE f.borrower_phone = c.recipient_phone
          HAVING count(*) = 1)) END AS loan_file_id
      FROM cards c
)
"""
_CARDS = text(_RESOLVED_CARDS + "SELECT * FROM resolved WHERE loan_file_id IS NOT NULL ORDER BY action_id")
_EDITS = text(_RESOLVED_CARDS + """
    SELECT e.id, e.action_id, e.revision_no, e.template_version, e.before, e.after, e.instruction, e.edited_by,
           e.edited_at, r.loan_file_id
      FROM lending.card_edits e JOIN resolved r ON r.action_id = e.action_id
     WHERE r.loan_file_id IS NOT NULL ORDER BY e.id
""")

_APPROVED_STATES = frozenset({"approved", "sending", "sent", "failed", "blocked"})


def _decider(decided_by: Optional[str]) -> tuple[str, bool]:
    """(ledger actor, auto-approved) for a card's ``decided_by``."""
    if decided_by == AUTO_GRADUATED_ACTOR:
        return system("standing_send"), True
    return (human(decided_by), False) if decided_by else (system("cora_card"), False)


def sync_card_events(db: Session, days: int = 3) -> int:
    """Ledger the drafts, approvals, rejections and edits of cards touched in the last ``days`` days that belong to a
    loan file.  Safe to run repeatedly (each event has a dedupe key).  Returns how many events were added.
    The caller commits."""
    added = 0
    for card in db.execute(_CARDS, {"days": days}).mappings():
        file_id = int(card["loan_file_id"])
        ref = {"ref_table": "pending_actions", "ref_id": card["action_id"]}
        base = {"action_id": card["action_id"], "tool_name": card["tool_name"], "channel": card["channel"],
                "template_version": card["template_version"]}
        events = [("cora_draft", ACTOR_CORA, {**base, "requested_by": card["requested_by"], "summary": (card["summary"] or "")[:200]},
                   f"card:{card['action_id']}:draft", card["created_at"])]
        if card["decided_at"] is not None and card["status"] in _APPROVED_STATES | {"rejected"}:
            actor, auto = _decider(card["decided_by"])
            kind = "card_rejected" if card["status"] == "rejected" else "card_approved"
            # Keyed by the decision's time: Standing Sends can send an automatic approval back to pending, and the
            # person's later approval or rejection is a separate decision that must not be skipped as a duplicate.
            events.append((kind, actor, {**base, "auto_approved": auto},
                           f"card:{card['action_id']}:decided:{card['decided_at'].isoformat()}", card["decided_at"]))
        for event_type, actor, detail, key, at in events:
            if record(db, file_id, event_type, actor=actor, detail=detail, dedupe_key=key, occurred_at=at, **ref) is not None:
                added += 1
    for edit in db.execute(_EDITS, {"days": days}).mappings():
        who = human(edit["edited_by"]) if edit["edited_by"] else system("revise")
        added += record(
            db, int(edit["loan_file_id"]), "human_edit", actor=who, diff=diff_fields(edit["before"] or {}, edit["after"] or {}),
            detail={"action_id": edit["action_id"], "revision_no": edit["revision_no"],
                    "template_version": edit["template_version"], "instruction": edit["instruction"]},
            ref_table="card_edits", ref_id=edit["id"], dedupe_key=f"card_edit:{edit['id']}", occurred_at=edit["edited_at"]) is not None
    if added:
        logger.info("[deal-ledger] %d card event(s) recorded", added)
    return added


# ── Command line ───────────────────────────────────────────────────────────────

def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Print a loan file's whole lifecycle from the audit ledger.")
    parser.add_argument("--loan-file", type=int, required=True)
    parser.add_argument("--json", action="store_true", help="print the rows as JSON")
    args = parser.parse_args(argv)
    from src.lending.db import lending_session

    with lending_session() as db:
        events = lifecycle(db, args.loan_file)
    if args.json:
        json.dump(events, sys.stdout, indent=1, default=str)
        sys.stdout.write("\n")
    else:
        sys.stdout.write("\n".join(describe(e) for e in events) + ("\n" if events else "(no events)\n"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
