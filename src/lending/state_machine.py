"""R1 item 0.1 — the 8-stage loan lifecycle, the relational loan state, and event idempotency.

INTAKE -> FIT -> SUBMITTED -> ACCEPTED -> TERM_SHEET_ISSUED -> CLOSING -> FUNDED -> PAID, one step forward at a time.
Every other jump raises ``IllegalStageTransitionError`` (the API answers HTTP 400).  A dead deal is not a stage: it
gets ``closed_reason`` / ``closed_at`` and its stage freezes where it died.

``lending.loan_files.stage`` is the source of truth.  ``loan_files.status`` (the partner-notice vocabulary) is derived
from it (``STATUS_FOR_STAGE``), so partner notices and the ledger export keep working unchanged.

Idempotency: every event carries a ``(source, source_event_id)`` key stored once in ``lending.loan_stage_events``.
A replayed event finds its row and changes nothing.  A move to the stage the file is already at is a no-op.  A
payment is stored once per ``(source, payment_id)`` and the balance is recalculated only when a new row went in.

Unknown financial inputs (rehab budget, purchase price, payoff balance) are None / NULL.  They are never 0.

None of these functions commit: the caller's transaction decides.  ``transition`` records a rejected event before it
raises, so a caller that wants the rejection kept (the API, a webhook) must commit in its ``except`` branch.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any, Mapping, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.lending.audit import deal_ledger
from src.lending.contracts import (
    LOAN_STAGE_ORDER,
    BorrowerRecord,
    DeclineReason,
    EntitySchema,
    LoanFile,
    LoanStage,
    PropertySchema,
)
from src.lending.cora_inbox import add_to_inbox_safely
from src.lending.partners import record_status_change
from src.services.phone_utils import normalize as normalize_phone

logger = logging.getLogger(__name__)

# Partner-notice vocabulary derived from the stage.  A closed file reads "declined".
STATUS_FOR_STAGE: dict[LoanStage, str] = {
    LoanStage.INTAKE: "received",
    LoanStage.FIT: "held",
    LoanStage.SUBMITTED: "routed",
    LoanStage.ACCEPTED: "approved",
    LoanStage.TERM_SHEET_ISSUED: "approved",
    LoanStage.CLOSING: "approved",
    LoanStage.FUNDED: "funded",
    LoanStage.PAID: "funded",
}
CLOSED_STATUS = "declined"

OUTCOME_APPLIED = "applied"
OUTCOME_NOOP = "noop"
OUTCOME_DUPLICATE = "duplicate"
OUTCOME_REJECTED = "rejected"


class LoanFileNotFound(LookupError):
    """No loan file with that id."""


class IllegalStageTransitionError(ValueError):
    """A stage jump the lifecycle does not allow (out of order, backwards, or on a closed file)."""

    def __init__(self, loan_file_id: int, current: LoanStage, target: LoanStage, reason: str) -> None:
        super().__init__(f"loan file {loan_file_id}: {current.value} -> {target.value} not allowed ({reason})")
        self.loan_file_id = loan_file_id
        self.current = current
        self.target = target
        self.reason = reason


@dataclass(frozen=True)
class TransitionResult:
    loan_file_id: int
    from_stage: LoanStage
    to_stage: LoanStage
    outcome: str  # applied / noop / duplicate

    @property
    def changed(self) -> bool:
        return self.outcome == OUTCOME_APPLIED


# ── Pure rules ─────────────────────────────────────────────────────────────────

def next_stage(stage: LoanStage) -> Optional[LoanStage]:
    """The one stage a file at ``stage`` may move to; None at PAID."""
    index = LOAN_STAGE_ORDER.index(stage)
    return LOAN_STAGE_ORDER[index + 1] if index + 1 < len(LOAN_STAGE_ORDER) else None


def check_transition(current: LoanStage, target: LoanStage, *, closed: bool = False) -> Optional[str]:
    """None when ``current -> target`` is a legal forward step; otherwise why it is not."""
    if closed:
        return "file_closed"
    if next_stage(current) is not target:
        return f"illegal_jump {current.value}->{target.value}"
    return None


def amount_or_none(raw: Any) -> Optional[int]:
    """A financial input from a loose source: a whole number, or None when unknown.  Blank, "unknown" and
    non-numeric input are None, never 0; a real 0 stays 0."""
    if raw is None or isinstance(raw, bool):
        return None
    if isinstance(raw, int):
        return raw
    if isinstance(raw, float):
        return int(raw) if raw == raw and raw not in (float("inf"), float("-inf")) else None
    cleaned = re.sub(r"[,$\s]", "", str(raw))
    if not cleaned or not re.fullmatch(r"-?\d+(\.\d+)?", cleaned):
        return None
    return int(float(cleaned))


def normalize_email(raw: Optional[str]) -> Optional[str]:
    cleaned = (raw or "").strip().lower()
    return cleaned or None


def _squash(value: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", value.lower())).strip()


def normalize_address(address: str, city: Optional[str] = None, state: Optional[str] = None,
                      zip_code: Optional[str] = None) -> str:
    """One key per property: punctuation and case removed, plus city, state and the 5-digit zip when known."""
    parts = [_squash(address), _squash(city or ""), _squash(state or ""), (zip_code or "").strip()[:5]]
    return " ".join(p for p in parts if p)


_ENTITY_SUFFIX = re.compile(r"\b(llc|l l c|inc|corp|corporation|ltd|lc|llp|lp)$")


def normalize_entity_name(name: str) -> str:
    """"Smith Holdings, LLC" and "SMITH HOLDINGS LLC" are one entity."""
    squashed = _squash(name)
    return _ENTITY_SUFFIX.sub("", squashed).strip() or squashed


def outstanding_after(payoff_balance: Optional[int], paid_total: int) -> Optional[int]:
    """Balance left after payments.  Unknown payoff stays unknown (None), never 0."""
    if payoff_balance is None:
        return None
    return max(payoff_balance - paid_total, 0)


# ── Stage transitions ──────────────────────────────────────────────────────────

def transition(db: Session, loan_file_id: int, to_stage: LoanStage | str, *, source: str, actor: str,
               source_event_id: Optional[str] = None, reason: Optional[str] = None) -> TransitionResult:
    """Move a file one step forward.

    * A replayed ``(source, source_event_id)`` returns ``duplicate`` and writes nothing.
    * A move to the current stage returns ``noop``.
    * Anything else that is not the next stage (or any move on a closed file) records a ``rejected`` event, queues
      a card for Cora's inbox, and raises ``IllegalStageTransitionError``.
    """
    target = LoanStage(to_stage)
    row = db.execute(text("SELECT stage, status, closed_at FROM lending.loan_files WHERE id = :id FOR UPDATE"),
                     {"id": loan_file_id}).first()
    if row is None:
        raise LoanFileNotFound(f"loan file {loan_file_id} not found")
    current = LoanStage(row.stage)
    closed = row.closed_at is not None

    why = None
    if closed:
        outcome, why = OUTCOME_REJECTED, "file_closed"
    elif target is current:
        outcome = OUTCOME_NOOP
    else:
        why = check_transition(current, target)
        outcome = OUTCOME_REJECTED if why else OUTCOME_APPLIED

    event_id = db.execute(
        text("INSERT INTO lending.loan_stage_events (loan_file_id, source, source_event_id, from_stage, to_stage, "
             "actor, outcome, reason) VALUES (:file, :source, :event, :from_stage, :to_stage, :actor, :outcome, :reason) "
             "ON CONFLICT (source, source_event_id) WHERE source_event_id IS NOT NULL DO NOTHING RETURNING id"),
        {"file": loan_file_id, "source": source, "event": source_event_id, "from_stage": current.value,
         "to_stage": target.value, "actor": actor, "outcome": outcome, "reason": (why or reason or None) and (why or reason)[:200]},
    ).scalar_one_or_none()
    if event_id is None:  # this event was already handled: change nothing
        return TransitionResult(loan_file_id, current, target, OUTCOME_DUPLICATE)

    ledger_actor = deal_ledger.tag_actor(actor, source=source)
    if outcome == OUTCOME_REJECTED:
        _park_rejected(db, event_id, loan_file_id, current, target, why or "", source)
        deal_ledger.record(db, loan_file_id, "stage_rejected", actor=ledger_actor,
                           diff={"stage": {"before": current.value, "after": target.value}},
                           detail={"source": source, "source_event_id": source_event_id, "reason": why},
                           ref_table="loan_stage_events", ref_id=event_id)
        raise IllegalStageTransitionError(loan_file_id, current, target, why or "")
    if outcome == OUTCOME_NOOP:
        return TransitionResult(loan_file_id, current, target, OUTCOME_NOOP)

    new_status = STATUS_FOR_STAGE[target]
    db.execute(
        text("UPDATE lending.loan_files SET stage = :stage, updated_at = now(), "
             "funded_at = CASE WHEN :stage = 'FUNDED' THEN COALESCE(funded_at, now()) ELSE funded_at END WHERE id = :id"),
        {"stage": target.value, "id": loan_file_id})
    if new_status != row.status:
        # The shared lifecycle write: status, its history and the partner notice (queued for the retry sweep). The
        # ledger event below records the move, so this write adds no second one.
        record_status_change(db, loan_file_id, new_status, reason or f"stage {target.value}", record_ledger=False)
    deal_ledger.record(db, loan_file_id, "stage_transition", actor=ledger_actor,
                       diff={"stage": {"before": current.value, "after": target.value},
                             **deal_ledger.diff_fields({"status": row.status}, {"status": new_status})},
                       detail={"source": source, "source_event_id": source_event_id, "reason": reason},
                       ref_table="loan_stage_events", ref_id=event_id)
    return TransitionResult(loan_file_id, current, target, OUTCOME_APPLIED)


def _park_rejected(db: Session, event_id: int, loan_file_id: int, current: LoanStage, target: LoanStage,
                   why: str, source: str) -> None:
    """Queue a card for Josh (through Cora's inbox): the rejected event is stored, nothing moved."""
    add_to_inbox_safely(
        db, event_type="loan_stage_rejected", dedup_key=f"loan-stage-rejected:{event_id}",
        summary=(f"File #{loan_file_id}: {source} asked for {target.value}, but the file is at {current.value} "
                 f"({why}). Step it through the missing stages to continue."),
        payload={"loan_file_id": loan_file_id, "from_stage": current.value, "to_stage": target.value,
                 "source": source, "reason": why})


def close_file(db: Session, loan_file_id: int, reason: DeclineReason | str, *, actor: str,
               source: str = "manual", source_event_id: Optional[str] = None) -> bool:
    """Mark a deal dead (declined, withdrew, unresponsive).  The stage stays where it is; every later move is
    rejected.  Returns False when the file was already closed or the event already seen."""
    code = DeclineReason(reason)
    row = db.execute(text("SELECT stage, status, closed_at FROM lending.loan_files WHERE id = :id FOR UPDATE"), {"id": loan_file_id}).first()
    if row is None:
        raise LoanFileNotFound(f"loan file {loan_file_id} not found")
    if row.closed_at is not None:
        return False
    event_id = db.execute(
        text("INSERT INTO lending.loan_stage_events (loan_file_id, source, source_event_id, from_stage, to_stage, actor, outcome, reason) "
             "VALUES (:file, :source, :event, :stage, :stage, :actor, 'applied', :reason) "
             "ON CONFLICT (source, source_event_id) WHERE source_event_id IS NOT NULL DO NOTHING RETURNING id"),
        {"file": loan_file_id, "source": source, "event": source_event_id, "stage": row.stage, "actor": actor,
         "reason": f"closed: {code.value}"}).scalar_one_or_none()
    if event_id is None:
        return False
    db.execute(text("UPDATE lending.loan_files SET closed_reason = :reason, closed_at = now(), updated_at = now() "
                    "WHERE id = :id AND closed_at IS NULL"), {"reason": code.value, "id": loan_file_id})
    # The shared lifecycle write records the status and queues the partner's "declined" notice.
    record_status_change(db, loan_file_id, CLOSED_STATUS, code.value, record_ledger=False)
    deal_ledger.record(db, loan_file_id, "closed", actor=deal_ledger.tag_actor(actor, source=source),
                       diff={"closed_reason": {"before": None, "after": code.value},
                             **deal_ledger.diff_fields({"status": row.status}, {"status": CLOSED_STATUS})},
                       detail={"stage": row.stage, "source": source, "source_event_id": source_event_id},
                       ref_table="loan_stage_events", ref_id=event_id)
    return True


# ── Payments ───────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class PaymentResult:
    loan_file_id: int
    recorded: bool  # False: this payment id was already stored, nothing recalculated
    outstanding_balance: Optional[int]
    paid: bool  # the file moved to PAID because of this payment


def record_payment(db: Session, loan_file_id: int, *, source: str, payment_id: str, amount: Optional[int] = None,
                   actor: str, mark_paid: bool = False) -> PaymentResult:
    """Store a payment once.  A duplicate ``(source, payment_id)`` inserts nothing and recalculates nothing.

    The balance is ``payoff_balance`` minus the payments (None while the payoff is unknown).  A FUNDED file moves
    to PAID when the balance reaches 0, or when ``mark_paid`` is set (v1: Josh moves the GHL deal to "Paid").
    """
    locked = db.execute(text("SELECT stage, payoff_balance, outstanding_balance FROM lending.loan_files WHERE id = :id FOR UPDATE"),
                        {"id": loan_file_id}).first()
    if locked is None:
        raise LoanFileNotFound(f"loan file {loan_file_id} not found")
    inserted = db.execute(
        text("INSERT INTO lending.loan_payments (loan_file_id, source, payment_id, amount) VALUES (:file, :source, :pid, :amount) "
             "ON CONFLICT (source, payment_id) DO NOTHING RETURNING id"),
        {"file": loan_file_id, "source": source, "pid": payment_id, "amount": amount}).scalar_one_or_none()
    if inserted is None:
        return PaymentResult(loan_file_id, False, locked.outstanding_balance, False)

    paid_total = db.execute(text("SELECT COALESCE(SUM(amount), 0) FROM lending.loan_payments WHERE loan_file_id = :id"),
                            {"id": loan_file_id}).scalar_one()
    balance = outstanding_after(locked.payoff_balance, int(paid_total))
    db.execute(text("UPDATE lending.loan_files SET outstanding_balance = :b, updated_at = now() WHERE id = :id"),
               {"b": balance, "id": loan_file_id})
    deal_ledger.record(db, loan_file_id, "payment_recorded", actor=deal_ledger.tag_actor(actor, source=source),
                       diff=deal_ledger.diff_fields({"outstanding_balance": locked.outstanding_balance},
                                                    {"outstanding_balance": balance}),
                       detail={"source": source, "payment_id": payment_id, "amount": amount},
                       ref_table="loan_payments", ref_id=inserted)

    paid = False
    if LoanStage(locked.stage) is LoanStage.FUNDED and (mark_paid or balance == 0):
        result = transition(db, loan_file_id, LoanStage.PAID, source=f"payment:{source}", actor=actor,
                            source_event_id=f"payment:{payment_id}")
        paid = result.changed
    return PaymentResult(loan_file_id, True, balance, paid)


# ── Ingest and lender submissions ──────────────────────────────────────────────

@dataclass(frozen=True)
class IngestResult:
    loan_file_id: int
    created: bool
    borrower_id: int
    property_id: int
    entity_id: Optional[int]


def _borrower_id(db: Session, borrower: BorrowerRecord) -> int:
    phone = normalize_phone(borrower.phone) if borrower.phone else None
    email = normalize_email(borrower.email)
    for column, value in (("phone", phone), ("email", email)):
        if value:
            found = db.execute(text(f"SELECT id FROM lending.borrower_profiles WHERE {column} = :v"), {"v": value}).scalar_one_or_none()
            if found:
                return int(found)
    return int(db.execute(
        text("INSERT INTO lending.borrower_profiles (first_name, last_name, phone, email, credit_band, schema_version) "
             "VALUES (:first, :last, :phone, :email, :band, :version) RETURNING id"),
        {"first": borrower.first_name[:80], "last": borrower.last_name[:80], "phone": phone, "email": email,
         "band": borrower.credit_band, "version": borrower.schema_version}).scalar_one())


def _property_id(db: Session, prop: PropertySchema) -> int:
    key = normalize_address(prop.address, prop.city, prop.state, prop.zip_code)
    row = db.execute(
        text("INSERT INTO lending.properties (address_norm, address, city, state, zip_code, schema_version) "
             "VALUES (:key, :address, :city, :state, :zip, :version) "
             "ON CONFLICT (address_norm) DO UPDATE SET address_norm = EXCLUDED.address_norm RETURNING id"),
        {"key": key, "address": prop.address[:200], "city": prop.city, "state": (prop.state or None) and prop.state.upper()[:2],
         "zip": prop.zip_code, "version": prop.schema_version}).scalar_one()
    return int(row)


def _entity_id(db: Session, entity: EntitySchema) -> int:
    key = normalize_entity_name(entity.name)
    return int(db.execute(
        text("INSERT INTO lending.entities (name_norm, name, state, sunbiz_status, schema_version) "
             "VALUES (:key, :name, :state, :sunbiz, :version) "
             "ON CONFLICT (name_norm) DO UPDATE SET name_norm = EXCLUDED.name_norm RETURNING id"),
        {"key": key, "name": entity.name[:200], "state": (entity.state or None) and entity.state.upper()[:2],
         "sunbiz": entity.sunbiz_status, "version": entity.schema_version}).scalar_one())


def ingest_loan_file(db: Session, loan: LoanFile, *, actor: str) -> IngestResult:
    """Create (or find) the file for this borrower and property.

    One borrower profile per phone/email, one property per normalized address, one loan file per
    (borrower, property).  Re-ingesting the same pair returns the existing file and changes nothing.
    """
    borrower_id = _borrower_id(db, loan.borrower)
    property_id = _property_id(db, loan.property)
    entity_id = _entity_id(db, loan.entity) if loan.entity else None

    existing = db.execute(text("SELECT id FROM lending.loan_files WHERE borrower_id = :b AND property_id = :p"),
                          {"b": borrower_id, "p": property_id}).scalar_one_or_none()
    if existing is not None:
        return IngestResult(int(existing), False, borrower_id, property_id, entity_id)

    prop, borrower = loan.property, loan.borrower
    file_id = int(db.execute(
        text("INSERT INTO lending.loan_files (source, borrower_first_name, borrower_last_name, borrower_phone, borrower_email, "
             "product, loan_amount, property_address, property_city, property_state, property_zip, purchase_price, "
             "rehab_budget, arv, credit_band, payoff_balance, stage, status, borrower_id, property_id, entity_id, schema_version) "
             "VALUES (:source, :first, :last, :phone, :email, :product, :loan_amount, :address, :city, :state, :zip, "
             ":purchase, :rehab, :arv, :band, :payoff, 'INTAKE', 'received', :b, :p, :e, :version) RETURNING id"),
        {"source": loan.source[:20], "first": borrower.first_name[:80], "last": borrower.last_name[:80],
         "phone": normalize_phone(borrower.phone) if borrower.phone else None, "email": normalize_email(borrower.email),
         "product": loan.product[:20], "loan_amount": loan.loan_amount, "address": prop.address[:200], "city": prop.city,
         "state": (prop.state or None) and prop.state.upper()[:2], "zip": prop.zip_code, "purchase": loan.purchase_price,
         "rehab": loan.rehab_budget, "arv": loan.arv, "band": borrower.credit_band, "payoff": loan.payoff_balance,
         "b": borrower_id, "p": property_id, "e": entity_id, "version": loan.schema_version}).scalar_one())
    db.execute(
        text("INSERT INTO lending.loan_stage_events (loan_file_id, source, from_stage, to_stage, actor, outcome, reason) "
             "VALUES (:file, 'ingest', NULL, 'INTAKE', :actor, 'applied', 'file created')"),
        {"file": file_id, "actor": actor})
    db.execute(text("INSERT INTO lending.loan_file_status_events (loan_file_id, status, reason) VALUES (:id, 'received', 'file created')"),
               {"id": file_id})
    deal_ledger.record(db, file_id, "file_created", actor=deal_ledger.tag_actor(actor, source="ingest"),
                       detail={"source": loan.source, "product": loan.product, "borrower_id": borrower_id,
                               "property_id": property_id, "entity_id": entity_id})
    return IngestResult(file_id, True, borrower_id, property_id, entity_id)


def record_lender_submission(db: Session, loan_file_id: int, lender_key: str, *, status: str = "submitted",
                             actor: Optional[str] = None) -> int:
    """One more attempt with a lender.  Always a new row: a re-submit to the same lender is attempt 2, 3, ..."""
    locked = db.execute(text("SELECT id FROM lending.loan_files WHERE id = :id FOR UPDATE"), {"id": loan_file_id}).scalar_one_or_none()
    if locked is None:
        raise LoanFileNotFound(f"loan file {loan_file_id} not found")
    attempt = db.execute(
        text("SELECT COALESCE(MAX(attempt_no), 0) + 1 FROM lending.lender_submissions WHERE loan_file_id = :id AND lender_key = :lender"),
        {"id": loan_file_id, "lender": lender_key}).scalar_one()
    submission_id = int(db.execute(
        text("INSERT INTO lending.lender_submissions (loan_file_id, lender_key, attempt_no, status) "
             "VALUES (:id, :lender, :attempt, :status) RETURNING id"),
        {"id": loan_file_id, "lender": lender_key, "attempt": attempt, "status": status}).scalar_one())
    deal_ledger.record(db, loan_file_id, "lender_submission", actor=deal_ledger.tag_actor(actor, source="lender_submission"),
                       detail={"lender_key": lender_key, "attempt_no": attempt, "status": status},
                       ref_table="lender_submissions", ref_id=submission_id)
    return submission_id


# ── Field edits ────────────────────────────────────────────────────────────────

AMOUNT_FIELDS = frozenset({"loan_amount", "purchase_price", "rehab_budget", "arv", "payoff_balance"})
EDITABLE_FIELDS = AMOUNT_FIELDS | {"property_address", "property_city", "property_state", "property_zip", "credit_band",
                                   "completed_projects", "timeline", "occupancy", "notes"}


def update_fields(db: Session, loan_file_id: int, changes: Mapping[str, Any], *, actor: str,
                  source: str = "manual") -> dict[str, dict]:
    """Change fields of a loan file and record exactly what changed, who changed it and under which rule version.

    Only ``EDITABLE_FIELDS`` can be edited (raises ``ValueError`` otherwise).  An amount that is blank or not a number
    becomes None (unknown), never 0.  A new ``payoff_balance`` recalculates ``outstanding_balance`` from the payments
    already stored.  Returns the diff (empty, and nothing written, when no value changed).  The caller commits."""
    unknown = set(changes) - EDITABLE_FIELDS
    if unknown:
        raise ValueError(f"not editable: {sorted(unknown)}")
    wanted = {name: (amount_or_none(value) if name in AMOUNT_FIELDS else value) for name, value in changes.items()}
    columns = ", ".join(sorted(set(wanted) | ({"payoff_balance", "outstanding_balance"} if "payoff_balance" in wanted else set())))
    row = db.execute(text(f"SELECT {columns} FROM lending.loan_files WHERE id = :id FOR UPDATE"), {"id": loan_file_id}).mappings().first()
    if row is None:
        raise LoanFileNotFound(f"loan file {loan_file_id} not found")
    diff = deal_ledger.diff_fields({name: row[name] for name in wanted}, wanted)
    if not diff:
        return {}
    new_values = {name: change["after"] for name, change in diff.items()}
    if "payoff_balance" in diff:
        paid_total = db.execute(text("SELECT COALESCE(SUM(amount), 0) FROM lending.loan_payments WHERE loan_file_id = :id"),
                                {"id": loan_file_id}).scalar_one()
        balance = outstanding_after(new_values["payoff_balance"], int(paid_total))
        diff.update(deal_ledger.diff_fields({"outstanding_balance": row["outstanding_balance"]}, {"outstanding_balance": balance}))
        new_values["outstanding_balance"] = balance
    assignments = ", ".join(f"{name} = :{name}" for name in sorted(new_values))  # names come from EDITABLE_FIELDS
    db.execute(text(f"UPDATE lending.loan_files SET {assignments}, updated_at = now() WHERE id = :id"),
               {**new_values, "id": loan_file_id})
    deal_ledger.record(db, loan_file_id, "field_update", actor=deal_ledger.tag_actor(actor, source=source), diff=diff,
                       detail={"source": source})
    return diff
