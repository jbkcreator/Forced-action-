"""Lender rules from the database (T-05, Josh A1): a field edit or a "confirmed" mark without a deploy.

``config/lender_matrix.py`` still defines the lenders, the field names and the defaults. When
``LENDING_RULES_FROM_DB`` is on, each row of ``lending.lender_rule_values`` overrides one field's value
and status for one lender. Anything the table does not cover keeps its config value, so an empty or
missing table behaves exactly like the config alone.

A row that fails validation is skipped and logged at ERROR, so a typo in an edit never silently turns a
cap off; if the merged matrix is itself invalid the whole table is ignored. A lender key the config does
not know is ignored (adding a lender is still a code change).

A row whose ``effective_from`` is after today (ET) is not applied yet; until then that field keeps its config
value and status.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, replace
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.lender_matrix import (
    LENDER_MATRIX,
    PARAMETER_FIELDS,
    LenderRules,
    field_status_of,
    validate_lender_matrix,
)
from config.settings import get_settings
from src.lending.contracts import FieldStatus, LoanType
from src.lending.cora_inbox import add_to_inbox_safely
from src.lending.models import LENDING_SCHEMA

logger = logging.getLogger(__name__)

_DECIMAL_FIELDS = frozenset({
    "min_loan_amount", "max_loan_amount", "max_ltc", "max_ltv", "min_purchase_price",
    "origination_points", "rate_spread",
})
_NULLABLE_INT_FIELDS = frozenset({"credit_floor"})
_INT_FIELDS = {"min_completed_projects": 0, "hold_months": 1}  # field -> minimum
_SET_FIELDS = frozenset({"loan_types", "approved_states", "allowed_property_types"})
_BOOL_FIELDS = frozenset({"requires_ground_up_build"})

_SELECT_SQL = (
    f"SELECT lender_key, field, value, status FROM {LENDING_SCHEMA}.lender_rule_values"
    " WHERE effective_from <= (now() AT TIME ZONE 'America/New_York')::date"
)


class RuleValueError(ValueError):
    """A stored rule value or status that cannot be used."""


def encode_value(value: Any) -> Any:
    """The JSON form of a ``LenderRules`` parameter, as stored in ``lender_rule_values.value``."""
    if isinstance(value, frozenset):
        return sorted(item.value if isinstance(item, LoanType) else str(item) for item in value)
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    return value


def _decimal(raw: Any) -> Optional[Decimal]:
    if raw is None:
        return None
    if isinstance(raw, bool) or not isinstance(raw, (int, float, str)):
        raise RuleValueError("expected a number or null")
    try:
        number = Decimal(str(raw).strip())
    except InvalidOperation as exc:
        raise RuleValueError("expected a number or null") from exc
    if not number.is_finite():
        raise RuleValueError("expected a finite number")
    return number


def _int(raw: Any, *, minimum: Optional[int], nullable: bool) -> Optional[int]:
    if raw is None and nullable:
        return None
    number = _decimal(raw)
    if number is None or number != number.to_integral_value():
        raise RuleValueError("expected a whole number")
    if minimum is not None and number < minimum:
        raise RuleValueError(f"must be at least {minimum}")
    return int(number)


def _set(field: str, raw: Any) -> frozenset:
    if not isinstance(raw, list) or not all(isinstance(item, str) for item in raw):
        raise RuleValueError("expected a list of strings")
    if field == "loan_types":
        try:
            return frozenset(LoanType(item) for item in raw)
        except ValueError as exc:
            raise RuleValueError(f"unknown loan type in {raw}") from exc
    if field == "approved_states":
        return frozenset(item.strip().upper() for item in raw)
    return frozenset(raw)


def decode_value(field: str, raw: Any) -> Any:
    """The ``LenderRules`` value for a stored JSON value. Raises ``RuleValueError`` when unusable."""
    if field in _DECIMAL_FIELDS:
        return _decimal(raw)
    if field in _NULLABLE_INT_FIELDS:
        return _int(raw, minimum=0, nullable=True)
    if field in _INT_FIELDS:
        return _int(raw, minimum=_INT_FIELDS[field], nullable=False)
    if field in _SET_FIELDS:
        return _set(field, raw)
    if field in _BOOL_FIELDS:
        if not isinstance(raw, bool):
            raise RuleValueError("expected true or false")
        return raw
    raise RuleValueError("unknown field")


def _decode_row(field: str, raw_value: Any, raw_status: Any) -> tuple[Any, FieldStatus]:
    if field not in PARAMETER_FIELDS:
        raise RuleValueError("unknown field")
    try:
        status = FieldStatus(raw_status)
    except ValueError as exc:
        raise RuleValueError(f"unknown status {raw_status!r}") from exc
    return decode_value(field, raw_value), status


def apply_rows(base: tuple[LenderRules, ...], rows: Iterable[tuple[str, str, Any, str]]) -> tuple[LenderRules, ...]:
    """``base`` with each valid ``(lender_key, field, value, status)`` row applied.

    A lender is ``verified`` only when every one of its fields ends up confirmed.
    """
    known = {rules.key for rules in base}
    overrides: dict[str, dict[str, tuple[Any, FieldStatus]]] = {}
    for lender_key, field, raw_value, raw_status in rows:
        if lender_key not in known:
            logger.warning("[lender-rules] row ignored: unknown lender %s", lender_key)
            continue
        try:
            overrides.setdefault(lender_key, {})[field] = _decode_row(field, raw_value, raw_status)
        except RuleValueError as exc:
            logger.error("[lender-rules] row skipped: lender=%s field=%s: %s", lender_key, field, exc)

    merged = []
    for rules in base:
        changes = overrides.get(rules.key)
        if not changes:
            merged.append(rules)
            continue
        statuses = {name: field_status_of(rules, name) for name in PARAMETER_FIELDS}
        statuses.update({name: status for name, (_, status) in changes.items()})
        merged.append(replace(
            rules,
            **{name: value for name, (value, _) in changes.items()},
            field_status=statuses,
            verified=all(status is FieldStatus.CONFIRMED for status in statuses.values()),
        ))
    result = tuple(merged)
    try:
        validate_lender_matrix(result)
    except ValueError as exc:
        logger.error("[lender-rules] table ignored, merged rules are invalid: %s", exc)
        return base
    return result


def load_matrix(session: Session, base: tuple[LenderRules, ...] = LENDER_MATRIX) -> tuple[LenderRules, ...]:
    """The matrix with the table applied; ``base`` when the table is missing or cannot be read."""
    try:
        with session.begin_nested():
            rows = [(r.lender_key, r.field, r.value, r.status) for r in session.execute(text(_SELECT_SQL))]
    except Exception:
        logger.exception("[lender-rules] could not read lender_rule_values; using config/lender_matrix.py")
        return base
    return apply_rows(base, rows)


def current_matrix() -> tuple[LenderRules, ...]:
    """The matrix the evaluators use: the table when ``LENDING_RULES_FROM_DB`` is on, else the config."""
    if not get_settings().lending_rules_from_db:
        return LENDER_MATRIX
    try:
        from src.core.database import get_db_context

        with get_db_context() as session:
            return load_matrix(session)
    except Exception:
        logger.exception("[lender-rules] database unavailable; using config/lender_matrix.py")
        return LENDER_MATRIX


def routing_priorities() -> dict[str, int]:
    """T-05 lender key -> R-1 ``/lenders`` priority (lower first), the cost tie-break in ``evaluate_lender_fit``.

    A key linked from several R-1 lenders takes the best priority. Empty when the database is unavailable,
    so equal costs keep matrix order rather than failing the evaluation.
    """
    try:
        from src.core.database import get_db_context

        with get_db_context() as session:
            rows = session.execute(text(
                "SELECT key, MIN(priority) AS priority FROM lending.lender_routing_rules, unnest(rule_keys) AS key "
                "GROUP BY key"
            )).all()
    except Exception:
        logger.exception("[lender-rules] routing priorities unavailable; equal costs keep matrix order")
        return {}
    return {row.key: int(row.priority) for row in rows}


# ── R1 0.7: versioned snapshots, validated writes, rollback ────────────────────
# ``lending.lender_rule_snapshots`` holds the full rule set after every change (written by a trigger on
# ``lender_rule_values``, so a hand-edited SQL change is versioned too; migrations/apply_lending_lender_rule_snapshots.py).
# A rollback copies an old snapshot back and is itself a new snapshot: history is never rewritten.

SNAPSHOT_LOCK_KEY = 4207001  # pg_advisory_xact_lock key shared with the snapshot function in the migration

_FRACTION_FIELDS = frozenset({"max_ltc", "max_ltv"})
_AMOUNT_FIELDS = frozenset({"min_loan_amount", "max_loan_amount", "min_purchase_price"})


class SnapshotNotFound(LookupError):
    """No snapshot with that version number."""


@dataclass(frozen=True)
class RollbackResult:
    from_version: Optional[int]
    to_version: int
    new_version: int

    @property
    def changed(self) -> bool:
        return self.new_version != self.from_version


def check_rule_change(matrix: tuple[LenderRules, ...], lender_key: str, field: str, raw_value: Any,
                      raw_status: Any) -> tuple[Any, FieldStatus]:
    """Refuse a value that is hard-invalid: wrong type, an unknown lender or field, leverage outside (0, 1], a
    negative amount, or a set of rules that no longer validates.  A value that passes but is wrong for the business
    (Central max LTV 0.90 typed as 0.99) is accepted: that is what a rollback is for."""
    target = next((rules for rules in matrix if rules.key == lender_key), None)
    if target is None:
        raise RuleValueError("unknown lender")
    value, status = _decode_row(field, raw_value, raw_status)
    if field in _FRACTION_FIELDS and value is not None and not (0 < value <= 1):
        raise RuleValueError(f"{field} must be above 0 and at most 1")
    if field in _AMOUNT_FIELDS and value is not None and value < 0:
        raise RuleValueError(f"{field} must not be negative")
    statuses = {name: field_status_of(target, name) for name in PARAMETER_FIELDS}
    statuses[field] = status
    candidate = replace(target, **{field: value}, field_status=statuses,
                        verified=all(s is FieldStatus.CONFIRMED for s in statuses.values()))
    try:
        validate_lender_matrix(tuple(candidate if rules.key == lender_key else rules for rules in matrix))
    except ValueError as exc:
        raise RuleValueError(str(exc)) from exc
    return value, status


def _tag_session(session: Session, actor: str, reason: str) -> None:
    """Who and why for the snapshot the next change writes (transaction-local settings read by the trigger)."""
    session.execute(text("SELECT set_config('lending.rule_actor', :actor, true), set_config('lending.rule_reason', :reason, true)"),
                    {"actor": actor[:80], "reason": reason[:200]})


def set_rule_value(session: Session, lender_key: str, field: str, value: Any, *, status: str, actor: str,
                   reason: str, note: Optional[str] = None) -> Optional[int]:
    """Validate then store one rule value; the trigger writes the new snapshot.  Returns the version now in force.
    Raises ``RuleValueError`` for a hard-invalid value; nothing is written.  The caller commits."""
    check_rule_change(load_matrix(session), lender_key, field, value, status)
    _tag_session(session, actor, reason)
    session.execute(
        text("INSERT INTO lending.lender_rule_values (lender_key, field, value, status, note, updated_by) "
             "VALUES (:lender, :field, CAST(:value AS jsonb), :status, :note, :actor) "
             "ON CONFLICT (lender_key, field) DO UPDATE SET value = EXCLUDED.value, status = EXCLUDED.status, "
             "note = EXCLUDED.note, updated_by = EXCLUDED.updated_by, updated_at = now()"),
        {"lender": lender_key, "field": field, "value": json.dumps(value), "status": status, "note": note, "actor": actor[:60]})
    return current_rule_version(session)


def current_rule_version(session: Session) -> Optional[int]:
    """The rule version in force: the newest snapshot (what the audit ledger records).  None before the first."""
    return session.execute(text("SELECT max(version_no) FROM lending.lender_rule_snapshots")).scalar_one()


def list_snapshots(session: Session, limit: int = 20) -> list[dict]:
    rows = session.execute(
        text("SELECT version_no, created_by, reason, created_at FROM lending.lender_rule_snapshots "
             "ORDER BY version_no DESC LIMIT :limit"), {"limit": limit}).mappings().all()
    return [dict(row) for row in rows]


def get_snapshot(session: Session, version_no: int) -> list[dict]:
    """The rule rows of one snapshot.  Raises ``SnapshotNotFound``."""
    rules = session.execute(text("SELECT rules FROM lending.lender_rule_snapshots WHERE version_no = :v"),
                            {"v": version_no}).scalar_one_or_none()
    if rules is None:
        raise SnapshotNotFound(f"no snapshot version {version_no}")
    return list(rules)


def current_rules(session: Session) -> list[dict]:
    """The table now, in the snapshot's row shape."""
    rows = session.execute(text(
        "SELECT lender_key, field, value, status, note, updated_by, effective_from::text AS effective_from "
        "FROM lending.lender_rule_values ORDER BY lender_key, field")).mappings().all()
    return [dict(row) for row in rows]


def diff_rules(old: Iterable[dict], new: Iterable[dict]) -> list[str]:
    """One line per rule that differs between two row lists (``old`` -> ``new``); empty when identical."""
    def key(row: dict) -> tuple[str, str]:
        return row["lender_key"], row["field"]

    def shown(row: Optional[dict]) -> str:
        if row is None:
            return "(none)"
        start = f" from {row['effective_from']}" if row.get("effective_from") else ""
        return f"{json.dumps(row['value'])} [{row['status']}]{start}"

    before, after = {key(r): r for r in old}, {key(r): r for r in new}
    lines = []
    for name in sorted(before.keys() | after.keys()):
        a, b = before.get(name), after.get(name)
        if a is None or b is None or _applied(a) != _applied(b):
            lines.append(f"{name[0]}.{name[1]}: {shown(a)} -> {shown(b)}")
    return lines


def _applied(row: dict) -> tuple:
    """What a rule row means for quotes and routing: value, status and start date (the live reader skips a rule
    whose start date is in the future). ``note`` and ``updated_by`` are bookkeeping."""
    return row["value"], row["status"], row.get("effective_from")


def _same_rules(old: Iterable[dict], new: Iterable[dict]) -> bool:
    """True when two rule lists hold the same rules (value, status, start date and note); who last wrote them is
    ignored, so restoring the rules in force as another operator is not a change."""
    def content(rows: Iterable[dict]) -> dict:
        return {(r["lender_key"], r["field"]): (*_applied(r), r.get("note")) for r in rows}

    return content(old) == content(new)


def _lock(session: Session) -> None:
    """The rule write lock (the snapshot trigger takes the same one), held until the transaction ends."""
    session.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": SNAPSHOT_LOCK_KEY})


_DELETE_NOT_IN = text("""
    DELETE FROM lending.lender_rule_values v
     WHERE NOT EXISTS (SELECT 1 FROM jsonb_to_recordset(CAST(:rules AS jsonb)) AS x(lender_key text, field text)
                        WHERE x.lender_key = v.lender_key AND x.field = v.field)
""")
_UPSERT_ALL = text("""
    INSERT INTO lending.lender_rule_values (lender_key, field, value, status, note, updated_by, effective_from)
    SELECT x.lender_key, x.field, item -> 'value', x.status, x.note, :actor, x.effective_from
      FROM jsonb_array_elements(CAST(:rules AS jsonb)) AS item
      CROSS JOIN LATERAL jsonb_to_record(item)
           AS x(lender_key text, field text, status text, note text, updated_by text, effective_from date)
    ON CONFLICT (lender_key, field) DO UPDATE
       SET value = EXCLUDED.value, status = EXCLUDED.status, note = EXCLUDED.note,
           updated_by = EXCLUDED.updated_by, updated_at = now(), effective_from = EXCLUDED.effective_from
""")


def replace_rules(session: Session, rules: list[dict], *, actor: str, reason: str) -> int:
    """Make ``lender_rule_values`` exactly ``rules`` and write one snapshot for it (none when the table already
    matched the newest snapshot, or already holds ``rules`` apart from who wrote them).  Returns the version now in
    force.  The caller commits."""
    _lock(session)
    in_force = current_rule_version(session)
    if in_force is not None and _same_rules(current_rules(session), rules):
        return in_force
    payload = json.dumps(rules)
    session.execute(text("SELECT set_config('lending.rule_snapshot_skip', 'on', true)"))
    try:
        session.execute(_DELETE_NOT_IN, {"rules": payload})
        session.execute(_UPSERT_ALL, {"rules": payload, "actor": actor[:60]})
    finally:
        session.execute(text("SELECT set_config('lending.rule_snapshot_skip', 'off', true)"))
    return int(session.execute(text("SELECT lending.write_lender_rule_snapshot(:actor, :reason)"),
                               {"actor": actor[:80], "reason": reason[:200]}).scalar_one())


def rollback_to(session: Session, version_no: int, *, actor: str, reason: Optional[str] = None) -> RollbackResult:
    """Restore the rules of snapshot ``version_no``.  Written as a new snapshot "rollback to vN" (history is kept),
    posted to Cora's inbox, and a no-op when the table already holds those rules.  The caller commits."""
    _lock(session)  # before reading the version replaced, so an edit committed meanwhile is the one reported
    rules = get_snapshot(session, version_no)
    before = current_rule_version(session)
    new_version = replace_rules(session, rules, actor=actor, reason=reason or f"rollback to v{version_no}")
    result = RollbackResult(before, version_no, new_version)
    if result.changed:
        announce_rollback(session, actor, result)
    return result


def announce_rollback(session: Session, actor: str, result: RollbackResult) -> None:
    """Queue the rollback for Cora's channel (#cora): who, from which version, to which."""
    add_to_inbox_safely(
        session, event_type="lender_rules_rolled_back", dedup_key=f"lender-rules-rollback:{result.new_version}",
        summary=(f"Lender rules rolled back by {actor}: v{result.from_version} replaced by the rules of "
                 f"v{result.to_version} (now v{result.new_version})."),
        payload={"actor": actor, "from_version": result.from_version, "to_version": result.to_version,
                 "new_version": result.new_version})
