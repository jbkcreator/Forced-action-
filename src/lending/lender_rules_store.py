"""Lender rules from the database (T-05, Josh A1): a field edit or a "confirmed" mark without a deploy.

``config/lender_matrix.py`` still defines the lenders, the field names and the defaults. When
``LENDING_RULES_FROM_DB`` is on, each row of ``lending.lender_rule_values`` overrides one field's value
and status for one lender. Anything the table does not cover keeps its config value, so an empty or
missing table behaves exactly like the config alone.

A row that fails validation is skipped and logged at ERROR, so a typo in an edit never silently turns a
cap off; if the merged matrix is itself invalid the whole table is ignored. A lender key the config does
not know is ignored (adding a lender is still a code change).
"""
from __future__ import annotations

import logging
from dataclasses import replace
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

_SELECT_SQL = f"SELECT lender_key, field, value, status FROM {LENDING_SCHEMA}.lender_rule_values"


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
