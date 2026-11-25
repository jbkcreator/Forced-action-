"""R1 0.9 Shadow store — the ONLY module that writes shadow tables.

No other module in the codebase ever INSERTs or UPDATEs ``lending.shadow_classes`` or
``lending.shadow_decisions``.  Suppression by construction (D3): this module never
receives a ``SendGate``, ``PendingActionQueue`` or write session other than its own.

Public interface
----------------
set_mode(db, class_name, mode, changed_by) → None
    Set a class's mode (off | shadow | live).  Creates the row if absent.

get_mode(db, class_name) → str | None
    "off" | "shadow" | "live", or None when the class has no row.

get_all_shadow_classes(db) → list[dict]
    All rows in shadow_classes, for the CLI list command.

insert_decision(db, *, class_name, class_version, subject_kind, subject_ref,
                inputs, rule_version, confidence, proposed) → int | None
    Insert-if-absent (unique guard on class_name + subject_ref).  Returns the row id,
    or None when the row already existed (sweep is idempotent).

fill_actual(db, subject_ref, actual, decided_at, decision_seconds, edit_distance,
            send_result) → int
    Fill in the human decision on all shadow rows for this subject_ref.  Returns rows updated.

fill_outcome(db, subject_ref, deal_outcome, outcome_at) → int
    Fill in the settled deal outcome.  Returns rows updated.

rows_for_digest(db, class_name, since, until) → list[dict]
    Rows for the shadow digest section, with decided_at between since and until.

shadow_sent_count(db) → int
    Count of pending_actions whose requested_by starts with "shadow:" (must always be 0).
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

_UPSERT_CLASS = text("""
    INSERT INTO lending.shadow_classes (class_name, mode, started_at, ended_at, changed_by, updated_at)
    VALUES (:class_name, :mode,
            CASE WHEN :mode = 'shadow' THEN now() ELSE NULL END,
            CASE WHEN :mode = 'off' THEN now() ELSE NULL END,
            :changed_by,
            now())
    ON CONFLICT (class_name) DO UPDATE
        SET mode       = EXCLUDED.mode,
            started_at = CASE WHEN EXCLUDED.mode = 'shadow' THEN now() ELSE lending.shadow_classes.started_at END,
            ended_at   = CASE WHEN EXCLUDED.mode = 'off'    THEN now() ELSE lending.shadow_classes.ended_at END,
            changed_by = EXCLUDED.changed_by,
            updated_at = now()
""")

_GET_MODE = text("SELECT mode, started_at FROM lending.shadow_classes WHERE class_name = :class_name")

_GET_ALL = text("SELECT class_name, mode, started_at, ended_at, changed_by, updated_at FROM lending.shadow_classes ORDER BY class_name")

_INSERT_DECISION = text("""
    INSERT INTO lending.shadow_decisions
        (class_name, class_version, subject_kind, subject_ref, inputs, rule_version, confidence, proposed)
    VALUES
        (:class_name, :class_version, :subject_kind, :subject_ref,
         CAST(:inputs AS jsonb), :rule_version, :confidence, CAST(:proposed AS jsonb))
    ON CONFLICT (class_name, subject_ref) DO NOTHING
    RETURNING id
""")

_FILL_ACTUAL = text("""
    UPDATE lending.shadow_decisions
       SET actual           = :actual,
           decision_match   = (proposed->>'decision' = :actual),
           decided_at       = :decided_at,
           decision_seconds = :decision_seconds,
           edit_distance    = :edit_distance,
           send_result      = :send_result
     WHERE subject_ref = :subject_ref
       AND actual IS NULL
""")

_FILL_OUTCOME = text("""
    UPDATE lending.shadow_decisions
       SET deal_outcome = :deal_outcome,
           outcome_at   = :outcome_at
     WHERE subject_ref = :subject_ref
       AND deal_outcome IS NULL
""")

_ROWS_FOR_DIGEST = text("""
    SELECT id, class_name, class_version, subject_kind, subject_ref, proposed, confidence,
           actual, decision_match, decided_at, decision_seconds, edit_distance,
           send_result, deal_outcome, outcome_at, created_at,
           payload->>'first_name' AS first_name,
           inputs->>'message_type' AS message_type
      FROM lending.shadow_decisions
     WHERE class_name = :class_name
       AND (:since::timestamptz IS NULL OR created_at >= :since)
       AND (:until::timestamptz IS NULL OR created_at < :until)
     ORDER BY created_at
""")

_SHADOW_SENT_COUNT = text("""
    SELECT COUNT(*) FROM lending.pending_actions
     WHERE requested_by LIKE 'shadow:%'
""")


def set_mode(db: Session, class_name: str, mode: str, changed_by: str = "cli") -> None:
    """Set a class's mode.  Creates the row if absent."""
    if mode not in ("off", "shadow", "live"):
        raise ValueError(f"invalid mode {mode!r}; must be off | shadow | live")
    db.execute(_UPSERT_CLASS, {"class_name": class_name, "mode": mode, "changed_by": changed_by})


def get_mode(db: Session, class_name: str) -> Optional[str]:
    """Return current mode, or None when the class has no row."""
    row = db.execute(_GET_MODE, {"class_name": class_name}).mappings().first()
    return row["mode"] if row else None


def get_started_at(db: Session, class_name: str) -> Optional[datetime]:
    """Return started_at timestamp, or None."""
    row = db.execute(_GET_MODE, {"class_name": class_name}).mappings().first()
    return row["started_at"] if row else None


def get_all_shadow_classes(db: Session) -> list[dict]:
    return [dict(r) for r in db.execute(_GET_ALL).mappings()]


def insert_decision(
    db: Session,
    *,
    class_name: str,
    class_version: int,
    subject_kind: str,
    subject_ref: str,
    inputs: dict[str, Any],
    rule_version: Optional[int],
    confidence: Optional[float],
    proposed: dict[str, Any],
) -> Optional[int]:
    """Insert-if-absent.  Returns the new row id, or None when the row already existed."""
    import json as _json
    row = db.execute(
        _INSERT_DECISION,
        {
            "class_name": class_name,
            "class_version": class_version,
            "subject_kind": subject_kind,
            "subject_ref": subject_ref,
            "inputs": _json.dumps(inputs),
            "rule_version": rule_version,
            "confidence": confidence,
            "proposed": _json.dumps(proposed),
        },
    ).mappings().first()
    return row["id"] if row else None


def fill_actual(
    db: Session,
    subject_ref: str,
    actual: str,
    decided_at: Optional[datetime],
    decision_seconds: Optional[float],
    edit_distance: Optional[float],
    send_result: Optional[str],
) -> int:
    """Fill the human decision for all shadow rows for this subject_ref.  Returns rows updated."""
    result = db.execute(
        _FILL_ACTUAL,
        {
            "subject_ref": subject_ref,
            "actual": actual,
            "decided_at": decided_at,
            "decision_seconds": decision_seconds,
            "edit_distance": edit_distance,
            "send_result": send_result,
        },
    )
    return result.rowcount


def fill_outcome(db: Session, subject_ref: str, deal_outcome: str, outcome_at: datetime) -> int:
    """Fill in the settled deal outcome.  Returns rows updated."""
    result = db.execute(_FILL_OUTCOME, {"subject_ref": subject_ref, "deal_outcome": deal_outcome, "outcome_at": outcome_at})
    return result.rowcount


def rows_for_digest(
    db: Session,
    class_name: str,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
) -> list[dict]:
    return [dict(r) for r in db.execute(_ROWS_FOR_DIGEST, {"class_name": class_name, "since": since, "until": until}).mappings()]


def shadow_sent_count(db: Session) -> int:
    """Count pending_actions whose requested_by starts with 'shadow:'.  Must always be 0."""
    return db.execute(_SHADOW_SENT_COUNT).scalar_one()
