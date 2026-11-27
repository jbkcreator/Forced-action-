"""R1 0.9 shadow store: the only module that writes ``lending.shadow_classes`` and ``lending.shadow_decisions``."""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.lending.slack_names import names_by_phone

MODES = ("off", "shadow", "live")

_UPSERT_CLASS = text("""
    INSERT INTO lending.shadow_classes (class_name, mode, started_at, ended_at, changed_by, updated_at)
    VALUES (:class_name, :mode,
            CASE WHEN :mode = 'shadow' THEN now() END,
            CASE WHEN :mode = 'off' THEN now() END,
            :changed_by, now())
    ON CONFLICT (class_name) DO UPDATE
        SET mode       = EXCLUDED.mode,
            started_at = CASE WHEN EXCLUDED.mode = 'shadow' THEN now() ELSE lending.shadow_classes.started_at END,
            ended_at   = CASE WHEN EXCLUDED.mode = 'off' THEN now()
                              WHEN EXCLUDED.mode = 'shadow' THEN NULL
                              ELSE lending.shadow_classes.ended_at END,
            changed_by = EXCLUDED.changed_by,
            updated_at = now()
""")

_GET_CLASS = text("SELECT mode, started_at FROM lending.shadow_classes WHERE class_name = :class_name")

_GET_ALL = text("SELECT class_name, mode, started_at, ended_at, changed_by, updated_at "
                "FROM lending.shadow_classes ORDER BY class_name")

_INSERT_DECISION = text("""
    INSERT INTO lending.shadow_decisions
        (class_name, class_version, subject_kind, subject_ref, inputs, rule_version, confidence, proposed)
    VALUES (:class_name, :class_version, :subject_kind, :subject_ref,
            CAST(:inputs AS jsonb), :rule_version, :confidence, CAST(:proposed AS jsonb))
    ON CONFLICT (class_name, subject_ref) DO NOTHING
    RETURNING id
""")

# Settled = a real decision, sent or rejected, on a funded deal. A declined deal stays open: the file can reopen and fund.
_OPEN_ROWS = text("""
    SELECT subject_ref, proposed, actual, send_result, deal_outcome, created_at
      FROM lending.shadow_decisions
     WHERE class_name = :class_name
       AND created_at >= CAST(:since AS timestamptz)
       AND NOT (COALESCE(deal_outcome, '') = 'funded' AND COALESCE(actual, '') IN ('approve', 'edit', 'reject')
                AND (actual = 'reject' OR COALESCE(send_result, '') IN ('sent', 'failed')))
     ORDER BY created_at
""")

_UPDATE_MATCH = text("""
    UPDATE lending.shadow_decisions
       SET actual = :actual, decision_match = :decision_match, decided_at = :decided_at,
           decision_seconds = :decision_seconds, edit_distance = :edit_distance, send_result = :send_result
     WHERE class_name = :class_name AND subject_ref = :subject_ref
       AND (actual IS DISTINCT FROM :actual OR decided_at IS DISTINCT FROM CAST(:decided_at AS timestamptz)
            OR send_result IS DISTINCT FROM :send_result
            OR edit_distance IS DISTINCT FROM CAST(:edit_distance AS numeric))
""")

_SET_OUTCOME = text("""
    UPDATE lending.shadow_decisions SET deal_outcome = :deal_outcome, outcome_at = :outcome_at
     WHERE class_name = :class_name AND subject_ref = :subject_ref
       AND COALESCE(deal_outcome, '') <> 'funded' AND deal_outcome IS DISTINCT FROM :deal_outcome
""")

_DISCARD = text("DELETE FROM lending.shadow_decisions WHERE class_name = :class_name AND subject_ref = :subject_ref")

_ROWS_FOR_DIGEST = text("""
    SELECT sd.class_name, sd.subject_ref, sd.proposed, sd.confidence, sd.actual, sd.decision_match,
           sd.decided_at, sd.send_result, sd.deal_outcome, sd.outcome_at, sd.created_at,
           pa.channel, pa.recipient_phone
      FROM lending.shadow_decisions sd
      LEFT JOIN lending.pending_actions pa
        ON sd.subject_kind = 'pending_action' AND sd.subject_ref ~ '^action:[0-9]+$'
       AND pa.action_id = substr(sd.subject_ref, 8)::bigint
     WHERE sd.class_name = :class_name AND sd.created_at >= CAST(:since AS timestamptz)
     ORDER BY sd.created_at
""")

_SHADOW_SENT_COUNT = text("SELECT count(*) FROM lending.pending_actions WHERE requested_by LIKE 'shadow:%'")


def set_mode(db: Session, class_name: str, mode: str, changed_by: str) -> None:
    if mode not in MODES:
        raise ValueError(f"invalid mode {mode!r}; must be one of {MODES}")
    if mode == "live":
        raise ValueError("live is refused in R1 0.9: going live is P1.4 graduation's job")
    db.execute(_UPSERT_CLASS, {"class_name": class_name, "mode": mode, "changed_by": changed_by})


def get_mode(db: Session, class_name: str) -> Optional[str]:
    row = db.execute(_GET_CLASS, {"class_name": class_name}).mappings().first()
    return row["mode"] if row else None


def get_started_at(db: Session, class_name: str) -> Optional[datetime]:
    row = db.execute(_GET_CLASS, {"class_name": class_name}).mappings().first()
    return row["started_at"] if row else None


def get_all_shadow_classes(db: Session) -> list[dict]:
    return [dict(r) for r in db.execute(_GET_ALL).mappings()]


def insert_decision(db: Session, *, class_name: str, class_version: int, subject_kind: str, subject_ref: str,
                    inputs: dict[str, Any], rule_version: Optional[int], confidence: Optional[float],
                    proposed: dict[str, Any]) -> Optional[int]:
    """Insert-if-absent; None when this class already scored this subject."""
    row = db.execute(_INSERT_DECISION, {
        "class_name": class_name, "class_version": class_version, "subject_kind": subject_kind,
        "subject_ref": subject_ref, "inputs": json.dumps(inputs), "rule_version": rule_version,
        "confidence": confidence, "proposed": json.dumps(proposed),
    }).mappings().first()
    return row["id"] if row else None


def open_rows(db: Session, class_name: str, since: datetime) -> list[dict]:
    """This class's rows created since ``since`` that may still change."""
    return [dict(r) for r in db.execute(_OPEN_ROWS, {"class_name": class_name, "since": since}).mappings()]


def update_match(db: Session, class_name: str, subject_ref: str, *, actual: str, decision_match: Optional[bool],
                 decided_at: Optional[datetime], decision_seconds: Optional[float], edit_distance: Optional[float],
                 send_result: Optional[str]) -> int:
    """Write the current decision fields; returns 1 only when something changed."""
    return db.execute(_UPDATE_MATCH, {
        "class_name": class_name, "subject_ref": subject_ref, "actual": actual, "decision_match": decision_match,
        "decided_at": decided_at, "decision_seconds": decision_seconds, "edit_distance": edit_distance,
        "send_result": send_result,
    }).rowcount


def set_outcome(db: Session, class_name: str, subject_ref: str, deal_outcome: Optional[str],
                outcome_at: Optional[datetime]) -> int:
    """Record funded or declined; a decline can later become funded, or clear (None) when the file reopens."""
    return db.execute(_SET_OUTCOME, {"class_name": class_name, "subject_ref": subject_ref,
                                      "deal_outcome": deal_outcome, "outcome_at": outcome_at}).rowcount


def discard(db: Session, class_name: str, subject_ref: str) -> int:
    """Drop a row whose card turned out to be excluded (auto-approved after it was scored)."""
    return db.execute(_DISCARD, {"class_name": class_name, "subject_ref": subject_ref}).rowcount


def rows_for_digest(db: Session, class_name: str, since: datetime) -> list[dict]:
    """Rows since ``since`` with the contact's first name and phone last 4 only; the phone itself is dropped."""
    rows = [dict(r) for r in db.execute(_ROWS_FOR_DIGEST, {"class_name": class_name, "since": since}).mappings()]
    names = names_by_phone(db, (r["recipient_phone"] for r in rows))
    for row in rows:
        phone = row.pop("recipient_phone")
        name = names.get(phone) if phone else None
        row["first_name"] = name.split()[0] if name and " or " not in name else None
        row["last4"] = phone[-4:] if phone else None
    return rows


def shadow_sent_count(db: Session) -> int:
    """Pending actions requested by a shadow class. Must always be 0."""
    return db.execute(_SHADOW_SENT_COUNT).scalar_one()
