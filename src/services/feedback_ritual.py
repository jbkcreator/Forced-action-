from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from config.feedback_ritual import (
    FEEDBACK_RITUAL_ALLOWED_OUTCOMES,
    FEEDBACK_RITUAL_CONFIDENCE_THRESHOLD,
    FEEDBACK_RITUAL_REASON_CODES,
)
from src.core.models import AgentDecision, LifecycleTrainingOverride


def build_feedback_ritual_snapshot(decision: dict[str, Any]) -> dict[str, Any]:
    review_capture = ((decision.get("summary") or {}).get("review_capture") or {})
    return {
        "decision_id": decision.get("decision_id"),
        "graph_name": decision.get("graph_name"),
        "event_type": decision.get("event_type"),
        "terminal_status": decision.get("terminal_status"),
        "subscriber_id": decision.get("subscriber_id"),
        "raw_input_text": review_capture.get("raw_input_text"),
        "generated_output_text": review_capture.get("generated_output_text"),
        "confidence_score": review_capture.get("confidence_score"),
        "confidence_reason": review_capture.get("confidence_reason"),
        "review_flag": bool(review_capture.get("review_flag")),
        "review_flag_reason": review_capture.get("review_flag_reason"),
        "summary_review_capture": review_capture,
    }


def should_queue_feedback_ritual(decision: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
    terminal_status = decision.get("terminal_status")
    if terminal_status in {"aborted", "failed"}:
        return True, {"queue_reason": f"terminal_status:{terminal_status}"}

    review_capture = ((decision.get("summary") or {}).get("review_capture") or {})
    if review_capture.get("review_flag"):
        return True, {
            "queue_reason": f"review_flag:{review_capture.get('review_flag_reason') or 'flagged'}"
        }

    confidence_score = review_capture.get("confidence_score")
    if confidence_score is not None and confidence_score <= FEEDBACK_RITUAL_CONFIDENCE_THRESHOLD:
        return True, {"queue_reason": f"confidence_score:{confidence_score}"}

    return False, {"queue_reason": None}


def build_feedback_ritual_queue_payload(
    decision: dict[str, Any],
    *,
    actor: str,
) -> dict[str, Any]:
    _, metadata = should_queue_feedback_ritual(decision)
    return {
        "source": "feedback_ritual",
        "subject_type": "agent_decision",
        "subject_ref": decision.get("decision_id"),
        "correction_reason": None,
        "dampener_active": False,
        "queue_status": "pending",
        "created_by": actor,
        "snapshot_payload": build_feedback_ritual_snapshot(decision),
        "source_metadata": metadata,
    }


def enqueue_feedback_ritual(
    db,
    decision: dict[str, Any],
    *,
    actor: str,
):
    should_queue, _ = should_queue_feedback_ritual(decision)
    if not should_queue:
        return None

    subject_ref = decision.get("decision_id")
    existing = (
        db.query(LifecycleTrainingOverride)
        .filter(
            LifecycleTrainingOverride.source == "feedback_ritual",
            LifecycleTrainingOverride.subject_type == "agent_decision",
            LifecycleTrainingOverride.subject_ref == subject_ref,
        )
        .first()
    )
    if existing is not None:
        return existing

    payload = build_feedback_ritual_queue_payload(decision, actor=actor)
    row = LifecycleTrainingOverride(**payload)
    db.add(row)
    return row


def serialize_feedback_ritual(row: LifecycleTrainingOverride) -> dict[str, Any]:
    return {
        "id": row.id,
        "source": row.source,
        "subject_type": row.subject_type,
        "subject_ref": row.subject_ref,
        "queue_status": row.queue_status,
        "review_outcome": row.review_outcome,
        "correction_reason": row.correction_reason,
        "corrected_output": row.corrected_output,
        "note": row.note,
        "snapshot_payload": row.snapshot_payload,
    }


def apply_feedback_ritual_review(
    db,
    row: LifecycleTrainingOverride,
    *,
    review_outcome: str,
    correction_reason: str | None,
    corrected_output: str | None,
    note: str | None,
    reviewer: str,
) -> LifecycleTrainingOverride:
    if review_outcome not in FEEDBACK_RITUAL_ALLOWED_OUTCOMES:
        raise ValueError("invalid review_outcome")
    if correction_reason is not None and correction_reason not in FEEDBACK_RITUAL_REASON_CODES:
        raise ValueError("invalid correction_reason")
    if review_outcome == "needs_correction" and correction_reason is None:
        raise ValueError("correction_reason required")

    row.review_outcome = review_outcome
    row.correction_reason = correction_reason
    row.corrected_output = corrected_output
    row.note = note
    row.reviewed_by = reviewer
    row.reviewed_at = datetime.now(timezone.utc)
    if review_outcome == "discarded":
        row.queue_status = "discarded"
    db.commit()
    return row


def process_feedback_ritual_candidate(
    db,
    decision_id: str,
    *,
    actor: str,
):
    decision_row = (
        db.query(AgentDecision)
        .filter(AgentDecision.decision_id == decision_id)
        .first()
    )
    if decision_row is None:
        return None

    decision = {
        "decision_id": decision_row.decision_id,
        "graph_name": decision_row.graph_name,
        "event_type": decision_row.event_type,
        "terminal_status": decision_row.terminal_status,
        "subscriber_id": decision_row.subscriber_id,
        "summary": decision_row.summary or {},
    }
    return enqueue_feedback_ritual(db, decision, actor=actor)


def publish_feedback_ritual_candidate(
    *,
    decision_id: str,
    graph_name: str,
    terminal_status: str | None,
) -> None:
    from src.agents.events.ingestion import publish_lifecycle_event

    publish_lifecycle_event({
        "event_type": "feedback_ritual_candidate",
        "payload": {
            "decision_id": decision_id,
            "graph_name": graph_name,
            "terminal_status": terminal_status,
        },
    })
