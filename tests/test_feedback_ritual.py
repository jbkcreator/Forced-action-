from __future__ import annotations

from unittest.mock import MagicMock
from uuid import uuid4

import pytest


def test_suspicious_cora_touch_builds_feedback_ritual_snapshot():
    from src.services.feedback_ritual import build_feedback_ritual_snapshot

    decision = {
        "decision_id": "dec-123",
        "graph_name": "retention",
        "event_type": "retention_summary_due",
        "terminal_status": "aborted",
        "subscriber_id": 42,
        "summary": {
            "review_capture": {
                "raw_input_text": "Can I stop messages and get a refund?",
                "generated_output_text": "I can help you upgrade your plan.",
                "confidence_score": 0.41,
                "confidence_reason": "intent tie",
                "review_flag": True,
                "review_flag_reason": "classifier_low_confidence",
            }
        },
    }

    snapshot = build_feedback_ritual_snapshot(decision)

    assert snapshot["decision_id"] == "dec-123"
    assert snapshot["graph_name"] == "retention"
    assert snapshot["terminal_status"] == "aborted"
    assert snapshot["raw_input_text"] == "Can I stop messages and get a refund?"
    assert snapshot["generated_output_text"] == "I can help you upgrade your plan."
    assert snapshot["confidence_score"] == 0.41
    assert snapshot["review_flag"] is True
    assert snapshot["summary_review_capture"] == decision["summary"]["review_capture"]


def test_aborted_cora_touch_is_marked_for_feedback_ritual_queue():
    from src.services.feedback_ritual import should_queue_feedback_ritual

    should_queue, metadata = should_queue_feedback_ritual(
        {
            "decision_id": "dec-124",
            "graph_name": "retention",
            "terminal_status": "aborted",
            "summary": {"review_capture": {}},
        }
    )

    assert should_queue is True
    assert metadata["queue_reason"] == "terminal_status:aborted"


def test_suspicious_cora_touch_builds_queue_payload_for_shared_override_table():
    from src.services.feedback_ritual import build_feedback_ritual_queue_payload

    decision = {
        "decision_id": "dec-125",
        "graph_name": "retention",
        "event_type": "retention_summary_due",
        "terminal_status": "aborted",
        "subscriber_id": 7,
        "summary": {
            "review_capture": {
                "raw_input_text": "Stop texting me",
                "generated_output_text": "Would you like to upgrade first?",
                "confidence_score": 0.33,
                "confidence_reason": "intent tie",
                "review_flag": True,
                "review_flag_reason": "classifier_low_confidence",
            }
        },
    }

    payload = build_feedback_ritual_queue_payload(decision, actor="system")

    assert payload["source"] == "feedback_ritual"
    assert payload["subject_type"] == "agent_decision"
    assert payload["subject_ref"] == "dec-125"
    assert payload["queue_status"] == "pending"
    assert payload["created_by"] == "system"
    assert payload["dampener_active"] is False
    assert payload["correction_reason"] is None
    assert payload["snapshot_payload"]["decision_id"] == "dec-125"
    assert payload["source_metadata"]["queue_reason"] == "terminal_status:aborted"


def test_low_confidence_cora_touch_is_marked_for_feedback_ritual_queue():
    from src.services.feedback_ritual import should_queue_feedback_ritual

    should_queue, metadata = should_queue_feedback_ritual(
        {
            "decision_id": "dec-126",
            "graph_name": "retention",
            "terminal_status": "completed",
            "summary": {
                "review_capture": {
                    "confidence_score": 0.41,
                }
            },
        }
    )

    assert should_queue is True
    assert metadata["queue_reason"] == "confidence_score:0.41"


def test_review_flagged_cora_touch_is_marked_for_feedback_ritual_queue():
    from src.services.feedback_ritual import should_queue_feedback_ritual

    should_queue, metadata = should_queue_feedback_ritual(
        {
            "decision_id": "dec-127",
            "graph_name": "retention",
            "terminal_status": "completed",
            "summary": {
                "review_capture": {
                    "confidence_score": 0.91,
                    "review_flag": True,
                    "review_flag_reason": "fallback_after_policy_ambiguity",
                }
            },
        }
    )

    assert should_queue is True
    assert metadata["queue_reason"] == "review_flag:fallback_after_policy_ambiguity"


def test_enqueue_feedback_ritual_adds_shared_override_row_for_suspicious_touch():
    from src.services.feedback_ritual import enqueue_feedback_ritual

    session = MagicMock()
    session.query.return_value.filter.return_value.first.return_value = None
    decision = {
        "decision_id": "dec-128",
        "graph_name": "retention",
        "event_type": "retention_summary_due",
        "terminal_status": "aborted",
        "subscriber_id": 9,
        "summary": {
            "review_capture": {
                "raw_input_text": "Stop texting me",
                "generated_output_text": "Would you like to upgrade first?",
                "confidence_score": 0.33,
                "review_flag": True,
                "review_flag_reason": "classifier_low_confidence",
            }
        },
    }

    created = enqueue_feedback_ritual(session, decision, actor="system")

    assert created is not None
    saved_row = session.add.call_args.args[0]
    assert saved_row.source == "feedback_ritual"
    assert saved_row.subject_type == "agent_decision"
    assert saved_row.subject_ref == "dec-128"
    assert saved_row.queue_status == "pending"
    assert saved_row.snapshot_payload["decision_id"] == "dec-128"
    assert saved_row.source_metadata["queue_reason"] == "terminal_status:aborted"


def test_enqueue_feedback_ritual_is_idempotent_for_same_decision_ref():
    from src.services.feedback_ritual import enqueue_feedback_ritual

    existing = MagicMock()
    existing.id = 88
    existing.source = "feedback_ritual"
    existing.subject_type = "agent_decision"
    existing.subject_ref = "dec-129"

    query = MagicMock()
    session = MagicMock()
    session.query.return_value.filter.return_value.first.return_value = existing

    decision = {
        "decision_id": "dec-129",
        "graph_name": "retention",
        "terminal_status": "aborted",
        "summary": {"review_capture": {}},
    }

    created = enqueue_feedback_ritual(session, decision, actor="system")

    assert created is existing
    session.add.assert_not_called()


def test_process_feedback_ritual_candidate_loads_decision_and_enqueues_when_suspicious():
    from src.services.feedback_ritual import process_feedback_ritual_candidate

    decision_row = MagicMock()
    decision_row.decision_id = "dec-130"
    decision_row.graph_name = "retention"
    decision_row.event_type = "retention_summary_due"
    decision_row.terminal_status = "aborted"
    decision_row.subscriber_id = 11
    decision_row.summary = {
        "review_capture": {
            "raw_input_text": "Stop texting me",
            "generated_output_text": "Would you like to upgrade first?",
            "confidence_score": 0.33,
        }
    }

    session = MagicMock()
    session.query.return_value.filter.return_value.first.side_effect = [
        decision_row,  # AgentDecision lookup
        None,          # No existing feedback_ritual row
    ]

    created = process_feedback_ritual_candidate(session, "dec-130", actor="system")

    assert created is not None
    saved_row = session.add.call_args.args[0]
    assert saved_row.subject_ref == "dec-130"
    assert saved_row.source == "feedback_ritual"


def test_publish_feedback_ritual_candidate_emits_cora_event(monkeypatch):
    from src.services.feedback_ritual import publish_feedback_ritual_candidate

    published = {}

    def fake_publish(event):
        published.update(event)

    monkeypatch.setattr(
        "src.agents.events.ingestion.publish_cora_event",
        fake_publish,
    )

    publish_feedback_ritual_candidate(
        decision_id="dec-131",
        graph_name="retention",
        terminal_status="aborted",
    )

    assert published["event_type"] == "feedback_ritual_candidate"
    assert published["payload"]["decision_id"] == "dec-131"
    assert published["payload"]["graph_name"] == "retention"
    assert published["payload"]["terminal_status"] == "aborted"


def test_feedback_ritual_persists_queue_row_from_logged_agent_decision(fresh_db):
    from src.agents.tools.write_tools import log_decision
    from src.core.models import CoraTrainingOverride
    from src.services.feedback_ritual import process_feedback_ritual_candidate

    decision_id = str(uuid4())

    log_decision(
        decision_id=decision_id,
        graph_name="retention",
        subscriber_id=None,
        event_type="retention_summary_due",
        terminal_status="aborted",
        summary={
            "failure_reason": "retention:status_churned",
            "early_abort": True,
            "review_capture": {
                "raw_input_text": "retention_summary_due tier=wallet",
                "generated_output_text": "",
                "confidence_score": None,
                "confidence_reason": None,
                "review_flag": True,
                "review_flag_reason": "retention:status_churned",
            },
        },
        session=fresh_db,
    )
    fresh_db.flush()

    created = process_feedback_ritual_candidate(fresh_db, decision_id, actor="system")
    fresh_db.flush()

    assert created is not None
    saved_row = (
        fresh_db.query(CoraTrainingOverride)
        .filter(CoraTrainingOverride.source == "feedback_ritual")
        .filter(CoraTrainingOverride.subject_ref == decision_id)
        .one()
    )
    assert saved_row.subject_type == "agent_decision"
    assert saved_row.queue_status == "pending"
    assert saved_row.snapshot_payload["decision_id"] == decision_id
    assert (
        saved_row.snapshot_payload["raw_input_text"]
        == "retention_summary_due tier=wallet"
    )
    assert saved_row.source_metadata["queue_reason"] == "terminal_status:aborted"
