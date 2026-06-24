from __future__ import annotations

from unittest.mock import MagicMock

from fastapi.testclient import TestClient


def _make_test_client(mock_session):
    from src.api.admin_router import get_current_admin
    from src.api.deps import get_db
    from src.api.main import app

    app.dependency_overrides[get_db] = lambda: mock_session
    app.dependency_overrides[get_current_admin] = lambda: {"sub": "test-admin"}
    return app, TestClient(app, raise_server_exceptions=False)


def _cleanup(app):
    app.dependency_overrides.clear()


def test_admin_can_stamp_feedback_ritual_review():
    session = MagicMock()
    row = MagicMock()
    row.id = 55
    row.source = "feedback_ritual"
    row.queue_status = "pending"
    row.review_outcome = None
    row.correction_reason = None
    row.corrected_output = None
    row.note = None
    row.snapshot_payload = {"decision_id": "dec-555"}
    session.get.return_value = row

    app, client = _make_test_client(session)
    try:
        resp = client.post(
            "/api/admin/feedback-ritual/55/review",
            json={
                "review_outcome": "needs_correction",
                "correction_reason": "wrong_intent",
                "corrected_output": "I can help you stop messages first.",
                "note": "Intent was opt-out, not upsell.",
            },
            headers={"Authorization": "Bearer test"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["id"] == 55
        assert body["review_outcome"] == "needs_correction"
        assert body["correction_reason"] == "wrong_intent"
        assert body["corrected_output"] == "I can help you stop messages first."
        assert body["queue_status"] == "pending"
        assert row.review_outcome == "needs_correction"
        assert row.correction_reason == "wrong_intent"
        assert row.corrected_output == "I can help you stop messages first."
        assert row.note == "Intent was opt-out, not upsell."
        assert row.reviewed_by == "test-admin"
        session.commit.assert_called_once()
    finally:
        _cleanup(app)


def test_needs_correction_requires_reason_code():
    session = MagicMock()
    row = MagicMock()
    row.id = 56
    row.source = "feedback_ritual"
    row.queue_status = "pending"
    session.get.return_value = row

    app, client = _make_test_client(session)
    try:
        resp = client.post(
            "/api/admin/feedback-ritual/56/review",
            json={"review_outcome": "needs_correction"},
            headers={"Authorization": "Bearer test"},
        )
        assert resp.status_code == 422
    finally:
        _cleanup(app)


def test_admin_can_fetch_feedback_ritual_replay_payload():
    session = MagicMock()
    row = MagicMock()
    row.id = 57
    row.source = "feedback_ritual"
    row.subject_type = "agent_decision"
    row.subject_ref = "dec-557"
    row.queue_status = "pending"
    row.review_outcome = None
    row.correction_reason = None
    row.corrected_output = None
    row.note = None
    row.snapshot_payload = {
        "decision_id": "dec-557",
        "raw_input_text": "Stop texting me",
        "generated_output_text": "Would you like to upgrade first?",
    }
    session.get.return_value = row

    app, client = _make_test_client(session)
    try:
        resp = client.get(
            "/api/admin/feedback-ritual/57",
            headers={"Authorization": "Bearer test"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["id"] == 57
        assert body["subject_ref"] == "dec-557"
        assert body["snapshot_payload"]["decision_id"] == "dec-557"
    finally:
        _cleanup(app)


def test_admin_can_list_pending_feedback_ritual_rows():
    session = MagicMock()
    row1 = MagicMock()
    row1.id = 60
    row1.source = "feedback_ritual"
    row1.subject_type = "agent_decision"
    row1.subject_ref = "dec-600"
    row1.queue_status = "pending"
    row1.review_outcome = None
    row1.correction_reason = None
    row1.corrected_output = None
    row1.note = None
    row1.snapshot_payload = {"decision_id": "dec-600"}

    row2 = MagicMock()
    row2.id = 61
    row2.source = "feedback_ritual"
    row2.subject_type = "agent_decision"
    row2.subject_ref = "dec-601"
    row2.queue_status = "pending"
    row2.review_outcome = None
    row2.correction_reason = None
    row2.corrected_output = None
    row2.note = None
    row2.snapshot_payload = {"decision_id": "dec-601"}

    session.query.return_value.filter.return_value.order_by.return_value.limit.return_value.offset.return_value.all.return_value = [
        row1, row2
    ]

    app, client = _make_test_client(session)
    try:
        resp = client.get(
            "/api/admin/feedback-ritual?queue_status=pending&limit=50&offset=0",
            headers={"Authorization": "Bearer test"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["count"] == 2
        assert body["items"][0]["id"] == 60
        assert body["items"][1]["subject_ref"] == "dec-601"
    finally:
        _cleanup(app)
