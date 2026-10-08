"""WP-GL-5: nurture routing — contact resolution and the GHL push outcome."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.services.calendar import nurture


def _no_real_alert():
    return patch(
        "src.services.relay.exceptions_alert_queue.enqueue_and_attempt",
        return_value=True,
    )


class _FakeSession:
    """Maps a SQL fragment to the row it should return, like the booking
    suite's _RecordingSession but read-only (nurture.py only reads here)."""

    def __init__(self, rows: dict):
        self._rows = rows
        self.calls = []
        self.commits = 0
        self.rollbacks = 0

    def execute(self, stmt, params=None):
        sql = " ".join(str(stmt).split())
        self.calls.append((sql, params or {}))
        for fragment, row in self._rows.items():
            if fragment in sql:
                result = MagicMock()
                result.mappings.return_value.first.return_value = row
                return result
        result = MagicMock()
        result.mappings.return_value.first.return_value = None
        return result

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


class TestResolveContact:
    def test_no_person_id_returns_none(self):
        session = _FakeSession({})
        assert nurture._resolve_contact(session, None) is None

    def test_unknown_person_id_returns_none(self):
        session = _FakeSession({})
        assert nurture._resolve_contact(session, "missing") is None

    def test_resolves_phone_and_email(self):
        session = _FakeSession({
            "FROM fa_max_persons": {
                "person_id": "p1", "phone": "+18135551234", "email": "a@example.invalid",
                "full_name": "Maria Lopez", "merged_into_id": None,
            }
        })
        contact = nurture._resolve_contact(session, "p1")
        assert contact["phone"] == "+18135551234"
        assert contact["full_name"] == "Maria Lopez"

    def test_person_with_no_contact_method_returns_none(self):
        session = _FakeSession({
            "FROM fa_max_persons": {
                "person_id": "p1", "phone": None, "email": None,
                "full_name": "Maria Lopez", "merged_into_id": None,
            }
        })
        assert nurture._resolve_contact(session, "p1") is None

    def test_follows_a_merged_person_to_the_survivor(self):
        rows_by_call = [
            {"person_id": "p1", "phone": None, "email": None, "full_name": None, "merged_into_id": "p2"},
            {"person_id": "p2", "phone": "+18135551234", "email": None, "full_name": "Maria Lopez", "merged_into_id": None},
        ]

        class _SeqSession(_FakeSession):
            def __init__(self):
                super().__init__({})
                self._seq = iter(rows_by_call)

            def execute(self, stmt, params=None):
                result = MagicMock()
                result.mappings.return_value.first.return_value = next(self._seq, None)
                return result

        contact = nurture._resolve_contact(_SeqSession(), "p1")
        assert contact["person_id"] == "p2"
        assert contact["phone"] == "+18135551234"


class TestRouteToGhlNurture:
    def test_no_person_id_returns_false(self):
        session = _FakeSession({})
        result = nurture._route_to_ghl_nurture(
            session, person_id=None, gate_id="g1", failed_field="occupancy", fail_reason="homestead",
        )
        assert result is False

    def test_pushes_with_resolved_contact(self):
        session = _FakeSession({
            "FROM fa_max_persons": {
                "person_id": "p1", "phone": "+18135551234", "email": None,
                "full_name": "Maria Lopez", "merged_into_id": None,
            }
        })
        with patch(
            "src.services.calendar.ghl_pipeline.push_gate_fail_to_nurture_stage",
            return_value=True,
        ) as mock_push:
            result = nurture._route_to_ghl_nurture(
                session, person_id="p1", gate_id="g1", failed_field="occupancy", fail_reason="homestead",
            )
        assert result is True
        assert mock_push.call_args.kwargs["phone"] == "+18135551234"
        assert mock_push.call_args.kwargs["first_name"] == "Maria"

    def test_push_exception_returns_false_not_raise(self):
        session = _FakeSession({
            "FROM fa_max_persons": {
                "person_id": "p1", "phone": "+18135551234", "email": None,
                "full_name": "Maria Lopez", "merged_into_id": None,
            }
        })
        with patch(
            "src.services.calendar.ghl_pipeline.push_gate_fail_to_nurture_stage",
            side_effect=RuntimeError("GHL down"),
        ):
            result = nurture._route_to_ghl_nurture(
                session, person_id="p1", gate_id="g1", failed_field="occupancy", fail_reason="homestead",
            )
        assert result is False


class TestEnqueueNurture:
    def test_successful_push_marks_the_queue_row_routed(self):
        session = _FakeSession({"INSERT INTO fa_max_nurture_queue": {"id": 42}})
        with (
            _no_real_alert(),
            patch("src.services.calendar.nurture._route_to_ghl_nurture", return_value=True),
        ):
            queue_id = nurture.enqueue_nurture(
                session, gate_id="g1", person_id="p1",
                failed_field="occupancy", fail_reason="homestead",
            )
        assert queue_id == 42
        update_calls = [c for c in session.calls if "SET status = 'routed'" in c[0]]
        assert len(update_calls) == 1
        assert update_calls[0][1]["id"] == 42

    def test_failed_push_alerts_exceptions_and_leaves_pending(self):
        session = _FakeSession({"INSERT INTO fa_max_nurture_queue": {"id": 42}})
        with (
            patch("src.services.calendar.nurture._route_to_ghl_nurture", return_value=False),
            patch("src.services.relay.exceptions_alert_queue.enqueue_and_attempt", return_value=True) as mock_alert,
        ):
            queue_id = nurture.enqueue_nurture(
                session, gate_id="g1", person_id=None,
                failed_field="occupancy", fail_reason="homestead",
            )
        assert queue_id == 42
        mock_alert.assert_called_once()
        update_calls = [c for c in session.calls if "SET status = 'routed'" in c[0]]
        assert update_calls == []

    def test_db_write_failure_returns_negative_one_and_rolls_back(self):
        class _FailingSession(_FakeSession):
            def execute(self, stmt, params=None):
                raise RuntimeError("db gone")

        session = _FailingSession({})
        with _no_real_alert():
            queue_id = nurture.enqueue_nurture(session, gate_id="g1")
        assert queue_id == -1
        assert session.rollbacks == 1


class TestInsertFailure:
    def test_failed_insert_still_pushes_and_alerts(self):
        session = MagicMock()
        session.execute.side_effect = RuntimeError("db down")
        with patch.object(nurture, "_route_to_ghl_nurture", return_value=True) as push, \
             patch.object(nurture, "_alert_exceptions") as alert:
            assert nurture.enqueue_nurture(session, gate_id="g1", failed_field="homestead") == -1
        push.assert_called_once()
        alert.assert_called_once()
        assert alert.call_args.kwargs["queue_id"] is None
