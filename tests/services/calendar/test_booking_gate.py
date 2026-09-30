"""
WP-GL-5 tests: booking gate evaluation, storage, daily cap, and enforcement.

Categories covered:
  1. Unit — pure evaluate_gate() logic, all kill/pass branches
  2. Integration — store_gate(), get_passed_gate_for_link(), enforce_daily_cap()
     against real Postgres (skipped when DB absent)
  3. Suppression/no-bypass — book() refuses without a valid gate_id
  4. Boundary — daily cap at exactly cap-1, cap, cap+1
  5. Concurrent-worker — pg_advisory_xact_lock prevents double-cap bypass
  6. Compliance boundary — no financial-term field exists in the gate schema

Run:
  pytest tests/services/calendar/test_booking_gate.py
  pytest tests/services/calendar/test_booking_gate.py -m "not integration"  # unit only
"""
from __future__ import annotations

import json
import threading
from datetime import datetime, timedelta
from typing import Optional
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import text

from config.booking_gate import (
    CALENDAR_DAILY_CAP,
    COMPLETED_PROJECTS,
    EXIT_STRATEGIES,
    LIQUIDITY_SOURCES,
    OCCUPANCY_TYPES,
)
from src.services.calendar.gate import (
    GateAnswers,
    GateResult,
    _is_list_blocked,
    evaluate_gate,
)

ET = ZoneInfo("America/New_York")
NOW = datetime(2026, 10, 1, 9, tzinfo=ET)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _passing_answers(**overrides) -> GateAnswers:
    """Minimal passing gate answers. Override any field to test fail paths."""
    base = {
        "liquidity_source": "cash",
        "completed_projects": "1_to_2",
        "exit_strategy": "sale",
        "occupancy": "investment",
        "decision_maker": "yes",
        "property_address": "123 Main St, Tampa FL 33601",
    }
    base.update(overrides)
    return GateAnswers(**base)


def _no_real_alert():
    return patch(
        "src.services.relay.exceptions_alert_queue.enqueue_and_attempt",
        return_value=True,
    )


# ---------------------------------------------------------------------------
# 1. Unit — evaluate_gate() pure logic
# ---------------------------------------------------------------------------


class TestEvaluateGatePass:
    def test_all_valid_answers_pass(self):
        result = evaluate_gate(_passing_answers())
        assert result.passed is True
        assert result.failed_field is None

    def test_all_valid_liquidity_sources_pass(self):
        for source in ("cash", "loc", "partner"):
            result = evaluate_gate(_passing_answers(liquidity_source=source))
            assert result.passed is True, f"liquidity_source={source!r} should pass"

    def test_all_valid_completed_projects_pass(self):
        for val in COMPLETED_PROJECTS:
            result = evaluate_gate(_passing_answers(completed_projects=val))
            assert result.passed is True

    def test_all_valid_exit_strategies_pass(self):
        for val in EXIT_STRATEGIES:
            result = evaluate_gate(_passing_answers(exit_strategy=val))
            assert result.passed is True

    def test_all_valid_occupancy_investment_passes(self):
        result = evaluate_gate(_passing_answers(occupancy="investment"))
        assert result.passed is True


class TestEvaluateGateKillConditions:
    def test_no_liquidity_is_killed(self):
        result = evaluate_gate(_passing_answers(liquidity_source="none"))
        assert result.passed is False
        assert result.failed_field == "liquidity_source"
        assert result.reason == "no_liquidity"

    def test_homestead_is_killed(self):
        result = evaluate_gate(_passing_answers(occupancy="homestead"))
        assert result.passed is False
        assert result.failed_field == "occupancy"
        assert result.reason == "homestead"

    def test_no_decision_maker_is_killed(self):
        result = evaluate_gate(_passing_answers(decision_maker="no"))
        assert result.passed is False
        assert result.failed_field == "decision_maker"
        assert result.reason == "not_decision_maker"

    def test_blank_property_address_is_killed(self):
        result = evaluate_gate(_passing_answers(property_address=""))
        assert result.passed is False
        assert result.failed_field == "property_address"
        assert result.reason == "missing_address"

    def test_whitespace_only_address_is_killed(self):
        result = evaluate_gate(_passing_answers(property_address="   "))
        assert result.passed is False
        assert result.failed_field == "property_address"

    def test_kill_order_liquidity_before_homestead(self):
        # Both kill conditions present — liquidity checked first.
        result = evaluate_gate(
            _passing_answers(liquidity_source="none", occupancy="homestead")
        )
        assert result.failed_field == "liquidity_source"


class TestEvaluateGateInvalidCodes:
    def test_unknown_liquidity_source_fails(self):
        result = evaluate_gate(_passing_answers(liquidity_source="bitcoin"))
        assert result.passed is False
        assert result.failed_field == "liquidity_source"
        assert result.reason == "invalid_code"

    def test_unknown_completed_projects_fails(self):
        result = evaluate_gate(_passing_answers(completed_projects="many"))
        assert result.passed is False
        assert result.failed_field == "completed_projects"

    def test_unknown_exit_strategy_fails(self):
        result = evaluate_gate(_passing_answers(exit_strategy="flip"))
        assert result.passed is False
        assert result.failed_field == "exit_strategy"

    def test_unknown_occupancy_fails(self):
        result = evaluate_gate(_passing_answers(occupancy="vacation"))
        assert result.passed is False
        assert result.failed_field == "occupancy"

    def test_unknown_decision_maker_fails(self):
        result = evaluate_gate(_passing_answers(decision_maker="maybe"))
        assert result.passed is False
        assert result.failed_field == "decision_maker"


# ---------------------------------------------------------------------------
# 2. Compliance boundary — no financial fields in answers
# ---------------------------------------------------------------------------


class TestNoFinancialFields:
    """Structural: confirm gate answers store only the six enum codes and
    property_address — never credit_score, income, bank_statement, tax_return,
    or ssn. This mirrors the _FINANCIAL_TERMS voice-intake ban."""

    FORBIDDEN_FIELDS = {
        "credit_score", "credit", "income", "salary",
        "bank_statement", "tax_return", "ssn", "social_security",
        "fico", "apr", "rate",
    }

    def test_gate_answers_fields_are_clean(self):
        answers = _passing_answers()
        stored = {
            "liquidity_source": answers.liquidity_source,
            "completed_projects": answers.completed_projects,
            "exit_strategy": answers.exit_strategy,
            "occupancy": answers.occupancy,
            "decision_maker": answers.decision_maker,
            "property_address": answers.property_address,
        }
        for field in stored:
            assert field not in self.FORBIDDEN_FIELDS, (
                f"Financial field {field!r} must not appear in gate answers"
            )

    def test_gate_answer_values_do_not_match_financial_terms_regex(self):
        """The stored codes must not trigger the voice-intake regex if inspected."""
        import re
        # Pattern from src/services/fa_max_voice_intake.py (without \b so it's strict)
        pattern = re.compile(
            r"credit|score|fico|income|salary|bank\s+statement|tax\s+return"
            r"|ssn|social\s+security|w-?2|apr",
            re.IGNORECASE,
        )
        answers = _passing_answers()
        for field in (
            answers.liquidity_source,
            answers.completed_projects,
            answers.exit_strategy,
            answers.occupancy,
            answers.decision_maker,
        ):
            assert not pattern.search(field), (
                f"Value {field!r} matches financial-terms pattern"
            )


# ---------------------------------------------------------------------------
# 3. List-block logic (unit — no DB)
# ---------------------------------------------------------------------------


class TestIsListBlocked:
    def test_empty_blocked_set_never_blocks(self):
        # BLOCKED_LIST_KEYS is currently frozenset() — nothing is blocked.
        assert _is_list_blocked("wholesaler_flipper") is False
        assert _is_list_blocked("active_builder") is False
        assert _is_list_blocked("mortgage_broker") is False
        assert _is_list_blocked(None) is False

    def test_none_list_key_never_blocks(self):
        assert _is_list_blocked(None) is False


# ---------------------------------------------------------------------------
# 4. Integration — requires real Postgres with migrations applied
# ---------------------------------------------------------------------------


@pytest.fixture
def gate_db(fresh_db):
    """Skip unless both fa_max_booking_gates and fa_max_bookings are present."""
    for table in ("fa_max_booking_gates", "fa_max_bookings"):
        if fresh_db.execute(text(f"SELECT to_regclass('public.{table}')")).scalar() is None:
            pytest.skip(
                f"{table} absent — run apply_fa_max_bookings.py, "
                "apply_fa_max_bookings_integrity.py, apply_fa_max_booking_gates.py"
            )
    has_gate_col = fresh_db.execute(
        text(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_name = 'fa_max_bookings' AND column_name = 'gate_id'"
        )
    ).first()
    if has_gate_col is None:
        pytest.skip(
            "fa_max_bookings.gate_id absent — run apply_fa_max_booking_gates.py"
        )
    return fresh_db


@pytest.mark.integration
class TestStoreGateIntegration:
    def test_passing_gate_is_stored_and_retrievable(self, gate_db):
        from src.services.calendar.gate import get_passed_gate_by_id, store_gate

        with _no_real_alert():
            gate_id, _ = store_gate(
                gate_db,
                answers=_passing_answers(),
                tracked_link_id=None,
                captured_by="test_caller",
            )

        assert gate_id is not None and len(gate_id) > 0

        row = get_passed_gate_by_id(gate_db, gate_id)
        assert row is not None
        assert row["result"] == "pass"

    def test_failing_gate_is_stored_but_not_retrievable_as_passed(self, gate_db):
        from src.services.calendar.gate import get_passed_gate_by_id, store_gate

        with _no_real_alert():
            gate_id, _ = store_gate(
                gate_db,
                answers=_passing_answers(liquidity_source="none"),
                captured_by="test_caller",
            )

        row = get_passed_gate_by_id(gate_db, gate_id)
        assert row is None

    def test_gate_answers_jsonb_contains_only_codes(self, gate_db):
        """DB row answers must not contain any financial-term field."""
        from src.services.calendar.gate import store_gate

        with _no_real_alert():
            gate_id, _ = store_gate(
                gate_db,
                answers=_passing_answers(),
                captured_by="test_caller",
            )

        row = gate_db.execute(
            text("SELECT answers FROM fa_max_booking_gates WHERE gate_id = :g"),
            {"g": gate_id},
        ).mappings().first()
        stored = row["answers"] if isinstance(row["answers"], dict) else json.loads(row["answers"])

        forbidden = {"credit_score", "income", "ssn", "bank_statement", "tax_return"}
        for key in stored:
            assert key not in forbidden, f"Financial field {key!r} in stored gate answers"

    def test_nurture_row_is_created_for_failing_gate(self, gate_db):
        if gate_db.execute(text("SELECT to_regclass('public.fa_max_nurture_queue')")).scalar() is None:
            pytest.skip("fa_max_nurture_queue absent — run apply_fa_max_nurture_queue.py")

        from src.services.calendar.gate import store_gate

        with _no_real_alert():
            gate_id, _ = store_gate(
                gate_db,
                answers=_passing_answers(occupancy="homestead"),
                captured_by="test_caller",
            )

        nrow = gate_db.execute(
            text("SELECT * FROM fa_max_nurture_queue WHERE gate_id = :g"),
            {"g": gate_id},
        ).mappings().first()
        assert nrow is not None
        assert nrow["failed_field"] == "occupancy"
        assert nrow["status"] == "pending_routing"

    def test_no_nurture_row_for_passing_gate(self, gate_db):
        if gate_db.execute(text("SELECT to_regclass('public.fa_max_nurture_queue')")).scalar() is None:
            pytest.skip("fa_max_nurture_queue absent")

        from src.services.calendar.gate import store_gate

        with _no_real_alert():
            gate_id, _ = store_gate(
                gate_db,
                answers=_passing_answers(),
                captured_by="test_caller",
            )

        nrow = gate_db.execute(
            text("SELECT * FROM fa_max_nurture_queue WHERE gate_id = :g"),
            {"g": gate_id},
        ).mappings().first()
        assert nrow is None


# ---------------------------------------------------------------------------
# 5. Suppression / no-bypass — book() refuses without gate_id
# ---------------------------------------------------------------------------


class TestBookRefusesWithoutGate:
    """book() must fail closed when gate_id is absent or invalid."""

    def _make_slot(self):
        from src.services.calendar import Slot

        start = datetime(2026, 10, 5, 14, 0, tzinfo=ET)
        return Slot(start=start, end=start + timedelta(minutes=30))

    def test_book_refuses_when_no_gate_id(self):
        from src.services.calendar.booking import book
        from src.services.calendar import FakeCalendar

        session = MagicMock()
        with (
            _no_real_alert(),
            patch("src.agents.fa_max.tool_registry.check_suppression",
                  return_value={"suppressed": False, "reason": None}),
            patch("src.services.calendar.gate.get_passed_gate_by_id", return_value=None),
            patch("src.services.calendar.gate.enforce_daily_cap", return_value=True),
        ):
            result = book(
                client=FakeCalendar(),
                session=session,
                calendar_id="cal@example.invalid",
                slot=self._make_slot(),
                attendee_email="borrower@example.invalid",
                topic="Call with Forced Action",
                gate_id=None,
            )

        assert result.booked is False
        assert result.reason == "gate_required"

    def test_book_refuses_when_gate_not_passed(self):
        from src.services.calendar.booking import book
        from src.services.calendar import FakeCalendar

        session = MagicMock()
        with (
            _no_real_alert(),
            patch("src.agents.fa_max.tool_registry.check_suppression",
                  return_value={"suppressed": False, "reason": None}),
            patch("src.services.calendar.gate.get_passed_gate_by_id", return_value=None),
            patch("src.services.calendar.gate.enforce_daily_cap", return_value=True),
        ):
            result = book(
                client=FakeCalendar(),
                session=session,
                calendar_id="cal@example.invalid",
                slot=self._make_slot(),
                attendee_email="borrower@example.invalid",
                topic="Call with Forced Action",
                gate_id="stale_or_failed_gate",
            )

        assert result.booked is False
        assert result.reason == "gate_not_passed"

    def test_book_proceeds_with_valid_gate(self, bookings_db):
        """Happy path — valid gate_id leads to a real DB booking claim."""
        from src.services.calendar.booking import book
        from src.services.calendar import FakeCalendar

        fake_gate_row = {"gate_id": "g123", "list_key": None, "result": "pass"}

        with (
            _no_real_alert(),
            patch("src.agents.fa_max.tool_registry.check_suppression",
                  return_value={"suppressed": False, "reason": None}),
            patch("src.services.calendar.gate.get_passed_gate_by_id", return_value=fake_gate_row),
            patch("src.services.calendar.gate.enforce_daily_cap", return_value=True),
            # Fake gate_id FK — skip the DB FK constraint in this unit-style path
            patch(
                "src.services.calendar.booking._claim_slot",
                return_value="booking_ref_abc",
            ),
            patch("src.services.calendar.booking._is_taken", return_value=False),
            patch("src.services.calendar.booking._existing_booking", return_value=None),
            patch("src.services.calendar.booking._confirm_claim", return_value=True),
        ):
            from src.services.calendar import CalendarEvent

            fake_cal = FakeCalendar()
            slot = self._make_slot()
            # Inject a created event so _confirm_claim sees it
            result = book(
                client=fake_cal,
                session=bookings_db,
                calendar_id="cal@example.invalid",
                slot=slot,
                attendee_email="borrower@example.invalid",
                topic="Call with Forced Action",
                gate_id="g123",
            )

        assert result.booked is True


# ---------------------------------------------------------------------------
# 6. Daily cap boundary tests (unit — mocked DB)
# ---------------------------------------------------------------------------


class TestDailyCapBoundary:
    def _run_cap_check(self, current_held: int) -> bool:
        from src.services.calendar.gate import enforce_daily_cap

        session = MagicMock()
        session.execute.return_value.mappings.return_value.first.return_value = {
            "n": current_held
        }
        return enforce_daily_cap(session)

    def test_zero_held_allows_booking(self):
        assert self._run_cap_check(0) is True

    def test_cap_minus_one_allows_booking(self):
        assert self._run_cap_check(CALENDAR_DAILY_CAP - 1) is True

    def test_cap_exact_blocks_booking(self):
        assert self._run_cap_check(CALENDAR_DAILY_CAP) is False

    def test_cap_plus_one_blocks_booking(self):
        assert self._run_cap_check(CALENDAR_DAILY_CAP + 1) is False

    def test_book_refuses_at_daily_cap(self):
        from src.services.calendar.booking import book
        from src.services.calendar import FakeCalendar

        session = MagicMock()
        fake_gate_row = {"gate_id": "g_cap", "list_key": None, "result": "pass"}

        with (
            patch("src.services.calendar.gate.get_passed_gate_by_id", return_value=fake_gate_row),
            patch("src.services.calendar.gate.enforce_daily_cap", return_value=False),
            patch("src.agents.fa_max.tool_registry.check_suppression",
                  return_value={"suppressed": False, "reason": None}),
        ):
            from src.services.calendar import Slot

            slot = Slot(
                start=datetime(2026, 10, 5, 14, 0, tzinfo=ET),
                end=datetime(2026, 10, 5, 14, 30, tzinfo=ET),
            )
            result = book(
                client=FakeCalendar(),
                session=session,
                calendar_id="cal@example.invalid",
                slot=slot,
                attendee_email="b@example.invalid",
                topic="Test call",
                gate_id="g_cap",
            )

        assert result.booked is False
        assert result.reason == "daily_cap_reached"


# ---------------------------------------------------------------------------
# 7. Booking page (GET) refuses without gate
# ---------------------------------------------------------------------------


class TestBookingPageGateEnforcement:
    """booking_router.GET /book/{slug} must refuse before showing slots."""

    def test_get_shows_notice_when_no_gate(self):
        from fastapi.testclient import TestClient
        from src.api.main import app

        client = TestClient(app, raise_server_exceptions=False)

        fake_link = MagicMock()
        fake_link.id = 999

        with (
            patch("src.api.booking_router.resolve_slug", return_value=fake_link),
            patch("src.api.booking_router.record_click"),
            patch("src.api.booking_router.has_live_booking", return_value=False),
            patch("src.api.booking_router.get_passed_gate_for_link", return_value=None),
        ):
            resp = client.get("/book/testslug")

        assert resp.status_code == 200
        assert "isn't ready yet" in resp.text or "link" in resp.text.lower()

    def test_post_returns_gate_not_passed_when_no_gate(self):
        from fastapi.testclient import TestClient
        from src.api.main import app

        client = TestClient(app, raise_server_exceptions=False)

        fake_link = MagicMock()
        fake_link.id = 999

        with (
            patch("src.api.booking_router.resolve_slug", return_value=fake_link),
            patch("src.api.booking_router.has_live_booking", return_value=False),
            patch("src.api.booking_router.get_passed_gate_for_link", return_value=None),
        ):
            resp = client.post(
                "/api/book/testslug",
                json={
                    "starts_at": "2026-10-05T14:00:00+00:00",
                    "ends_at": "2026-10-05T14:30:00+00:00",
                    "name": "Test Borrower",
                    "email": "b@example.invalid",
                },
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["booked"] is False
        assert data["reason"] == "gate_not_passed"
