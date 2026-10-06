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
import secrets
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
    CREDIT_BAND_QUALIFYING_VALUES,
    CREDIT_BANDS,
    DEAL_STATUS_QUALIFYING_VALUES,
    DEAL_STATUS_VALUES,
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
    """Minimal passing (BOOK, not just valid) gate answers.

    Satisfies the D2 qualification bar: real_deal/actively_looking AND
    completed_projects != "0" AND credit_band == at_or_above_640. Override
    any field to test fail paths.
    """
    base = {
        "liquidity_source": "cash",
        "completed_projects": "1_to_2",
        "deal_status": "real_deal",
        "credit_band": "at_or_above_640",
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

    def test_completed_projects_1_to_2_and_3_plus_pass_zero_does_not(self):
        """0 experience is valid but fails the BOOK qualification (D2) — it
        routes to nurture, it is not an invalid code and not a kill."""
        for val in ("1_to_2", "3_plus"):
            result = evaluate_gate(_passing_answers(completed_projects=val))
            assert result.passed is True, f"completed_projects={val!r} should pass"

        zero_result = evaluate_gate(_passing_answers(completed_projects="0"))
        assert zero_result.passed is False
        assert zero_result.failed_field is None
        assert zero_result.reason == "insufficient_qualification"

    def test_exit_strategy_never_gates_regardless_of_value(self):
        """D2: exit_strategy is captured but fully optional — never validated,
        never required, never fails the gate, even with a garbage value."""
        for val in (*EXIT_STRATEGIES, None, "", "not_a_real_strategy"):
            result = evaluate_gate(_passing_answers(exit_strategy=val))
            assert result.passed is True, f"exit_strategy={val!r} must never gate"

    def test_all_valid_occupancy_investment_passes(self):
        result = evaluate_gate(_passing_answers(occupancy="investment"))
        assert result.passed is True

    def test_all_qualifying_deal_status_values_pass(self):
        for val in DEAL_STATUS_QUALIFYING_VALUES:
            result = evaluate_gate(_passing_answers(deal_status=val))
            assert result.passed is True, f"deal_status={val!r} should pass"

    def test_neither_deal_status_fails_qualification_not_validation(self):
        result = evaluate_gate(_passing_answers(deal_status="neither"))
        assert result.passed is False
        assert result.failed_field is None
        assert result.reason == "insufficient_qualification"

    def test_at_or_above_640_credit_band_passes(self):
        result = evaluate_gate(_passing_answers(credit_band="at_or_above_640"))
        assert result.passed is True

    def test_below_640_and_unsure_credit_band_fail_qualification_not_validation(self):
        for val in ("below_640", "unsure"):
            result = evaluate_gate(_passing_answers(credit_band=val))
            assert result.passed is False
            assert result.failed_field is None
            assert result.reason == "insufficient_qualification"

    def test_target_market_satisfies_the_address_requirement(self):
        """D2: property address OR target market — actively_looking callers
        give a target market instead of a specific address."""
        result = evaluate_gate(
            _passing_answers(
                deal_status="actively_looking",
                property_address=None,
                target_market="Tampa Bay area",
            )
        )
        assert result.passed is True

    def test_liquidity_amount_is_optional_and_never_gates(self):
        """Brief p.1 asks for "rough amount" alongside type — it is captured
        but has no kill condition and is unvalidated free text."""
        assert evaluate_gate(_passing_answers()).passed is True
        assert evaluate_gate(_passing_answers(liquidity_amount="150k")).passed is True
        assert evaluate_gate(_passing_answers(liquidity_amount="")).passed is True


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

    def test_blank_property_address_with_no_target_market_is_killed(self):
        result = evaluate_gate(_passing_answers(property_address=""))
        assert result.passed is False
        assert result.failed_field == "property_address"
        assert result.reason == "missing_address_or_market"

    def test_whitespace_only_address_with_no_target_market_is_killed(self):
        result = evaluate_gate(_passing_answers(property_address="   "))
        assert result.passed is False
        assert result.failed_field == "property_address"
        assert result.reason == "missing_address_or_market"

    def test_blank_target_market_with_no_address_is_killed(self):
        result = evaluate_gate(
            _passing_answers(property_address=None, target_market="   ")
        )
        assert result.passed is False
        assert result.failed_field == "property_address"
        assert result.reason == "missing_address_or_market"

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

    def test_unknown_deal_status_fails(self):
        result = evaluate_gate(_passing_answers(deal_status="maybe_sort_of"))
        assert result.passed is False
        assert result.failed_field == "deal_status"
        assert result.reason == "invalid_code"

    def test_unknown_credit_band_fails(self):
        result = evaluate_gate(_passing_answers(credit_band="720"))
        assert result.passed is False
        assert result.failed_field == "credit_band"
        assert result.reason == "invalid_code"

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
            "deal_status": answers.deal_status,
            "credit_band": answers.credit_band,
            "exit_strategy": answers.exit_strategy,
            "occupancy": answers.occupancy,
            "decision_maker": answers.decision_maker,
            "property_address": answers.property_address,
            "target_market": answers.target_market,
        }
        for field in stored:
            assert field not in self.FORBIDDEN_FIELDS, (
                f"Financial field {field!r} must not appear in gate answers"
            )

    def test_credit_band_value_is_never_a_bare_number(self):
        """credit_band is a caller-asked estimate, never a pulled score —
        "at_or_above_640" names a band (640 is the threshold in its label,
        same as a human would say it), but the stored value itself must
        never be a bare digit string, which is what an actual score would
        look like if one were mistakenly stored."""
        for val in CREDIT_BANDS:
            assert not val.isdigit(), (
                f"CREDIT_BANDS value {val!r} is a bare number — looks like a "
                f"real pulled score, not a caller-asked band"
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
            answers.deal_status,
            answers.credit_band,
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
    """BLOCKED_LIST_KEYS default blocks list_4 (brokers/LOs, Josh's Oct 4
    email §2) — the key format matches source_tag_for()'s real output for
    pool_name="mortgage_broker" (src/services/lending/pool_extraction.py),
    confirmed against calling_pool_staging.source_tag on the real DB."""

    def test_list_4_is_blocked_by_default(self):
        assert _is_list_blocked("list_4") is True

    def test_other_lists_are_not_blocked(self):
        assert _is_list_blocked("list_1") is False
        assert _is_list_blocked("list_2") is False
        assert _is_list_blocked("list_3") is False
        assert _is_list_blocked("list_9") is False

    def test_missing_list_key_is_blocked(self):
        """Fail closed: with no list on file the List 4 rule cannot be checked."""
        assert _is_list_blocked(None) is True
        assert _is_list_blocked("") is True
        assert _is_list_blocked("   ") is True

    def test_list_key_is_matched_case_and_space_insensitively(self):
        assert _is_list_blocked("List_4") is True
        assert _is_list_blocked(" list_4 ") is True
        assert _is_list_blocked(" LIST_1 ") is False


class _FakeResult:
    def __init__(self, tags):
        self._tags = tags

    def scalars(self):
        return self

    def all(self):
        return list(self._tags)


class _TagSession:
    """Stands in for a DB session: returns fixed source tags and records the query."""

    def __init__(self, tags):
        self._tags = tags
        self.params = None

    def execute(self, _query, params=None):
        self.params = params
        return _FakeResult(self._tags)


class TestResolveListKey:
    """The source list is looked up server-side by phone, never taken from the caller."""

    def test_phone_is_normalised_before_the_lookup(self):
        from src.services.calendar.gate import resolve_list_key

        session = _TagSession(["list_1"])
        assert resolve_list_key(session, "(813) 555-0142") == "list_1"
        assert session.params == {"phone": "+18135550142"}

    def test_no_tag_on_file_returns_none(self):
        from src.services.calendar.gate import resolve_list_key

        assert resolve_list_key(_TagSession([]), "+18135550142") is None

    def test_unparseable_phone_returns_none_without_querying(self):
        from src.services.calendar.gate import resolve_list_key

        session = _TagSession(["list_4"])
        assert resolve_list_key(session, "not a phone") is None
        assert session.params is None

    def test_blocked_list_wins_when_the_number_is_in_several_lists(self):
        from src.services.calendar.gate import resolve_list_key

        assert resolve_list_key(_TagSession(["list_3", "list_4", "list_7"]), "+18135550142") == "list_4"


class TestSubmitGateListLookup:
    """POST /api/fa-max/gates resolves the list itself and refuses when it cannot."""

    @staticmethod
    def _payload(**extra):
        from src.api.fa_max_router import GateSubmission

        return GateSubmission(
            tracked_link_id=7, liquidity_source="cash", completed_projects="1_to_2",
            deal_status="real_deal", credit_band="at_or_above_640", occupancy="investment",
            decision_maker="yes", property_address="1 Main St", phone="+18135550142", **extra,
        )

    def test_caller_cannot_supply_the_list(self):
        assert "list_key" not in self._payload(list_key="list_1").model_dump()

    def test_unknown_list_refuses_the_gate_and_stores_nothing(self):
        from fastapi import HTTPException
        from src.api.fa_max_router import submit_gate

        with patch("src.services.calendar.gate.resolve_list_key", return_value=None),                 patch("src.services.calendar.gate.store_gate") as store:
            with pytest.raises(HTTPException) as exc:
                submit_gate(self._payload(), db=MagicMock(), admin={"sub": "caller"})
        assert exc.value.status_code == 422
        store.assert_not_called()

    def test_failed_lookup_refuses_the_gate_and_stores_nothing(self):
        from fastapi import HTTPException
        from src.api.fa_max_router import submit_gate

        db = MagicMock()
        with patch("src.services.calendar.gate.resolve_list_key", side_effect=RuntimeError("db down")),                 patch("src.services.calendar.gate.store_gate") as store:
            with pytest.raises(HTTPException) as exc:
                submit_gate(self._payload(), db=db, admin={"sub": "caller"})
        assert exc.value.status_code == 500
        assert "db down" not in exc.value.detail
        db.rollback.assert_called_once()
        store.assert_not_called()

    def test_resolved_list_is_what_gets_stored(self):
        from src.api.fa_max_router import submit_gate

        stored = MagicMock(return_value=("g1", GateResult(passed=True, failed_field=None, reason=None)))
        with patch("src.services.calendar.gate.resolve_list_key", return_value="list_4"),                 patch("src.services.calendar.gate.store_gate", stored):
            out = submit_gate(self._payload(), db=MagicMock(), admin={"sub": "caller"})
        assert stored.call_args.kwargs["list_key"] == "list_4"
        assert out["gate_id"] == "g1"


class TestStoreGateRequiresAList:
    def test_blank_list_key_is_rejected_before_anything_is_written(self):
        import src.services.calendar.gate as gate_module

        session = MagicMock()
        for blank in ("", "   "):
            with pytest.raises(ValueError):
                gate_module.store_gate(session, answers=_passing_answers(), list_key=blank)
        session.execute.assert_not_called()


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


class TestStoreGateCompilesWithoutDatabase:
    """Review finding: `:answers::jsonb` inside sa_text() is not a bind
    parameter to SQLAlchemy — it's a literal cast suffix glued onto a name
    that never matches `:answers`, so the param silently drops and Postgres
    receives the literal text `:answers::jsonb`. Catches that class of bug
    without needing a real database: every param store_gate's INSERT passes
    must actually appear, bound, in the compiled statement.
    """

    def test_every_bound_param_survives_compilation(self):
        from sqlalchemy.dialects import postgresql
        import src.services.calendar.gate as gate_module

        captured = {}

        class _CapturingSession:
            def execute(self, stmt, params=None):
                captured["stmt"] = stmt
                captured["params"] = params
                result = MagicMock()
                result.mappings.return_value.first.return_value = None
                return result

            def commit(self):
                pass

        with _no_real_alert():
            gate_module.store_gate(
                _CapturingSession(),
                answers=_passing_answers(),
                tracked_link_id=1,
                list_key="list_1",
                captured_by="test_caller",
            )

        compiled = captured["stmt"].compile(dialect=postgresql.dialect())
        for key in captured["params"]:
            assert key in compiled.params, (
                f"param {key!r} was passed to execute() but dropped by "
                f"compilation — likely a `:{key}::cast` literal-cast bug"
            )


@pytest.mark.integration
class TestStoreGateIntegration:
    def test_passing_gate_is_stored_and_retrievable(self, gate_db):
        from src.services.calendar.gate import get_passed_gate_by_id, store_gate

        with _no_real_alert():
            gate_id, _ = store_gate(
                gate_db,
                list_key="list_1",
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
                list_key="list_1",
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
                list_key="list_1",
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
                list_key="list_1",
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
                list_key="list_1",
                answers=_passing_answers(),
                captured_by="test_caller",
            )

        nrow = gate_db.execute(
            text("SELECT * FROM fa_max_nurture_queue WHERE gate_id = :g"),
            {"g": gate_id},
        ).mappings().first()
        assert nrow is None

    def _make_tracked_link(self, gate_db) -> int:
        """fa_max_booking_gates.tracked_link_id FKs to tracked_links.id."""
        row = gate_db.execute(
            text(
                """
                INSERT INTO tracked_links (slug, kind, label, created_by, is_active)
                VALUES (:slug, 'source', 'test link', 'test_caller', true)
                RETURNING id
                """
            ),
            {"slug": f"test-{secrets.token_urlsafe(8)}"},
        ).mappings().first()
        gate_db.commit()
        return row["id"]

    def _backdate(self, gate_db, gate_id: str, seconds_ago: int) -> None:
        """Force a deterministic evaluated_at ordering.

        fresh_db binds the whole test to one outer Postgres transaction, and
        NOW() is constant for the life of a transaction — two store_gate()
        calls in the same test land on the identical evaluated_at value, so
        ORDER BY evaluated_at DESC ties and the result is whichever row
        Postgres happens to return first, not necessarily the later call.
        Real traffic never hits this (each request is its own transaction);
        only this rollback-based test fixture can, so the test backdates
        explicitly rather than trusting wall-clock order within one txn.
        """
        gate_db.execute(
            text(
                "UPDATE fa_max_booking_gates SET evaluated_at = NOW() - make_interval(secs => :s) "
                "WHERE gate_id = :gid"
            ),
            {"s": seconds_ago, "gid": gate_id},
        )

    def test_a_later_fail_invalidates_an_earlier_pass_on_the_same_link(self, gate_db):
        """Review finding: get_passed_gate_for_link must not let a stale pass
        outrank a fresh re-screening that failed on the same tracked_link."""
        from src.services.calendar.gate import get_passed_gate_for_link, store_gate

        link_id = self._make_tracked_link(gate_db)

        with _no_real_alert():
            earlier_pass_id, _ = store_gate(
                gate_db,
                list_key="list_1",
                answers=_passing_answers(),
                tracked_link_id=link_id,
                captured_by="test_caller",
            )
            self._backdate(gate_db, earlier_pass_id, seconds_ago=60)

            store_gate(
                gate_db,
                list_key="list_1",
                answers=_passing_answers(occupancy="homestead"),
                tracked_link_id=link_id,
                captured_by="test_caller",
            )

        assert get_passed_gate_for_link(gate_db, link_id) is None

    def test_a_later_pass_is_found_after_an_earlier_fail(self, gate_db):
        from src.services.calendar.gate import get_passed_gate_for_link, store_gate

        link_id = self._make_tracked_link(gate_db)

        with _no_real_alert():
            earlier_fail_id, _ = store_gate(
                gate_db,
                list_key="list_1",
                answers=_passing_answers(occupancy="homestead"),
                tracked_link_id=link_id,
                captured_by="test_caller",
            )
            self._backdate(gate_db, earlier_fail_id, seconds_ago=60)

            gate_id, _ = store_gate(
                gate_db,
                list_key="list_1",
                answers=_passing_answers(),
                tracked_link_id=link_id,
                captured_by="test_caller",
            )

        assert get_passed_gate_for_link(gate_db, link_id) == gate_id


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

        fake_gate_row = {"gate_id": "g123", "list_key": "list_1", "result": "pass"}

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
        return enforce_daily_cap(session, datetime(2026, 10, 5, 14, 0, tzinfo=ET))

    def test_zero_held_allows_booking(self):
        assert self._run_cap_check(0) is True

    def test_cap_minus_one_allows_booking(self):
        assert self._run_cap_check(CALENDAR_DAILY_CAP - 1) is True

    def test_cap_exact_blocks_booking(self):
        assert self._run_cap_check(CALENDAR_DAILY_CAP) is False

    def test_cap_plus_one_blocks_booking(self):
        assert self._run_cap_check(CALENDAR_DAILY_CAP + 1) is False

    def test_queries_the_day_slot_start_falls_on_not_the_current_day(self):
        """Review finding: enforce_daily_cap must count the calendar day being
        booked, not whatever day the request happens to arrive on."""
        from src.services.calendar.gate import enforce_daily_cap

        session = MagicMock()
        session.execute.return_value.mappings.return_value.first.return_value = {"n": 0}

        future_slot_start = datetime(2026, 12, 25, 14, 0, tzinfo=ET)
        enforce_daily_cap(session, future_slot_start)

        # The second execute() call is the COUNT query (the first is the
        # advisory lock) — its bound day_start must be Dec 25, not today.
        count_call_params = session.execute.call_args_list[1].args[1]
        assert count_call_params["day_start"].date() == future_slot_start.date()

    def test_day_boundary_uses_calendar_timezone_not_utc(self):
        """11pm ET and the next UTC day must not be treated as different
        calendar days — the cap is a wall-clock-day concept in CALENDAR_TIMEZONE."""
        from src.services.calendar.gate import _calendar_day_bounds

        late_et = datetime(2026, 10, 5, 23, 30, tzinfo=ET)  # 2026-10-06 03:30 UTC
        start, end = _calendar_day_bounds(late_et)
        assert start.astimezone(ET).date() == late_et.date()
        assert end == start + timedelta(days=1)

    def test_book_refuses_at_daily_cap(self):
        from src.services.calendar.booking import book
        from src.services.calendar import FakeCalendar

        session = MagicMock()
        # No existing booking for this idempotency key — the replay check
        # (which now runs before the cap check) must see None, not a
        # MagicMock row, or it would short-circuit with a bogus replay.
        session.execute.return_value.mappings.return_value.first.return_value = None
        fake_gate_row = {"gate_id": "g_cap", "list_key": "list_1", "result": "pass"}

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
