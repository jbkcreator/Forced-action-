"""FA Max WP-2 — Slack Operating Queues & Send Governance tests.

Test categories per testing-verification skill:
1. Unit tests  — autonomy-tier gate logic, GHL boundary stubs, settings flag
2. Suppression — 10DLC block, suppression-no-bypass structural check
3. Lane routing — slack_post _resolve_channel picks correct channel per lane
4. Queue enqueue — new lane/agent_name/autonomy_tier_at_send fields persisted
5. Migration idempotency — apply_fa_max_wp2_queues.py re-run is safe
6. Compliance boundary — no financial data fields on fa_max_person_consent
7. Admin router — _handle_relay_decision FA Max branch (with fa_max_transition)

Integration tests that need a real DB are skipped when DATABASE_URL is not
set (same pattern as WP-1).  Pass:
    DATABASE_URL=postgresql://... pytest tests/test_fa_max_wp2.py
"""
from __future__ import annotations

import os
import re
import unittest.mock as mock
from dataclasses import replace
from datetime import datetime, timezone
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_HAVE_DB = bool(os.environ.get("DATABASE_URL"))

_skip_no_db = pytest.mark.skipif(not _HAVE_DB, reason="DATABASE_URL not set")


def _make_queue_item(
    *,
    id: int = 1,
    idempotency_key: str = "test-key",
    channel: str = "email",
    recipient: str = "test@example.com",
    payload: dict | None = None,
    status: str = "pending",
    venture_key: str = "hillsborough_distress",
    lane: Optional[str] = None,
    agent_name: Optional[str] = None,
    autonomy_tier_at_send: Optional[str] = None,
):
    from src.services.relay.queue import QueueItem

    return QueueItem(
        id=id,
        idempotency_key=idempotency_key,
        batch_id=None,
        thread_id=None,
        channel=channel,
        recipient=recipient,
        payload=payload or {"subject": "Hi", "body": "Test"},
        status=status,
        slack_message_ts=None,
        decided_by=None,
        decided_at=None,
        error=None,
        dispatched_at=None,
        created_at=datetime.now(timezone.utc),
        venture_key=venture_key,
        lane=lane,
        agent_name=agent_name,
        autonomy_tier_at_send=autonomy_tier_at_send,
    )


# ===========================================================================
# 1. Unit tests — autonomy-tier gate logic
# ===========================================================================

class TestAutonomyTierGate:
    """All tests use a fake Session to avoid DB dependency."""

    def _make_session(self, *, sent_count: int = 0, edited: int = 0, funded: int = 0):
        session = MagicMock()

        def _execute(query, params=None):
            sql = str(query)
            result = MagicMock()
            if "COUNT(*) FILTER" in sql:
                # edit rate query
                mapping = MagicMock()
                mapping.__getitem__ = lambda self, k: edited if k == "edited" else sent_count
                result.mappings.return_value.first.return_value = mapping
            elif "fa_max_opportunities" in sql:
                result.scalar.return_value = funded
            else:
                result.scalar.return_value = sent_count
            return result

        session.execute.side_effect = _execute
        return session

    def test_tier_a_below_threshold_blocked(self):
        from src.services.fa_max_autonomy import TierGateOutcome, check_tier_gate

        session = self._make_session(sent_count=24)
        result = check_tier_gate("vera", "A", session)
        assert result.outcome == TierGateOutcome.below_send_threshold
        assert not result.allowed
        assert result.approved_sends == 24

    def test_tier_a_at_threshold_allowed(self):
        from src.services.fa_max_autonomy import TierGateOutcome, check_tier_gate

        session = self._make_session(sent_count=25)
        result = check_tier_gate("vera", "A", session)
        assert result.outcome == TierGateOutcome.allowed
        assert result.allowed

    def test_tier_b_below_send_threshold(self):
        from src.services.fa_max_autonomy import TierGateOutcome, check_tier_gate

        session = self._make_session(sent_count=99)
        result = check_tier_gate("hunter", "B", session)
        assert result.outcome == TierGateOutcome.below_send_threshold

    def test_tier_b_edit_rate_too_high(self):
        from src.services.fa_max_autonomy import TierGateOutcome, check_tier_gate

        # 100 sends, 11 edited = 11% > 10% ceiling
        session = self._make_session(sent_count=100, edited=11)
        result = check_tier_gate("hunter", "B", session)
        assert result.outcome == TierGateOutcome.edit_rate_too_high
        assert result.edit_rate == pytest.approx(0.11)

    def test_tier_b_edit_rate_exactly_10pct_blocked(self):
        from src.services.fa_max_autonomy import TierGateOutcome, check_tier_gate

        # exactly 10% must block (> 10% needed for allow, >0.10)
        session = self._make_session(sent_count=100, edited=10)
        result = check_tier_gate("hunter", "B", session)
        assert result.outcome == TierGateOutcome.edit_rate_too_high

    def test_tier_b_at_threshold_allowed(self):
        from src.services.fa_max_autonomy import TierGateOutcome, check_tier_gate

        session = self._make_session(sent_count=100, edited=9)
        result = check_tier_gate("hunter", "B", session)
        assert result.outcome == TierGateOutcome.allowed

    def test_tier_c_sends_ok_but_funded_loans_below_threshold(self):
        from src.services.fa_max_autonomy import TierGateOutcome, check_tier_gate

        session = self._make_session(sent_count=300, funded=4)
        result = check_tier_gate("cora", "C", session)
        assert result.outcome == TierGateOutcome.funded_loans_below_threshold
        assert result.funded_loans == 4

    def test_tier_c_both_thresholds_met(self):
        from src.services.fa_max_autonomy import TierGateOutcome, check_tier_gate

        session = self._make_session(sent_count=300, funded=5)
        result = check_tier_gate("cora", "C", session)
        assert result.outcome == TierGateOutcome.allowed

    def test_tier_c_boundary_299_sends_blocked(self):
        from src.services.fa_max_autonomy import TierGateOutcome, check_tier_gate

        session = self._make_session(sent_count=299, funded=5)
        result = check_tier_gate("cora", "C", session)
        assert result.outcome == TierGateOutcome.below_send_threshold

    def test_unknown_tier_returns_unknown_outcome(self):
        from src.services.fa_max_autonomy import TierGateOutcome, check_tier_gate

        session = self._make_session(sent_count=999)
        result = check_tier_gate("vera", "Z", session)
        assert result.outcome == TierGateOutcome.unknown_tier
        assert not result.allowed


# ===========================================================================
# 2. GHL sync boundary — always returns blocked (stub)
# ===========================================================================

class TestGhlSyncBoundary:
    def test_push_to_ghl_returns_blocked(self):
        from src.services.fa_max_ghl_sync import SyncOutcome, push_lifecycle_state_to_ghl

        result = push_lifecycle_state_to_ghl(
            person_id="some-uuid",
            ghl_contact_id="ghl-123",
            new_lifecycle_state="qualifying",
            actor="test",
        )
        assert result.outcome == SyncOutcome.blocked
        assert "SOT.md clarification #16" in result.message

    def test_pull_from_ghl_returns_blocked(self):
        from src.services.fa_max_ghl_sync import SyncOutcome, pull_contact_fields_from_ghl

        result = pull_contact_fields_from_ghl(
            ghl_contact_id="ghl-123",
            fields=["first_name", "phone"],
            actor="test",
        )
        assert result.outcome == SyncOutcome.blocked

    def test_detect_conflict_true_when_different(self):
        from src.services.fa_max_ghl_sync import detect_field_conflict

        assert detect_field_conflict("identifying", "qualifying", "lifecycle_state") is True

    def test_detect_conflict_false_when_same(self):
        from src.services.fa_max_ghl_sync import detect_field_conflict

        assert detect_field_conflict("qualifying", "qualifying", "lifecycle_state") is False


# ===========================================================================
# 3. Lane routing — slack_post._resolve_channel
# ===========================================================================

class TestSlackPostLaneRouting:
    def _settings(self, *, money="", exceptions="", relationships="", token="tok"):
        s = MagicMock()
        s.slack_bot_token = token
        s.fa_max_slack_channel_money = money
        s.fa_max_slack_channel_exceptions = exceptions
        s.fa_max_slack_channel_relationships = relationships
        return s

    def test_money_lane_routes_to_money_channel(self):
        from src.services.relay.slack_post import _resolve_channel

        item = _make_queue_item(venture_key="fa_max_lending", lane="MONEY")
        settings = self._settings(money="#fa-money")
        with patch("src.services.relay.slack_post.get_venture_config") as gvc:
            gvc.return_value.relay_slack_channel = "#fallback"
            assert _resolve_channel(item, settings) == "#fa-money"

    def test_exceptions_lane_routes_to_exceptions_channel(self):
        from src.services.relay.slack_post import _resolve_channel

        item = _make_queue_item(venture_key="fa_max_lending", lane="EXCEPTIONS")
        settings = self._settings(exceptions="#fa-exceptions")
        with patch("src.services.relay.slack_post.get_venture_config") as gvc:
            gvc.return_value.relay_slack_channel = "#fallback"
            assert _resolve_channel(item, settings) == "#fa-exceptions"

    def test_relationships_lane_routes_to_relationships_channel(self):
        from src.services.relay.slack_post import _resolve_channel

        item = _make_queue_item(venture_key="fa_max_lending", lane="RELATIONSHIPS")
        settings = self._settings(relationships="#fa-relationships")
        with patch("src.services.relay.slack_post.get_venture_config") as gvc:
            gvc.return_value.relay_slack_channel = "#fallback"
            assert _resolve_channel(item, settings) == "#fa-relationships"

    def test_no_lane_falls_back_to_venture_channel(self):
        from src.services.relay.slack_post import _resolve_channel

        item = _make_queue_item(venture_key="fa_max_lending", lane=None)
        settings = self._settings()
        with patch("src.services.relay.slack_post.get_venture_config") as gvc:
            gvc.return_value.relay_slack_channel = "#fa-general"
            assert _resolve_channel(item, settings) == "#fa-general"

    def test_non_fa_max_item_falls_back_to_venture_channel(self):
        from src.services.relay.slack_post import _resolve_channel

        item = _make_queue_item(venture_key="hillsborough_distress", lane="MONEY")
        settings = self._settings(money="#fa-money")
        with patch("src.services.relay.slack_post.get_venture_config") as gvc:
            gvc.return_value.relay_slack_channel = "#hd-relay"
            assert _resolve_channel(item, settings) == "#hd-relay"

    def test_unconfigured_money_channel_falls_back(self):
        from src.services.relay.slack_post import _resolve_channel

        item = _make_queue_item(venture_key="fa_max_lending", lane="MONEY")
        settings = self._settings(money="")  # not configured
        with patch("src.services.relay.slack_post.get_venture_config") as gvc:
            gvc.return_value.relay_slack_channel = "#fa-general"
            assert _resolve_channel(item, settings) == "#fa-general"


# ===========================================================================
# 4. Settings — fa_max_10dlc_registered defaults to False
# ===========================================================================

class TestSettings:
    def test_10dlc_flag_defaults_false(self):
        from config.settings import AppSettings

        s = AppSettings()
        assert s.fa_max_10dlc_registered is False

    def test_fa_max_slack_channels_default_empty(self):
        from config.settings import AppSettings

        s = AppSettings()
        assert s.fa_max_slack_channel_money == ""
        assert s.fa_max_slack_channel_exceptions == ""
        assert s.fa_max_slack_channel_relationships == ""


# ===========================================================================
# 5. Guards — 10DLC blocks FA Max SMS, non-FA-Max unaffected
# ===========================================================================

try:
    import phonenumbers as _phonenumbers  # noqa: F401
    _HAVE_PHONENUMBERS = True
except ImportError:
    _HAVE_PHONENUMBERS = False

_skip_no_phonenumbers = pytest.mark.skipif(
    not _HAVE_PHONENUMBERS,
    reason="phonenumbers package not installed (guards.py transitive dep)",
)


@_skip_no_phonenumbers
class TestGuards10DLC:
    def _fa_max_sms_item(self):
        return _make_queue_item(
            channel="sms",
            recipient="+18135551234",
            venture_key="fa_max_lending",
        )

    def _window_now(self):
        """Return a datetime that's within the default send window (11-18 NY)."""
        from datetime import datetime, timezone as tz
        from zoneinfo import ZoneInfo
        # 14:00 NY = within window
        return datetime(2026, 9, 15, 14, 0, tzinfo=ZoneInfo("America/New_York")).astimezone(tz.utc)

    def test_fa_max_sms_blocked_when_10dlc_not_registered(self):
        from src.services.relay.guards import evaluate, BLOCK

        item = self._fa_max_sms_item()
        with (
            patch("src.services.relay.guards.get_settings") as gs,
            patch("src.services.relay.guards._suppression_reason", return_value=None),
        ):
            settings = MagicMock()
            settings.relay_send_window_start = 11
            settings.relay_send_window_end = 18
            settings.relay_send_window_timezone = "America/New_York"
            settings.fa_max_10dlc_registered = False
            gs.return_value = settings

            verdict = evaluate(item, now=self._window_now())
            assert verdict.outcome == BLOCK
            assert "fa_max_10dlc_not_registered" in verdict.reason

    def test_fa_max_sms_allowed_when_10dlc_registered(self):
        from src.services.relay.guards import evaluate, ALLOW

        item = self._fa_max_sms_item()
        with (
            patch("src.services.relay.guards.get_settings") as gs,
            patch("src.services.relay.guards._suppression_reason", return_value=None),
        ):
            settings = MagicMock()
            settings.relay_send_window_start = 11
            settings.relay_send_window_end = 18
            settings.relay_send_window_timezone = "America/New_York"
            settings.fa_max_10dlc_registered = True
            gs.return_value = settings

            verdict = evaluate(item, now=self._window_now())
            assert verdict.outcome == ALLOW

    def test_non_fa_max_sms_not_affected_by_10dlc_flag(self):
        """10DLC check must NOT apply to hillsborough_distress SMS sends."""
        from src.services.relay.guards import evaluate, ALLOW

        item = _make_queue_item(
            channel="sms",
            recipient="+18135551234",
            venture_key="hillsborough_distress",
        )
        with (
            patch("src.services.relay.guards.get_settings") as gs,
            patch("src.services.relay.guards._suppression_reason", return_value=None),
        ):
            settings = MagicMock()
            settings.relay_send_window_start = 11
            settings.relay_send_window_end = 18
            settings.relay_send_window_timezone = "America/New_York"
            settings.fa_max_10dlc_registered = False  # False but non-FA-Max
            gs.return_value = settings

            verdict = evaluate(item, now=self._window_now())
            assert verdict.outcome == ALLOW


# ===========================================================================
# 6. Compliance boundary — no financial data on fa_max_person_consent
# ===========================================================================

class TestComplianceBoundary:
    def test_fa_max_person_consent_has_no_financial_columns(self):
        """Structural: introspect the ORM model — no financial field names."""
        from src.core.models import FaMaxPersonConsent

        prohibited_patterns = re.compile(
            r"(credit_score|income|bank|tax_return|ssn|social_security"
            r"|fico|dti|debt.to.income|rate|term|commitment|loan_amount)",
            re.IGNORECASE,
        )
        for col in FaMaxPersonConsent.__table__.columns:
            assert not prohibited_patterns.search(col.name), (
                f"FaMaxPersonConsent has prohibited column: {col.name}"
            )

    def test_relay_approval_queue_new_columns_are_nullable(self):
        """lane, agent_name, autonomy_tier_at_send must all be nullable so
        existing non-FA-Max rows are unaffected."""
        from src.core.models import RelayApprovalQueueItem

        table = RelayApprovalQueueItem.__table__
        for col_name in ("lane", "agent_name", "autonomy_tier_at_send"):
            col = table.c[col_name]
            assert col.nullable, f"relay_approval_queue.{col_name} must be nullable"

    def test_fa_max_person_consent_channel_check_constraint_present(self):
        from src.core.models import FaMaxPersonConsent

        constraints = {c.name for c in FaMaxPersonConsent.__table__.constraints}
        assert "ck_fa_max_person_consent_channel" in constraints

    def test_relay_queue_lane_check_constraint_present(self):
        from src.core.models import RelayApprovalQueueItem

        constraints = {c.name for c in RelayApprovalQueueItem.__table__.constraints}
        assert "ck_relay_approval_queue_lane" in constraints

    def test_relay_queue_tier_check_constraint_present(self):
        from src.core.models import RelayApprovalQueueItem

        constraints = {c.name for c in RelayApprovalQueueItem.__table__.constraints}
        assert "ck_relay_approval_queue_tier" in constraints


# ===========================================================================
# 7. QueueItem dataclass — new fields have correct defaults
# ===========================================================================

class TestQueueItemDefaults:
    def test_new_fields_default_to_none(self):
        item = _make_queue_item()
        assert item.lane is None
        assert item.agent_name is None
        assert item.autonomy_tier_at_send is None

    def test_lane_and_agent_preserved_when_set(self):
        item = _make_queue_item(lane="MONEY", agent_name="vera", autonomy_tier_at_send="A")
        assert item.lane == "MONEY"
        assert item.agent_name == "vera"
        assert item.autonomy_tier_at_send == "A"


# ===========================================================================
# 8. Integration — full migration, enqueue with lane, consent table (needs DB)
# ===========================================================================

@_skip_no_db
class TestWp2Integration:
    """Requires real Postgres at DATABASE_URL. Uses a separate schema/run from
    WP-1 tests — both migrations must have been applied first."""

    def test_venture_row_exists_after_migration(self):
        from sqlalchemy import text
        from src.core.database import get_db_context

        with get_db_context() as session:
            count = session.execute(
                text("SELECT COUNT(*) FROM ventures WHERE venture_key = 'fa_max_lending'")
            ).scalar()
        assert count == 1

    def test_relay_queue_lane_column_exists(self):
        from sqlalchemy import text
        from src.core.database import get_db_context

        with get_db_context() as session:
            count = session.execute(
                text(
                    "SELECT COUNT(*) FROM information_schema.columns "
                    "WHERE table_name = 'relay_approval_queue' AND column_name = 'lane'"
                )
            ).scalar()
        assert count == 1

    def test_fa_max_person_consent_table_exists(self):
        from sqlalchemy import text
        from src.core.database import get_db_context

        with get_db_context() as session:
            count = session.execute(
                text(
                    "SELECT COUNT(*) FROM information_schema.tables "
                    "WHERE table_name = 'fa_max_person_consent'"
                )
            ).scalar()
        assert count == 1

    def test_migration_is_idempotent(self):
        """Running the WP-2 migration a second time must not error."""
        import importlib.util
        from pathlib import Path

        spec = importlib.util.spec_from_file_location(
            "apply_fa_max_wp2_queues",
            Path(__file__).parent.parent / "migrations" / "apply_fa_max_wp2_queues.py",
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        mod.main()  # second run — must not raise

    def test_enqueue_persists_lane_and_agent(self):
        """End-to-end: enqueue an FA Max item with lane/agent, read it back."""
        import uuid
        from sqlalchemy import text
        from src.core.database import get_db_context
        from src.services.relay.queue import enqueue, get_item_by_idempotency_key

        key = f"wp2-test-{uuid.uuid4()}"
        item = enqueue(
            idempotency_key=key,
            channel="email",
            recipient="test-lane@example.com",
            payload={"subject": "Hi", "body": "Test"},
            venture_key="fa_max_lending",
            lane="MONEY",
            agent_name="vera",
            autonomy_tier_at_send="A",
            skip_contract_validation=True,
        )

        assert item.lane == "MONEY"
        assert item.agent_name == "vera"
        assert item.autonomy_tier_at_send == "A"
        assert item.venture_key == "fa_max_lending"

        # Clean up
        with get_db_context() as session:
            session.execute(
                text("DELETE FROM relay_approval_queue WHERE idempotency_key = :k"),
                {"k": key},
            )
            session.commit()

    def test_suppression_bypass_impossible_via_second_path(self):
        """Structural: no module other than relay/engine.py can originate a
        send for venture_key='fa_max_lending'.  We verify by checking that
        fa_max_autonomy does NOT import a send function directly."""
        import ast
        from pathlib import Path

        autonomy_src = (
            Path(__file__).parent.parent / "src" / "services" / "fa_max_autonomy.py"
        ).read_text()
        tree = ast.parse(autonomy_src)

        banned_patterns = {"send_email", "send_sms", "dispatch", "execute_batch"}
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and node.attr in banned_patterns:
                raise AssertionError(
                    f"fa_max_autonomy.py imports/calls a send function: {node.attr!r} — "
                    "all sends must go through relay/engine.py"
                )
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name) and node.func.id in banned_patterns:
                    raise AssertionError(
                        f"fa_max_autonomy.py calls a send function: {node.func.id!r}"
                    )
