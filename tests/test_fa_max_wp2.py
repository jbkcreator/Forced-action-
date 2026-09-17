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
# 2. GHL sync ownership boundary
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
        assert "live field mapping" in result.message

    def test_pull_from_ghl_returns_blocked(self):
        from src.services.fa_max_ghl_sync import SyncOutcome, pull_contact_fields_from_ghl

        result = pull_contact_fields_from_ghl(
            ghl_contact_id="ghl-123",
            fields=["first_name", "phone"],
            actor="test",
        )
        assert result.outcome == SyncOutcome.conflict
        assert result.conflict_fields == ["first_name", "phone"]

    def test_ghl_cannot_overwrite_relationship_or_contact_fields(self):
        from src.services.fa_max_ghl_sync import (
            SyncOutcome, ghl_may_update_field, pull_contact_fields_from_ghl,
        )

        for field in ("borrower_id", "first_name", "email", "phone",
                      "relationship_state", "lifecycle_state", "opportunity_stage"):
            assert not ghl_may_update_field(field)
            result = pull_contact_fields_from_ghl(
                ghl_contact_id="ghl-123", fields=[field], actor="test",
            )
            assert result.outcome == SyncOutcome.conflict
            assert result.fields_synced == []
            assert result.conflict_fields == [field]

    def test_ghl_loan_status_is_allowlisted_but_no_adapter_writes_it(self):
        from src.services.fa_max_ghl_sync import (
            SyncOutcome, ghl_may_update_field, pull_contact_fields_from_ghl,
        )

        assert ghl_may_update_field("backflip_loan_status")
        result = pull_contact_fields_from_ghl(
            ghl_contact_id="ghl-123", fields=["backflip_loan_status"], actor="test",
        )
        assert result.outcome == SyncOutcome.blocked
        assert result.fields_synced == []

    def test_detect_conflict_true_when_different(self):
        from src.services.fa_max_ghl_sync import detect_field_conflict

        assert detect_field_conflict("identifying", "qualifying", "lifecycle_state") is True

    def test_detect_conflict_false_when_same(self):
        from src.services.fa_max_ghl_sync import detect_field_conflict

        assert detect_field_conflict("qualifying", "qualifying", "lifecycle_state") is False


class TestFaMaxCardRecovery:
    def test_slack_history_reconciles_card_after_crash_without_reposting(self):
        from types import SimpleNamespace
        from src.services.relay.slack_post import _card_ref, post_for_approval

        item = _make_queue_item(venture_key="fa_max_lending", lane="MONEY")
        lease = datetime.now(timezone.utc)
        settings = SimpleNamespace(slack_bot_token=MagicMock(get_secret_value=lambda: "xoxb-test"))
        with patch("src.services.relay.slack_post.get_settings", return_value=settings), \
             patch("src.services.relay.slack_post._resolve_channel", return_value="C_TEST"), \
             patch("src.services.relay.slack_post.queue.claim_slack_post", return_value=lease), \
             patch("src.services.relay.slack_post.queue.set_slack_message_ts") as saved, \
             patch("src.services.relay.slack_post.queue.release_slack_post") as released, \
             patch("slack_sdk.WebClient") as client_type:
            client = client_type.return_value
            client.conversations_history.return_value = {
                "messages": [{"text": f"*Relay approval needed* (#1)  Ref: `{_card_ref(item)}`", "ts": "123.456"}],
                "response_metadata": {"next_cursor": ""},
            }
            post_for_approval(item)
            client.chat_postMessage.assert_not_called()
            saved.assert_called_once_with(1, "123.456", lease_until=lease)
            released.assert_called_once_with(1, lease)

    def test_slack_failure_leaves_card_retryable(self):
        from types import SimpleNamespace
        from src.services.relay.slack_post import post_for_approval

        item = _make_queue_item(venture_key="fa_max_lending", lane="MONEY")
        lease = datetime.now(timezone.utc)
        settings = SimpleNamespace(slack_bot_token=MagicMock(get_secret_value=lambda: "xoxb-test"))
        with patch("src.services.relay.slack_post.get_settings", return_value=settings), \
             patch("src.services.relay.slack_post._resolve_channel", return_value="C_TEST"), \
             patch("src.services.relay.slack_post.queue.claim_slack_post", return_value=lease), \
             patch("src.services.relay.slack_post.queue.set_slack_message_ts") as saved, \
             patch("src.services.relay.slack_post.queue.release_slack_post") as released, \
             patch("slack_sdk.WebClient") as client_type:
            client = client_type.return_value
            client.conversations_history.return_value = {"messages": [], "response_metadata": {}}
            client.chat_postMessage.side_effect = RuntimeError("Slack unavailable")
            post_for_approval(item)
            saved.assert_not_called()
            released.assert_called_once_with(1, lease)


def test_fa_max_dispatch_rejects_row_without_durable_human_approval():
    from src.services.relay.guards import _fa_max_compliance_reason

    item = _make_queue_item(
        venture_key="fa_max_lending", lane="MONEY", agent_name="vera",
        autonomy_tier_at_send="A", channel="email",
    )
    item = replace(item, person_id="00000000-0000-0000-0000-000000000001")
    with patch("src.services.relay.guards.get_settings") as gs:
        settings = MagicMock()
        settings.fa_max_relay_send_mode = "live"
        gs.return_value = settings
        assert _fa_max_compliance_reason(item) == "human_approval_required"


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
            settings.fa_max_relay_send_mode = "live"
            settings.fa_max_10dlc_registered = False
            gs.return_value = settings

            verdict = evaluate(item, now=self._window_now())
            assert verdict.outcome == BLOCK
            assert "fa_max_10dlc_not_registered" in verdict.reason

    def test_fa_max_sms_allowed_when_10dlc_registered(self):
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
            settings.fa_max_relay_send_mode = "live"
            settings.fa_max_10dlc_registered = True
            gs.return_value = settings

            verdict = evaluate(item, now=self._window_now())
            assert verdict.outcome == BLOCK
            assert "missing_governance_fields" in verdict.reason

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

    def test_fa_max_send_blocked_while_relay_send_mode_is_not_live(self):
        """Code-review finding: channels_email.py/channels_sms.py's fake-mode
        branch returns normally, and the engine treats any normal return as
        a real send -- mark_sent() then writes a durable WP-1 interaction and
        a Slack "sent" receipt. fa_max_relay_send_mode defaults to "fake", so
        without this gate an approved item reaching a real production sweep
        would be permanently recorded as sent with no actual send. This must
        block BEFORE the 10DLC check (email isn't even sms-gated by it) --
        while mode isn't "live", nothing FA Max dispatches, any channel."""
        from src.services.relay.guards import evaluate, BLOCK

        item = _make_queue_item(
            channel="email", recipient="a@example.com", venture_key="fa_max_lending",
        )
        with (
            patch("src.services.relay.guards.get_settings") as gs,
            patch("src.services.relay.guards._suppression_reason", return_value=None),
        ):
            settings = MagicMock()
            settings.relay_send_window_start = 11
            settings.relay_send_window_end = 18
            settings.relay_send_window_timezone = "America/New_York"
            settings.fa_max_relay_send_mode = "fake"
            gs.return_value = settings

            verdict = evaluate(item, now=self._window_now())
            assert verdict.outcome == BLOCK
            assert "fa_max_relay_send_mode_not_live" in verdict.reason

    def test_fa_max_send_not_blocked_by_mode_gate_once_live(self):
        """Sanity check for the guard above: once fa_max_relay_send_mode is
        "live", this specific gate must not be what blocks the item --
        whatever blocks it next (missing_governance_fields here) must be a
        real, later check, not this one masking it."""
        from src.services.relay.guards import evaluate, BLOCK

        item = _make_queue_item(
            channel="email", recipient="a@example.com", venture_key="fa_max_lending",
        )
        with (
            patch("src.services.relay.guards.get_settings") as gs,
            patch("src.services.relay.guards._suppression_reason", return_value=None),
        ):
            settings = MagicMock()
            settings.relay_send_window_start = 11
            settings.relay_send_window_end = 18
            settings.relay_send_window_timezone = "America/New_York"
            settings.fa_max_relay_send_mode = "live"
            gs.return_value = settings

            verdict = evaluate(item, now=self._window_now())
            assert verdict.outcome == BLOCK
            assert "fa_max_relay_send_mode_not_live" not in verdict.reason


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
        person_id = str(uuid.uuid4())
        agent_name = f"vera-{uuid.uuid4()}"
        with get_db_context() as session:
            session.execute(
                text("INSERT INTO fa_max_persons (person_id, lifecycle_state, source) "
                     "VALUES (CAST(:person_id AS uuid), 'identified', 'wp2_test')"),
                {"person_id": person_id},
            )
            session.execute(
                text("INSERT INTO fa_max_person_consent "
                     "(person_id, channel, consented, source) "
                     "VALUES (CAST(:person_id AS uuid), 'email', true, 'wp2_test')"),
                {"person_id": person_id},
            )
            session.execute(
                text("INSERT INTO fa_max_backflip_campaign_feed (id, last_success_at) "
                     "VALUES (1, now()) ON CONFLICT (id) DO UPDATE SET last_success_at = now()")
            )
            session.execute(
                text("INSERT INTO relay_approval_queue "
                     "(idempotency_key, channel, recipient, payload, status, venture_key, "
                     "lane, agent_name, autonomy_tier_at_send, person_id, dispatched_at) "
                     "SELECT :prefix || n, 'noop', 'audit-only', '{}'::jsonb, 'sent', "
                     "'fa_max_lending', 'MONEY', :agent, 'A', CAST(:person_id AS uuid), now() "
                     "FROM generate_series(1, 25) n"),
                {"prefix": f"wp2-history-{uuid.uuid4()}-", "agent": agent_name,
                 "person_id": person_id},
            )
        item = enqueue(
            idempotency_key=key,
            channel="email",
            recipient="test-lane@example.com",
            payload={"subject": "Hi", "body": "Test"},
            venture_key="fa_max_lending",
            lane="MONEY",
            agent_name=agent_name,
            autonomy_tier_at_send="A",
            person_id=person_id,
            skip_contract_validation=True,
        )

        assert item.lane == "MONEY"
        assert item.agent_name == agent_name
        assert item.autonomy_tier_at_send == "A"
        assert item.venture_key == "fa_max_lending"

        # Clean up
        with get_db_context() as session:
            session.execute(text("DELETE FROM relay_approval_queue WHERE person_id = CAST(:p AS uuid)"), {"p": person_id})
            session.execute(text("DELETE FROM fa_max_persons WHERE person_id = CAST(:p AS uuid)"), {"p": person_id})
            session.execute(text("DELETE FROM fa_max_backflip_campaign_feed WHERE id = 1"))
            session.commit()

    def test_new_agent_can_queue_human_review_before_tier_a_graduation(self):
        import uuid
        from sqlalchemy import text
        from src.core.database import get_db_context
        from src.services.relay.queue import enqueue

        person_id = str(uuid.uuid4())
        with get_db_context() as session:
            session.execute(text("INSERT INTO fa_max_persons (person_id, lifecycle_state, source) "
                                 "VALUES (CAST(:p AS uuid), 'identified', 'wp2_test')"), {"p": person_id})
            session.execute(text("INSERT INTO fa_max_person_consent "
                                 "(person_id, channel, consented, source) "
                                 "VALUES (CAST(:p AS uuid), 'email', true, 'wp2_test')"), {"p": person_id})
            session.execute(text("INSERT INTO fa_max_backflip_campaign_feed (id, last_success_at) "
                                 "VALUES (1, now()) ON CONFLICT (id) DO UPDATE SET last_success_at = now()"))
        try:
            item = enqueue(
                idempotency_key=f"wp2-new-agent-{uuid.uuid4()}", channel="email",
                recipient=f"wp2-{uuid.uuid4()}@example.com",
                payload={"subject": "Review", "body": "Hello"},
                venture_key="fa_max_lending", lane="MONEY", agent_name=f"new-{uuid.uuid4()}",
                autonomy_tier_at_send="A", person_id=person_id,
                skip_contract_validation=True,
            )
            assert item.status == "pending"
            assert item.autonomy_gate_reason == "below_send_threshold"
        finally:
            with get_db_context() as session:
                session.execute(text("DELETE FROM relay_approval_queue WHERE person_id = CAST(:p AS uuid)"), {"p": person_id})
                session.execute(text("DELETE FROM fa_max_persons WHERE person_id = CAST(:p AS uuid)"), {"p": person_id})
                session.execute(text("DELETE FROM fa_max_backflip_campaign_feed WHERE id = 1"))

    def test_campaign_snapshot_replaces_membership_and_stale_feed_blocks(self, tmp_path):
        """Campaign removal must not erase a real opt-out or leave stale sends open."""
        import uuid
        from sqlalchemy import text
        from src.core.database import get_db_context
        from src.services.fa_max_send_governance import backflip_campaign_reason
        from scripts.import_backflip_suppression_csv import run

        first = f"wp2-{uuid.uuid4()}@example.com"
        second = f"wp2-{uuid.uuid4()}@example.com"
        csv_path = tmp_path / "campaign.csv"
        try:
            csv_path.write_text(f"email\n{first}\n", encoding="utf-8")
            assert run(csv_path) == 1
            with get_db_context() as session:
                assert backflip_campaign_reason(session, recipient=first, channel="email") == "backflip_active_campaign"
                assert session.execute(text("SELECT 1 FROM email_opt_outs WHERE email = :e"), {"e": first}).first() is None

            csv_path.write_text(f"email\n{second}\n", encoding="utf-8")
            assert run(csv_path) == 1
            with get_db_context() as session:
                assert backflip_campaign_reason(session, recipient=first, channel="email") is None
                assert backflip_campaign_reason(session, recipient=second, channel="email") == "backflip_active_campaign"
                session.execute(text("UPDATE fa_max_backflip_campaign_feed "
                                     "SET last_success_at = now() - interval '2 days' WHERE id = 1"))
            with get_db_context() as session:
                assert backflip_campaign_reason(session, recipient=first, channel="email") == "backflip_feed_stale"
            csv_path.write_text("email\n", encoding="utf-8")
            with pytest.raises(ValueError, match="empty campaign snapshot"):
                run(csv_path)
        finally:
            with get_db_context() as session:
                session.execute(text("DELETE FROM fa_max_backflip_campaign_contacts "
                                     "WHERE identifier_value IN (:a, :b)"), {"a": first, "b": second})
                session.execute(text("DELETE FROM fa_max_backflip_campaign_feed WHERE id = 1"))

    def test_database_rejects_financial_payload_even_on_direct_insert(self):
        import uuid
        from sqlalchemy import text
        from sqlalchemy.exc import IntegrityError
        from src.core.database import get_db_context

        person_id = str(uuid.uuid4())
        with get_db_context() as session:
            session.execute(text("INSERT INTO fa_max_persons (person_id, lifecycle_state, source) "
                                 "VALUES (CAST(:p AS uuid), 'identified', 'wp2_test')"), {"p": person_id})
        try:
            with pytest.raises(IntegrityError):
                with get_db_context() as session:
                    session.execute(
                        text("INSERT INTO relay_approval_queue "
                             "(idempotency_key, channel, recipient, payload, status, venture_key, "
                             "lane, agent_name, autonomy_tier_at_send, person_id) "
                             "VALUES (:k, 'noop', 'audit-only', "
                             "'{\"ssn\":\"123-45-6789\"}'::jsonb, 'pending', 'fa_max_lending', "
                             "'MONEY', 'vera', 'A', CAST(:p AS uuid))"),
                        {"k": f"wp2-financial-{uuid.uuid4()}", "p": person_id},
                    )
        finally:
            with get_db_context() as session:
                session.execute(text("DELETE FROM fa_max_persons WHERE person_id = CAST(:p AS uuid)"), {"p": person_id})

    def test_stale_claim_is_parked_uncertain_without_second_dispatch(self):
        import uuid
        from sqlalchemy import text
        from src.core.database import get_db_context
        from src.services.relay.queue import mark_uncertain_if_stale, try_claim_for_batch

        person_id = str(uuid.uuid4())
        with get_db_context() as session:
            session.execute(text("INSERT INTO fa_max_persons (person_id, lifecycle_state, source) "
                                 "VALUES (CAST(:p AS uuid), 'identified', 'wp2_test')"), {"p": person_id})
            item_id = session.execute(
                text("INSERT INTO relay_approval_queue "
                     "(idempotency_key, channel, recipient, payload, status, venture_key, "
                     "lane, agent_name, autonomy_tier_at_send, person_id, batch_id, updated_at) "
                     "VALUES (:k, 'noop', 'audit-only', '{}'::jsonb, 'approved', "
                     "'fa_max_lending', 'MONEY', 'vera', 'A', CAST(:p AS uuid), "
                     "'crashed-worker', now() - interval '20 minutes') RETURNING id"),
                {"k": f"wp2-uncertain-{uuid.uuid4()}", "p": person_id},
            ).scalar_one()
        try:
            assert not try_claim_for_batch(item_id, "retry-worker")
            assert mark_uncertain_if_stale(item_id)
            assert not try_claim_for_batch(item_id, "retry-worker")
            with get_db_context() as session:
                row = session.execute(text("SELECT status, error FROM relay_approval_queue WHERE id = :id"),
                                      {"id": item_id}).one()
                assert row.status == "uncertain"
                assert row.error == "provider_result_uncertain"
        finally:
            with get_db_context() as session:
                session.execute(text("DELETE FROM relay_approval_queue WHERE id = :id"), {"id": item_id})
                session.execute(text("DELETE FROM fa_max_persons WHERE person_id = CAST(:p AS uuid)"), {"p": person_id})

    def test_card_lease_and_timestamp_are_durable(self):
        import uuid
        from sqlalchemy import text
        from src.core.database import get_db_context
        from src.services.relay.queue import (
            claim_slack_post, set_slack_message_ts, unposted_fa_max_items,
        )

        person_id = str(uuid.uuid4())
        with get_db_context() as session:
            session.execute(text("INSERT INTO fa_max_persons (person_id, lifecycle_state, source) "
                                 "VALUES (CAST(:p AS uuid), 'identified', 'wp2_test')"), {"p": person_id})
            item_id = session.execute(
                text("INSERT INTO relay_approval_queue "
                     "(idempotency_key, channel, recipient, payload, status, venture_key, "
                     "lane, agent_name, autonomy_tier_at_send, person_id) "
                     "VALUES (:k, 'noop', 'audit-only', '{}'::jsonb, 'pending', "
                     "'fa_max_lending', 'MONEY', 'vera', 'A', CAST(:p AS uuid)) RETURNING id"),
                {"k": f"wp2-card-{uuid.uuid4()}", "p": person_id},
            ).scalar_one()
        try:
            assert any(item.id == item_id for item in unposted_fa_max_items())
            lease = claim_slack_post(item_id)
            assert lease is not None
            assert claim_slack_post(item_id) is None
            set_slack_message_ts(item_id, "123.456", lease_until=lease)
            with get_db_context() as session:
                row = session.execute(text("SELECT slack_message_ts, slack_post_lease_until "
                                           "FROM relay_approval_queue WHERE id = :id"), {"id": item_id}).one()
                assert row.slack_message_ts == "123.456"
                assert row.slack_post_lease_until is None
        finally:
            with get_db_context() as session:
                session.execute(text("DELETE FROM relay_approval_queue WHERE id = :id"), {"id": item_id})
                session.execute(text("DELETE FROM fa_max_persons WHERE person_id = CAST(:p AS uuid)"), {"p": person_id})


class TestWp2ClosureGuards:
    def test_prohibited_financial_payload_is_rejected(self):
        from src.services.fa_max_send_governance import GovernanceBlocked, validate_safe_payload

        with pytest.raises(GovernanceBlocked, match="prohibited_financial_field"):
            validate_safe_payload({"body": "hello", "credit_score": 720})
        with pytest.raises(GovernanceBlocked, match="prohibited_financial_content"):
            validate_safe_payload({"body": "Your interest rate is ready"})

    @_skip_no_phonenumbers
    def test_dispatch_gate_requires_fa_max_identity(self):
        from src.services.relay.guards import _fa_max_compliance_reason

        item = _make_queue_item(venture_key="fa_max_lending", lane="MONEY")
        with patch("src.services.relay.guards.get_settings") as gs:
            settings = MagicMock()
            settings.fa_max_relay_send_mode = "live"
            gs.return_value = settings
            assert _fa_max_compliance_reason(item) == "missing_governance_fields"

    def test_absent_withdrawn_and_cross_channel_consent_fail_closed(self):
        from types import SimpleNamespace
        from src.services.fa_max_send_governance import require_consent

        session = MagicMock()
        session.execute.return_value.fetchone.return_value = None
        assert require_consent(session, person_id="p", channel="email").reason == "consent_absent"
        session.execute.return_value.fetchone.return_value = SimpleNamespace(consented=False)
        assert require_consent(session, person_id="p", channel="sms").reason == "consent_withdrawn"
        session.execute.return_value.fetchone.return_value = SimpleNamespace(consented=True)
        assert require_consent(session, person_id="p", channel="email").allowed
        params = session.execute.call_args.args[1]
        assert params["channel"] == "email"

    def test_admin_approval_uses_one_transaction_and_required_source(self):
        from pathlib import Path

        source = (Path(__file__).parent.parent / "src" / "api" / "admin_router.py").read_text(encoding="utf-8")
        approval = source[source.index("def _handle_relay_decision"):source.index("# THROUGH-v2.2")]
        assert 'source_component="src.api.admin_router"' in approval
        assert "record_decision(\n                    item_id, approved=True, decided_by=user_id, session=_db" in approval
        assert "Approval held: required state transition failed" in approval

    def test_lane_message_update_reuses_posting_resolver(self):
        from pathlib import Path

        source = (Path(__file__).parent.parent / "src" / "api" / "admin_router.py").read_text(encoding="utf-8")
        update = source[source.index("def _update_relay_slack_message"):source.index("@router.post(\"/slack/relay-decision\")")]
        assert "_resolve_channel(item, settings)" in update
        assert "ts=item.slack_message_ts" in update

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
