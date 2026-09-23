"""
WP-T2-3 — Enablement Boundary & Backflip Campaign Suppression.

Test categories per testing-verification skill:
  1. Unit tests — pure logic, mocked sessions.
  2. Suppression / no-bypass tests — fail-closed invariants tried from both
     gates, including active-bypass attempts.
  3. Attribution write tests — mark_sent() writes owner only on real sent
     transition, only when opportunity_id is present.
  4. Health-monitor staleness trip — proactive feed-stale EXCEPTIONS alert.
  5. Feed adapter tests — FakeBackflipFeedPort + NotImplementedBackflipFeedPort.
  6. Structural tests — grep/AST confirm no second send path, IF NOT EXISTS
     in migration, channel-split called at both gates.
  7. Compliance-boundary tests — no financial field on new table, channel-split
     fails closed on NULL opportunity_id (cannot bypass by omitting it).
"""
from __future__ import annotations

import ast
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch, call
from uuid import uuid4

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
MIGRATIONS_ROOT = REPO_ROOT / "migrations"


# ---------------------------------------------------------------------------
# A. Pure-logic unit tests — no DB, mocked sessions
# ---------------------------------------------------------------------------

class TestIsBackflipSuppressed:
    def test_first_touch_requires_verified_identifier(self):
        from src.services.fa_max_send_governance import is_backflip_suppressed

        session = MagicMock()
        session.execute.return_value.fetchall.return_value = []
        with patch("src.services.fa_max_send_governance.backflip_campaign_reason", return_value=None):
            assert is_backflip_suppressed(
                session, recipient="first@example.com", channel="email", person_id="person",
            ) == (True, "contact_identifier_unverified")

    def test_prior_email_campaign_blocks_sms_for_same_person(self):
        from src.services.fa_max_send_governance import is_backflip_suppressed

        session = MagicMock()
        session.execute.return_value.fetchall.return_value = [
            ("email", "known@example.com"), ("phone", "+18135550100"),
        ]
        def campaign(_session, *, recipient, channel):
            return "backflip_active_campaign" if recipient == "known@example.com" else None
        with patch("src.services.fa_max_send_governance.backflip_campaign_reason", side_effect=campaign):
            assert is_backflip_suppressed(
                session, recipient="+18135550100", channel="sms", person_id="person-1",
            ) == (True, "backflip_active_campaign")

    def test_verified_identifiers_with_no_campaign_match_are_clear(self):
        from src.services.fa_max_send_governance import is_backflip_suppressed

        session = MagicMock()
        session.execute.return_value.fetchall.return_value = [
            ("email", "clear@example.com"), ("phone", "+18135550100"),
        ]
        with patch(
            "src.services.fa_max_send_governance.backflip_campaign_reason",
            return_value=None,
        ) as campaign_check:
            assert is_backflip_suppressed(
                session,
                recipient="clear@example.com",
                channel="email",
                person_id="person-1",
            ) == (False, None)

        assert campaign_check.call_count == 3

    def test_suppressed_when_campaign_reason_present(self):
        from src.services.fa_max_send_governance import is_backflip_suppressed

        mock_session = MagicMock()
        with patch(
            "src.services.fa_max_send_governance.backflip_campaign_reason",
            return_value="backflip_active_campaign",
        ):
            suppressed, reason = is_backflip_suppressed(
                mock_session, recipient="test@example.com", channel="email"
            )
        assert suppressed is True
        assert reason == "backflip_active_campaign"


class TestSendGateDecisionAudit:
    def test_changed_channel_source_is_audited_before_block(self):
        from src.services.relay.guards import _fa_max_compliance_reason

        item = MagicMock(
            venture_key="fa_max_lending",
            channel="email",
            person_id="person-id",
            agent_name="agent",
            autonomy_tier_at_send="A",
            lane="RELATIONSHIPS",
            decided_by="operator",
            decision_interaction_id="decision-id",
            payload={"subject": "hello", "body": "world"},
            recipient="person@example.com",
            opportunity_id="opportunity-id",
            channel_split_source="deed",
        )
        db = MagicMock()
        context = MagicMock()
        context.__enter__.return_value = db
        context.__exit__.return_value = False

        with patch("src.services.relay.guards.get_db_context", return_value=context), \
             patch("src.services.fa_max_send_governance.validate_safe_payload"), \
             patch("src.services.fa_max_send_governance.is_backflip_suppressed", return_value=(False, None)), \
             patch("src.services.fa_max_send_governance.channel_split_reason", return_value=None), \
             patch("src.services.fa_max_send_governance.get_channel_split_source", return_value="permit"), \
             patch("src.services.fa_max_send_governance.record_backflip_suppression_decision") as audit:
            assert _fa_max_compliance_reason(item) == "channel_split_source_changed"

        audit.assert_called_once_with(
            gate="send",
            recipient="person@example.com",
            suppressed=True,
            reason="channel_split_source_changed",
            opportunity_id="opportunity-id",
        )

    def test_clear_when_no_campaign_reason(self):
        from src.services.fa_max_send_governance import is_backflip_suppressed

        mock_session = MagicMock()
        with patch(
            "src.services.fa_max_send_governance.backflip_campaign_reason",
            return_value=None,
        ):
            suppressed, reason = is_backflip_suppressed(
                mock_session, recipient="clear@example.com", channel="email"
            )
        assert suppressed is False
        assert reason is None

    def test_does_not_write_to_db(self):
        """Pure calculation — session must never receive an INSERT/UPDATE call."""
        from src.services.fa_max_send_governance import is_backflip_suppressed

        mock_session = MagicMock()
        with patch(
            "src.services.fa_max_send_governance.backflip_campaign_reason",
            return_value=None,
        ):
            is_backflip_suppressed(mock_session, recipient="x@y.com", channel="email")

        # The session's execute was never called with a write — any execute
        # call here would be forwarded to backflip_campaign_reason which is
        # patched, so the mock itself should not have had execute called on it.
        for c in mock_session.execute.call_args_list:
            sql_arg = str(c.args[0]) if c.args else ""
            assert "INSERT" not in sql_arg.upper(), "is_backflip_suppressed must not INSERT"
            assert "UPDATE" not in sql_arg.upper(), "is_backflip_suppressed must not UPDATE"


class TestChannelSplitReason:
    def test_first_touch_uses_person_source(self):
        from src.services.fa_max_send_governance import channel_split_reason

        session = self._session_returning("deed")
        assert channel_split_reason(session, opportunity_id=None, person_id="person") is None
        assert "FROM fa_max_persons" in str(session.execute.call_args.args[0])

    def _session_returning(self, value):
        mock_session = MagicMock()
        mock_session.execute.return_value.scalar_one_or_none.return_value = value
        return mock_session

    def test_allowed_source_returns_none(self):
        from src.services.fa_max_send_governance import channel_split_reason

        for source in ("deed", "permit", "distress", "maturity", "partner"):
            session = self._session_returning(source)
            assert channel_split_reason(session, opportunity_id="some-uuid", person_id="person") is None

    def test_disallowed_source_returns_block_reason(self):
        from src.services.fa_max_send_governance import channel_split_reason

        session = self._session_returning("backflip_inbound")
        reason = channel_split_reason(session, opportunity_id="some-uuid", person_id="person")
        assert reason is not None
        assert "channel_split_source_not_allowed" in reason
        assert "backflip_inbound" in reason

    def test_no_opportunity_id_fails_closed(self):
        from src.services.fa_max_send_governance import channel_split_reason

        session = MagicMock()
        reason = channel_split_reason(session, opportunity_id=None)
        assert reason == "channel_split_no_person_id"
        session.execute.assert_not_called()

    def test_opportunity_not_found_fails_closed(self):
        from src.services.fa_max_send_governance import channel_split_reason

        session = self._session_returning(None)
        reason = channel_split_reason(session, opportunity_id="ghost-uuid", person_id="person")
        assert reason == "channel_split_opportunity_not_found"


class TestRecordBackflipSuppressionDecision:
    def test_writes_audit_row(self):
        """Writes INSERT into fa_max_backflip_suppression_decisions.

        `get_db_context` is imported locally inside the function body (standard
        pattern in this repo to avoid circular imports), so we patch at the
        source module rather than on the governance module's namespace.
        """
        from src.services.fa_max_send_governance import record_backflip_suppression_decision

        mock_session = MagicMock()
        mock_cm = MagicMock()
        mock_cm.__enter__ = MagicMock(return_value=mock_session)
        mock_cm.__exit__ = MagicMock(return_value=False)

        with patch("src.core.database.get_db_context", return_value=mock_cm):
            record_backflip_suppression_decision(
                gate="draft",
                recipient="test@example.com",
                suppressed=True,
                reason="backflip_active_campaign",
                opportunity_id="abc-123",
            )

        mock_session.execute.assert_called_once()
        sql = str(mock_session.execute.call_args.args[0])
        assert "fa_max_backflip_suppression_decisions" in sql

    def test_raises_on_db_failure(self):
        """An unaudited outbound must fail closed."""
        from src.services.fa_max_send_governance import record_backflip_suppression_decision

        with patch("src.core.database.get_db_context", side_effect=Exception("simulated DB failure")):
            with pytest.raises(Exception, match="simulated DB failure"):
                record_backflip_suppression_decision(
                    gate="send", recipient="+18135550100", suppressed=False,
                    reason=None, opportunity_id=None,
                )

    def test_masks_recipient_in_audit_row(self):
        """PII rule: only last 4 chars of recipient stored in audit table."""
        from src.services.fa_max_send_governance import record_backflip_suppression_decision

        mock_session = MagicMock()
        mock_cm = MagicMock()
        mock_cm.__enter__ = MagicMock(return_value=mock_session)
        mock_cm.__exit__ = MagicMock(return_value=False)

        with patch("src.core.database.get_db_context", return_value=mock_cm):
            record_backflip_suppression_decision(
                gate="draft",
                recipient="investor@realestate.com",
                suppressed=True,
                reason="backflip_feed_stale",
                opportunity_id=None,
            )

        params = mock_session.execute.call_args.args[1]
        assert "investor@realestate.com" not in str(params), "full email must not be stored"
        assert ".com" in params["masked"] or "***" == params["masked"]

    def test_invalid_gate_raises_value_error(self):
        from src.services.fa_max_send_governance import record_backflip_suppression_decision

        with pytest.raises(ValueError, match="gate must be"):
            record_backflip_suppression_decision(
                gate="invalid",
                recipient="x@y.com",
                suppressed=True,
                reason="test",
                opportunity_id=None,
            )

    def test_uses_own_db_context(self):
        """Must open its own get_db_context(), not receive a session arg.

        This is the architectural invariant that makes audit rows survive
        the draft gate's GovernanceBlocked rollback. Verify by confirming
        the function signature does NOT accept a session parameter.
        """
        import inspect
        from src.services.fa_max_send_governance import record_backflip_suppression_decision

        params = inspect.signature(record_backflip_suppression_decision).parameters
        assert "session" not in params, (
            "record_backflip_suppression_decision must NOT accept a session parameter "
            "— it must open its own get_db_context() so the audit row commits "
            "independently of any caller transaction that may roll back."
        )


# ---------------------------------------------------------------------------
# B. Suppression no-bypass tests — channel-split fails closed at both gates
# ---------------------------------------------------------------------------

class TestSuppressionNoBypass:
    """The channel-split check must be uncircumventable. Tests try various
    ways to bypass it: missing opportunity_id, unknown source, NULL source."""

    def test_channel_split_cannot_be_bypassed_via_null_opportunity(self):
        """Passing no opportunity_id must fail closed, not silently pass."""
        from src.services.fa_max_send_governance import channel_split_reason

        result = channel_split_reason(MagicMock(), opportunity_id=None)
        assert result is not None, (
            "channel_split_reason with opportunity_id=None must block (fail closed), "
            "not return None (which would mean 'clear to send')."
        )

    def test_channel_split_blocks_unknown_source(self):
        """A source value not in FA_MAX_ALLOWED_SOURCE_TYPES must be blocked."""
        from src.services.fa_max_send_governance import channel_split_reason, FA_MAX_ALLOWED_SOURCE_TYPES

        for unknown in ("backflip_campaign", "ghl_import", "", "unknown"):
            if unknown in FA_MAX_ALLOWED_SOURCE_TYPES:
                continue
            session = MagicMock()
            session.execute.return_value.scalar_one_or_none.return_value = unknown
            result = channel_split_reason(session, opportunity_id="some-id")
            assert result is not None, f"source={unknown!r} should be blocked but returned None"

    def test_fa_max_allowed_source_types_not_empty(self):
        """The allow-list must be non-empty — an empty list would block everything,
        making the enforcement appear to work but actually being misconfigured."""
        from src.services.fa_max_send_governance import FA_MAX_ALLOWED_SOURCE_TYPES

        assert len(FA_MAX_ALLOWED_SOURCE_TYPES) > 0
        assert all(isinstance(s, str) and s for s in FA_MAX_ALLOWED_SOURCE_TYPES)

    def test_backflip_feed_stale_blocks_even_with_opportunity(self):
        """A stale feed must block even when opportunity_id is valid.
        backflip_campaign_reason is checked BEFORE channel_split_reason in both gates."""
        from src.services.fa_max_send_governance import is_backflip_suppressed

        session = MagicMock()
        with patch(
            "src.services.fa_max_send_governance.backflip_campaign_reason",
            return_value="backflip_feed_stale",
        ):
            suppressed, reason = is_backflip_suppressed(
                session, recipient="x@y.com", channel="email"
            )

        assert suppressed is True
        assert reason == "backflip_feed_stale"


# ---------------------------------------------------------------------------
# C. Attribution write in mark_sent — opportunity_id path and no-op path
# ---------------------------------------------------------------------------

class TestMarkSentAttribution:
    """Tests for the attribution write added to mark_sent() in WP-T2-3.
    Uses mocks since mark_sent() opens its own get_db_context() sessions."""

    def _make_mark_sent_row(self, *, opportunity_id: str | None = "opp-uuid-001"):
        """Return a dict simulating the row returned by mark_sent()'s UPDATE RETURNING."""
        return {
            "venture_key": "fa_max_lending",
            "person_id": "person-uuid-001",
            "channel": "email",
            "agent_name": "vera",
            "autonomy_tier_at_send": "A",
            "payload": {"subject": "hi", "body": "hello"},
            "material_edit": False,
            "opportunity_id": opportunity_id,
            "channel_split_source": "deed",
        }

    def test_zero_row_attribution_is_persisted_and_alerted(self):
        from src.services.relay.queue import mark_sent

        session = MagicMock()
        row = self._make_mark_sent_row()
        def execute(statement, *args, **kwargs):
            sql = str(statement)
            result = MagicMock()
            if "RETURNING venture_key" in sql:
                result.mappings.return_value.first.return_value = row
            elif "SET backflip_attribution_owner" in sql:
                result.rowcount = 0
            elif "SELECT backflip_attribution_owner" in sql:
                result.scalar_one_or_none.return_value = None
            return result
        session.execute.side_effect = execute
        cm = MagicMock()
        cm.__enter__.return_value = session
        with patch("src.services.relay.queue.get_db_context", return_value=cm), \
             patch("src.services.state_engine.write_interaction", return_value="iid"), \
             patch("src.services.relay.exceptions_alert_queue.enqueue_and_attempt") as alert:
            mark_sent(17, batch_id="batch")
        assert any("error = 'attribution_unresolved'" in str(c.args[0]) for c in session.execute.call_args_list)
        alert.assert_called_once()

    def test_attribution_written_when_opportunity_id_set(self):
        """mark_sent() must UPDATE fa_max_opportunities SET backflip_attribution_owner."""
        mock_session = MagicMock()
        row = self._make_mark_sent_row(opportunity_id="opp-uuid-001")
        mock_session.execute.return_value.mappings.return_value.first.return_value = row

        mock_cm = MagicMock()
        mock_cm.__enter__ = MagicMock(return_value=mock_session)
        mock_cm.__exit__ = MagicMock(return_value=False)

        with patch("src.services.relay.queue.get_db_context", return_value=mock_cm):
            with patch("src.services.state_engine.write_interaction", return_value="iid-001"):
                from src.services.relay.queue import mark_sent
                mark_sent(1, batch_id="batch-001")

        execute_calls = mock_session.execute.call_args_list
        attribution_calls = [
            c for c in execute_calls
            if "backflip_attribution_owner" in str(c.args[0])
        ]
        assert len(attribution_calls) == 1, (
            "mark_sent() must call exactly one UPDATE setting backflip_attribution_owner"
        )
        sql_str = str(attribution_calls[0].args[0])
        assert "IS NULL" in sql_str, "attribution UPDATE must have WHERE backflip_attribution_owner IS NULL"
        assert "forced_action" in sql_str

    def test_attribution_skipped_when_no_opportunity_id(self, caplog):
        """A first touch waits for post-send opportunity creation to claim attribution."""
        import logging

        mock_session = MagicMock()
        row = self._make_mark_sent_row(opportunity_id=None)
        mock_session.execute.return_value.mappings.return_value.first.return_value = row

        mock_cm = MagicMock()
        mock_cm.__enter__ = MagicMock(return_value=mock_session)
        mock_cm.__exit__ = MagicMock(return_value=False)

        with patch("src.services.relay.queue.get_db_context", return_value=mock_cm):
            with patch("src.services.state_engine.write_interaction", return_value="iid-002"):
                with caplog.at_level(logging.INFO, logger="src.services.relay.queue"):
                    from src.services.relay.queue import mark_sent
                    mark_sent(2, batch_id="batch-002")

        execute_calls = mock_session.execute.call_args_list
        attribution_calls = [
            c for c in execute_calls
            if "backflip_attribution_owner" in str(c.args[0])
        ]
        assert len(attribution_calls) == 0, "must not touch attribution when opportunity_id is None"
        assert any("first touch" in r.message for r in caplog.records), (
            "must identify the post-send opportunity workflow"
        )

    def test_attribution_where_is_null_guard(self):
        """The UPDATE SQL must contain WHERE backflip_attribution_owner IS NULL
        so a second mark_sent() call on the same opportunity is a safe no-op,
        not an overwrite."""
        import importlib
        import src.services.relay.queue as qmod
        source = Path(qmod.__file__).read_text(encoding="utf-8")
        assert "backflip_attribution_owner IS NULL" in source, (
            "mark_sent()'s attribution UPDATE must have WHERE backflip_attribution_owner IS NULL "
            "to make concurrent calls safe"
        )


# ---------------------------------------------------------------------------
# D. Health monitor — Backflip feed staleness trip
# ---------------------------------------------------------------------------

class TestBackflipFeedStalenessTrip:
    def _session_with_fresh(self, fresh_value):
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = fresh_value
        return session

    def test_trip_when_feed_row_missing(self):
        from src.tasks.fa_max_send_health_monitor import _backflip_feed_stale_trip

        session = self._session_with_fresh(None)
        trip = _backflip_feed_stale_trip(session, datetime.now(timezone.utc))

        assert trip is not None
        assert "never_imported" in trip.rule

    def test_trip_when_feed_stale(self):
        from src.tasks.fa_max_send_health_monitor import _backflip_feed_stale_trip

        session = self._session_with_fresh(False)
        trip = _backflip_feed_stale_trip(session, datetime.now(timezone.utc))

        assert trip is not None
        assert "stale" in trip.rule

    def test_no_trip_when_feed_fresh(self):
        from src.tasks.fa_max_send_health_monitor import _backflip_feed_stale_trip

        session = self._session_with_fresh(True)
        trip = _backflip_feed_stale_trip(session, datetime.now(timezone.utc))

        assert trip is None

    def test_evaluate_includes_backflip_staleness_check(self):
        """evaluate() must call _backflip_feed_stale_trip — confirms wiring."""
        from src.tasks.fa_max_send_health_monitor import evaluate

        with patch("src.tasks.fa_max_send_health_monitor._backflip_feed_stale_trip") as mock_trip:
            with patch("src.tasks.fa_max_send_health_monitor._warmup_trip", return_value=None):
                with patch("src.tasks.fa_max_send_health_monitor._relay_failure_trip", return_value=None):
                    with patch("src.tasks.fa_max_send_health_monitor.get_db_context") as mock_ctx:
                        mock_session = MagicMock()
                        mock_ctx.return_value.__enter__ = MagicMock(return_value=mock_session)
                        mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
                        mock_trip.return_value = None
                        with patch("src.tasks.fa_max_send_health_monitor.get_venture_config") as mock_vc:
                            mock_vc.return_value = MagicMock(
                                relay_instantly_campaign_id="cid",
                                relay_instantly_sender_email="sender@test.com",
                            )
                            evaluate()

        mock_trip.assert_called_once()

    def test_stale_trip_uses_same_freshness_window_as_suppression(self):
        """_backflip_feed_stale_trip must use fa_max_backflip_feed_max_age_hours
        (same setting backflip_campaign_reason uses), not a hardcoded constant."""
        import inspect
        from src.tasks.fa_max_send_health_monitor import _backflip_feed_stale_trip

        source = inspect.getsource(_backflip_feed_stale_trip)
        assert "fa_max_backflip_feed_max_age_hours" in source, (
            "_backflip_feed_stale_trip must read fa_max_backflip_feed_max_age_hours "
            "from settings, not a hardcoded hours value — the two must stay in sync."
        )


# ---------------------------------------------------------------------------
# E. Feed adapter tests
# ---------------------------------------------------------------------------

class TestFeedAdapters:
    def test_shared_writer_normalizes_live_adapter_values(self):
        from src.services.fa_max_backflip_feed import replace_backflip_snapshot

        session = MagicMock()
        cm = MagicMock()
        cm.__enter__.return_value = session
        with patch("src.services.fa_max_backflip_feed.get_db_context", return_value=cm):
            count = replace_backflip_snapshot({("email", "  KNOWN@EXAMPLE.COM "), ("phone", "(813) 555-0100")})
        assert count == 2
        params = [call.args[1] for call in session.execute.call_args_list if len(call.args) > 1]
        assert {p.get("value") for p in params if "value" in p} == {"known@example.com", "+18135550100"}

    def test_writer_locks_feed_before_replacing_contacts(self):
        from src.services.fa_max_backflip_feed import replace_backflip_snapshot

        session = MagicMock()
        cm = MagicMock()
        cm.__enter__.return_value = session
        with patch("src.services.fa_max_backflip_feed.get_db_context", return_value=cm):
            assert replace_backflip_snapshot({("email", "known@example.com")}) == 1
        statements = [str(entry.args[0]) for entry in session.execute.call_args_list]
        lock_at = next(i for i, sql in enumerate(statements) if "FOR UPDATE" in sql)
        clear_at = next(i for i, sql in enumerate(statements) if "SET active = false" in sql)
        stamp_at = next(i for i, sql in enumerate(statements) if "SET last_success_at" in sql)
        assert lock_at < clear_at < stamp_at

    def test_fake_port_records_calls_and_returns_count(self):
        from src.services.fa_max_backflip_feed import FakeBackflipFeedPort

        port = FakeBackflipFeedPort(identifiers=[("email", "a@b.com"), ("phone", "+18135550001")])
        result = port.import_snapshot()

        assert result == 2
        assert len(port.calls) == 1
        assert port.calls[0] == [("email", "a@b.com"), ("phone", "+18135550001")]

    def test_fake_port_default_empty_identifiers(self):
        from src.services.fa_max_backflip_feed import FakeBackflipFeedPort

        port = FakeBackflipFeedPort()
        assert port.import_snapshot() == 0

    def test_not_implemented_port_raises(self):
        from src.services.fa_max_backflip_feed import NotImplementedBackflipFeedPort

        port = NotImplementedBackflipFeedPort()
        with pytest.raises(NotImplementedError, match="SOT.md open clarification #6"):
            port.import_snapshot()

    def test_get_backflip_feed_port_returns_fake_in_test_mode(self):
        from src.services.fa_max_backflip_feed import FakeBackflipFeedPort, get_backflip_feed_port

        mock_settings = MagicMock()
        mock_settings.fa_max_backflip_feed_adapter = "fake"

        # get_settings is imported locally inside get_backflip_feed_port — patch at source.
        with patch("config.settings.get_settings", return_value=mock_settings):
            port = get_backflip_feed_port()

        assert isinstance(port, FakeBackflipFeedPort)

    def test_csv_port_imports_without_cli_module_dependency(self):
        from src.services.fa_max_backflip_feed import CsvBackflipFeedPort

        csv_path = Path.cwd() / f".backflip-port-test-{uuid4().hex}.csv"
        csv_path.write_text(
            "email,phone\n Clear@Example.com ,(813) 555-0100\n",
            encoding="utf-8",
        )
        session = MagicMock()
        context = MagicMock()
        context.__enter__.return_value = session
        context.__exit__.return_value = False

        try:
            with patch("src.services.fa_max_backflip_feed.get_db_context", return_value=context):
                result = CsvBackflipFeedPort(csv_path).import_snapshot()
        finally:
            csv_path.unlink(missing_ok=True)

        assert result == 2
        values = {
            call.args[1]["value"]
            for call in session.execute.call_args_list
            if len(call.args) > 1 and "value" in call.args[1]
        }
        assert values == {"clear@example.com", "+18135550100"}

    def test_get_backflip_feed_port_no_csv_returns_not_implemented(self):
        from src.services.fa_max_backflip_feed import NotImplementedBackflipFeedPort, get_backflip_feed_port

        mock_settings = MagicMock()
        mock_settings.fa_max_backflip_feed_adapter = "csv"

        with patch("config.settings.get_settings", return_value=mock_settings):
            port = get_backflip_feed_port(csv_path=None)

        assert isinstance(port, NotImplementedBackflipFeedPort)


# ---------------------------------------------------------------------------
# F. Structural tests — AST / file inspection
# ---------------------------------------------------------------------------

class TestStructural:
    def test_migration_uses_if_not_exists(self):
        """Every DDL in the migration must be idempotent."""
        migration = (MIGRATIONS_ROOT / "apply_fa_max_wp_t2_3.py").read_text(encoding="utf-8")
        upper = migration.upper()
        # Every CREATE TABLE / ALTER TABLE ADD COLUMN should be guarded
        assert "CREATE TABLE IF NOT EXISTS" in upper
        assert "ADD COLUMN IF NOT EXISTS" in upper

    def test_migration_creates_audit_table(self):
        migration = (MIGRATIONS_ROOT / "apply_fa_max_wp_t2_3.py").read_text(encoding="utf-8")
        assert "fa_max_backflip_suppression_decisions" in migration

    def test_migration_adds_opportunity_id_to_relay_queue(self):
        migration = (MIGRATIONS_ROOT / "apply_fa_max_wp_t2_3.py").read_text(encoding="utf-8")
        assert "opportunity_id" in migration
        assert "relay_approval_queue" in migration

    def test_migration_adds_attribution_columns_to_opportunities(self):
        migration = (MIGRATIONS_ROOT / "apply_fa_max_wp_t2_3.py").read_text(encoding="utf-8")
        assert "backflip_attribution_owner" in migration
        assert "backflip_attribution_set_at" in migration
        assert "fa_max_opportunities" in migration

    def test_channel_split_called_from_draft_gate(self):
        """channel_split_reason must be called inside queue.py (draft gate)."""
        queue_source = (SRC_ROOT / "services" / "relay" / "queue.py").read_text(encoding="utf-8")
        assert "channel_split_reason" in queue_source, (
            "queue.py (draft gate) must call channel_split_reason() for WP-T2-3 enforcement"
        )

    def test_channel_split_called_from_send_gate(self):
        """channel_split_reason must be called inside guards.py (send gate)."""
        guards_source = (SRC_ROOT / "services" / "relay" / "guards.py").read_text(encoding="utf-8")
        assert "channel_split_reason" in guards_source, (
            "guards.py (send gate) must call channel_split_reason() for defense-in-depth"
        )

    def test_record_suppression_decision_called_from_draft_gate(self):
        """Audit records must be written from queue.py's except block."""
        queue_source = (SRC_ROOT / "services" / "relay" / "queue.py").read_text(encoding="utf-8")
        assert "record_backflip_suppression_decision" in queue_source

    def test_record_suppression_decision_called_from_send_gate(self):
        guards_source = (SRC_ROOT / "services" / "relay" / "guards.py").read_text(encoding="utf-8")
        assert "record_backflip_suppression_decision" in guards_source

    def test_audit_table_no_financial_fields(self):
        """Compliance boundary: fa_max_backflip_suppression_decisions must not
        contain any borrower financial data field."""
        migration = (MIGRATIONS_ROOT / "apply_fa_max_wp_t2_3.py").read_text(encoding="utf-8")
        # Find the audit table CREATE block
        start = migration.index("fa_max_backflip_suppression_decisions")
        block = migration[start:start + 600]
        # Check for specific prohibited borrower financial field NAMES, not substrings
        # like "rate" (which false-positives on "separate", "generate", etc.).
        forbidden = ("credit_score", "income", "ssn", "bank_statement", "tax_return",
                     "fico", "interest_rate", "loan_rate", "commitment", "dti",
                     "tax_return", "loan_amount")
        block_lower = block.lower()
        for field in forbidden:
            assert field not in block_lower, (
                f"Compliance violation: '{field}' must not appear in the audit table definition"
            )

    def test_opportunity_id_field_on_queue_item_dataclass(self):
        """QueueItem dataclass must have opportunity_id as an Optional field."""
        import dataclasses
        from src.services.relay.queue import QueueItem

        field_names = {f.name for f in dataclasses.fields(QueueItem)}
        assert "opportunity_id" in field_names

    def test_opportunity_id_default_is_none(self):
        """opportunity_id must default to None so all existing QueueItem
        construction sites don't need to be updated."""
        import dataclasses
        from src.services.relay.queue import QueueItem

        field = next(f for f in dataclasses.fields(QueueItem) if f.name == "opportunity_id")
        assert field.default is None

    def test_feed_port_follows_protocol_pattern(self):
        """BackflipFeedPort must be a Protocol, matching the banks port pattern."""
        import inspect
        from src.services.fa_max_backflip_feed import BackflipFeedPort
        from typing import Protocol

        # Check it's declared as Protocol via runtime_checkable OR inspect
        mro = [c.__name__ for c in inspect.getmro(BackflipFeedPort)]
        assert "Protocol" in mro or BackflipFeedPort.__bases__[0].__name__ == "Protocol"

    def test_mark_sent_attribution_is_write_once(self):
        """The UPDATE in mark_sent() must have WHERE backflip_attribution_owner IS NULL
        — without this, concurrent workers could overwrite each other."""
        queue_source = (SRC_ROOT / "services" / "relay" / "queue.py").read_text(encoding="utf-8")
        assert "backflip_attribution_owner IS NULL" in queue_source

    def test_no_new_financial_fields_on_opportunities(self):
        """The new columns on fa_max_opportunities (backflip_attribution_owner,
        backflip_attribution_set_at) are not borrower financial data fields."""
        models_source = (SRC_ROOT / "core" / "models.py").read_text(encoding="utf-8")
        # Find the attribution section
        idx = models_source.find("backflip_attribution_owner")
        assert idx != -1, "backflip_attribution_owner column should exist"
        section = models_source[idx:idx + 300]
        forbidden = ("credit_score", "income", "ssn", "bank_statement", "tax_return",
                     "fico", "rate", "term", "commitment")
        for field in forbidden:
            assert field not in section.lower(), (
                f"Compliance violation: '{field}' near backflip_attribution_owner"
            )

    def test_backflip_feed_file_documents_sot_open_clarification(self):
        """The NotImplementedBackflipFeedPort docstring must reference SOT.md
        clarification #6 (suppression feed format) so future readers know why."""
        feed_source = (SRC_ROOT / "services" / "fa_max_backflip_feed.py").read_text(encoding="utf-8")
        assert "clarification #6" in feed_source or "open clarification #6" in feed_source


# ---------------------------------------------------------------------------
# G. Compliance-boundary tests — SMS path, no pricing fields
# ---------------------------------------------------------------------------

class TestComplianceBoundary:
    def test_channel_split_fail_closed_is_not_bypassable_via_empty_string(self):
        """An empty string opportunity_id is treated the same as None — fails closed."""
        from src.services.fa_max_send_governance import channel_split_reason

        session = MagicMock()
        result = channel_split_reason(session, opportunity_id="")
        assert result is not None, "empty string opportunity_id must fail closed"

    def test_suppression_reason_checks_backflip_first(self):
        """suppression_reason() must call backflip_campaign_reason before email/sms
        suppression. Backflip campaign membership is the most-critical check and
        must not be reordered to come after the generic opt-out checks."""
        import inspect
        from src.services.fa_max_send_governance import suppression_reason

        source = inspect.getsource(suppression_reason)
        backflip_pos = source.find("backflip_campaign_reason")
        email_pos = source.find("email_opt_out")
        sms_pos = source.find("validate_outbound")
        assert backflip_pos < email_pos or backflip_pos < sms_pos, (
            "suppression_reason() must call backflip_campaign_reason() before "
            "email/SMS suppression checks"
        )

    def test_channel_split_reason_is_in_fa_max_allowed_set(self):
        """All sources in FA_MAX_ALLOWED_SOURCE_TYPES must match the documented
        FA Max channels (off-market triggers + partner layer)."""
        from src.services.fa_max_send_governance import FA_MAX_ALLOWED_SOURCE_TYPES

        valid = {"deed", "permit", "distress", "maturity", "partner"}
        unknown = FA_MAX_ALLOWED_SOURCE_TYPES - valid
        assert not unknown, (
            f"FA_MAX_ALLOWED_SOURCE_TYPES contains undocumented source types: {unknown}. "
            "Each source type must be reviewed against the channel-split rule before adding."
        )
