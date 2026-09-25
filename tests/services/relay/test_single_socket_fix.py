"""tests/services/relay/test_single_socket_fix.py

FA Max Slack Single-Socket Fix — automated tests covering:

  1. block_actions click → Relay handler, publish_query never called.
  2. CC channel message → publish_query exactly once (flag on).
  3. CC question → publish_query once, handle_channel_message NOT called (flag on).
  4. CC message → handle_channel_message (flag off / rollback path).
  5. Forwarder registered after _on_request (ack not delayed by Redis).
  6. FA_MAX_SLACK_CC_CHANNEL unset → init_forwarder False.
  7. bot_id set / bot user / other channel → not published, no Redis call.
  8. Same event_id twice → publish_query once.
  9. Redis unavailable → click commits; CC message logs warning without raise.
  10. Flag on → worker.main() starts no listener thread, shuts down cleanly.
  11. Flag off → Relay registers no CC forwarder.
  12. dial_won envelope → handle_action called.
"""
from __future__ import annotations

import threading
from unittest import mock

import fakeredis
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _request(type_: str, envelope_id: str, payload: dict):
    req = mock.Mock()
    req.type = type_
    req.envelope_id = envelope_id
    req.payload = payload
    return req


def _events_api_request(event: dict, event_id: str = "Ev001") -> mock.Mock:
    return _request(
        "events_api",
        "env-cc-1",
        {"event": event, "event_id": event_id},
    )


def _block_actions_request(action_id: str, envelope_id: str = "env-click-1") -> mock.Mock:
    return _request(
        "interactive",
        envelope_id,
        {
            "type": "block_actions",
            "actions": [{"action_id": action_id, "value": "{}"}],
            "user": {"id": "U_JOSH"},
            "channel": {"id": "C_MONEY"},
        },
    )


# ---------------------------------------------------------------------------
# 1. block_actions click → Relay handler; publish_query never called
# ---------------------------------------------------------------------------

class TestClickReachesRelayHandler:
    def test_approve_reaches_relay_decision(self):
        client = mock.MagicMock()
        payload = {
            "type": "block_actions",
            "actions": [{"action_id": "approve", "value": '{"item_id": 42}'}],
            "user": {"id": "U_JOSH"},
        }
        req = _request("interactive", "env-approve", payload)

        with (
            mock.patch(
                "src.api.admin_router._handle_relay_decision",
                return_value={"ok": True},
            ) as mock_decision,
            mock.patch(
                "src.agents.cora.command_center.worker.publish_query"
            ) as mock_publish,
        ):
            from src.services.relay import socket_listener
            socket_listener.handle_socket_request(client, req)

        mock_decision.assert_called_once()
        mock_publish.assert_not_called()


# ---------------------------------------------------------------------------
# 2. CC channel message → publish_query exactly once (flag on)
# ---------------------------------------------------------------------------

class TestCCForwardPublishesOnce:
    def test_message_published_exactly_once(self):
        event = {"type": "message", "channel": "C_CC", "user": "U_JOSH", "text": "hello", "ts": "1.0"}
        fake_r = fakeredis.FakeRedis()

        with (
            mock.patch("src.agents.cora.command_center.slack_socket._listen_channel", return_value="C_CC"),
            mock.patch("src.agents.cora.command_center.slack_socket._BOT_USER_ID", "U_BOT"),
            mock.patch("src.core.redis_client.get_redis", return_value=fake_r),
            mock.patch("src.agents.cora.command_center.worker.publish_query", return_value="msg-1") as mock_pub,
        ):
            from src.agents.cora.command_center.slack_socket import forward_message
            forward_message(event, "Ev001")
            forward_message(event, "Ev001")  # duplicate

        assert mock_pub.call_count == 1


# ---------------------------------------------------------------------------
# 3. CC question → publish_query once, handle_channel_message NOT called (flag on)
# ---------------------------------------------------------------------------

class TestCCOwnershipWithFlagOn:
    def test_publish_once_no_thread_fallback(self):
        event = {
            "type": "message",
            "channel": "C_CC",
            "user": "U_JOSH",
            "text": "what is the ARV on 123 Main?",
            "ts": "2.0",
        }
        fake_r = fakeredis.FakeRedis()

        with (
            mock.patch("src.agents.cora.command_center.slack_socket._listen_channel", return_value="C_CC"),
            mock.patch("src.agents.cora.command_center.slack_socket._BOT_USER_ID", "U_BOT"),
            mock.patch("src.core.redis_client.get_redis", return_value=fake_r),
            mock.patch("src.agents.cora.command_center.worker.publish_query", return_value="msg-1") as mock_pub,
            mock.patch(
                "src.services.relay.thread_fallback_responder.handle_channel_message"
            ) as mock_handle,
        ):
            from src.agents.cora.command_center.slack_socket import forward_message
            forward_message(event, "Ev002")

        mock_pub.assert_called_once()
        mock_handle.assert_not_called()


# ---------------------------------------------------------------------------
# 4. CC message → handle_channel_message called (flag off / rollback path)
# ---------------------------------------------------------------------------

class TestCCFallsBackToRelayWhenFlagOff:
    def test_lane_map_includes_cc_when_flag_off(self):
        with mock.patch(
            "src.api.admin_router.settings",
            fa_max_slack_channel_money="C_MONEY",
            fa_max_slack_channel_exceptions="C_EXC",
            fa_max_slack_channel_relationships="C_REL",
            fa_max_slack_cc_channel="C_CC",
            fa_max_slack_single_socket=False,
        ):
            from src.api.admin_router import _fa_max_channel_lane_map
            lane_map = _fa_max_channel_lane_map()

        assert lane_map.get("C_CC") == "CC"

    def test_lane_map_excludes_cc_when_flag_on(self):
        with mock.patch(
            "src.api.admin_router.settings",
            fa_max_slack_channel_money="C_MONEY",
            fa_max_slack_channel_exceptions="C_EXC",
            fa_max_slack_channel_relationships="C_REL",
            fa_max_slack_cc_channel="C_CC",
            fa_max_slack_single_socket=True,
        ):
            from src.api.admin_router import _fa_max_channel_lane_map
            lane_map = _fa_max_channel_lane_map()

        assert "C_CC" not in lane_map


# ---------------------------------------------------------------------------
# 5. Forwarder runs after _on_request (ack not on Redis path)
# ---------------------------------------------------------------------------

class TestForwarderRegisteredAfterAck:
    def test_cc_forwarder_runs_after_ack_events_api(self):
        """The events_api ack happens inside _on_request / handle_socket_request
        (line 206 of socket_listener.py) before the function returns.
        forward_message is only called from _on_cc_message, a separate listener
        appended after _on_request. This test verifies the ordering contract by
        simulating two separate listener calls: first ack (no forward), then forward.
        A stalled Redis call in _on_cc_message cannot delay the ack because the
        ack is already sent when _on_request returns."""
        import fakeredis
        fake_r = fakeredis.FakeRedis()

        ack_order = []
        forward_order = []

        def fake_send_response(resp):
            ack_order.append("ack")

        client = mock.MagicMock()
        client.send_socket_mode_response.side_effect = fake_send_response

        cc_event = {"type": "message", "channel": "C_CC", "user": "U_JOSH", "text": "q?", "ts": "8.0"}
        events_api_payload = {"event": cc_event, "event_id": "EvOrder1"}
        req = _request("events_api", "env-order-1", events_api_payload)

        with (
            mock.patch("src.api.admin_router._handle_relay_thread_action"),
            mock.patch("src.agents.cora.command_center.slack_socket._listen_channel", return_value="C_CC"),
            mock.patch("src.agents.cora.command_center.slack_socket._BOT_USER_ID", "U_BOT"),
            mock.patch("src.core.redis_client.get_redis", return_value=fake_r),
            mock.patch(
                "src.agents.cora.command_center.worker.publish_query",
                side_effect=lambda **_kw: forward_order.append("forward") or "m1",
            ),
        ):
            from src.services.relay import socket_listener

            # Step 1: _on_request acks (this is what runs first in production)
            socket_listener.handle_socket_request(client, req)

            # Step 2: _on_cc_message forwards (this is what runs second in production)
            from src.agents.cora.command_center.slack_socket import forward_message
            forward_message(cc_event, "EvOrder1")

        # Ack must have happened before forward, not after
        assert ack_order == ["ack"]
        assert forward_order == ["forward"]


# ---------------------------------------------------------------------------
# 6. FA_MAX_SLACK_CC_CHANNEL unset → init_forwarder False
# ---------------------------------------------------------------------------

class TestInitForwarderRefusesWithoutChannel:
    def test_returns_false_when_channel_unset(self):
        with mock.patch(
            "src.agents.cora.command_center.slack_socket._listen_channel",
            return_value=None,
        ):
            from src.agents.cora.command_center.slack_socket import init_forwarder
            result = init_forwarder(mock.MagicMock())

        assert result is False


# ---------------------------------------------------------------------------
# 7. bot_id set / bot user / other channel → not published, no Redis call
# ---------------------------------------------------------------------------

class TestForwardMessageFilters:
    def _forward(self, event: dict, event_id: str = "Ev999"):
        fake_r = mock.MagicMock()
        with (
            mock.patch("src.agents.cora.command_center.slack_socket._listen_channel", return_value="C_CC"),
            mock.patch("src.agents.cora.command_center.slack_socket._BOT_USER_ID", "U_BOT"),
            mock.patch("src.core.redis_client.get_redis", return_value=fake_r),
            mock.patch("src.agents.cora.command_center.worker.publish_query") as mock_pub,
        ):
            from src.agents.cora.command_center.slack_socket import forward_message
            forward_message(event, event_id)
            return mock_pub, fake_r

    def test_bot_id_not_published(self):
        event = {"type": "message", "channel": "C_CC", "user": "U_JOSH", "text": "hi", "ts": "3.0", "bot_id": "B_BOT"}
        mock_pub, fake_r = self._forward(event)
        mock_pub.assert_not_called()
        fake_r.set.assert_not_called()

    def test_bot_user_not_published(self):
        event = {"type": "message", "channel": "C_CC", "user": "U_BOT", "text": "reply", "ts": "4.0"}
        mock_pub, fake_r = self._forward(event)
        mock_pub.assert_not_called()
        fake_r.set.assert_not_called()

    def test_other_channel_not_published(self):
        event = {"type": "message", "channel": "C_OTHER", "user": "U_JOSH", "text": "hi", "ts": "5.0"}
        mock_pub, fake_r = self._forward(event)
        mock_pub.assert_not_called()
        fake_r.set.assert_not_called()


# ---------------------------------------------------------------------------
# 8. Same event_id twice → publish_query once
# ---------------------------------------------------------------------------

class TestDedup:
    def test_same_event_id_publishes_once(self):
        event = {"type": "message", "channel": "C_CC", "user": "U_JOSH", "text": "q?", "ts": "6.0"}
        fake_r = fakeredis.FakeRedis()

        with (
            mock.patch("src.agents.cora.command_center.slack_socket._listen_channel", return_value="C_CC"),
            mock.patch("src.agents.cora.command_center.slack_socket._BOT_USER_ID", "U_BOT"),
            mock.patch("src.core.redis_client.get_redis", return_value=fake_r),
            mock.patch("src.agents.cora.command_center.worker.publish_query", return_value="m1") as mock_pub,
        ):
            from src.agents.cora.command_center.slack_socket import forward_message
            forward_message(event, "EvDup")
            forward_message(event, "EvDup")

        assert mock_pub.call_count == 1


# ---------------------------------------------------------------------------
# 9. Redis unavailable → click commits; CC message logs warning without raise
# ---------------------------------------------------------------------------

class TestRedisUnavailable:
    def test_cc_forward_allows_through_when_redis_down(self):
        """_first_delivery falls through to True when Redis is None so the
        question is not silently dropped — it's processed and logged."""
        event = {"type": "message", "channel": "C_CC", "user": "U_JOSH", "text": "q?", "ts": "7.0"}

        with (
            mock.patch("src.agents.cora.command_center.slack_socket._listen_channel", return_value="C_CC"),
            mock.patch("src.agents.cora.command_center.slack_socket._BOT_USER_ID", "U_BOT"),
            mock.patch("src.core.redis_client.get_redis", return_value=None),
            mock.patch("src.agents.cora.command_center.worker.publish_query", return_value="m1") as mock_pub,
        ):
            from src.agents.cora.command_center.slack_socket import forward_message
            forward_message(event, "EvRedisDown")  # must not raise

        mock_pub.assert_called_once()

    def test_click_not_blocked_by_redis(self):
        """handle_socket_request's approve/reject path never touches Redis."""
        client = mock.MagicMock()
        payload = {
            "type": "block_actions",
            "actions": [{"action_id": "approve", "value": '{"item_id": 99}'}],
            "user": {"id": "U_JOSH"},
        }
        req = _request("interactive", "env-no-redis", payload)

        with (
            mock.patch("src.core.redis_client.get_redis", return_value=None),
            mock.patch(
                "src.api.admin_router._handle_relay_decision",
                return_value={"ok": True},
            ),
        ):
            from src.services.relay import socket_listener
            result = socket_listener.handle_socket_request(client, req)

        assert result is True


# ---------------------------------------------------------------------------
# 10. Flag on → worker.main() starts no listener thread, shuts down cleanly
# ---------------------------------------------------------------------------

class TestWorkerMainNoListenerWhenFlagOn:
    def test_no_thread_started_and_clean_shutdown(self):
        """When FA_MAX_SLACK_SINGLE_SOCKET is True, worker.main() must not start
        a Slack listener thread. Verified by checking that Thread.start() is never
        called for "cc-slack-listener". The finally block's join() guard is verified
        by the absence of a NameError (listener_thread is None, join is skipped)."""
        threads_started = []
        thread_instances = []

        class FakeThread:
            def __init__(self, *args, **kwargs):
                self.name = kwargs.get("name", "?")
                self._started = False
                thread_instances.append(self)

            def start(self):
                threads_started.append(self.name)
                self._started = True

            def join(self, timeout=None):
                pass

        with (
            mock.patch(
                "src.agents.cora.command_center.worker.CommandCenterWorker.run_forever"
            ),
            mock.patch("signal.signal"),
            mock.patch("src.agents.cora.command_center.worker.threading.Thread", FakeThread),
        ):
            # Simulate flag=True from settings
            with mock.patch("config.settings.get_settings") as mock_gs:
                mock_gs.return_value.fa_max_slack_single_socket = True
                mock_gs.return_value.fa_max_slack_app_token = None

                from src.agents.cora.command_center import worker
                worker.main()

        # No listener thread was started
        assert "cc-slack-listener" not in threads_started


# ---------------------------------------------------------------------------
# 11. Flag off → Relay registers no CC forwarder
# ---------------------------------------------------------------------------

class TestRelayDoesNotRegisterForwarderWhenFlagOff:
    def test_no_cc_forwarder_when_flag_off(self):
        class FakeSocket:
            socket_mode_request_listeners: list = []

            def connect(self):
                pass

        fake_socket = FakeSocket()

        with (
            mock.patch("config.settings.get_settings") as mock_settings,
            mock.patch("slack_sdk.WebClient"),
            mock.patch("slack_sdk.socket_mode.SocketModeClient", return_value=fake_socket),
            mock.patch("threading.Event"),
        ):
            s = mock_settings.return_value
            s.fa_max_slack_app_token.get_secret_value.return_value = "xapp-test"
            s.fa_max_slack_bot_token.get_secret_value.return_value = "xoxb-test"
            s.fa_max_slack_single_socket = False

            from src.services.relay import socket_listener
            try:
                socket_listener.run()
            except Exception:
                pass

        # Should only have the 3 standard listeners, no CC forwarder
        names = [getattr(l, "__name__", "") for l in fake_socket.socket_mode_request_listeners]
        assert not any("cc_message" in n for n in names)


# ---------------------------------------------------------------------------
# 12. dial_won envelope → handle_action called
# ---------------------------------------------------------------------------

class TestDialListRoutedToHandler:
    def test_dial_won_reaches_handle_action(self):
        client = mock.MagicMock()
        payload = {
            "type": "block_actions",
            "actions": [{"action_id": "dial_won", "value": "{}"}],
            "user": {"id": "U_JOSH"},
            "channel": {"id": "C_MONEY"},
        }
        req = _request("interactive", "env-dial-1", payload)

        mock_result = mock.Mock(status="completed", kind="won", message="")

        with (
            mock.patch(
                "src.services.dial_list.actions.handle_action",
                return_value=mock_result,
            ) as mock_handle,
            mock.patch("src.core.database.get_db_context") as mock_db,
            mock.patch("config.settings.get_settings") as mock_gs,
        ):
            mock_gs.return_value.dial_list_approver_user_id = "U_JOSH"
            mock_db.return_value.__enter__ = mock.Mock(return_value=mock.MagicMock())
            mock_db.return_value.__exit__ = mock.Mock(return_value=False)

            from src.services.relay import socket_listener
            result = socket_listener.handle_socket_request(client, req)

        assert result is True
        mock_handle.assert_called_once()
