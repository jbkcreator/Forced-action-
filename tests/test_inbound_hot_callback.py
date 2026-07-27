"""TDD for Block 11 / B11-03 — routing a hot inbound to the shared Block 2 path.

Two things under test:
  1. _trigger_hot_inbound_callback (src.api.main) publishes inbound_hot_callback
     only when the inbound scored hot AND resolved to a subscriber, and does so
     via publish_after_commit (not publish_cora_event directly) so the event
     is never picked up before the request's own transaction commits.
  2. router.EVENT_TO_GRAPH routes inbound_hot_callback to the SAME runner/graph
     as new_lead_signup — zero new call code (ticket 03 decision).
"""

from unittest.mock import MagicMock, patch

from src.agents.router import EVENT_TO_GRAPH
from src.api.main import _trigger_hot_inbound_callback


class TestRouterSharesBlock2Path:
    def test_inbound_hot_callback_maps_to_same_graph_as_new_lead_signup(self):
        inbound_spec = EVENT_TO_GRAPH["inbound_hot_callback"]
        signup_spec = EVENT_TO_GRAPH["new_lead_signup"]
        assert inbound_spec.graph_name == signup_spec.graph_name == "new_lead_voice_call"
        assert inbound_spec.runner is signup_spec.runner


class TestTriggerHotInboundCallback:
    def test_not_hot_does_not_publish(self):
        db = MagicMock()
        with patch("src.agents.events.ingestion.publish_after_commit") as mock_publish:
            _trigger_hot_inbound_callback(
                db=db,
                intent={"is_hot": False, "score": 30, "matched_signals": ["transcript_intent"]},
                subscriber_id=42,
                vertical="roofing",
                call_id="call-1",
            )
        mock_publish.assert_not_called()

    def test_hot_without_subscriber_id_does_not_publish(self):
        db = MagicMock()
        with patch("src.agents.events.ingestion.publish_after_commit") as mock_publish:
            _trigger_hot_inbound_callback(
                db=db,
                intent={"is_hot": True, "score": 60, "matched_signals": ["intent_slot"]},
                subscriber_id=None,
                vertical="roofing",
                call_id="call-2",
            )
        mock_publish.assert_not_called()

    def test_hot_with_subscriber_id_publishes_after_commit(self):
        db = MagicMock()
        with patch("src.agents.events.ingestion.publish_after_commit") as mock_publish:
            _trigger_hot_inbound_callback(
                db=db,
                intent={"is_hot": True, "score": 65, "matched_signals": ["intent_slot", "known_caller"]},
                subscriber_id=42,
                vertical="roofing",
                call_id="call-3",
            )
        mock_publish.assert_called_once()
        (published_db, event), _kwargs = mock_publish.call_args
        # Must be the request's own session — publish_after_commit hooks its
        # after_commit listener onto THIS session, not a throwaway one.
        assert published_db is db
        assert event["event_type"] == "inbound_hot_callback"
        assert event["subscriber_id"] == 42
        assert event["decision_id"] == "call-3"
        assert event["payload"]["vertical"] == "roofing"
        assert event["payload"]["score"] == 65
        assert event["payload"]["matched_signals"] == ["intent_slot", "known_caller"]
