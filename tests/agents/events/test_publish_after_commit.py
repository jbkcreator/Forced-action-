"""
Tests for ingestion.publish_after_commit — PR #140 issue 4.

The signup path must publish the new_lead_signup event only after the request
transaction commits, so the agents process (a separate transaction) can never
consume the event before the subscriber row is visible. If the transaction
rolls back, the event must never be published.
"""
from unittest.mock import patch

from src.agents.events.ingestion import publish_after_commit


def test_event_not_published_before_commit(fresh_db):
    event = {"event_type": "new_lead_signup", "subscriber_id": 1, "idempotency_key": "k"}
    with patch("src.agents.events.ingestion.publish_cora_event") as mock_pub:
        publish_after_commit(fresh_db, event)
        # Registered but the session has not committed yet.
        mock_pub.assert_not_called()

        fresh_db.commit()
        mock_pub.assert_called_once_with(event)


def test_event_not_published_on_rollback(fresh_db):
    event = {"event_type": "new_lead_signup", "subscriber_id": 2, "idempotency_key": "k2"}
    with patch("src.agents.events.ingestion.publish_cora_event") as mock_pub:
        publish_after_commit(fresh_db, event)
        fresh_db.rollback()
        mock_pub.assert_not_called()


def test_listener_is_one_shot(fresh_db):
    """A single registration fires exactly once, not on every later commit."""
    event = {"event_type": "new_lead_signup", "subscriber_id": 3, "idempotency_key": "k3"}
    with patch("src.agents.events.ingestion.publish_cora_event") as mock_pub:
        publish_after_commit(fresh_db, event)
        fresh_db.commit()
        fresh_db.commit()
        assert mock_pub.call_count == 1
