"""Tests for broker admin service — Layer 1 of Broker State Machine."""
import uuid
from unittest.mock import MagicMock

import pytest

from src.services.broker_admin import (
    BrokerAlreadyExists,
    BrokerNotFound,
    create_broker,
    get_broker,
    list_brokers,
    set_broker_active,
)


# ---------------------------------------------------------------------------
# Unit tests (mock_db — no real DB required)
# ---------------------------------------------------------------------------

class TestCreateBrokerUnit:
    def test_success(self, mock_db):
        mock_db.execute.return_value.first.return_value = None

        create_broker(mock_db, "User@Example.com", "  Alice Smith  ")

        mock_db.add.assert_called_once()
        mock_db.flush.assert_called_once()
        added = mock_db.add.call_args[0][0]
        assert added.email == "user@example.com"
        assert added.name == "Alice Smith"

    def test_email_normalized_to_lowercase(self, mock_db):
        mock_db.execute.return_value.first.return_value = None

        create_broker(mock_db, "  BROKER@Domain.COM  ", "Bob")

        added = mock_db.add.call_args[0][0]
        assert added.email == "broker@domain.com"

    def test_role_is_broker(self, mock_db):
        mock_db.execute.return_value.first.return_value = None

        create_broker(mock_db, "broker@example.com", "Carol")

        added = mock_db.add.call_args[0][0]
        assert added.role == "broker"

    def test_is_active_true_on_create(self, mock_db):
        mock_db.execute.return_value.first.return_value = None

        create_broker(mock_db, "broker@example.com", "Dan")

        added = mock_db.add.call_args[0][0]
        assert added.is_active is True

    def test_reset_token_generated(self, mock_db):
        mock_db.execute.return_value.first.return_value = None

        create_broker(mock_db, "broker@example.com", "Eve")

        added = mock_db.add.call_args[0][0]
        assert added.reset_token is not None
        assert len(added.reset_token) > 20

    def test_duplicate_email_raises(self, mock_db):
        fake_row = MagicMock()
        mock_db.execute.return_value.first.return_value = fake_row

        with pytest.raises(BrokerAlreadyExists):
            create_broker(mock_db, "duplicate@example.com", "Frank")

        mock_db.add.assert_not_called()


# ---------------------------------------------------------------------------
# Integration tests (fresh_db — real Postgres, rolls back after each test)
# ---------------------------------------------------------------------------

class TestCreateBrokerIntegration:
    def test_create_returns_broker_with_id(self, fresh_db):
        broker = create_broker(fresh_db, "alpha@example.com", "Alpha Broker")
        assert broker.broker_id is not None
        assert broker.email == "alpha@example.com"
        assert broker.name == "Alpha Broker"

    def test_email_normalized(self, fresh_db):
        broker = create_broker(fresh_db, "  BETA@Example.COM  ", "Beta")
        assert broker.email == "beta@example.com"

    def test_role_is_broker(self, fresh_db):
        broker = create_broker(fresh_db, "gamma@example.com", "Gamma")
        assert broker.role == "broker"

    def test_reset_token_generated(self, fresh_db):
        broker = create_broker(fresh_db, "delta@example.com", "Delta")
        assert broker.reset_token is not None

    def test_duplicate_email_raises(self, fresh_db):
        create_broker(fresh_db, "epsilon@example.com", "Epsilon")
        with pytest.raises(BrokerAlreadyExists):
            create_broker(fresh_db, "EPSILON@example.com", "Epsilon 2")


class TestListBrokersIntegration:
    def test_returns_active_by_default(self, fresh_db):
        b1 = create_broker(fresh_db, "list1@example.com", "List One")
        b2 = create_broker(fresh_db, "list2@example.com", "List Two")
        set_broker_active(fresh_db, b2.broker_id, False)

        brokers = list_brokers(fresh_db)
        emails = [r.email for r in brokers]
        assert "list1@example.com" in emails
        assert "list2@example.com" not in emails

    def test_include_inactive_returns_all(self, fresh_db):
        b1 = create_broker(fresh_db, "ia1@example.com", "IA One")
        b2 = create_broker(fresh_db, "ia2@example.com", "IA Two")
        set_broker_active(fresh_db, b2.broker_id, False)

        brokers = list_brokers(fresh_db, include_inactive=True)
        emails = [r.email for r in brokers]
        assert "ia1@example.com" in emails
        assert "ia2@example.com" in emails


class TestGetBrokerIntegration:
    def test_get_existing_broker(self, fresh_db):
        created = create_broker(fresh_db, "get1@example.com", "Get One")
        fetched = get_broker(fresh_db, created.broker_id)
        assert fetched.email == "get1@example.com"

    def test_missing_broker_raises(self, fresh_db):
        with pytest.raises(BrokerNotFound):
            get_broker(fresh_db, str(uuid.uuid4()))


class TestSetBrokerActiveIntegration:
    def test_deactivate_hides_from_default_list(self, fresh_db):
        broker = create_broker(fresh_db, "deact@example.com", "Deact")
        set_broker_active(fresh_db, broker.broker_id, False)

        brokers = list_brokers(fresh_db)
        assert all(r.email != "deact@example.com" for r in brokers)

    def test_reactivate_appears_in_default_list(self, fresh_db):
        broker = create_broker(fresh_db, "react@example.com", "React")
        set_broker_active(fresh_db, broker.broker_id, False)
        set_broker_active(fresh_db, broker.broker_id, True)

        brokers = list_brokers(fresh_db)
        assert any(r.email == "react@example.com" for r in brokers)

    def test_missing_broker_raises(self, fresh_db):
        with pytest.raises(BrokerNotFound):
            set_broker_active(fresh_db, str(uuid.uuid4()), False)
