"""
Tests for event handlers — each source's raw payload normalizes into an Event.
"""

import json

import pytest

from src.agents.events.handlers import from_admin, from_cron, from_postgres, from_redis


def test_from_redis_parses_bytes():
	raw = json.dumps({
		"event_type": "retention_summary_due",
		"subscriber_id": 42,
		"payload": {"tier": "wallet"},
	}).encode("utf-8")
	ev = from_redis(raw)
	assert ev.event_type == "retention_summary_due"
	assert ev.subscriber_id == 42
	assert ev.payload == {"tier": "wallet"}
	assert ev.source == "redis"


def test_from_postgres_parses_string():
	raw = json.dumps({
		"event_type": "competitor_acted_on_lead",
		"subscriber_id": 1,
		"payload": {"zip_code": "33647"},
	})
	ev = from_postgres(raw)
	assert ev.event_type == "competitor_acted_on_lead"
	assert ev.source == "postgres"


def test_from_cron_and_admin_use_dicts_directly():
	payload = {
		"event_type": "retention_summary_due",
		"subscriber_id": 7,
		"payload": {"tier": "lock"},
	}
	ev_cron = from_cron(payload)
	ev_admin = from_admin(payload)
	assert ev_cron.source == "cron"
	assert ev_admin.source == "admin"
	assert ev_cron.event_type == ev_admin.event_type == "retention_summary_due"


def test_missing_event_type_raises():
	with pytest.raises(ValueError, match="event_type"):
		from_cron({"subscriber_id": 1, "payload": {}})


def test_decision_id_preserved_when_provided():
	ev = from_cron({
		"event_type": "abandonment_click_no_complete",
		"subscriber_id": 1,
		"payload": {},
		"decision_id": "shared-uuid",
	})
	assert ev.decision_id == "shared-uuid"


def test_to_dispatch_dict_shape():
	ev = from_admin({
		"event_type": "competitor_acted_on_lead",
		"subscriber_id": 107,
		"payload": {"zip_code": "33647"},
		"decision_id": "abc",
	})
	d = ev.to_dispatch_dict()
	assert d == {
		"event_type": "competitor_acted_on_lead",
		"subscriber_id": 107,
		"payload": {"zip_code": "33647"},
		"decision_id": "abc",
	}
