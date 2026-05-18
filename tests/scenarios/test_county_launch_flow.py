"""
Scenario test: full county launch flow.

Markers: scenario_platform
Requires: fakeredis, Slack stubs, in-memory DB with JSONB support.
Skip if no Postgres available.
"""
import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest


pytestmark = pytest.mark.scenario_platform


@pytest.fixture
def fake_redis_store(monkeypatch):
    store = {}

    def _rget(key):
        return store.get(key)

    def _rset(key, value, ttl_seconds=None):
        store[key] = value

    def _available():
        return True

    monkeypatch.setattr("src.tasks.county_launch_evaluator.rget", _rget)
    monkeypatch.setattr("src.tasks.county_launch_evaluator.redis_available", _available)
    monkeypatch.setattr("src.tasks.county_launch_runner.rget", _rget)
    return store


def _seed_all_green(store, county_id="hillsborough"):
    store[f"fa:ks_metric:{county_id}:first_payment_rate"] = "35.0"
    store[f"fa:ks_metric:{county_id}:saved_card_rate"] = "75.0"
    store[f"fa:ks_metric:{county_id}:wallet_adoption"] = "20.0"
    store[f"fa:ks_metric:{county_id}:lock_conversion"] = "7.0"
    store[f"fa:ks_metric:{county_id}:retention_30d"] = "75.0"
    store[f"fa:ks_metric:{county_id}:free_tier_cost_ratio"] = "35.0"
    store[f"fa:ks_metric:{county_id}:county_profitability"] = "1.0"


@pytest.mark.skipif(
    True,  # Remove this when running against real Postgres
    reason="Requires Postgres with JSONB support",
)
def test_full_launch_flow(fake_redis_store, monkeypatch, fresh_db):
    """Seed candidate → evaluate → approve → run → assert launched."""
    from src.core.models import CountyLaunchAudit, ExpansionCandidate

    # Step 1: seed candidate
    candidate = ExpansionCandidate(county_id="pinellas", priority=100, status="queued")
    fresh_db.add(candidate)
    fresh_db.commit()

    # Step 2: seed all-green Redis
    _seed_all_green(fake_redis_store)

    # Step 3: mock Slack
    mock_slack = MagicMock()
    mock_slack.chat_postMessage.return_value = {"ts": "111.222"}
    monkeypatch.setattr("src.tasks.county_launch_evaluator.WebClient", lambda token: mock_slack)
    monkeypatch.setattr("config.settings.settings.slack_bot_token",
                        MagicMock(get_secret_value=lambda: "xoxb-test"))
    monkeypatch.setattr("config.settings.settings.county_launch_slack_channel", "#expansion")

    # Step 4: run evaluator
    from src.tasks.county_launch_evaluator import run_county_launch_evaluator
    result = run_county_launch_evaluator(dry_run=False)
    assert result.get("posted") is True
    mock_slack.chat_postMessage.assert_called_once()

    # Refresh candidate
    fresh_db.expire(candidate)
    assert candidate.last_slack_message_ts == "111.222"

    # Step 5: simulate Slack approval
    candidate.status = "approved"
    candidate.approved_at = datetime.now(timezone.utc)
    candidate.approved_by_slack_user = "U_APPROVER"
    fresh_db.commit()

    # Step 6: run launch runner
    mock_slack.chat_postMessage.reset_mock()
    from src.tasks.county_launch_runner import run_county_launch_runner
    launch_result = run_county_launch_runner(dry_run=False)
    assert launch_result.get("launched") is True

    fresh_db.expire(candidate)
    assert candidate.status == "launched"

    audit_rows = fresh_db.query(CountyLaunchAudit).filter_by(county_id="pinellas").all()
    event_types = [r.event_type for r in audit_rows]
    assert "posted" in event_types
    assert "launch_started" in event_types
    assert "launched" in event_types


@pytest.mark.skipif(
    True,
    reason="Requires Postgres with JSONB support",
)
def test_gate_regresses_between_approve_and_launch(fake_redis_store, monkeypatch, fresh_db):
    """Gate goes red between approval and launch → candidate aborted."""
    from src.core.models import CountyLaunchAudit, ExpansionCandidate

    candidate = ExpansionCandidate(
        county_id="sarasota", priority=100, status="approved",
        approved_at=datetime.now(timezone.utc),
        approved_by_slack_user="U_APPROVER",
        last_slack_message_ts="123.456",
    )
    fresh_db.add(candidate)
    fresh_db.commit()

    # Flip one gate red
    _seed_all_green(fake_redis_store)
    fake_redis_store["fa:ks_metric:hillsborough:first_payment_rate"] = "10.0"  # red

    mock_slack = MagicMock()
    monkeypatch.setattr("src.tasks.county_launch_runner.WebClient", lambda token: mock_slack)
    monkeypatch.setattr("config.settings.settings.slack_bot_token",
                        MagicMock(get_secret_value=lambda: "xoxb-test"))
    monkeypatch.setattr("config.settings.settings.county_launch_slack_channel", "#expansion")

    from src.tasks.county_launch_runner import run_county_launch_runner
    result = run_county_launch_runner(dry_run=False)

    assert result.get("aborted") is True
    fresh_db.expire(candidate)
    assert candidate.status == "queued"

    audit_rows = fresh_db.query(CountyLaunchAudit).filter_by(county_id="sarasota").all()
    assert any(r.event_type == "launch_aborted_gate_red" for r in audit_rows)
