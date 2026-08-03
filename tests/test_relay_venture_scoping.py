"""
CLONE-v2.2 CL3 — Relay's per-venture scoping.

Covers the seams that would let two ventures contaminate each other if they
were missed: the approval-queue batch, the sweep's config resolution, the
kill-switch key, and the Slack approval channel.

queue.approved_batch() and queue.enqueue() open their own sessions, so these
tests must COMMIT their fixtures rather than ride fresh_db's nested
transaction. Every one of them therefore deletes what it wrote in a `finally`,
children before parents — the CL2 rollup test proved that a committed fixture
without matching teardown silently inflates later runs.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy import text

from config.venture_template import DEFAULT_VENTURE_KEY
from src.services.relay import guards, queue as relay_queue, slack_post, sweep
from src.services.relay.queue import QueueItem
from src.utils import venture_config


@pytest.fixture(autouse=True)
def clean_cache():
    venture_config.invalidate_cache()
    yield
    venture_config.invalidate_cache()


@pytest.fixture
def two_ventures(fresh_db):
    """Two committed ventures with distinct Slack channels and ceilings, plus
    one 'approved' queue row each. Torn down in full."""
    suffix = uuid.uuid4().hex[:8]
    keys = (f"vs_a_{suffix}", f"vs_b_{suffix}")
    idempotency_keys = [f"vs-key-{suffix}-{key}" for key in keys]

    try:
        for index, key in enumerate(keys):
            fresh_db.execute(text("""
                INSERT INTO ventures (
                    venture_key, display_name, brand_name, relay_slack_channel,
                    relay_daily_ceiling, kill_switch_feature
                )
                VALUES (:vk, :name, :name, :channel, :ceiling, :kill)
            """), {
                "vk": key,
                "name": f"Venture {key}",
                "channel": f"#approvals-{key}",
                "ceiling": 10 + index,
                "kill": f"relay_{key}",
            })
            fresh_db.execute(text("""
                INSERT INTO relay_approval_queue (
                    idempotency_key, channel, recipient, payload, status, venture_key
                )
                VALUES (:ik, 'noop', :recipient, '{"subject": "Hi"}'::jsonb, 'approved', :vk)
            """), {
                "ik": idempotency_keys[index],
                "recipient": f"prospect-{key}@example.com",
                "vk": key,
            })
        fresh_db.commit()

        yield {"keys": keys, "idempotency_keys": idempotency_keys}
    finally:
        fresh_db.execute(
            text("DELETE FROM relay_approval_queue WHERE venture_key = ANY(:keys)"),
            {"keys": list(keys)},
        )
        fresh_db.execute(
            text("DELETE FROM ventures WHERE venture_key = ANY(:keys)"),
            {"keys": list(keys)},
        )
        fresh_db.commit()


# ---------------------------------------------------------------------------
# Queue scoping
# ---------------------------------------------------------------------------

def test_approved_batch_filters_to_one_venture(two_ventures):
    first, second = two_ventures["keys"]

    batch = relay_queue.approved_batch(limit=50, venture_key=first)

    assert [item.venture_key for item in batch] == [first]
    assert second not in {item.venture_key for item in batch}


def test_approved_batch_without_a_venture_key_spans_ventures(two_ventures):
    """None preserves the pre-CL3 behavior, so any caller that does not care
    about ventures is unaffected."""
    first, second = two_ventures["keys"]

    seen = {item.venture_key for item in relay_queue.approved_batch(limit=500)}

    assert {first, second} <= seen


def test_enqueue_records_the_venture(fresh_db):
    suffix = uuid.uuid4().hex[:8]
    venture_key = f"vs_e_{suffix}"
    idempotency_key = f"vs-enq-{suffix}"
    try:
        fresh_db.execute(text("""
            INSERT INTO ventures (venture_key, display_name, brand_name)
            VALUES (:vk, 'Enqueue Venture', 'Enqueue Venture')
        """), {"vk": venture_key})
        fresh_db.commit()

        item = relay_queue.enqueue(
            idempotency_key=idempotency_key,
            channel="noop",
            recipient="prospect@example.com",
            payload={"subject": "Hi"},
            venture_key=venture_key,
        )

        assert item.venture_key == venture_key
        assert relay_queue.get_item(item.id).venture_key == venture_key
    finally:
        fresh_db.execute(
            text("DELETE FROM relay_approval_queue WHERE idempotency_key = :ik"),
            {"ik": idempotency_key},
        )
        fresh_db.execute(
            text("DELETE FROM ventures WHERE venture_key = :vk"), {"vk": venture_key}
        )
        fresh_db.commit()


def test_enqueue_defaults_to_venture_one(fresh_db):
    idempotency_key = f"vs-default-{uuid.uuid4().hex[:8]}"
    try:
        item = relay_queue.enqueue(
            idempotency_key=idempotency_key,
            channel="noop",
            recipient="prospect@example.com",
            payload={"subject": "Hi"},
        )
        assert item.venture_key == DEFAULT_VENTURE_KEY
    finally:
        fresh_db.execute(
            text("DELETE FROM relay_approval_queue WHERE idempotency_key = :ik"),
            {"ik": idempotency_key},
        )
        fresh_db.commit()


# ---------------------------------------------------------------------------
# Sweep scoping
# ---------------------------------------------------------------------------

def test_sweep_passes_only_that_ventures_items_and_config(monkeypatch, two_ventures):
    first, second = two_ventures["keys"]
    captured: dict = {}

    monkeypatch.setattr(sweep, "sync_unsubscribes", lambda venture_key=None: 0)
    monkeypatch.setattr(
        sweep, "execute_batch",
        lambda items, *, batch_id, venture=None: captured.update(
            items=items, venture=venture
        ) or sweep.BatchResult(),
    )

    sweep.run_sweep(venture_key=first)

    assert [item.venture_key for item in captured["items"]] == [first]
    assert captured["venture"].venture_key == first
    assert captured["venture"].relay_slack_channel == f"#approvals-{first}"
    assert captured["venture"].kill_switch_feature == f"relay_{first}"
    assert captured["venture"].relay_daily_ceiling == 10
    assert second not in {item.venture_key for item in captured["items"]}


def test_sweep_defaults_to_venture_one(monkeypatch):
    captured: dict = {}
    monkeypatch.setattr(sweep, "sync_unsubscribes", lambda venture_key=None: 0)
    monkeypatch.setattr(
        sweep.queue, "approved_batch",
        lambda limit=50, venture_key=None: captured.update(venture_key=venture_key) or [],
    )

    sweep.run_sweep()

    assert captured["venture_key"] == DEFAULT_VENTURE_KEY


# ---------------------------------------------------------------------------
# Engine: kill switch key comes from the venture
# ---------------------------------------------------------------------------

def _item(venture_key: str = DEFAULT_VENTURE_KEY) -> QueueItem:
    return QueueItem(
        id=1,
        idempotency_key="k",
        batch_id=None,
        thread_id=None,
        channel="noop",
        recipient="prospect@example.com",
        payload={"subject": "Hi"},
        status="approved",
        slack_message_ts=None,
        decided_by="U",
        decided_at=None,
        error=None,
        dispatched_at=None,
        created_at=datetime.now(timezone.utc),
        venture_key=venture_key,
    )


def test_engine_checks_the_ventures_own_kill_switch_key(monkeypatch):
    from types import SimpleNamespace

    from src.services.relay import engine as relay_engine

    checked: list[str] = []
    monkeypatch.setattr(
        relay_engine, "get_kill_switch_status",
        lambda feature: checked.append(feature) or {"color": "red"},
    )
    venture = SimpleNamespace(
        venture_key="venture_two",
        kill_switch_feature="relay_venture_two",
        relay_send_window_start=0,
        relay_send_window_end=24,
        relay_send_window_timezone="America/New_York",
        relay_daily_ceiling=20,
    )

    result = relay_engine.execute_batch([_item("venture_two")], batch_id="b1", venture=venture)

    assert result.halted is True
    assert checked == ["relay_venture_two"]


def test_engine_evaluates_guards_against_the_ventures_send_window(monkeypatch):
    """A venture in another timezone must be gated on ITS window, not
    venture #1's."""
    from types import SimpleNamespace

    from src.services.relay import engine as relay_engine

    monkeypatch.setattr(relay_engine, "get_kill_switch_status", lambda feature: {"color": "green"})
    seen: dict = {}
    monkeypatch.setattr(
        relay_engine.guards, "evaluate",
        lambda item, *, now, venture=None: seen.update(venture=venture)
        or guards.Verdict(guards.DEFER, "outside_send_window"),
    )
    venture = SimpleNamespace(
        venture_key="venture_two",
        kill_switch_feature="relay_venture_two",
        relay_send_window_start=9,
        relay_send_window_end=17,
        relay_send_window_timezone="America/Chicago",
        relay_daily_ceiling=42,
    )

    result = relay_engine.execute_batch([_item("venture_two")], batch_id="b1", venture=venture)

    assert result.deferred == 1
    assert seen["venture"] is venture


# ---------------------------------------------------------------------------
# Approver authorization scoping
# ---------------------------------------------------------------------------

def test_fleet_approvers_authorize_every_venture(monkeypatch):
    """RELAY_APPROVERS is the fleet operator list — it must work for every
    venture without being duplicated onto each row. Checked without touching
    the DB or the venture cache, so a config-resolution problem can never
    silently widen or narrow authorization."""
    from src.api import admin_router

    monkeypatch.setattr(admin_router.settings, "relay_approvers", ["U_FLEET"])

    assert admin_router._relay_approver_authorized("U_FLEET", "venture_two") is True
    assert admin_router._relay_approver_authorized("U_FLEET", None) is True


def test_venture_specific_approver_is_scoped_to_that_venture(monkeypatch):
    from types import SimpleNamespace

    from src.api import admin_router

    monkeypatch.setattr(admin_router.settings, "relay_approvers", [])
    monkeypatch.setattr(
        "src.utils.venture_config.get_venture_config",
        lambda key: SimpleNamespace(
            relay_approvers=("U_TWO",) if key == "venture_two" else ()
        ),
    )

    assert admin_router._relay_approver_authorized("U_TWO", "venture_two") is True
    assert admin_router._relay_approver_authorized("U_TWO", "venture_three") is False
    # No venture context (the fleet-wide /slack/kill path) never consults a
    # venture list.
    assert admin_router._relay_approver_authorized("U_TWO", None) is False


def test_authorization_fails_closed_when_both_lists_are_empty(monkeypatch):
    from types import SimpleNamespace

    from src.api import admin_router

    monkeypatch.setattr(admin_router.settings, "relay_approvers", [])
    monkeypatch.setattr(
        "src.utils.venture_config.get_venture_config",
        lambda key: SimpleNamespace(relay_approvers=()),
    )

    assert admin_router._relay_approver_authorized("U_ANYONE", "venture_two") is False
    assert admin_router._relay_approver_authorized("", "venture_two") is False


# ---------------------------------------------------------------------------
# Slack channel scoping
# ---------------------------------------------------------------------------

def test_slack_post_uses_the_items_venture_channel(monkeypatch):
    """Two ventures sharing one channel would make it impossible to tell whose
    prospect an approve button belongs to."""
    from types import SimpleNamespace

    seen: list[str] = []
    monkeypatch.setattr(
        slack_post, "get_venture_config",
        lambda key: seen.append(key) or SimpleNamespace(relay_slack_channel="#approvals-two"),
    )
    # No bot token in the test env, so post_for_approval no-ops after
    # resolving the channel — which is exactly the lookup under test.
    slack_post.post_for_approval(_item("venture_two"))

    assert seen == ["venture_two"]


def test_slack_post_no_ops_when_the_venture_has_no_channel(monkeypatch):
    from types import SimpleNamespace

    posted: list[int] = []
    monkeypatch.setattr(
        slack_post, "get_venture_config",
        lambda key: SimpleNamespace(relay_slack_channel=""),
    )
    monkeypatch.setattr(slack_post.queue, "set_slack_message_ts", lambda *a, **k: posted.append(1))

    slack_post.post_for_approval(_item("venture_two"))

    assert posted == []
