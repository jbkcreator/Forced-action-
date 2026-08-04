"""
Tests for src.services.relay.queue (RELAY-v2.2 sub-task R1).

Covers the pure, no-DB logic: the row->QueueItem mapping helper and the
column-list consistency between QueueItem's fields and the SQL SELECT
column list (a regression guard against a future field being added to one
but not the other). The DB-touching functions (enqueue, record_decision,
approved_batch, try_claim_for_batch) were validated live end-to-end against
Postgres during implementation (seed -> approve -> sweep -> sent, plus a
verified no-op re-sweep) — the same convention Vera's V2/V3 CLI validation
used on staging.
"""
from __future__ import annotations

import inspect
from dataclasses import fields
from datetime import datetime, timezone

from config.venture_template import DEFAULT_VENTURE_KEY
from src.services.relay import queue as relay_queue
from src.services.relay.queue import QueueItem


def test_enqueue_signature_matches_documented_contract():
    """RELAY-v2.2 sub-task R4: enqueue()'s parameter list IS the
    batch-intake contract (see the module docstring). If a future change
    adds, removes, or renames a parameter, this must fail loudly rather
    than let the contract doc silently drift from the real function."""
    sig = inspect.signature(relay_queue.enqueue)
    params = sig.parameters

    assert set(params) == {
        "idempotency_key", "channel", "recipient", "payload", "thread_id",
        # CLONE-v2.2 / CL3 — which venture proposed the action.
        "venture_key",
    }

    required = {name for name, p in params.items() if p.default is inspect.Parameter.empty}
    assert required == {"idempotency_key", "channel", "recipient", "payload"}
    assert params["thread_id"].default is None
    # Defaulted, so every pre-CL3 call site keeps enqueueing to venture #1.
    assert params["venture_key"].default == DEFAULT_VENTURE_KEY

    # All keyword-only (enqueue is called with kwargs everywhere -- __main__.py,
    # this contract doc, and Cora's future call site all rely on that).
    assert all(p.kind == inspect.Parameter.KEYWORD_ONLY for p in params.values())


def test_columns_sql_matches_dataclass_fields():
    """If a field is ever added to QueueItem without updating the SELECT
    column list (or vice versa), this catches it immediately instead of
    surfacing as a silent KeyError at runtime."""
    dataclass_field_names = {f.name for f in fields(QueueItem)}
    assert set(relay_queue._QUEUE_ITEM_COLUMNS) == dataclass_field_names
    assert relay_queue._COLUMNS_SQL == ", ".join(relay_queue._QUEUE_ITEM_COLUMNS)


def test_row_to_item_maps_all_columns():
    now = datetime.now(timezone.utc)
    row = {
        "id": 1,
        "idempotency_key": "key-1",
        "batch_id": "batch-1",
        "thread_id": "OPP-2026-00001",
        "channel": "noop",
        "recipient": "test@example.com",
        "payload": {"subject": "hi"},
        "status": "sent",
        "slack_message_ts": "123.456",
        "decided_by": "U_TEST",
        "decided_at": now,
        "error": None,
        "dispatched_at": now,
        "created_at": now,
        "venture_key": "hillsborough_distress",
    }

    item = relay_queue._row_to_item(row)

    assert isinstance(item, QueueItem)
    assert item.id == 1
    assert item.idempotency_key == "key-1"
    assert item.status == "sent"
    assert item.payload == {"subject": "hi"}
    assert item.venture_key == "hillsborough_distress"
