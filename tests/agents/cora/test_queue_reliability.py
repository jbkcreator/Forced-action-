from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src.agents.cora import locks, queue, worker
from tests.agents.cora.helpers import new_consumer_name


@pytest.fixture
def stub_main_graph(monkeypatch):
    """Decouples queue/worker reliability tests from needing a real DB/Claude call."""
    stub = MagicMock(return_value={"terminal_status": "completed"})
    monkeypatch.setattr(worker, "run_cora_main", stub)
    return stub


def _payload(thread_id: str) -> dict:
    return {"buyer_entity": {"opportunity_thread_id": thread_id}}


# Acceptance item 9 (queue layer): duplicate event -> no reprocessing.
def test_idempotency_prevents_reprocessing(stub_main_graph):
    thread_id = "OPP-QUEUE-TEST-1"
    key = queue.make_idempotency_key("target.ready", thread_id, "same-content")
    queue.publish("target.ready", _payload(thread_id), idempotency_key=key)

    w = worker.Worker(consumer_name=new_consumer_name())
    [w._process_one(m) for m in queue.read_batch(w.consumer_name, count=10, block_ms=200)]
    assert stub_main_graph.call_count == 1
    assert queue.pending_count() == 0

    # Republish with the SAME idempotency_key — must be acked without reprocessing.
    queue.publish("target.ready", _payload(thread_id), idempotency_key=key)
    [w._process_one(m) for m in queue.read_batch(w.consumer_name, count=10, block_ms=200)]
    assert stub_main_graph.call_count == 1  # unchanged
    assert queue.pending_count() == 0


def test_ack_happens_only_after_handler_returns(stub_main_graph):
    thread_id = "OPP-QUEUE-TEST-2"
    queue.publish("target.ready", _payload(thread_id))
    w = worker.Worker(consumer_name=new_consumer_name())
    messages = queue.read_batch(w.consumer_name, count=10, block_ms=200)
    assert len(messages) == 1
    assert queue.pending_count() == 1  # delivered but not yet processed/acked

    w._process_one(messages[0])
    assert queue.pending_count() == 0  # acked only after the handler returned cleanly


def test_handler_exception_leaves_message_unacked_for_retry(monkeypatch):
    thread_id = "OPP-QUEUE-TEST-3"
    monkeypatch.setattr(worker, "run_cora_main", MagicMock(side_effect=RuntimeError("boom")))
    queue.publish("target.ready", _payload(thread_id))
    w = worker.Worker(consumer_name=new_consumer_name())
    messages = queue.read_batch(w.consumer_name, count=10, block_ms=200)
    w._process_one(messages[0])
    assert queue.pending_count() == 1  # NOT acked — left for claim_stale to retry


# One-active-execution-per-opportunity_thread_id.
def test_lock_contention_leaves_message_unacked(stub_main_graph):
    thread_id = "OPP-QUEUE-TEST-4"
    queue.publish("target.ready", _payload(thread_id))
    w = worker.Worker(consumer_name=new_consumer_name())
    messages = queue.read_batch(w.consumer_name, count=10, block_ms=200)

    acquired = locks.acquire(thread_id, owner="some-other-worker")
    assert acquired is True
    w._process_one(messages[0])
    assert stub_main_graph.call_count == 0  # never even attempted — lock held elsewhere
    assert queue.pending_count() == 1  # left unacked for retry

    locks.release(thread_id, owner="some-other-worker")


# Acceptance item 14: failure after 3 attempts -> dead-letter.
def test_max_deliveries_dead_letters(monkeypatch):
    monkeypatch.setattr(worker, "run_cora_main", MagicMock(side_effect=RuntimeError("persistent failure")))
    thread_id = "OPP-QUEUE-TEST-5"
    queue.publish("target.ready", _payload(thread_id))

    w = worker.Worker(consumer_name=new_consumer_name())
    messages = queue.read_batch(w.consumer_name, count=10, block_ms=200)
    w._process_one(messages[0])  # delivery 1 — fails, stays pending

    for _ in range(3):  # reclaim + fail two more times (deliveries 2 and 3), then dead-letter on the 4th claim
        reclaimed = queue.claim_stale(w.consumer_name, min_idle_ms=0)
        for m in reclaimed:
            w._process_one(m)

    assert queue.pending_count() == 0
    assert queue.dlq_depth() == 1


def test_worker_reclaims_and_completes_after_transient_failure(monkeypatch):
    thread_id = "OPP-QUEUE-TEST-6"
    call_count = {"n": 0}

    def flaky(event_type, payload, thread_id_arg):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise RuntimeError("transient")
        return {"terminal_status": "completed"}

    monkeypatch.setattr(worker, "run_cora_main", flaky)
    queue.publish("target.ready", _payload(thread_id))

    w = worker.Worker(consumer_name=new_consumer_name())
    messages = queue.read_batch(w.consumer_name, count=10, block_ms=200)
    w._process_one(messages[0])
    assert queue.pending_count() == 1

    reclaimed = queue.claim_stale(w.consumer_name, min_idle_ms=0)
    assert len(reclaimed) == 1
    w._process_one(reclaimed[0])
    assert queue.pending_count() == 0
    assert queue.dlq_depth() == 0
    assert call_count["n"] == 2
