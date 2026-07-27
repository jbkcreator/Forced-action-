"""
Cora worker metrics — prometheus_client, the dependency already used by
src/api/metrics_router.py (import confirmed there). Module-level global
Counter/Histogram/Gauge, standard prometheus_client pattern for a
long-running worker process (distinct from metrics_router.py's own
per-request Gauge pattern, which fits an HTTP pull endpoint, not a worker
loop).
"""
from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

CORA_EVENTS_PROCESSED = Counter(
    "cora_events_processed_total",
    "Cora worker: events processed, by event_type and outcome.",
    ["event_type", "outcome"],  # outcome: completed | rejected | failed
)

CORA_EVENT_LATENCY_SECONDS = Histogram(
    "cora_event_latency_seconds",
    "Cora worker: time from dequeue to ack, by event_type.",
    ["event_type"],
)

CORA_QUEUE_DEPTH = Gauge(
    "cora_queue_depth",
    "Cora worker: unacked backlog on the cora:events consumer group.",
)

CORA_DLQ_DEPTH = Gauge(
    "cora_dlq_depth",
    "Cora worker: total entries on the cora:dlq dead-letter stream.",
)


def record_event(event_type: str, outcome: str, duration_seconds: float) -> None:
    CORA_EVENTS_PROCESSED.labels(event_type=event_type, outcome=outcome).inc()
    CORA_EVENT_LATENCY_SECONDS.labels(event_type=event_type).observe(duration_seconds)


def refresh_queue_gauges() -> None:
    from src.agents.cora import queue
    CORA_QUEUE_DEPTH.set(queue.pending_count())
    CORA_DLQ_DEPTH.set(queue.dlq_depth())
