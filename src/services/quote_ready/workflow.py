"""Durable Quote Ready build and delivery, consumed by the qualification worker.

The work queue is also the delivery outbox. Results and delivery tasks commit
together. A stable delivery UUID accompanies every Slack retry; ambiguous
provider success remains at-least-once, not a claim of exactly-once delivery.
"""
from __future__ import annotations

import json
import logging
from typing import Any

from sqlalchemy import text

from config.fa_max_qualification import CHECKLIST_VERSION, PENDING_CONTRACT_APPROVAL_TYPES
from src.core.database import get_db_context
from src.services.state_engine import enqueue_work_item

BUILD_QUEUE = "fa_max_quote_ready"
DELIVERY_QUEUE = "fa_max_quote_ready_delivery"
WORKFLOW_QUEUES = (BUILD_QUEUE, DELIVERY_QUEUE)
logger = logging.getLogger(__name__)


def enqueue_transition_build(session, *, opportunity_id: str) -> None:
    """Standalone Tier 1 trigger; qualification enqueues its own stricter task."""
    row = session.execute(text("""
        SELECT o.state_version, COALESCE(f.facts_revision, 0) AS revision
        FROM fa_max_opportunities o LEFT JOIN fa_max_opportunity_facts f
          ON f.opportunity_id = o.opportunity_id
        WHERE o.opportunity_id = :oid ::uuid
    """), {"oid": opportunity_id}).mappings().one()
    enqueue_work_item(
        session=session, queue_name=BUILD_QUEUE,
        payload={"opportunity_id": opportunity_id, "facts_revision": row["revision"],
                 "checklist_version": CHECKLIST_VERSION, "standalone": True},
        idempotency_key=f"quote_ready_stage:{opportunity_id}:{row['state_version']}",
    )


def _eligible(session, payload) -> bool:
    from src.services.fa_max_qualification import _ensure_facts_row
    oid = payload["opportunity_id"]
    # Create the empty row for legacy opportunities, then always lock in the
    # qualification writer's order: facts, opportunity.
    _ensure_facts_row(session, oid)
    revision = session.execute(text("""
        SELECT facts_revision FROM fa_max_opportunity_facts
        WHERE opportunity_id = :oid ::uuid FOR UPDATE
    """), {"oid": oid}).scalar_one()
    opp = session.execute(text("""
        SELECT outcome, opportunity_type FROM fa_max_opportunities
        WHERE opportunity_id = :oid ::uuid FOR UPDATE
    """), {"oid": oid}).mappings().one()
    return (
        revision == payload["facts_revision"]
        and payload["checklist_version"] == CHECKLIST_VERSION
        and opp["outcome"] == "open"
        and opp["opportunity_type"] not in PENDING_CONTRACT_APPROVAL_TYPES
    )


def _finish(session, item, worker_id, payload):
    session.execute(text("""
        UPDATE fa_max_work_queue SET status='done', done_at=now(), updated_at=now(),
            payload=:payload ::jsonb
        WHERE work_item_id=:wid ::uuid AND worker_id=:worker AND status='claimed'
    """), {"wid": item["work_item_id"], "worker": worker_id, "payload": json.dumps(payload)})


def process_work_item(item: dict[str, Any], *, worker_id: str) -> None:
    """Process a claimed build/delivery with durable, capped-backoff retries.

Delivery holds the opportunity/facts locks for the bounded (15 second) Slack
request, preventing a correction or closure from overtaking publication.
Only the queue's own row is locked before those locks. No API call occurs
inside the transaction that creates the scenario or its delivery task.
"""
    from src.services.quote_ready.dossier import compute_and_persist_quote_ready, post_quote_ready_dossier
    try:
        with get_db_context() as session:
            owned = session.execute(text("""
                SELECT payload FROM fa_max_work_queue
                WHERE work_item_id=:wid ::uuid AND worker_id=:worker
                  AND status='claimed' AND lease_expires_at > now()
                FOR UPDATE
            """), {"wid": item["work_item_id"], "worker": worker_id}).mappings().first()
            if owned is None:
                return
            payload = dict(owned["payload"])
            if not _eligible(session, payload):
                payload["completion_reason"] = "superseded_or_closed_or_unapproved"
                _finish(session, item, worker_id, payload)
                return

            if item["queue_name"] == BUILD_QUEUE:
                result_id = compute_and_persist_quote_ready(
                    session, opportunity_id=payload["opportunity_id"], return_existing=True,
                )
                if result_id is None:
                    raise RuntimeError("Scenario requires a linked subject property")
                delivery_payload = {**payload, "result_id": str(result_id)}
                enqueue_work_item(
                    session=session, queue_name=DELIVERY_QUEUE, payload=delivery_payload,
                    idempotency_key=f"quote_ready_delivery:{item['work_item_id']}",
                    person_id=item.get("person_id"),
                )
                payload["result_id"] = str(result_id)
                payload["completion_reason"] = "delivery_queued"
            else:
                status = session.execute(text("""
                    SELECT status FROM fa_max_quote_ready_results
                    WHERE result_id=:rid ::uuid
                """), {"rid": payload["result_id"]}).scalar_one()
                if status not in ("computed", "incomplete"):
                    payload["completion_reason"] = "result_no_longer_current"
                else:
                    ts = post_quote_ready_dossier(
                        session, payload["result_id"], delivery_id=str(item["work_item_id"]),
                    )
                    if not ts:
                        raise RuntimeError("Slack delivery failed or is not configured")
                    payload["slack_message_ts"] = ts
                    payload["completion_reason"] = "delivered"
            payload.pop("last_error", None)
            _finish(session, item, worker_id, payload)
    except Exception as exc:
        # Both the result/outbox transaction and the claim's completion roll
        # back on failure. Keep the SAME item retryable, including after a
        # successful compute: delivery is a separate durable item.
        delay = min(300, 5 * 2 ** min(int(item.get("attempt_count", 1)), 6))
        with get_db_context() as session:
            session.execute(text("""
                UPDATE fa_max_work_queue
                SET status='available', worker_id=NULL, claimed_at=NULL,
                    lease_expires_at=NULL, available_at=now()+(:delay * interval '1 second'),
                    updated_at=now(), payload=jsonb_set(payload, '{last_error}', :error ::jsonb)
                WHERE work_item_id=:wid ::uuid AND worker_id=:worker AND status='claimed'
            """), {"wid": item["work_item_id"], "worker": worker_id, "delay": delay,
                   "error": json.dumps(f"{type(exc).__name__}: {str(exc)[:300]}")})
        logger.exception("Quote Ready %s failed; durable retry in %ss", item["work_item_id"], delay)
