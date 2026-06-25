from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from src.core.models import Subscriber, SubscriberMemorySummary, UnifiedSubscriberMemory

_STREAM_SOURCES = {"STRIPE", "GHL", "SMS", "SYNTHFLOW", "UNDERWRITING"}


def get_subscriber_memory(
    db: Session,
    subscriber_id: int,
    *,
    limit: int = 20,
) -> dict[str, Any]:
    """Read a subscriber's unified memory for Cora.

    Returns ``{"timeline": [...newest first...], "summary": {...}}``:
      - ``timeline``: up to ``limit`` recent events, newest first. Each item is
        ``{id, stream_source, event_type, event_payload, property_id, created_at}``.
      - ``summary``: the current-state snapshot (``{}`` if none yet).

    Never raises on the empty case — returns empty timeline / summary instead.
    """
    timeline_rows = db.execute(
        text(
            """
            SELECT id, stream_source, event_type, event_payload,
                   property_id, created_at
            FROM unified_subscriber_memory
            WHERE subscriber_id = :sid
            ORDER BY (event_payload->>'occurred_at') DESC NULLS LAST,
                     created_at DESC
            LIMIT :limit
            """
        ),
        {"sid": subscriber_id, "limit": limit},
    ).mappings().all()

    timeline = [
        {
            "id": str(r["id"]),
            "stream_source": r["stream_source"],
            "event_type": r["event_type"],
            "event_payload": r["event_payload"],
            "property_id": r["property_id"],
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
        }
        for r in timeline_rows
    ]

    summary_row = db.execute(
        text("SELECT * FROM subscriber_memory_summary WHERE subscriber_id = :sid"),
        {"sid": subscriber_id},
    ).mappings().first()

    summary: dict[str, Any] = {}
    if summary_row is not None:
        for key, value in summary_row.items():
            summary[key] = value.isoformat() if isinstance(value, datetime) else value

    return {"timeline": timeline, "summary": summary}


def append_memory_event(
    db: Session,
    *,
    subscriber_id: int,
    stream_source: str,
    event_type: str,
    source_event_id: str,
    source_event_name: str,
    occurred_at: datetime,
    status: str,
    summary: str,
    channel: str,
    actor: dict[str, Any],
    raw: Optional[dict[str, Any]] = None,
    customer_account_id: Optional[str] = None,
    loan_file_id: Optional[str] = None,
    external_contact_id: Optional[str] = None,
    message_id: Optional[str] = None,
    call_id: Optional[str] = None,
    lead_id: Optional[int] = None,
) -> bool:
    """Append one normalized subscriber-memory event and update summary state.

    Returns True when a new row is created, False when the source event was
    already projected.
    """
    if stream_source not in _STREAM_SOURCES:
        raise ValueError(f"Unsupported stream_source: {stream_source}")
    if db.get(Subscriber, subscriber_id) is None:
        raise ValueError(f"Subscriber {subscriber_id} does not exist")

    # Validate source_event_id (issue #3 — prevent empty/malformed IDs)
    if not source_event_id or not source_event_id.strip():
        raise ValueError("source_event_id must be non-empty")

    occurred_at = _normalize_dt(occurred_at)
    existing = db.execute(
        select(UnifiedSubscriberMemory).where(
            UnifiedSubscriberMemory.stream_source == stream_source,
            UnifiedSubscriberMemory.event_type == event_type,
            UnifiedSubscriberMemory.event_payload["source_event_id"].astext == source_event_id,
        )
    ).scalar_one_or_none()
    if existing is not None:
        return False

    payload = {
        "source_event_id": source_event_id,
        "source_event_name": source_event_name,
        "occurred_at": occurred_at.isoformat(),
        "status": status,
        "summary": summary,
        "channel": channel,
        "actor": actor,
        "customer_account_id": customer_account_id,
        "loan_file_id": loan_file_id,
        "external_contact_id": external_contact_id,
        "message_id": message_id,
        "call_id": call_id,
        "raw": raw or {},
    }

    memory_row = UnifiedSubscriberMemory(
        subscriber_id=subscriber_id,
        property_id=lead_id,
        stream_source=stream_source,
        event_type=event_type,
        event_payload=payload,
    )
    db.add(memory_row)
    db.flush()

    _apply_summary_update(
        db,
        subscriber_id=subscriber_id,
        stream_source=stream_source,
        event_type=event_type,
        occurred_at=occurred_at,
        status=status,
        lead_id=lead_id,
    )
    db.flush()
    return True


def _apply_summary_update(
    db: Session,
    *,
    subscriber_id: int,
    stream_source: str,
    event_type: str,
    occurred_at: datetime,
    status: str,
    lead_id: Optional[int],
) -> None:
    summary_row = db.get(SubscriberMemorySummary, subscriber_id)
    if summary_row is None:
        summary_row = SubscriberMemorySummary(subscriber_id=subscriber_id)
        db.add(summary_row)

    summary_row.last_event_at = occurred_at
    summary_row.last_event_type = event_type
    summary_row.updated_at = datetime.now(timezone.utc)
    if lead_id is not None:
        summary_row.last_lead_id = lead_id

    if stream_source == "STRIPE":
        summary_row.last_stripe_event_at = occurred_at
        summary_row.last_stripe_event_type = event_type
        if event_type == "checkout_completed":
            summary_row.latest_checkout_state = status
        elif event_type in {"payment_failed", "subscription_activated", "subscription_canceled"}:
            summary_row.latest_payment_state = status
    elif stream_source == "SMS":
        summary_row.last_sms_event_at = occurred_at
        summary_row.last_sms_event_type = event_type
        summary_row.latest_sms_state = status
        if event_type == "sms_replied":
            summary_row.last_sms_reply_at = occurred_at
        if event_type == "sms_opt_out":
            summary_row.sms_opted_out = True
    elif stream_source == "GHL":
        if event_type == "crm_stage_changed":
            summary_row.latest_crm_stage = status
        elif event_type == "crm_status_changed":
            summary_row.latest_crm_status = status
    elif stream_source == "SYNTHFLOW":
        summary_row.last_voice_event_at = occurred_at
        summary_row.last_voice_event_type = event_type
    elif stream_source == "UNDERWRITING":
        summary_row.last_underwriting_event_at = occurred_at
        summary_row.last_underwriting_event_type = event_type
        summary_row.latest_underwriting_state = status
        if event_type == "underwriting_milestone_reached":
            summary_row.latest_underwriting_milestone = status


def _normalize_dt(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)