"""
DFY-Lite service layer — authorization, generation-count enforcement,
status-transition orchestration, and order retrieval.

All DB writes use session.execute(sa.text(...)). The service is transaction-aware:
callers hold the session; this module flushes but does not commit until
create_pitch_order reaches its final state (or error state), then commits once.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Optional

import sqlalchemy as sa
from sqlalchemy.orm import Session

from src.agents.pitch_builder import (
    MAX_GENERATIONS_PER_PAIR,
    build_property_pitch_context,
    generate_pitch_with_claude,
)
from src.core.models import DfyLiteOrder

logger = logging.getLogger(__name__)


# ── Custom exceptions ─────────────────────────────────────────────────────────

class DfyLitePermissionError(Exception):
    """Subscriber does not have an authorization path to generate a pitch for this property."""


class DfyLiteLimitError(Exception):
    """Subscriber has reached the per-property generation limit."""


# ── Authorization ─────────────────────────────────────────────────────────────

def can_subscriber_generate_pitch(
    session: Session,
    subscriber_id: int,
    property_id: int,
) -> bool:
    """
    Return True if the subscriber has access to this property via any of the
    three authorization paths (mirrors the feed unlock endpoint logic).

    Paths checked:
      1. sent_leads       — $4 hot-lead unlock
      2. bundle_purchases — active bundle containing this property
      3. lead_pack_purchases — delivered lead pack containing this property
    """
    row = session.execute(
        sa.text("""
            (
                SELECT 1
                FROM sent_leads
                WHERE subscriber_id = :sub_id
                  AND property_id   = :prop_id
                LIMIT 1
            )
            UNION ALL
            (
                SELECT 1
                FROM bundle_purchases
                WHERE subscriber_id = :sub_id
                  AND status        = 'active'
                  AND :prop_id      = ANY(lead_ids)
                LIMIT 1
            )
            UNION ALL
            (
                SELECT 1
                FROM lead_pack_purchases
                WHERE subscriber_id = :sub_id
                  AND status        = 'delivered'
                  AND :prop_id      = ANY(lead_ids)
                LIMIT 1
            )
        """),
        {"sub_id": subscriber_id, "prop_id": property_id},
    ).first()
    return row is not None


# ── Generation count ──────────────────────────────────────────────────────────

def count_completed_generations(
    session: Session,
    subscriber_id: int,
    property_id: int,
) -> int:
    """Count non-cancelled/non-failed orders for this subscriber+property pair."""
    row = session.execute(
        sa.text("""
            SELECT COUNT(*)
            FROM dfy_lite_orders
            WHERE subscriber_id = :sub_id
              AND property_id   = :prop_id
              AND status NOT IN ('Cancelled', 'Signal_Failed', 'Pitch_Failed')
        """),
        {"sub_id": subscriber_id, "prop_id": property_id},
    ).scalar()
    return int(row or 0)


# ── Internal helpers ──────────────────────────────────────────────────────────

def _update_order(session: Session, order_id: int, **fields) -> None:
    """Execute a single-column or multi-column UPDATE on dfy_lite_orders."""
    fields["updated_at"] = datetime.now(timezone.utc)
    set_clauses = ", ".join(f"{k} = :{k}" for k in fields)
    session.execute(
        sa.text(f"UPDATE dfy_lite_orders SET {set_clauses} WHERE id = :order_id"),
        {"order_id": order_id, **fields},
    )


def _update_order_jsonb(
    session: Session,
    order_id: int,
    status: str,
    jsonb_col: str,
    jsonb_val: dict,
) -> None:
    """Update status + one JSONB column in a single query using CAST."""
    now = datetime.now(timezone.utc)
    session.execute(
        sa.text(f"""
            UPDATE dfy_lite_orders
            SET status      = :status,
                {jsonb_col} = CAST(:json_val AS JSONB),
                updated_at  = :now
            WHERE id = :order_id
        """),
        {
            "status": status,
            "json_val": json.dumps(jsonb_val, default=str),
            "now": now,
            "order_id": order_id,
        },
    )


# ── Main order flow ───────────────────────────────────────────────────────────

def create_pitch_order(
    session: Session,
    subscriber_id: int,
    property_id: int,
    request_options: dict,
) -> DfyLiteOrder:
    """
    Full lifecycle: authorize → insert → compile signals → call Claude → mark for review.

    Raises:
        DfyLitePermissionError — subscriber lacks an authorization path.
        DfyLiteLimitError      — generation limit reached.
        ValueError             — Claude output was unparseable (re-raised after Pitch_Failed).
    """
    if not can_subscriber_generate_pitch(session, subscriber_id, property_id):
        raise DfyLitePermissionError(
            f"Subscriber {subscriber_id} does not have access to property {property_id}"
        )

    count = count_completed_generations(session, subscriber_id, property_id)
    if count >= MAX_GENERATIONS_PER_PAIR:
        raise DfyLiteLimitError(
            f"Generation limit of {MAX_GENERATIONS_PER_PAIR} reached for "
            f"subscriber {subscriber_id} / property {property_id}"
        )

    now = datetime.now(timezone.utc)

    # Step 1 — insert with Order_Received
    order = DfyLiteOrder(
        subscriber_id=subscriber_id,
        property_id=property_id,
        status="Order_Received",
        pitch_type=request_options["pitch_type"],
        offer_angle=request_options.get("offer_angle"),
        target_vertical=request_options["target_vertical"],
        selected_output_formats=request_options.get("selected_output_formats", []),
        custom_instructions=request_options.get("custom_instructions"),
        pitch_generation_number=count + 1,
        pitch_generation_limit=MAX_GENERATIONS_PER_PAIR,
        generated_by="claude",
        created_at=now,
        updated_at=now,
    )
    session.add(order)
    session.flush()

    # Step 2 — compile context → Signal_Compiled
    try:
        context = build_property_pitch_context(session, property_id)
    except Exception as exc:
        logger.warning("DFY-Lite signal compilation failed for order %s: %s", order.id, exc)
        _update_order(session, order.id, status="Signal_Failed", error_reason=str(exc)[:1000])
        session.commit()
        raise

    property_snap = {k: v for k, v in (context.get("property") or {}).items() if v is not None}
    _update_order_jsonb(session, order.id, "Signal_Compiled", "distress_stack_json", context)
    session.execute(
        sa.text("""
            UPDATE dfy_lite_orders
            SET property_snapshot_json = CAST(:json_val AS JSONB),
                updated_at             = :now
            WHERE id = :order_id
        """),
        {
            "json_val": json.dumps(property_snap, default=str),
            "now": datetime.now(timezone.utc),
            "order_id": order.id,
        },
    )
    session.flush()

    # Step 3 — call Claude → Pitch_Generated
    try:
        generated = generate_pitch_with_claude(
            context=context,
            request_options=request_options,
            subscriber_id=subscriber_id,
            db=session,
        )
    except Exception as exc:
        logger.warning("DFY-Lite pitch generation failed for order %s: %s", order.id, exc)
        _update_order(session, order.id, status="Pitch_Failed", error_reason=str(exc)[:1000])
        session.commit()
        raise

    _update_order_jsonb(session, order.id, "Pitch_Generated", "generated_outputs_json", generated)
    session.flush()

    # Step 4 — Needs_Review
    _update_order(session, order.id, status="Needs_Review")
    session.commit()
    session.refresh(order)
    return order


# ── Review / delivery ─────────────────────────────────────────────────────────

def mark_reviewed(session: Session, order_id: int, subscriber_id: int) -> None:
    """Set reviewed_at on an order the subscriber owns."""
    row = session.execute(
        sa.text("""
            SELECT id FROM dfy_lite_orders
            WHERE id = :order_id AND subscriber_id = :sub_id
        """),
        {"order_id": order_id, "sub_id": subscriber_id},
    ).first()
    if row is None:
        raise PermissionError(f"Order {order_id} not found or not owned by subscriber {subscriber_id}")

    now = datetime.now(timezone.utc)
    session.execute(
        sa.text("""
            UPDATE dfy_lite_orders
            SET reviewed_at = :now, updated_at = :now
            WHERE id = :order_id
        """),
        {"now": now, "order_id": order_id},
    )
    session.commit()


def mark_delivered(
    session: Session,
    order_id: int,
    subscriber_id: Optional[int] = None,
) -> None:
    """Set status=Delivered and delivered_at. Pass subscriber_id=None for admin calls."""
    scope = "AND subscriber_id = :sub_id" if subscriber_id is not None else ""
    params: dict = {
        "order_id": order_id,
        "now": datetime.now(timezone.utc),
    }
    if subscriber_id is not None:
        params["sub_id"] = subscriber_id

    session.execute(
        sa.text(f"""
            UPDATE dfy_lite_orders
            SET status       = 'Delivered',
                delivered_at = :now,
                updated_at   = :now
            WHERE id = :order_id {scope}
        """),
        params,
    )
    session.commit()


# ── Edit ─────────────────────────────────────────────────────────────────────

_EDITABLE_OUTPUT_KEYS: frozenset[str] = frozenset({
    "email_subject", "email_pitch", "sms_pitch",
    "call_script", "linkedin_message", "evidence_summary",
})


def update_pitch_outputs(
    session: Session,
    order_id: int,
    subscriber_id: int,
    updates: dict,
) -> dict:
    """
    Merge subscriber edits into generated_outputs_json.
    Only keys in _EDITABLE_OUTPUT_KEYS are accepted; metadata is preserved.
    Raises PermissionError if the order does not belong to this subscriber.
    """
    row = session.execute(
        sa.text("""
            SELECT id, generated_outputs_json
            FROM dfy_lite_orders
            WHERE id = :order_id AND subscriber_id = :sub_id
        """),
        {"order_id": order_id, "sub_id": subscriber_id},
    ).mappings().first()

    if row is None:
        raise PermissionError(f"Order {order_id} not found or not owned by subscriber {subscriber_id}")

    existing: dict = row["generated_outputs_json"] or {}
    safe_updates = {k: v for k, v in updates.items() if k in _EDITABLE_OUTPUT_KEYS}
    merged = {**existing, **safe_updates}

    session.execute(
        sa.text("""
            UPDATE dfy_lite_orders
            SET generated_outputs_json = CAST(:json_val AS JSONB),
                updated_at             = :now
            WHERE id = :order_id
        """),
        {
            "json_val": json.dumps(merged, default=str),
            "now": datetime.now(timezone.utc),
            "order_id": order_id,
        },
    )
    session.commit()
    return merged


# ── Retrieval ─────────────────────────────────────────────────────────────────

def get_orders_for_lead(
    session: Session,
    subscriber_id: int,
    property_id: int,
) -> dict:
    """Return all orders for a subscriber+property pair, newest first."""
    rows = session.execute(
        sa.text("""
            SELECT
                id,
                status,
                pitch_type,
                offer_angle,
                target_vertical,
                selected_output_formats,
                generated_outputs_json,
                pitch_generation_number,
                pitch_generation_limit,
                error_reason,
                reviewed_at,
                delivered_at,
                created_at,
                updated_at
            FROM dfy_lite_orders
            WHERE subscriber_id = :sub_id
              AND property_id   = :prop_id
            ORDER BY created_at DESC
        """),
        {"sub_id": subscriber_id, "prop_id": property_id},
    ).mappings().all()

    count = sum(
        1 for r in rows
        if r["status"] not in ("Cancelled", "Signal_Failed", "Pitch_Failed")
    )

    return {
        "orders": [_row_to_dict(r) for r in rows],
        "count": count,
        "remaining": max(0, MAX_GENERATIONS_PER_PAIR - count),
        "limit": MAX_GENERATIONS_PER_PAIR,
    }


def get_order(session: Session, order_id: int, subscriber_id: int) -> dict:
    """Fetch a single order; raises PermissionError if subscriber mismatch."""
    row = session.execute(
        sa.text("""
            SELECT
                id,
                subscriber_id,
                property_id,
                status,
                pitch_type,
                offer_angle,
                target_vertical,
                selected_output_formats,
                generated_outputs_json,
                pitch_generation_number,
                pitch_generation_limit,
                error_reason,
                reviewed_at,
                delivered_at,
                created_at,
                updated_at
            FROM dfy_lite_orders
            WHERE id = :order_id
        """),
        {"order_id": order_id},
    ).mappings().first()

    if row is None:
        raise KeyError(f"Order {order_id} not found")

    if row["subscriber_id"] != subscriber_id:
        raise PermissionError(f"Order {order_id} does not belong to subscriber {subscriber_id}")

    return _row_to_dict(row)


def _row_to_dict(row) -> dict:
    d = dict(row)
    for col in ("reviewed_at", "delivered_at", "created_at", "updated_at"):
        val = d.get(col)
        if val and hasattr(val, "isoformat"):
            d[col] = val.isoformat()
    return d
