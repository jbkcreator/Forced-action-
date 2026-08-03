"""
DFY-Lite API router — pitch generation and order management for unlocked leads.

All endpoints are scoped to /api/feed/{feed_uuid}/dfy-lite and require a valid
subscriber JWT via get_current_subscriber.
"""

from __future__ import annotations

import logging
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field, field_validator
from sqlalchemy.orm import Session

from src.agents.pitch_builder import (
    DEFAULT_OUTPUT_FORMATS,
    VALID_OFFER_ANGLES,
    VALID_OUTPUT_FORMATS,
    VALID_PITCH_TYPES,
    VALID_TARGET_VERTICALS,
    MAX_GENERATIONS_PER_PAIR,
)
from src.api.deps import get_db
from src.services.dfy_lite_service import (
    DfyLiteLimitError,
    DfyLitePermissionError,
    create_pitch_order,
    get_order,
    get_orders_for_lead,
    mark_delivered,
    mark_reviewed,
    update_pitch_outputs,
)
from src.services.subscriber_auth import get_current_subscriber

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/feed/{feed_uuid}", tags=["dfy-lite"])


# ── Pydantic schemas ──────────────────────────────────────────────────────────

class GeneratePitchRequest(BaseModel):
    property_id: int
    target_vertical: str
    pitch_type: str
    offer_angle: Optional[str] = None
    selected_output_formats: list[str] = Field(default_factory=lambda: list(DEFAULT_OUTPUT_FORMATS))
    custom_instructions: Optional[str] = Field(default=None, max_length=500)

    @field_validator("target_vertical")
    @classmethod
    def validate_vertical(cls, v: str) -> str:
        if v not in VALID_TARGET_VERTICALS:
            raise ValueError(f"target_vertical must be one of: {sorted(VALID_TARGET_VERTICALS)}")
        return v

    @field_validator("pitch_type")
    @classmethod
    def validate_pitch_type(cls, v: str) -> str:
        if v not in VALID_PITCH_TYPES:
            raise ValueError(f"pitch_type must be one of: {sorted(VALID_PITCH_TYPES)}")
        return v

    @field_validator("offer_angle")
    @classmethod
    def validate_offer_angle(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and v not in VALID_OFFER_ANGLES:
            raise ValueError(f"offer_angle must be one of: {sorted(VALID_OFFER_ANGLES)}")
        return v

    @field_validator("selected_output_formats")
    @classmethod
    def validate_formats(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("At least one output format must be selected")
        invalid = set(v) - VALID_OUTPUT_FORMATS
        if invalid:
            raise ValueError(f"Invalid output formats: {sorted(invalid)}. Valid: {sorted(VALID_OUTPUT_FORMATS)}")
        return v


class UpdatePitchRequest(BaseModel):
    email_subject: Optional[str] = None
    email_pitch: Optional[str] = None
    sms_pitch: Optional[str] = None
    call_script: Optional[str] = None
    linkedin_message: Optional[str] = None
    evidence_summary: Optional[str] = None


class PitchOrderResponse(BaseModel):
    order_id: int
    status: str
    property_id: int
    pitch_generation_number: int
    generation_limit: int
    remaining_generations: int
    generated_outputs: Optional[dict[str, Any]] = None
    created_at: Optional[str] = None
    completed_at: Optional[str] = None


def _order_to_response(order_dict: dict) -> PitchOrderResponse:
    count = order_dict.get("pitch_generation_number", 1)
    limit = order_dict.get("pitch_generation_limit", MAX_GENERATIONS_PER_PAIR)
    completed = order_dict.get("updated_at") if order_dict.get("status") in (
        "Needs_Review", "Delivered"
    ) else None
    return PitchOrderResponse(
        order_id=order_dict["id"],
        status=order_dict["status"],
        property_id=order_dict["property_id"],
        pitch_generation_number=count,
        generation_limit=limit,
        remaining_generations=max(0, limit - count),
        generated_outputs=order_dict.get("generated_outputs_json"),
        created_at=order_dict.get("created_at"),
        completed_at=completed,
    )


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.post("/dfy-lite/generate", status_code=202)
def generate_pitch(
    feed_uuid: str,
    payload: GeneratePitchRequest,
    db: Session = Depends(get_db),
    _auth=Depends(get_current_subscriber),
) -> PitchOrderResponse:
    """
    Create a DFY-Lite pitch order and dispatch it to the Lifecycle agent for async generation.

    Returns 202 Accepted immediately — poll GET /order/{order_id} until status = Needs_Review.

    - 202 on accepted
    - 403 if the subscriber does not own/have access to the property
    - 422 if the 3-pitch limit has been reached
    """
    from sqlalchemy import text as sa_text
    from src.agents.events.ingestion import publish_lifecycle_event

    subscriber = db.execute(
        sa_text("SELECT id FROM subscribers WHERE event_feed_uuid = :uuid"),
        {"uuid": feed_uuid},
    ).mappings().first()
    if not subscriber:
        raise HTTPException(status_code=404, detail="Feed not found")
    subscriber_id: int = subscriber["id"]

    request_options = {
        "property_id":             payload.property_id,
        "target_vertical":         payload.target_vertical,
        "pitch_type":              payload.pitch_type,
        "offer_angle":             payload.offer_angle,
        "selected_output_formats": payload.selected_output_formats,
        "custom_instructions":     payload.custom_instructions,
    }

    try:
        order = create_pitch_order(
            session=db,
            subscriber_id=subscriber_id,
            property_id=payload.property_id,
            request_options=request_options,
        )
    except DfyLitePermissionError:
        raise HTTPException(status_code=403, detail="You do not have access to this lead")
    except DfyLiteLimitError:
        raise HTTPException(
            status_code=422,
            detail=f"You have reached the {MAX_GENERATIONS_PER_PAIR}-pitch limit for this lead",
        )
    except Exception as exc:
        logger.error("DFY-Lite order creation failed for subscriber %s: %s", subscriber_id, exc, exc_info=True)
        raise HTTPException(status_code=500, detail="Order creation failed — please try again")

    publish_lifecycle_event({
        "event_type":    "dfy_lite_pitch_requested",
        "subscriber_id": subscriber_id,
        "payload":       {"order_id": order.id},
    })
    logger.info("DFY-Lite order %s queued for subscriber %s", order.id, subscriber_id)

    order_dict = {
        "id":                      order.id,
        "status":                  order.status,
        "property_id":             order.property_id,
        "pitch_generation_number": order.pitch_generation_number,
        "pitch_generation_limit":  order.pitch_generation_limit,
        "generated_outputs_json":  order.generated_outputs_json,
        "created_at":              order.created_at.isoformat() if order.created_at else None,
        "updated_at":              order.updated_at.isoformat() if order.updated_at else None,
    }
    return _order_to_response(order_dict)


@router.get("/dfy-lite/history/{property_id}")
def pitch_history(
    feed_uuid: str,
    property_id: int,
    db: Session = Depends(get_db),
    _auth=Depends(get_current_subscriber),
) -> dict:
    """List orders for this subscriber+property pair with count and remaining."""
    import sqlalchemy as sa_mod

    subscriber = db.execute(
        sa_mod.text("SELECT id FROM subscribers WHERE event_feed_uuid = :uuid"),
        {"uuid": feed_uuid},
    ).mappings().first()
    if not subscriber:
        raise HTTPException(status_code=404, detail="Feed not found")

    return get_orders_for_lead(db, subscriber["id"], property_id)


@router.get("/dfy-lite/order/{order_id}")
def get_single_order(
    feed_uuid: str,
    order_id: int,
    db: Session = Depends(get_db),
    _auth=Depends(get_current_subscriber),
) -> dict:
    """Fetch a single order. Returns 403 if the order does not belong to this subscriber."""
    import sqlalchemy as sa_mod

    subscriber = db.execute(
        sa_mod.text("SELECT id FROM subscribers WHERE event_feed_uuid = :uuid"),
        {"uuid": feed_uuid},
    ).mappings().first()
    if not subscriber:
        raise HTTPException(status_code=404, detail="Feed not found")

    try:
        return get_order(db, order_id, subscriber["id"])
    except KeyError:
        raise HTTPException(status_code=404, detail="Order not found")
    except PermissionError:
        raise HTTPException(status_code=403, detail="Access denied")


@router.patch("/dfy-lite/order/{order_id}", status_code=200)
def edit_pitch_output(
    feed_uuid: str,
    order_id: int,
    payload: UpdatePitchRequest,
    db: Session = Depends(get_db),
    _auth=Depends(get_current_subscriber),
) -> dict:
    """Merge subscriber edits into generated_outputs_json. Only output fields are accepted."""
    import sqlalchemy as sa_mod

    subscriber = db.execute(
        sa_mod.text("SELECT id FROM subscribers WHERE event_feed_uuid = :uuid"),
        {"uuid": feed_uuid},
    ).mappings().first()
    if not subscriber:
        raise HTTPException(status_code=404, detail="Feed not found")

    updates = {k: v for k, v in payload.model_dump().items() if v is not None}
    try:
        merged = update_pitch_outputs(db, order_id, subscriber["id"], updates)
    except PermissionError:
        raise HTTPException(status_code=403, detail="Access denied")

    return {"ok": True, "generated_outputs": merged}


@router.post("/dfy-lite/order/{order_id}/review", status_code=200)
def review_order(
    feed_uuid: str,
    order_id: int,
    db: Session = Depends(get_db),
    _auth=Depends(get_current_subscriber),
) -> dict:
    """Mark an order as reviewed."""
    import sqlalchemy as sa_mod

    subscriber = db.execute(
        sa_mod.text("SELECT id FROM subscribers WHERE event_feed_uuid = :uuid"),
        {"uuid": feed_uuid},
    ).mappings().first()
    if not subscriber:
        raise HTTPException(status_code=404, detail="Feed not found")

    try:
        mark_reviewed(db, order_id, subscriber["id"])
    except PermissionError:
        raise HTTPException(status_code=403, detail="Access denied")

    return {"ok": True}


@router.post("/dfy-lite/order/{order_id}/deliver", status_code=200)
def deliver_order(
    feed_uuid: str,
    order_id: int,
    db: Session = Depends(get_db),
    _auth=Depends(get_current_subscriber),
) -> dict:
    """Mark an order as delivered (subscriber or admin)."""
    import sqlalchemy as sa_mod

    subscriber = db.execute(
        sa_mod.text("SELECT id FROM subscribers WHERE event_feed_uuid = :uuid"),
        {"uuid": feed_uuid},
    ).mappings().first()
    if not subscriber:
        raise HTTPException(status_code=404, detail="Feed not found")

    mark_delivered(db, order_id, subscriber["id"])
    return {"ok": True}


@router.get("/dfy-lite/options")
def get_options(
    feed_uuid: str,
    _auth=Depends(get_current_subscriber),
) -> dict:
    """Return all allowed constant values for frontend dropdowns."""
    return {
        "target_verticals": sorted(VALID_TARGET_VERTICALS),
        "pitch_types": sorted(VALID_PITCH_TYPES),
        "offer_angles": sorted(VALID_OFFER_ANGLES),
        "output_formats": sorted(VALID_OUTPUT_FORMATS),
        "default_output_formats": DEFAULT_OUTPUT_FORMATS,
        "max_generations_per_pair": MAX_GENERATIONS_PER_PAIR,
    }
