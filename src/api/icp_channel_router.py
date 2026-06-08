"""
ICP Channel Admin API (fa066).

Endpoints for managing ICP channels: list, detail, activate, pause, kill,
config-update, metrics, and subscribers.

ICP vs vertical:
  ICP = customer group (contractor, rei_investor, ...)
  Vertical = product category (roofing, restoration, ...) — verticals can
  overlap between ICPs so all scoping uses icp_channel_key explicitly.

Auth: admin JWT (Depends(get_current_admin)) — same as admin_router.py.

Force activation rules:
  - Requires non-empty `reason` in request body
  - Writes audit row with is_force_activate=True, force_reason, gate_snapshot
  - Only allowed for admin role (same guard as every other admin endpoint)
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

from config.icp_channels import (
    DEFAULT_ICP_CHANNEL_KEY,
    ICP_CHANNELS,
    get_icp_channel,
    is_gate_required,
)
from src.api.admin_router import get_current_admin
from src.api.deps import get_db as _get_db
from src.services.icp_kill_switch import (
    compute_icp_gate_snapshot,
    gate_blocking_reasons,
    is_gate_clear,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/admin/icp-channels", tags=["icp-channels"])


# ── request models ────────────────────────────────────────────────────────────

class ActivateRequest(BaseModel):
    reason: Optional[str] = None  # required for force=True


class PauseKillRequest(BaseModel):
    reason: Optional[str] = None


class PatchRequest(BaseModel):
    display_name: Optional[str] = None
    persona: Optional[str] = None
    data_source: Optional[str] = None
    feed_scope: Optional[str] = None
    landing_slug: Optional[str] = None


# ── DB helpers ────────────────────────────────────────────────────────────────

def _get_db_channel(db: Session, key: str):
    return db.execute(sa_text("""
        SELECT id, key, display_name, price_monthly, persona, data_source,
               feed_scope, landing_slug, status, created_at, updated_at,
               launch_started_at, launch_ends_at,
               killswitch_decision, killswitch_reason,
               killswitch_decided_at, killswitch_decided_by
        FROM expansion_icp_channels
        WHERE key = :key LIMIT 1
    """), {"key": key}).first()


def _write_audit(
    db: Session,
    *,
    channel_key: str,
    event_type: str,
    actor: str,
    prev_status: Optional[str] = None,
    new_status: Optional[str] = None,
    gate_snapshot: Optional[dict] = None,
    is_force_activate: bool = False,
    force_reason: Optional[str] = None,
    detail: Optional[dict] = None,
) -> None:
    db.execute(sa_text("""
        INSERT INTO icp_channel_launch_audit
            (channel_key, event_type, actor, is_force_activate, force_reason,
             gate_snapshot, prev_status, new_status, detail, created_at)
        VALUES
            (:channel_key, :event_type, :actor, :is_force, :force_reason,
             CAST(:gate_snapshot AS jsonb), :prev_status, :new_status,
             CAST(:detail AS jsonb), NOW())
    """), {
        "channel_key": channel_key,
        "event_type": event_type,
        "actor": actor,
        "is_force": is_force_activate,
        "force_reason": force_reason,
        "gate_snapshot": json.dumps(gate_snapshot) if gate_snapshot else None,
        "prev_status": prev_status,
        "new_status": new_status,
        "detail": json.dumps(detail) if detail else None,
    })


def _set_status(db: Session, channel_id: int, new_status: str) -> None:
    db.execute(sa_text("""
        UPDATE expansion_icp_channels
        SET status = :status, updated_at = NOW()
        WHERE id = :id
    """), {"status": new_status, "id": channel_id})


def _killswitch_status(row, snapshot: dict) -> dict:
    """Derive 4-week kill-switch status from launch window and gate snapshot."""
    from datetime import datetime, timezone as tz
    now = datetime.now(tz.utc)
    if not row.launch_started_at:
        return {"status": "not_started", "days_elapsed": None, "days_remaining": None}
    started = row.launch_started_at
    if started.tzinfo is None:
        started = started.replace(tzinfo=tz.utc)
    ends = row.launch_ends_at
    if ends and ends.tzinfo is None:
        ends = ends.replace(tzinfo=tz.utc)
    days_elapsed = (now - started).days
    days_remaining = max(0, (ends - now).days) if ends else None
    # Overall colour = worst gate colour
    colours = [v.get("color", "unknown") for v in snapshot.values()]
    overall = "green"
    if any(c == "red" for c in colours):
        overall = "red"
    elif any(c in ("yellow", "unknown") for c in colours):
        overall = "yellow"
    return {
        "status": overall,
        "days_elapsed": days_elapsed,
        "days_remaining": days_remaining,
        "launch_started_at": started.isoformat(),
        "launch_ends_at": ends.isoformat() if ends else None,
        "decision": row.killswitch_decision,
        "decision_reason": row.killswitch_reason,
        "decided_at": row.killswitch_decided_at.isoformat() if row.killswitch_decided_at else None,
        "decided_by": row.killswitch_decided_by,
    }


def _channel_to_dict(row, static: dict, snapshot: dict) -> dict:
    from config.icp_channels import get_icp_price_cents, get_icp_stripe_price_key
    blocking = gate_blocking_reasons(snapshot)
    return {
        "key": row.key,
        "display_name": row.display_name,
        "status": row.status,
        "is_default": static.get("is_default", False),
        "gate_required": static.get("gate_required", True),
        "verticals": static.get("verticals", []),
        "description": static.get("description"),
        "target_audience": static.get("target_audience"),
        # ICP-scoped pricing (static config); DB price_monthly is a legacy field
        "price_monthly_cents": get_icp_price_cents(row.key) or (
            int(float(row.price_monthly) * 100) if row.price_monthly else None
        ),
        "stripe_price_key": get_icp_stripe_price_key(row.key),
        "persona": row.persona,
        "data_source": row.data_source,
        "feed_scope": row.feed_scope,
        "landing_slug": row.landing_slug,
        "gate_blocked": bool(blocking),
        "blocking_reasons": blocking,
        "gates": snapshot,
        "killswitch": _killswitch_status(row, snapshot),
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


# ── endpoints ─────────────────────────────────────────────────────────────────

@router.get("")
def list_icp_channels(
    db: Session = Depends(_get_db),
    _admin: dict = Depends(get_current_admin),
):
    """List all ICP channels with config, DB state, and gate snapshot."""
    from config.settings import get_settings
    settings = get_settings()
    county_id = settings.county_launch_source_county or "hillsborough"

    channels = []
    for key, static in ICP_CHANNELS.items():
        db_row = _get_db_channel(db, key)
        snapshot = compute_icp_gate_snapshot(key, county_id, db)
        if db_row:
            channels.append(_channel_to_dict(db_row, static, snapshot))
        else:
            # Channel exists in config but not yet seeded in DB
            channels.append({
                "key": key,
                "display_name": static.get("display_name"),
                "status": "not_seeded",
                "is_default": static.get("is_default", False),
                "gate_required": static.get("gate_required", True),
                "verticals": static.get("verticals", []),
                "description": static.get("description"),
                "target_audience": static.get("target_audience"),
                "gate_blocked": True,
                "blocking_reasons": ["Channel not yet seeded in database"],
                "gates": snapshot,
            })
    return {"channels": channels, "total": len(channels)}


@router.get("/{key}")
def get_icp_channel_detail(
    key: str,
    db: Session = Depends(_get_db),
    _admin: dict = Depends(get_current_admin),
):
    """Detail: config + DB state + gate snapshot + 7-day metrics."""
    if key not in ICP_CHANNELS:
        raise HTTPException(status_code=404, detail=f"ICP channel '{key}' not found in registry")

    from config.settings import get_settings
    county_id = (get_settings().county_launch_source_county or "hillsborough")

    db_row = _get_db_channel(db, key)
    if not db_row:
        raise HTTPException(status_code=404, detail=f"ICP channel '{key}' not seeded in database")

    static = get_icp_channel(key)
    snapshot = compute_icp_gate_snapshot(key, county_id, db)

    # 7-day audit trail
    audit = db.execute(sa_text("""
        SELECT event_type, actor, is_force_activate, force_reason,
               prev_status, new_status, created_at
        FROM icp_channel_launch_audit
        WHERE channel_key = :key
        ORDER BY created_at DESC LIMIT 20
    """), {"key": key}).mappings().fetchall()

    result = _channel_to_dict(db_row, static, snapshot)
    result["recent_audit"] = [dict(r) for r in audit]
    return result


@router.post("/{key}/activate")
def activate_icp_channel(
    key: str,
    body: ActivateRequest,
    force: bool = False,
    db: Session = Depends(_get_db),
    _admin: dict = Depends(get_current_admin),
):
    """
    Activate a gated ICP channel.

    Normal activation: all gates must be green, no other expansion ICP active.
    Force activation (force=True): bypasses gate check; requires non-empty reason;
      always writes an audit row with is_force_activate=True.
    """
    if key == DEFAULT_ICP_CHANNEL_KEY:
        raise HTTPException(status_code=400, detail="Contractor ICP is always active — cannot be activated via this endpoint")

    if key not in ICP_CHANNELS:
        raise HTTPException(status_code=404, detail=f"ICP channel '{key}' not in registry")

    db_row = _get_db_channel(db, key)
    if not db_row:
        raise HTTPException(status_code=404, detail=f"ICP channel '{key}' not seeded in database")

    if db_row.status == "live":
        raise HTTPException(status_code=409, detail=f"ICP channel '{key}' is already active")

    from config.settings import get_settings
    county_id = (get_settings().county_launch_source_county or "hillsborough")
    snapshot = compute_icp_gate_snapshot(key, county_id, db)

    if force:
        if not body.reason or not body.reason.strip():
            raise HTTPException(
                status_code=400,
                detail="Force activation requires a non-empty 'reason' in the request body",
            )
        event_type = "force_activated"
        is_force = True
        force_reason = body.reason.strip()
    else:
        # Check one-expansion-at-a-time guardrail
        existing_active = db.execute(sa_text("""
            SELECT key FROM expansion_icp_channels
            WHERE status = 'live' AND key != 'contractor'
            LIMIT 1
        """)).first()
        if existing_active:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Expansion ICP '{existing_active.key}' is already active. "
                    f"Only one expansion ICP can be active at a time. "
                    f"Use force=true with a reason to override."
                ),
            )
        if not is_gate_clear(snapshot):
            reasons = gate_blocking_reasons(snapshot)
            raise HTTPException(
                status_code=422,
                detail={"message": "Gate check failed", "blocking_reasons": reasons},
            )
        event_type = "activated"
        is_force = False
        force_reason = None

    actor = _admin.get("sub", "admin")
    prev_status = db_row.status

    # Set status + 4-week launch window
    db.execute(sa_text("""
        UPDATE expansion_icp_channels
        SET status = 'live',
            launch_started_at = NOW(),
            launch_ends_at = NOW() + INTERVAL '28 days',
            updated_at = NOW()
        WHERE id = :id
    """), {"id": db_row.id})

    _write_audit(
        db,
        channel_key=key,
        event_type=event_type,
        actor=actor,
        prev_status=prev_status,
        new_status="live",
        gate_snapshot=snapshot,
        is_force_activate=is_force,
        force_reason=force_reason,
    )
    db.flush()

    logger.info("[icp-api] activated channel=%s actor=%s force=%s", key, actor, is_force)
    return {"key": key, "status": "live", "prev_status": prev_status, "force": is_force}


@router.post("/{key}/pause")
def pause_icp_channel(
    key: str,
    body: PauseKillRequest,
    db: Session = Depends(_get_db),
    _admin: dict = Depends(get_current_admin),
):
    """Pause an active ICP channel."""
    if key == DEFAULT_ICP_CHANNEL_KEY:
        raise HTTPException(status_code=400, detail="Cannot pause the default contractor ICP")

    db_row = _get_db_channel(db, key)
    if not db_row:
        raise HTTPException(status_code=404, detail=f"ICP channel '{key}' not found")
    if db_row.status != "live":
        raise HTTPException(status_code=409, detail=f"Channel is '{db_row.status}', not live")

    actor = _admin.get("sub", "admin")
    _set_status(db, db_row.id, "gated")
    _write_audit(
        db, channel_key=key, event_type="paused", actor=actor,
        prev_status="live", new_status="gated",
        detail={"reason": body.reason},
    )
    db.flush()
    return {"key": key, "status": "gated", "prev_status": "live"}


@router.post("/{key}/kill")
def kill_icp_channel(
    key: str,
    body: PauseKillRequest,
    db: Session = Depends(_get_db),
    _admin: dict = Depends(get_current_admin),
):
    """Kill (retire) an ICP channel. Terminal state."""
    if key == DEFAULT_ICP_CHANNEL_KEY:
        raise HTTPException(status_code=400, detail="Cannot kill the default contractor ICP")

    db_row = _get_db_channel(db, key)
    if not db_row:
        raise HTTPException(status_code=404, detail=f"ICP channel '{key}' not found")
    if db_row.status == "retired":
        raise HTTPException(status_code=409, detail="Channel is already retired")

    actor = _admin.get("sub", "admin")
    prev_status = db_row.status
    _set_status(db, db_row.id, "retired")
    _write_audit(
        db, channel_key=key, event_type="killed", actor=actor,
        prev_status=prev_status, new_status="retired",
        detail={"reason": body.reason},
    )
    db.flush()
    return {"key": key, "status": "retired", "prev_status": prev_status}


@router.patch("/{key}")
def update_icp_channel(
    key: str,
    body: PatchRequest,
    db: Session = Depends(_get_db),
    _admin: dict = Depends(get_current_admin),
):
    """Update editable config fields on an ICP channel."""
    db_row = _get_db_channel(db, key)
    if not db_row:
        raise HTTPException(status_code=404, detail=f"ICP channel '{key}' not found")

    sets, params = ["updated_at = NOW()"], {"id": db_row.id}
    for field, value in body.model_dump(exclude_none=True).items():
        sets.append(f"{field} = :{field}")
        params[field] = value

    if len(sets) == 1:
        raise HTTPException(status_code=400, detail="No fields to update")

    db.execute(sa_text(f"UPDATE expansion_icp_channels SET {', '.join(sets)} WHERE id = :id"), params)
    actor = _admin.get("sub", "admin")
    _write_audit(
        db, channel_key=key, event_type="config_updated", actor=actor,
        detail=body.model_dump(exclude_none=True),
    )
    db.flush()
    return {"key": key, "updated": body.model_dump(exclude_none=True)}


@router.get("/{key}/metrics")
def get_icp_metrics(
    key: str,
    days: int = 30,
    db: Session = Depends(_get_db),
    _admin: dict = Depends(get_current_admin),
):
    """7-day (or configurable) raw count + derived rate trends per channel."""
    if key not in ICP_CHANNELS:
        raise HTTPException(status_code=404, detail=f"ICP channel '{key}' not in registry")

    from config.settings import get_settings
    county_id = (get_settings().county_launch_source_county or "hillsborough")

    rows = db.execute(sa_text("""
        SELECT run_date, signup_count, payer_count, saved_card_count,
               sms_sent_count, sms_reply_count, active_subscriber_count,
               cancel_count, refund_count, mrr_cents
        FROM icp_daily_stats
        WHERE icp_channel_key = :key AND county_id = :county
          AND run_date >= CURRENT_DATE - :days
        ORDER BY run_date ASC
    """), {"key": key, "county": county_id, "days": days}).mappings().fetchall()

    trend = []
    for r in rows:
        s = r["signup_count"] or 0
        p = r["payer_count"] or 0
        sc = r["saved_card_count"] or 0
        sent = r["sms_sent_count"] or 0
        replied = r["sms_reply_count"] or 0
        trend.append({
            "date": r["run_date"].isoformat(),
            "raw": {
                "signup_count": s,
                "payer_count": p,
                "saved_card_count": sc,
                "sms_sent_count": sent,
                "sms_reply_count": replied,
                "active_subscriber_count": r["active_subscriber_count"],
                "cancel_count": r["cancel_count"],
                "refund_count": r["refund_count"],
                "mrr_usd": round((r["mrr_cents"] or 0) / 100, 2),
            },
            "rates": {
                "first_payment_rate": round(p / s * 100, 1) if s > 0 else None,
                "saved_card_rate": round(sc / p * 100, 1) if p > 0 else None,
                "sms_reply_rate": round(replied / sent * 100, 1) if sent > 0 else None,
            },
        })

    return {"key": key, "county_id": county_id, "days": days, "trend": trend}


@router.get("/{key}/subscribers")
def get_icp_subscribers(
    key: str,
    limit: int = 50,
    offset: int = 0,
    db: Session = Depends(_get_db),
    _admin: dict = Depends(get_current_admin),
):
    """List subscribers attributed to this ICP channel."""
    rows = db.execute(sa_text("""
        SELECT id, email, name, tier, status, vertical, county_id,
               plan_price, created_at, COUNT(*) OVER() AS _total
        FROM subscribers
        WHERE icp_channel_key = :key
        ORDER BY created_at DESC
        LIMIT :limit OFFSET :offset
    """), {"key": key, "limit": limit, "offset": offset}).mappings().fetchall()

    total = int(rows[0]["_total"]) if rows else 0
    return {
        "key": key,
        "total": total,
        "limit": limit,
        "offset": offset,
        "subscribers": [
            {
                "id": r["id"],
                "email": r["email"],
                "name": r["name"],
                "tier": r["tier"],
                "status": r["status"],
                "vertical": r["vertical"],
                "county_id": r["county_id"],
                "plan_price": float(r["plan_price"]) if r["plan_price"] else None,
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            }
            for r in rows
        ],
    }


# ── ICP-scoped pricing ─────────────────────────────────────────────────────────

@router.get("/{key}/pricing")
def get_icp_pricing(
    key: str,
    db: Session = Depends(_get_db),
    _admin: dict = Depends(get_current_admin),
):
    """Return pricing info and active Stripe price ID for this ICP channel."""
    from config.icp_channels import get_icp_price_cents, get_icp_stripe_price_key
    from config.settings import get_settings
    if key not in ICP_CHANNELS:
        raise HTTPException(status_code=404, detail=f"ICP channel '{key}' not in registry")

    price_key = get_icp_stripe_price_key(key)
    settings = get_settings()
    price_id = settings.active_stripe_price(price_key) if price_key else None
    return {
        "key": key,
        "price_monthly_cents": get_icp_price_cents(key),
        "stripe_price_key": price_key,
        "stripe_price_id": price_id,
        "contractor_pricing": key == DEFAULT_ICP_CHANNEL_KEY,
    }


class IcpCheckoutValidateRequest(BaseModel):
    subscriber_icp_channel_key: str
    stripe_price_key: str


@router.post("/validate-checkout")
def validate_icp_checkout(
    body: IcpCheckoutValidateRequest,
    db: Session = Depends(_get_db),
    _admin: dict = Depends(get_current_admin),
):
    """Validate that a subscriber's ICP matches the requested Stripe price.

    Returns 200 {valid: true} when the price belongs to the subscriber's ICP,
    or 409 when there is a cross-ICP mismatch.
    Contractor subscribers use existing tier pricing — always valid.
    """
    from config.icp_channels import STRIPE_KEY_TO_ICP
    sub_key = body.subscriber_icp_channel_key
    price_key = body.stripe_price_key

    # Contractor can buy any existing tier price (unchanged behaviour)
    if sub_key == DEFAULT_ICP_CHANNEL_KEY:
        return {"valid": True, "reason": "contractor ICP uses tier pricing"}

    # ICP-scoped price: check ownership
    owning_icp = STRIPE_KEY_TO_ICP.get(price_key)
    if owning_icp is None:
        # Not an ICP-scoped price — allow (standard tier price)
        return {"valid": True, "reason": "not an ICP-scoped price"}
    if owning_icp != sub_key:
        raise HTTPException(
            status_code=409,
            detail={
                "error": "cross_icp_purchase",
                "message": (
                    f"Price '{price_key}' belongs to ICP '{owning_icp}', "
                    f"but subscriber is attributed to '{sub_key}'. "
                    "Cross-ICP purchases are not allowed."
                ),
                "subscriber_icp": sub_key,
                "price_icp": owning_icp,
            },
        )
    return {"valid": True, "reason": "price matches subscriber ICP"}


# ── Kill-switch decision ───────────────────────────────────────────────────────

VALID_DECISIONS = ("keep", "adjust", "kill")


class KillswitchDecisionRequest(BaseModel):
    decision: str = Field(..., pattern="^(keep|adjust|kill)$")
    reason: str = Field(..., min_length=10)


@router.post("/{key}/killswitch-decision")
def record_killswitch_decision(
    key: str,
    body: KillswitchDecisionRequest,
    db: Session = Depends(_get_db),
    _admin: dict = Depends(get_current_admin),
):
    """Record the final kill-switch decision for a live ICP channel after the 4-week window.

    Decisions:
      keep   — channel performing well; continue
      adjust — underperforming but not killed; document required changes
      kill   — retire the channel (also sets status to retired)

    Always writes an audit row. The 'kill' decision also transitions status to retired.
    """
    if key == DEFAULT_ICP_CHANNEL_KEY:
        raise HTTPException(status_code=400, detail="Cannot set kill-switch decision on the default contractor ICP")

    db_row = _get_db_channel(db, key)
    if not db_row:
        raise HTTPException(status_code=404, detail=f"ICP channel '{key}' not found")
    if db_row.status not in ("live", "gated"):
        raise HTTPException(status_code=409, detail=f"Channel status is '{db_row.status}'; decision only applies to live/gated channels")

    actor = _admin.get("sub", "admin")
    decision = body.decision

    # Record decision on the channel row
    db.execute(sa_text("""
        UPDATE expansion_icp_channels
        SET killswitch_decision = :decision,
            killswitch_reason = :reason,
            killswitch_decided_at = NOW(),
            killswitch_decided_by = :actor,
            updated_at = NOW()
        WHERE id = :id
    """), {"decision": decision, "reason": body.reason, "actor": actor, "id": db_row.id})

    # Kill decision also retires the channel
    if decision == "kill":
        _set_status(db, db_row.id, "retired")

    event_type = f"killswitch_{decision}"
    _write_audit(
        db,
        channel_key=key,
        event_type=event_type,
        actor=actor,
        prev_status=db_row.status,
        new_status="retired" if decision == "kill" else db_row.status,
        detail={"decision": decision, "reason": body.reason},
    )
    db.flush()

    logger.info("[icp-api] killswitch decision=%s channel=%s actor=%s", decision, key, actor)
    return {
        "key": key,
        "decision": decision,
        "new_status": "retired" if decision == "kill" else db_row.status,
    }
