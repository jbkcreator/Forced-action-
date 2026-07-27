"""Operator CRM router (fa045).

Endpoints powering the /admin/subscribers surface:

  GET    /api/admin/subscribers                       — paginated list (filterable)
  GET    /api/admin/subscribers/hot                   — at-risk queue
  GET    /api/admin/subscribers/{id}                  — detail bundle
  GET    /api/admin/subscribers/{id}/conversation     — two-sided SMS+Chat timeline
  GET    /api/admin/subscribers/{id}/deals            — DealOutcome list
  PATCH  /api/admin/deals/{deal_id}                   — change pipeline_stage (audited)
  GET    /api/admin/subscribers/{id}/notes            — list notes
  POST   /api/admin/subscribers/{id}/notes            — create note
  PATCH  /api/admin/notes/{note_id}                   — update note (body, pinned)
  DELETE /api/admin/notes/{note_id}                   — delete note
  GET    /api/admin/subscribers/{id}/tags             — list tags
  POST   /api/admin/subscribers/{id}/tags             — add tag
  DELETE /api/admin/subscribers/{id}/tags/{tag}       — remove tag
  GET    /api/admin/subscriber-tag-suggestions        — curated suggestion list

Hot-queue spec:
  revenue_signal_score >= 60
  AND last_significant_action_at < now() - 7 days
  AND status != 'cancelled'
  AND last_significant_action_at IS NOT NULL  (excludes never-acted)
  ORDER BY last_significant_action_at ASC, revenue_signal_score DESC
  LIMIT 50

`status == 'grace'` rows are kept and surfaced with grace=True.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Body, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from src.api.admin_router import get_current_admin
from src.api.deps import get_db
from src.core.models import (
    DealOutcome,
    DealPipelineEvent,
    Subscriber,
    SubscriberNote,
    SubscriberTag,
)
from src.services.revenue_signal import get_revenue_signal_score

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/admin", tags=["operator_crm"])


_PIPELINE_STAGES = {
    "lead", "contacted", "qualified", "proposal",
    "negotiation", "closed_won", "closed_lost",
}


# ─────────────────────────────────────────────────────────────────────────────
# helpers
# ─────────────────────────────────────────────────────────────────────────────


def _subscriber_row(s: Subscriber) -> dict:
    return {
        "id":        s.id,
        "email":     s.email,
        "name":      s.name,
        "phone":     s.phone,
        "tier":      s.tier,
        "vertical":  s.vertical,
        "county_id": s.county_id,
        "status":    s.status,
        "grace":     s.status == "grace",
        "revenue_signal_score": s.revenue_signal_score or 0,
        "revenue_signal_band":  s.revenue_signal_band,
        "revenue_signal_updated_at":
            s.revenue_signal_updated_at.isoformat() if s.revenue_signal_updated_at else None,
        "created_at": s.created_at.isoformat() if s.created_at else None,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Subscriber list + Hot queue
# ─────────────────────────────────────────────────────────────────────────────


@router.get("/subscribers")
def list_subscribers(
    q: Optional[str] = Query(None, description="Search by email or name (ILIKE)"),
    vertical: Optional[str] = None,
    county_id: Optional[str] = None,
    tier: Optional[str] = None,
    band: Optional[str] = Query(None, description="RSS band: low/medium/high/very_high"),
    status: Optional[str] = Query(None, description="active/grace/churned/cancelled"),
    tag: Optional[str] = Query(None, description="Filter to subscribers with this tag"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
):
    where = ["1=1"]
    params: dict = {"limit": limit, "offset": offset}
    if q:
        where.append("(s.email ILIKE :q OR s.name ILIKE :q)")
        params["q"] = f"%{q}%"
    if vertical:
        where.append("s.vertical = :vertical"); params["vertical"] = vertical
    if county_id:
        where.append("s.county_id = :county_id"); params["county_id"] = county_id
    if tier:
        where.append("s.tier = :tier"); params["tier"] = tier
    if band:
        where.append("s.revenue_signal_band = :band"); params["band"] = band
    if status:
        where.append("s.status = :status"); params["status"] = status
    if tag:
        where.append(
            "EXISTS (SELECT 1 FROM subscriber_tags t "
            "WHERE t.subscriber_id = s.id AND t.tag = :tag)"
        )
        params["tag"] = tag

    where_sql = " AND ".join(where)
    total = db.execute(
        text(f"SELECT COUNT(*) FROM subscribers s WHERE {where_sql}"),
        params,
    ).scalar() or 0

    rows = db.execute(text(f"""
        SELECT s.id FROM subscribers s
        WHERE {where_sql}
        ORDER BY s.revenue_signal_score DESC NULLS LAST, s.id DESC
        LIMIT :limit OFFSET :offset
    """), params).fetchall()

    subs = db.query(Subscriber).filter(
        Subscriber.id.in_([r.id for r in rows])
    ).all() if rows else []
    sub_by_id = {s.id: s for s in subs}
    items = [_subscriber_row(sub_by_id[r.id]) for r in rows if r.id in sub_by_id]

    return {"total": total, "limit": limit, "offset": offset, "items": items}


@router.get("/subscribers/hot")
def hot_subscriber_queue(
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
):
    """Hot Subscriber Queue — at-risk subscribers ranked by cool-down depth.

    Definition: RSS >= 60 AND last_significant_action_at < now() - 7d
    AND status != 'cancelled' AND last_significant_action_at IS NOT NULL.
    """
    rows = db.execute(text("""
        SELECT s.id, us.last_significant_action_at
        FROM subscribers s
        JOIN user_segments us ON us.subscriber_id = s.id
        WHERE s.revenue_signal_score >= 60
          AND us.last_significant_action_at IS NOT NULL
          AND us.last_significant_action_at < (NOW() - INTERVAL '7 days')
          AND s.status != 'cancelled'
        ORDER BY us.last_significant_action_at ASC,
                 s.revenue_signal_score DESC
        LIMIT 50
    """)).fetchall()

    if not rows:
        return {"items": []}

    id_to_lsa = {r.id: r.last_significant_action_at for r in rows}
    subs = db.query(Subscriber).filter(Subscriber.id.in_(list(id_to_lsa))).all()
    sub_by_id = {s.id: s for s in subs}

    items = []
    now = datetime.now(timezone.utc)
    for r in rows:
        s = sub_by_id.get(r.id)
        if not s:
            continue
        lsa = id_to_lsa[r.id]
        days_cool = (now - lsa.replace(tzinfo=timezone.utc) if lsa.tzinfo is None
                     else now - lsa).days
        row = _subscriber_row(s)
        row["last_significant_action_at"] = lsa.isoformat() if lsa else None
        row["days_since_action"] = days_cool
        items.append(row)
    return {"items": items}


# ─────────────────────────────────────────────────────────────────────────────
# Subscriber detail bundle
# ─────────────────────────────────────────────────────────────────────────────


@router.get("/subscribers/{subscriber_id}")
def subscriber_detail(
    subscriber_id: int,
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
):
    s = db.get(Subscriber, subscriber_id)
    if not s:
        raise HTTPException(status_code=404, detail="subscriber not found")

    rss = get_revenue_signal_score(subscriber_id, db)
    tags = [t.tag for t in db.query(SubscriberTag)
            .filter(SubscriberTag.subscriber_id == subscriber_id)
            .order_by(SubscriberTag.created_at.desc()).all()]

    note_rows = db.query(SubscriberNote).filter(
        SubscriberNote.subscriber_id == subscriber_id
    ).order_by(
        SubscriberNote.pinned.desc(),
        SubscriberNote.created_at.desc(),
    ).limit(5).all()
    recent_notes = [
        {
            "id":         n.id,
            "author_email": n.author_email,
            "body":       n.body,
            "pinned":     n.pinned,
            "created_at": n.created_at.isoformat() if n.created_at else None,
            "updated_at": n.updated_at.isoformat() if n.updated_at else None,
        }
        for n in note_rows
    ]

    open_deals = db.query(DealOutcome).filter(
        DealOutcome.subscriber_id == subscriber_id,
        DealOutcome.pipeline_stage.notin_(["closed_won", "closed_lost"]),
    ).count()

    return {
        "subscriber": _subscriber_row(s),
        "rss":        rss,
        "tags":       tags,
        "recent_notes": recent_notes,
        "open_deals_count": open_deals,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Two-sided conversation
# ─────────────────────────────────────────────────────────────────────────────


@router.get("/subscribers/{subscriber_id}/conversation")
def subscriber_conversation(
    subscriber_id: int,
    limit: int = Query(100, ge=1, le=500),
    before: Optional[str] = Query(None, description="ISO timestamp; return items strictly before this"),
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
):
    """Unified conversation history: outbound Lifecycle SMS (sms_send_logs),
    inbound SMS reply storage (best-effort from sms_opt_outs keyword
    events), and ChatMessage rows (both directions) joined via
    ChatSession.subscriber_id.

    Items normalize to: {direction, channel, body, at, status, meta}.
    """
    s = db.get(Subscriber, subscriber_id)
    if not s:
        raise HTTPException(status_code=404, detail="subscriber not found")

    items: list[dict] = []
    cutoff = before  # raw ISO string interpolated into queries below

    # Outbound SMS (sms_send_logs has body_preview)
    out_sql = """
        SELECT id, body_preview, message_type, outcome,
               vendor, campaign, decision_id, created_at
        FROM sms_send_logs
        WHERE subscriber_id = :sid
    """
    params: dict = {"sid": subscriber_id, "limit": limit}
    if cutoff:
        out_sql += " AND created_at < :cutoff"
        params["cutoff"] = cutoff
    out_sql += " ORDER BY created_at DESC LIMIT :limit"
    for r in db.execute(text(out_sql), params).fetchall():
        items.append({
            "id":        f"sms_out:{r.id}",
            "direction": "outbound",
            "channel":   "sms",
            "body":      r.body_preview or "",
            "at":        r.created_at.isoformat() if r.created_at else None,
            "status":    r.outcome,
            "meta": {
                "message_type": r.message_type,
                "vendor":       r.vendor,
                "campaign":     r.campaign,
                "decision_id":  r.decision_id,
            },
        })

    # Inbound SMS — sms_opt_outs records STOP keyword events (only inbound
    # SMS surface that survives in storage today). Surfaced so the timeline
    # shows the opt-out turn instead of dropping it.
    if s.phone:
        in_sql = """
            SELECT id, keyword_used, opted_out_at
            FROM sms_opt_outs
            WHERE phone = :phone
        """
        ip: dict = {"phone": s.phone, "limit": limit}
        if cutoff:
            in_sql += " AND opted_out_at < :cutoff"
            ip["cutoff"] = cutoff
        in_sql += " ORDER BY opted_out_at DESC LIMIT :limit"
        for r in db.execute(text(in_sql), ip).fetchall():
            items.append({
                "id":        f"sms_in:{r.id}",
                "direction": "inbound",
                "channel":   "sms",
                "body":      r.keyword_used or "(inbound keyword)",
                "at":        r.opted_out_at.isoformat() if r.opted_out_at else None,
                "status":    "received",
                "meta":      {"event": "opt_out_keyword"},
            })

    # Chat — both directions, joined through ChatSession.
    chat_sql = """
        SELECT m.id, m.role, m.content, m.intent_label, m.created_at,
               m.session_id
        FROM chat_messages m
        JOIN chat_sessions cs ON cs.id = m.session_id
        WHERE cs.subscriber_id = :sid
    """
    cp: dict = {"sid": subscriber_id, "limit": limit}
    if cutoff:
        chat_sql += " AND m.created_at < :cutoff"
        cp["cutoff"] = cutoff
    chat_sql += " ORDER BY m.created_at DESC LIMIT :limit"
    for r in db.execute(text(chat_sql), cp).fetchall():
        direction = "inbound" if r.role == "user" else "outbound"
        items.append({
            "id":        f"chat:{r.id}",
            "direction": direction,
            "channel":   "chat",
            "body":      r.content or "",
            "at":        r.created_at.isoformat() if r.created_at else None,
            "status":    None,
            "meta": {
                "role":         r.role,
                "intent_label": r.intent_label,
                "session_id":   r.session_id,
            },
        })

    items.sort(key=lambda x: (x["at"] or ""), reverse=True)
    items = items[:limit]
    return {"subscriber_id": subscriber_id, "items": items}


# ─────────────────────────────────────────────────────────────────────────────
# Deals
# ─────────────────────────────────────────────────────────────────────────────


class DealStagePatch(BaseModel):
    pipeline_stage: str = Field(..., description="New pipeline stage")
    note: Optional[str] = None


@router.get("/subscribers/{subscriber_id}/deals")
def list_subscriber_deals(
    subscriber_id: int,
    include_closed: bool = Query(False, description="Include closed_won/closed_lost"),
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
):
    q = db.query(DealOutcome).filter(DealOutcome.subscriber_id == subscriber_id)
    if not include_closed:
        q = q.filter(DealOutcome.pipeline_stage.notin_(["closed_won", "closed_lost"]))
    rows = q.order_by(DealOutcome.created_at.desc()).all()
    return {"items": [
        {
            "id":               d.id,
            "subscriber_id":    d.subscriber_id,
            "property_id":      d.property_id,
            "pipeline_stage":   d.pipeline_stage,
            "deal_size_bucket": d.deal_size_bucket,
            "deal_amount":      float(d.deal_amount) if d.deal_amount is not None else None,
            "deal_date":        d.deal_date.isoformat() if d.deal_date else None,
            "lead_source":      d.lead_source,
            "days_to_close":    d.days_to_close,
            "created_at":       d.created_at.isoformat() if d.created_at else None,
        }
        for d in rows
    ]}


@router.patch("/deals/{deal_id}")
def update_deal_stage(
    deal_id: int,
    body: DealStagePatch,
    db: Session = Depends(get_db),
    admin: dict = Depends(get_current_admin),
):
    if body.pipeline_stage not in _PIPELINE_STAGES:
        raise HTTPException(
            status_code=400,
            detail=f"pipeline_stage must be one of {sorted(_PIPELINE_STAGES)}",
        )
    deal = db.get(DealOutcome, deal_id)
    if not deal:
        raise HTTPException(status_code=404, detail="deal not found")

    old_stage = deal.pipeline_stage
    if old_stage == body.pipeline_stage:
        return {"ok": True, "no_change": True, "deal_id": deal_id}

    deal.pipeline_stage = body.pipeline_stage
    db.add(DealPipelineEvent(
        deal_id=deal_id,
        from_stage=old_stage,
        to_stage=body.pipeline_stage,
        changed_by=admin.get("sub") or admin.get("username") or "admin",
        note=body.note,
    ))
    db.flush()

    # Phase 3 A5: pre-decision snapshot (idempotent — captures if not yet recorded)
    try:
        from src.services.snapshot_service import capture_snapshot, resolve_snapshot
        if body.pipeline_stage == "closed_won":
            capture_snapshot(property_id=deal.property_id, db=db,
                             deal_outcome_id=deal_id, outcome_status="funded")
            resolve_snapshot(deal_id, "funded", db)
        elif body.pipeline_stage in ("closed_lost", "declined"):
            capture_snapshot(property_id=deal.property_id, db=db,
                             deal_outcome_id=deal_id, outcome_status="lost")
            resolve_snapshot(deal_id, "lost", db)
        else:
            capture_snapshot(property_id=deal.property_id, db=db, deal_outcome_id=deal_id)
    except Exception as exc:
        logger.warning("[crm_patch] snapshot failed deal_id=%d: %s", deal_id, exc)

    # Phase 3 A1: fire loss autopsy when a deal is manually moved to a loss stage
    if body.pipeline_stage in ("closed_lost", "declined"):
        try:
            from src.services.loss_autopsy import run_loss_autopsy
            reason = "DECLINED" if body.pipeline_stage == "declined" else "CLOSED_LOST"
            run_loss_autopsy(
                property_id=deal.property_id,
                trigger_reason=reason,
                db=db,
                deal_outcome_id=deal_id,
            )
        except Exception as exc:
            logger.warning("[crm_patch] loss autopsy failed deal_id=%d: %s", deal_id, exc)

    return {
        "ok":         True,
        "deal_id":    deal_id,
        "from_stage": old_stage,
        "to_stage":   body.pipeline_stage,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Notes
# ─────────────────────────────────────────────────────────────────────────────


class NoteCreate(BaseModel):
    body: str = Field(..., min_length=1)
    pinned: bool = False


class NotePatch(BaseModel):
    body: Optional[str] = Field(None, min_length=1)
    pinned: Optional[bool] = None


def _note_to_dict(n: SubscriberNote) -> dict:
    return {
        "id":            n.id,
        "subscriber_id": n.subscriber_id,
        "author_email":  n.author_email,
        "body":          n.body,
        "pinned":        n.pinned,
        "created_at":    n.created_at.isoformat() if n.created_at else None,
        "updated_at":    n.updated_at.isoformat() if n.updated_at else None,
    }


@router.get("/subscribers/{subscriber_id}/notes")
def list_subscriber_notes(
    subscriber_id: int,
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
):
    rows = db.query(SubscriberNote).filter(
        SubscriberNote.subscriber_id == subscriber_id
    ).order_by(
        SubscriberNote.pinned.desc(),
        SubscriberNote.created_at.desc(),
    ).all()
    return {"items": [_note_to_dict(n) for n in rows]}


@router.post("/subscribers/{subscriber_id}/notes", status_code=201)
def create_subscriber_note(
    subscriber_id: int,
    body: NoteCreate,
    db: Session = Depends(get_db),
    admin: dict = Depends(get_current_admin),
):
    if not db.get(Subscriber, subscriber_id):
        raise HTTPException(status_code=404, detail="subscriber not found")
    n = SubscriberNote(
        subscriber_id=subscriber_id,
        author_email=admin.get("sub") or admin.get("username") or "admin",
        body=body.body,
        pinned=body.pinned,
    )
    db.add(n)
    db.flush()
    return _note_to_dict(n)


@router.patch("/notes/{note_id}")
def update_subscriber_note(
    note_id: int,
    body: NotePatch,
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
):
    n = db.get(SubscriberNote, note_id)
    if not n:
        raise HTTPException(status_code=404, detail="note not found")
    if body.body is not None:
        n.body = body.body
    if body.pinned is not None:
        n.pinned = body.pinned
    db.flush()
    return _note_to_dict(n)


@router.delete("/notes/{note_id}", status_code=204)
def delete_subscriber_note(
    note_id: int,
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
):
    n = db.get(SubscriberNote, note_id)
    if not n:
        raise HTTPException(status_code=404, detail="note not found")
    db.delete(n)
    db.flush()
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Tags
# ─────────────────────────────────────────────────────────────────────────────


class TagCreate(BaseModel):
    tag: str = Field(..., min_length=1, max_length=50)


@router.get("/subscribers/{subscriber_id}/tags")
def list_subscriber_tags(
    subscriber_id: int,
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
):
    rows = db.query(SubscriberTag).filter(
        SubscriberTag.subscriber_id == subscriber_id
    ).order_by(SubscriberTag.created_at.desc()).all()
    return {"items": [
        {"id": t.id, "tag": t.tag,
         "created_at": t.created_at.isoformat() if t.created_at else None}
        for t in rows
    ]}


@router.post("/subscribers/{subscriber_id}/tags", status_code=201)
def add_subscriber_tag(
    subscriber_id: int,
    body: TagCreate,
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
):
    if not db.get(Subscriber, subscriber_id):
        raise HTTPException(status_code=404, detail="subscriber not found")
    tag = body.tag.strip().lower().replace(" ", "_")
    existing = db.query(SubscriberTag).filter(
        SubscriberTag.subscriber_id == subscriber_id,
        SubscriberTag.tag == tag,
    ).first()
    if existing:
        return {"id": existing.id, "tag": existing.tag, "existing": True}
    t = SubscriberTag(subscriber_id=subscriber_id, tag=tag)
    db.add(t)
    db.flush()
    return {"id": t.id, "tag": t.tag,
            "created_at": t.created_at.isoformat() if t.created_at else None}


@router.delete("/subscribers/{subscriber_id}/tags/{tag}", status_code=204)
def remove_subscriber_tag(
    subscriber_id: int,
    tag: str,
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
):
    row = db.query(SubscriberTag).filter(
        SubscriberTag.subscriber_id == subscriber_id,
        SubscriberTag.tag == tag,
    ).first()
    if not row:
        raise HTTPException(status_code=404, detail="tag not found")
    db.delete(row)
    db.flush()
    return None


@router.get("/subscriber-tag-suggestions")
def tag_suggestions(_admin: dict = Depends(get_current_admin)):
    from config.subscriber_tags import suggestion_list
    return {"items": suggestion_list()}
