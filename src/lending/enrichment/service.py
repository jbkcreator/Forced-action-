"""Build the background enrichment card for a LendingFlow lead, route it, and post to the deal thread.

Entry points (all end in ``enrich_lead``):
- ``on_lead_created``: subscribed to ``LendingFlowLeadCreated`` by ``lending-api``.
- ``record_deal_facts`` + ``enrich_lead``: the deal-facts webhook, when the booking form or the
  after-call form supplies an address or target close date. Re-runs update the row quietly.
- the ``lending_enrichment_sweep`` cron: creates rows for emitted leads that have none and retries
  failed ones, so a crashed handler or a process that never subscribed loses nothing.

Nothing here contacts the borrower: the card, the fit and the thread post are internal. Logs carry ids
and counts only, never an address, name or phone.
"""
from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.lending_enrichment import (
    COVERED_STATE,
    MAX_ATTEMPTS,
    NOT_AVAILABLE,
    RETRY_BACKOFF_MINUTES,
    RUNNING_STALE_MINUTES,
    SWEEP_BATCH_SIZE,
)
from config.settings import get_settings
from src.lending.contracts import RoutingTag
from src.lending.enrichment.fit_card import Evaluator, FitView, evaluate_fit
from src.lending.enrichment.ports import (
    CompsPort,
    CompsView,
    DealThreadPort,
    ForcedActionComps,
    StreetViewPort,
    UnavailableDealThread,
    default_deal_thread,
    default_street_view,
)
from src.lending.enrichment.property_facts import PropertyFacts, load_property_facts, match_property
from src.lending.enrichment.routing import route_lead, today_et
from src.services.phone_utils import normalize

logger = logging.getLogger(__name__)

READY, FAILED, BUSY, SKIPPED = "ready", "failed", "busy", "skipped"


def enabled() -> bool:
    return bool(get_settings().lending_enrichment_enabled)


@dataclass(frozen=True)
class EnrichmentOutcome:
    lead_id: int
    status: str
    routing_tag: Optional[str] = None


# --------------------------------------------------------------------------- state

def ensure_row(db: Session, lead_id: int) -> None:
    db.execute(
        text("INSERT INTO lending.lendingflow_enrichment (lead_id) VALUES (:id) ON CONFLICT (lead_id) DO NOTHING"),
        {"id": lead_id},
    )


def record_deal_facts(
    db: Session, *, phone: str, address: Optional[str], target_close_date: Optional[date], source: str,
) -> Optional[int]:
    """Store a call- or form-captured address / close date for the phone's latest LendingFlow lead and
    queue a re-run. Only the facts supplied overwrite; None keeps what is stored. Returns the lead id, or
    None when the phone has no LendingFlow lead. Commits nothing."""
    normalized = normalize(phone)
    if not normalized:
        return None
    lead_id = db.execute(
        text("SELECT id FROM lending.lendingflow_leads WHERE phone = :phone AND NOT suppressed "
             "ORDER BY received_at DESC LIMIT 1"),
        {"phone": normalized},
    ).scalar()
    if lead_id is None:
        return None
    cleaned = " ".join(address.split())[:200] if address and address.strip() else None
    ensure_row(db, lead_id)
    db.execute(
        text("UPDATE lending.lendingflow_enrichment SET "
             "captured_address = COALESCE(:address, captured_address), "
             "target_close_date = COALESCE(:close, target_close_date), facts_source = :source, "
             "status = 'pending', attempts = 0, last_error = NULL, updated_at = now() WHERE lead_id = :id"),
        {"address": cleaned, "close": target_close_date, "source": source[:30], "id": lead_id},
    )
    return lead_id


_CLAIM_SQL = text("""
UPDATE lending.lendingflow_enrichment SET status = 'running', attempts = attempts + 1, last_attempt_at = now(),
       updated_at = now()
 WHERE lead_id = :id AND (status IN ('pending', 'failed')
       OR (status = 'running' AND last_attempt_at < now() - make_interval(mins => :stale)))
RETURNING captured_address, target_close_date, thread_ts, post_signature
""")

_LEAD_SQL = text("""
SELECT id, suppressed, property_address, property_city, property_zip, property_state, credit_band,
       credit_band_min_fico, loan_amount, loan_type
  FROM lending.lendingflow_leads WHERE id = :id
""")

_SAVE_SQL = text("""
UPDATE lending.lendingflow_enrichment SET status = 'ready', last_error = NULL, property_id = :property_id,
       match_confidence = :confidence, routing_tag = :tag, routing_reason = :reason, closer_priority = :priority,
       card = CAST(:card AS jsonb), fit = CAST(:fit AS jsonb), updated_at = now() WHERE lead_id = :id
""")


def _fail(db: Session, lead_id: int, reason: str) -> EnrichmentOutcome:
    db.execute(text("UPDATE lending.lendingflow_enrichment SET status = 'failed', last_error = :e, updated_at = now() "
                    "WHERE lead_id = :id"), {"e": reason[:200], "id": lead_id})
    db.commit()
    return EnrichmentOutcome(lead_id, FAILED)


# --------------------------------------------------------------------------- the card

def _money(value: Any) -> str:
    try:
        return f"${float(value):,.0f}"
    except (TypeError, ValueError):
        return NOT_AVAILABLE


def render_card_text(lead_id: int, address: Optional[str], tag: RoutingTag, facts: Optional[PropertyFacts],
                     fit: FitView, *, coverage_note: Optional[str]) -> str:
    lines = [f"*LendingFlow lead #{lead_id}*: {tag.value}" + (" (closer priority)" if tag is RoutingTag.FULL_MACHINE else ""),
             f"Property: {address or NOT_AVAILABLE}"]
    if facts is None:
        lines.append(f"Forced Action facts: {coverage_note or NOT_AVAILABLE}")
    else:
        officers = ", ".join(o["name"] for o in facts.officers) or NOT_AVAILABLE
        lines += [
            f"Sunbiz standing: {facts.sunbiz_standing or NOT_AVAILABLE}  |  Officers: {officers}",
            f"Prior deeds: {facts.deed_count}" + "".join(
                f"\n  - {d.get('record_date') or '?'} {d.get('deed_type') or ''} {_money(d.get('sale_price')) if d.get('sale_price') else ''}".rstrip()
                for d in facts.deeds),
            f"Permits: {facts.permit_count}" + "".join(
                f"\n  - {p.get('issue_date') or '?'} {p.get('permit_type') or ''} ({p.get('status') or 'n/a'})" for p in facts.permits),
        ]
    if fit.evaluated:
        score = f"{fit.score}" if fit.score is not None else f"{NOT_AVAILABLE} ({fit.note})"
        lines.append(f"Lender fit score: {score}")
        lines.append("Fitting lenders: " + (", ".join(fit.ranked) or "none"))
        lines += [f"Note: {s}" for s in fit.straddles]
    else:
        lines.append(f"Lender fit: {fit.note}")
    lines.append("Internal analysis only. Not a rate, term or commitment.")
    return "\n".join(lines)


def _render_comps_text(comps: CompsView, street_view: Optional[str]) -> str:
    lines = [f"Street View: {street_view or NOT_AVAILABLE}"]
    if not comps.available:
        lines.append(f"Forced Action comps: {NOT_AVAILABLE} ({comps.reason})")
        return "\n".join(lines)
    lines.append(f"Forced Action comps ({comps.comp_count}, {comps.confidence or 'n/a'} confidence): "
                 f"{_money(comps.low)} to {_money(comps.high)}, point {_money(comps.point)} (internal estimate)")
    lines += [f"  - {c['sale']} {_money(c['sale_price'])}, {c['sqft']} sqft" for c in comps.comps]
    return "\n".join(lines)


def _signature(address: Optional[str], close: Optional[date], tag: RoutingTag) -> str:
    return hashlib.sha256(f"{address or ''}|{close or ''}|{tag.value}".encode()).hexdigest()


# --------------------------------------------------------------------------- orchestration

def enrich_lead(
    db: Session,
    lead_id: int,
    *,
    street_view: Optional[StreetViewPort] = None,
    comps: Optional[CompsPort] = None,
    thread: Optional[DealThreadPort] = None,
    evaluator: Optional[Evaluator] = None,
    now: Optional[datetime] = None,
) -> EnrichmentOutcome:
    """Claim, build, route, store, then post. Commits its own steps; never raises."""
    try:
        claimed = db.execute(_CLAIM_SQL, {"id": lead_id, "stale": RUNNING_STALE_MINUTES}).mappings().first()
        db.commit()
        if claimed is None:
            return EnrichmentOutcome(lead_id, BUSY)
        lead = db.execute(_LEAD_SQL, {"id": lead_id}).mappings().first()
        if lead is None or lead["suppressed"]:
            db.execute(text("UPDATE lending.lendingflow_enrichment SET status = 'ready', routing_reason = 'skipped', "
                            "updated_at = now() WHERE lead_id = :id"), {"id": lead_id})
            db.commit()
            return EnrichmentOutcome(lead_id, SKIPPED)

        from_call = bool(claimed["captured_address"])
        address = claimed["captured_address"] or lead["property_address"]
        close = claimed["target_close_date"]

        match = match_property(db, address, state=lead["property_state"], city=lead["property_city"],
                               zip_code=lead["property_zip"])
        facts = load_property_facts(db, match.property_id) if match else None
        if facts is None and address and (lead["property_state"] or COVERED_STATE).upper() != COVERED_STATE:
            coverage = "outside Forced Action coverage"
        else:
            coverage = "no address" if not address else "no confident property match"
        fit = evaluate_fit(dict(lead), address=address, target_close_date=close, evaluator=evaluator)
        decision = route_lead(address, close, today=today_et(now))
        card = {"address_source": ("call" if from_call else "lendingflow") if address else None,
                "property_matched": match is not None, "facts": facts.to_json() if facts else None,
                "coverage_note": None if facts else coverage}
        db.execute(_SAVE_SQL, {
            "id": lead_id, "property_id": match.property_id if match else None,
            "confidence": match.confidence if match else None, "tag": decision.tag.value, "reason": decision.reason,
            "priority": decision.tag is RoutingTag.FULL_MACHINE, "card": json.dumps(card, default=str),
            "fit": json.dumps(fit.to_json()),
        })
        db.commit()
        logger.info("[enrichment] lead=%s tag=%s reason=%s matched=%s fit_score=%s", lead_id, decision.tag.value,
                    decision.reason, match is not None, fit.score)
    except Exception as exc:
        db.rollback()
        logger.error("[enrichment] lead=%s failed: %s", lead_id, type(exc).__name__)
        try:
            return _fail(db, lead_id, type(exc).__name__)
        except Exception:
            db.rollback()
            return EnrichmentOutcome(lead_id, FAILED)

    return _post_if_needed(
        db, lead_id, decision.tag, address, close, facts, fit, coverage, from_call=from_call,
        thread_ts=claimed["thread_ts"], last_signature=claimed["post_signature"], match=match,
        street_view=street_view, comps=comps, thread=thread,
    )


def _post_if_needed(db, lead_id, tag, address, close, facts, fit, coverage, *, from_call, thread_ts, last_signature,
                    match, street_view, comps, thread) -> EnrichmentOutcome:
    """Post the card to the deal thread for FULL_MACHINE leads and for any lead whose address was captured on
    the call, once per (address, close date, tag). A failed post marks the row failed so the sweep retries."""
    signature = _signature(address, close, tag)
    if not (tag is RoutingTag.FULL_MACHINE or from_call) or signature == last_signature:
        return EnrichmentOutcome(lead_id, READY, tag.value)
    try:
        thread = thread or default_deal_thread()
        if isinstance(thread, UnavailableDealThread):
            logger.warning("[enrichment] lead=%s deal thread not posted: Slack is not configured", lead_id)
            return EnrichmentOutcome(lead_id, READY, tag.value)
        root = thread.post(render_card_text(lead_id, address, tag, facts, fit, coverage_note=coverage),
                           thread_ts=thread_ts)
        if root is None:
            return _fail(db, lead_id, "deal_thread_unavailable")
        if from_call and address:
            link = (street_view or default_street_view()).link(address)
            comps_view = (comps or ForcedActionComps()).comps(db, match.property_id) if match else CompsView(
                available=False, reason="no_property_match")
            thread.post(_render_comps_text(comps_view, link), thread_ts=root)
        db.execute(text("UPDATE lending.lendingflow_enrichment SET thread_ts = :ts, post_signature = :sig, "
                        "posted_at = now(), updated_at = now() WHERE lead_id = :id"),
                   {"ts": root, "sig": signature, "id": lead_id})
        db.commit()
    except Exception as exc:
        db.rollback()
        logger.error("[enrichment] lead=%s deal thread post failed: %s", lead_id, type(exc).__name__)
        return _fail(db, lead_id, "deal_thread_post_failed")
    return EnrichmentOutcome(lead_id, READY, tag.value)


# --------------------------------------------------------------------------- triggers

def on_lead_created(event) -> None:
    """``LendingFlowLeadCreated`` handler. Opens its own session; the sweep is the safety net."""
    if not enabled():
        return
    from src.lending.db import lending_session

    with lending_session() as db:
        lead_id = db.execute(text("SELECT id FROM lending.lendingflow_leads WHERE lead_uuid = :u"),
                             {"u": event.lead_id}).scalar()
        if lead_id is None:
            return
        ensure_row(db, lead_id)
        db.commit()
        enrich_lead(db, lead_id)


def _due(row: Any, now: datetime) -> bool:
    if row["status"] == "pending" or row["last_attempt_at"] is None:
        return True
    if row["status"] == "running":
        return row["last_attempt_at"] < now - timedelta(minutes=RUNNING_STALE_MINUTES)
    if row["attempts"] >= MAX_ATTEMPTS:
        return False
    wait = RETRY_BACKOFF_MINUTES[min(max(row["attempts"], 1), len(RETRY_BACKOFF_MINUTES)) - 1]
    return row["last_attempt_at"] <= now - timedelta(minutes=wait)


def run_sweep(db: Session, *, now: Optional[datetime] = None, **ports) -> int:
    """Create rows for emitted, non-suppressed leads that have none, then run every due row. Returns rows run."""
    now = now or datetime.now(timezone.utc)
    db.execute(text(
        "INSERT INTO lending.lendingflow_enrichment (lead_id) "
        "SELECT l.id FROM lending.lendingflow_leads l WHERE l.event_emitted_at IS NOT NULL AND NOT l.suppressed "
        "AND NOT EXISTS (SELECT 1 FROM lending.lendingflow_enrichment e WHERE e.lead_id = l.id) "
        "ORDER BY l.id LIMIT :limit ON CONFLICT (lead_id) DO NOTHING"), {"limit": SWEEP_BATCH_SIZE})
    db.commit()
    rows = db.execute(text(
        "SELECT lead_id, status, attempts, last_attempt_at FROM lending.lendingflow_enrichment "
        "WHERE status IN ('pending', 'failed', 'running') ORDER BY updated_at LIMIT :limit"),
        {"limit": SWEEP_BATCH_SIZE * 4}).mappings().all()
    ran = 0
    for row in rows:
        if ran >= SWEEP_BATCH_SIZE:
            break
        if _due(row, now):
            enrich_lead(db, row["lead_id"], now=now, **ports)
            ran += 1
    return ran
