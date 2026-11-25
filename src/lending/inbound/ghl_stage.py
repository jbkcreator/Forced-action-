"""GHL stage change on the inbound journal (R1 0.5 T3, grill D2 / D4 / D5 / D7).

``handle_stage(db, raw)`` is the idempotent handler for journal source ``ghl_stage``: it stores the body's stage
event (``lending.ghl_stage_events``, the scoreboard), records the deal's current stage and feeds the loan bridge.

Read-through (D7): before recording the current stage it reads the opportunity from the Next Deal Lending GHL
sub-account. When GHL's current stage differs from the body (an out-of-order or late delivery), GHL's stage is
recorded and handed to the loan bridge; the body's event is still stored. A GHL read that fails, or no GHL account,
falls back to the body (the nightly reconcile corrects it). Logs carry exception classes and ids only.

Reconcile payloads (T7): ``{"opportunity_id", "stage_name", "pipeline_id", "phone"?, "source": "reconcile"}`` carry
GHL's own stage, so the handler records it without reading GHL again.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Optional

import requests
from sqlalchemy import text

from src.lending.closing.blockers import current_stage, upsert_current_stage
from src.lending.cora_inbox import add_to_inbox_safely
from src.lending.ghl_account import GhlAccount, ghl_headers, lending_ghl_account
from src.lending.ghl_consent_events import field as _field
from src.lending.ghl_stage_bridge import GHL_STAGE_TO_LOAN_STAGE, apply_stage
from src.lending.inbound import journal
from src.lending.webhook_failures import GHL_LOAN_STAGE, report_webhook_failure
from src.services import ghl_webhook
from src.services.phone_utils import normalize

logger = logging.getLogger(__name__)

Request = Callable[..., Any]

SOURCE = "ghl_stage"
RECONCILE = "reconcile"  # raw["source"] of a T7 reconcile payload: its stage is already GHL's

_TIMEOUT = 10  # seconds; the read is inside the webhook, so it is kept short


class GhlStageReadError(RuntimeError):
    """GHL could not say which stage the opportunity is in. The message never holds contact details."""


@dataclass(frozen=True)
class GhlStage:
    stage_name: str
    pipeline_id: Optional[str]


def _single_attempt(method: str, url: str, **kwargs: Any) -> requests.Response:
    return requests.request(method, url, timeout=_TIMEOUT, **kwargs)


class GhlStageReader:
    """The opportunity's current stage in GHL: ``GET /opportunities/{id}`` (pipelineId, pipelineStageId), named
    through ``GET /opportunities/pipelines``. One attempt per call, never retried here."""

    def __init__(self, account: GhlAccount, request: Optional[Request] = None) -> None:
        self._account = account
        self._request = request or _single_attempt

    def _get(self, what: str, path: str, **kwargs: Any) -> dict[str, Any]:
        try:
            response = self._request("GET", f"{ghl_webhook._GHL_BASE}{path}",
                                     headers=ghl_headers(self._account.api_key), **kwargs)
        except Exception as exc:  # class only: the message can carry request detail
            raise GhlStageReadError(f"GHL {what} request error ({type(exc).__name__})") from None
        if response.status_code >= 400:
            raise GhlStageReadError(f"GHL {what} failed: HTTP {response.status_code}")
        try:
            return response.json() or {}
        except ValueError:
            raise GhlStageReadError(f"GHL {what} returned no JSON") from None

    def __call__(self, opportunity_id: str) -> GhlStage:
        opportunity = self._get("opportunity read", f"/opportunities/{opportunity_id}").get("opportunity") or {}
        pipeline_id, stage_id = opportunity.get("pipelineId"), opportunity.get("pipelineStageId")
        if not pipeline_id or not stage_id:
            raise GhlStageReadError("GHL opportunity has no pipeline stage")
        pipelines = self._get("pipeline list", "/opportunities/pipelines",
                              params={"locationId": self._account.location_id}).get("pipelines") or []
        pipeline = next((p for p in pipelines if p.get("id") == pipeline_id), None) or {}
        stage = next((s for s in pipeline.get("stages") or () if s.get("id") == stage_id), None)
        name = ((stage or {}).get("name") or "").strip()
        if not name:
            raise GhlStageReadError("GHL stage id not found in its pipeline")
        return GhlStage(stage_name=name, pipeline_id=str(pipeline_id))


def default_reader() -> Optional[GhlStageReader]:
    """None until the Next Deal Lending GHL account is configured."""
    account = lending_ghl_account()
    return GhlStageReader(account) if account is not None else None


StageReader = Callable[[str], GhlStage]


# ---- stage event, current stage, loan bridge (moved here from src/api/lending_ghl_router.py) -------------------

# What a GHL merge field becomes when it does not resolve (the workflow's variable is wrong or empty). Saving such
# a value as a stage would hide the mistake: the event would never match "funded" and nobody would be told.
UNRESOLVED_VALUES = frozenset({"null", "none", "undefined", "nan"})

_INSERT_EVENT = text(
    "INSERT INTO lending.ghl_stage_events (ghl_opportunity_id, pipeline_id, stage_name, stage_key, phone, "
    "booked_by, raw_event) VALUES (:opp, :pipe, :stage, :key, :phone, :by, CAST(:raw AS jsonb)) "
    "ON CONFLICT (ghl_opportunity_id, stage_key) DO NOTHING")


def resolved(body: dict[str, Any], name: str) -> Optional[str]:
    """An optional field, or None when GHL sent an unresolved merge field ("null") in its place."""
    value = _field(body, name)
    return None if value is None or value.strip().lower() in UNRESOLVED_VALUES else value


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _insert_stage_event(db, *, opportunity_id: str, stage: str, pipeline_id: Optional[str], phone: Optional[str],
                        booked_by: Optional[str], raw: dict[str, Any]) -> bool:
    """The scoreboard's stage event; False when this (opportunity, stage) was already recorded."""
    return bool(db.execute(_INSERT_EVENT, {
        "opp": opportunity_id, "pipe": pipeline_id, "stage": stage, "key": stage.strip().lower(), "phone": phone,
        "by": booked_by, "raw": json.dumps(raw, default=str)}).rowcount)


def _flag_held_stage(db, opportunity_id: str, stage: str, now: datetime) -> None:
    """A repeated stage event held as a redelivery: logged, and one Cora inbox item, because a real move back made
    within the hour looks the same (GHL sends no event time)."""
    current = current_stage(db, opportunity_id)
    current_name = current[0] if current else "unknown"
    logger.warning("[lending-ghl] repeated stage event held as a redelivery (deal moved stage within the hour)")
    add_to_inbox_safely(
        db, event_type="ghl_stage_repeat_held", dedup_key=f"{opportunity_id}:{stage.strip().lower()}:{now:%Y%m%d%H}",
        summary=f"GHL sent \"{stage}\" again for deal {opportunity_id}, which moved to \"{current_name}\" less than an "
                "hour ago. Treated as a repeat delivery. If the deal really went back, move it in GHL again after "
                "the hour.", payload={"ghl_opportunity_id": opportunity_id, "stage": stage, "current": current_name})


def record_current_stage(db, opportunity_id: str, stage: str, phone: Optional[str], *, repeat: bool) -> None:
    """P2.4 current stage, in a savepoint: a failure is logged and never loses the scoreboard's stage event.
    ``repeat``: the event was already recorded; a redelivery never moves the deal (blockers.upsert_current_stage).

    R1 0.1: for a stage that moves a loan file (Funded / Paid) the failure is re-raised instead. The loan bridge applies
    an event only while it is the deal's current stage, so without this write it would skip a valid update as stale.
    Flag off: the webhook rolls back and answers 500. Journal: the event is saved to the DLQ and replayed."""
    try:
        with db.begin_nested():
            now = _now()
            if not upsert_current_stage(db, opportunity_id=opportunity_id, stage_name=stage, phone=phone, now=now,
                                        repeat=repeat):
                _flag_held_stage(db, opportunity_id, stage, now)
    except Exception as exc:  # class only: SQL errors embed bound parameters
        logger.error("[lending-ghl] current stage not recorded (%s)", type(exc).__name__)
        if stage.strip().lower() in GHL_STAGE_TO_LOAN_STAGE:
            raise


def apply_loan_stage(db, opportunity_id: str, stage: str, phone: Optional[str]) -> None:
    """R1 0.1: a Funded / Paid stage moves the loan file (``ghl_stage_bridge``), in a savepoint: a failure never loses
    the scoreboard's stage event, and the event is saved to ``lending.webhook_failures`` (with an ops alert) and
    replayed every 5 minutes until the file takes it. A refused move is stored and carded by the state machine.
    If the retry cannot be saved either, the error is re-raised."""
    try:
        with db.begin_nested():
            outcome = apply_stage(db, opportunity_id=opportunity_id, stage_name=stage, phone=phone)
        if outcome:
            logger.info("[lending-ghl] loan stage from GHL: %s", outcome)
    except Exception as exc:
        logger.error("[lending-ghl] loan stage not applied (%s); saving it for retry", type(exc).__name__)
        saved = report_webhook_failure(GHL_LOAN_STAGE,
                                       {"opportunity_id": opportunity_id, "stage_name": stage, "phone": phone}, exc)
        if saved is None:  # not saved for retry either: fail so the event is delivered / replayed again
            raise


def _stage_fields(body: dict[str, Any]) -> tuple[Optional[str], Optional[str]]:
    return _field(body, "opportunity_id") or _field(body, "id"), _field(body, "stage_name")


def _store_event(db, body: dict[str, Any], opportunity_id: str, stage: str) -> tuple[bool, Optional[str]]:
    phone = normalize(resolved(body, "phone"))
    inserted = _insert_stage_event(db, opportunity_id=opportunity_id, stage=stage,
                                   pipeline_id=_field(body, "pipeline_id"), phone=phone,
                                   booked_by=resolved(body, "booked_by"), raw=body)
    return inserted, phone


def record_stage_event(db, body: dict[str, Any], opportunity_id: str, stage: str) -> bool:
    """The pre-journal webhook path (flag off), unchanged: event, current stage from the body, loan bridge. The
    caller commits. True when the event was new."""
    inserted, phone = _store_event(db, body, opportunity_id, stage)
    record_current_stage(db, opportunity_id, stage, phone, repeat=not inserted)
    apply_loan_stage(db, opportunity_id, stage, phone)
    return inserted


# ---- journal handler -------------------------------------------------------------------------------------------

def dedup_key(opportunity_id: str, stage: str, now: Optional[datetime] = None) -> str:
    """D5: ``<opportunity>:<stage key>:<UTC YYYYMMDDHH>`` (the existing 1 h redelivery window)."""
    return f"{opportunity_id}:{stage.strip().lower()}:{(now or _now()).astimezone(timezone.utc):%Y%m%d%H}"


def _read_ghl(opportunity_id: str, reader: Optional[StageReader]) -> Optional[GhlStage]:
    """GHL's current stage, or None (no account, or the read failed: class only in the log)."""
    if reader is None:
        return None
    try:
        return reader(opportunity_id)
    except Exception as exc:
        logger.warning("[lending-ghl] GHL stage read failed for opportunity %s (%s); using the webhook's stage",
                       opportunity_id, type(exc).__name__)
        return None


_DEFAULT_READER: Any = object()


def handle_stage(db, raw: dict[str, Any], *, reader: Any = _DEFAULT_READER) -> dict[str, Any]:
    """Idempotent handler for journal source ``ghl_stage`` (webhook bodies and T7 reconcile payloads). Runs inside
    the journal's transaction and never commits. ``reader``: GHL stage reader (default: the lending account's;
    None = no read). Returns ``{"recorded": <event was new>, "stage": <stage recorded as current>}``."""
    opportunity_id, stage = _stage_fields(raw)
    if not opportunity_id or not stage:
        raise ValueError("ghl_stage payload has no opportunity_id / stage_name")
    inserted, phone = _store_event(db, raw, opportunity_id, stage)
    if raw.get("source") == RECONCILE:
        current, repeat = stage, False  # the reconcile read GHL itself
    else:
        ghl = _read_ghl(opportunity_id, default_reader() if reader is _DEFAULT_READER else reader)
        if ghl is None:
            current, repeat = stage, not inserted  # today's redelivery hold
        else:
            current, repeat = ghl.stage_name, False  # GHL's own stage: never a redelivery
            if current.strip().lower() != stage.strip().lower():
                logger.info("[lending-ghl] opportunity %s: GHL's current stage differs from the webhook's; "
                            "recording GHL's", opportunity_id)
    record_current_stage(db, opportunity_id, current, phone, repeat=repeat)
    apply_loan_stage(db, opportunity_id, current, phone)
    return {"recorded": inserted, "stage": current}


journal.register_source(SOURCE, handle_stage, label="GHL stage change")
