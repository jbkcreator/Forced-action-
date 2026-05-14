"""
Unified webhook audit logger.

One public function:

    log_webhook_event(
        source, event_type, direction="inbound",
        source_event_id=None, status="received", status_detail=None,
        subscriber_id=None, property_id=None,
        payload=None, payload_kind=None, duration_ms=None,
        db=None,
    )

Writes a `webhook_events` row best-effort. Failures are swallowed and
logged at WARNING level — a logging error must NEVER block the actual
webhook handler that called this.

Sanitization
------------
Callers may pass either:
  - A pre-built `payload_summary` dict via `payload=` (used as-is)
  - A raw vendor payload via `payload=...` AND `payload_kind=<source>`
    in which case the per-source sanitizer extracts safe fields only.

We do not persist raw payloads. PII (emails, message bodies, payment
details, addresses) is never written to this table.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public entrypoint
# ---------------------------------------------------------------------------

def log_webhook_event(
    source: str,
    event_type: str,
    direction: str = "inbound",
    source_event_id: Optional[str] = None,
    status: str = "received",
    status_detail: Optional[str] = None,
    subscriber_id: Optional[int] = None,
    property_id: Optional[int] = None,
    payload: Optional[Dict[str, Any]] = None,
    payload_kind: Optional[str] = None,
    duration_ms: Optional[int] = None,
    db: Optional[Session] = None,
) -> None:
    """
    Best-effort audit-log write. Never raises.

    If `db` is provided, the row is added to that session (the session's
    surrounding transaction commits it). If `db` is None, opens its own
    short-lived session so the audit row is durable even if the caller
    has no session in scope (e.g. mid-failure paths).
    """
    try:
        summary = _sanitize(payload_kind or source, payload)

        from src.core.models import WebhookEvent  # local import — avoid cycles

        row = WebhookEvent(
            source=source[:30],
            event_type=(event_type or "unknown")[:80],
            direction=direction if direction in ("inbound", "outbound") else "inbound",
            source_event_id=(source_event_id or None) if source_event_id is None else str(source_event_id)[:120],
            status=status if status in ("received", "processed", "failed", "duplicate", "skipped") else "received",
            status_detail=(status_detail or None) if status_detail is None else str(status_detail)[:2000],
            subscriber_id=subscriber_id,
            property_id=property_id,
            payload_summary=summary,
            duration_ms=duration_ms,
            processed_at=datetime.now(timezone.utc),
        )

        if db is not None:
            db.add(row)
            return

        # No caller session — open and commit our own
        from src.core.database import Database
        own_db = Database()
        with own_db.session_scope() as session:
            session.add(row)

    except Exception as exc:
        # Audit failures must NEVER break webhook handlers
        logger.warning(
            "[webhook_log] failed to persist event source=%s type=%s: %s",
            source, event_type, exc,
        )


# ---------------------------------------------------------------------------
# Sanitizers — per source, allowlist style. Never log raw payload.
# ---------------------------------------------------------------------------

def _sanitize(kind: Optional[str], payload: Optional[Dict[str, Any]]) -> Optional[dict]:
    if payload is None:
        return None
    if not isinstance(payload, dict):
        return {"_note": "non-dict payload omitted", "_type": type(payload).__name__}

    fn = _SANITIZERS.get((kind or "").lower())
    if fn is None:
        return _generic_summary(payload)
    try:
        return fn(payload)
    except Exception as exc:
        logger.debug("[webhook_log] sanitizer for %s raised %s — using generic", kind, exc)
        return _generic_summary(payload)


def _generic_summary(payload: Dict[str, Any]) -> dict:
    """Default: only top-level keys + their types. No values."""
    return {
        "_kind":   "generic",
        "fields":  sorted(list(payload.keys()))[:30],
        "size":    len(payload),
    }


def _stripe(payload: Dict[str, Any]) -> dict:
    """Stripe Event object — keep id/type/customer/livemode only."""
    obj = (payload.get("data") or {}).get("object") or {}
    return {
        "id":       payload.get("id"),
        "type":     payload.get("type"),
        "livemode": bool(payload.get("livemode")),
        "object":   obj.get("object"),               # e.g. "checkout.session"
        "customer": obj.get("customer"),             # cus_xxx, opaque ID
        "amount":   obj.get("amount") or obj.get("amount_total"),
        "currency": obj.get("currency"),
        "status":   obj.get("status"),
    }


def _ghl(payload: Dict[str, Any]) -> dict:
    """GHL webhook — keep ids and event labels, drop name/email/phone."""
    return {
        "type":        payload.get("type") or payload.get("event"),
        "contact_id":  payload.get("contactId") or payload.get("contact_id"),
        "location_id": payload.get("locationId") or payload.get("location_id"),
        "workflow_id": payload.get("workflowId"),
        "campaign_id": payload.get("campaignId"),
    }


def _synthflow(payload: Dict[str, Any]) -> dict:
    return {
        "event":          payload.get("event") or payload.get("event_type"),
        "call_id":        payload.get("call_id") or payload.get("id"),
        "agent_id":       payload.get("agent_id"),
        "status":         payload.get("status"),
        "duration_sec":   payload.get("duration_sec") or payload.get("duration"),
        "outcome":        payload.get("outcome"),
    }


def _batch_data(payload: Dict[str, Any]) -> dict:
    """BatchData skip-trace polling result — counts only, no contacts."""
    return {
        "batch_id":           payload.get("batch_id") or payload.get("id"),
        "request_count":      payload.get("request_count"),
        "match_count":        payload.get("match_count") or payload.get("matched"),
        "phone_count":        payload.get("phone_count"),
        "email_count":        payload.get("email_count"),
        "status":             payload.get("status"),
    }


def _nws(payload: Dict[str, Any]) -> dict:
    """NWS alert — keep alert id + event + area, drop description blobs."""
    props = (payload.get("properties") or payload).copy() if isinstance(payload.get("properties"), dict) else payload
    return {
        "alert_id":   props.get("id") or props.get("alert_id") or payload.get("id") or payload.get("alert_id"),
        "event":      props.get("event"),
        "severity":   props.get("severity"),
        "area_desc":  (props.get("areaDesc") or "")[:120],
        "sent":       props.get("sent"),
        "effective":  props.get("effective"),
    }


def _telnyx(payload: Dict[str, Any]) -> dict:
    """
    Telnyx SMS + voice webhook — keep event/message ids + carrier/status.
    Never log full message body. Phone numbers are kept only as last-4
    digits so the audit row is still useful for correlation but not PII.
    """
    data = (payload.get("data") or {})
    inner = (data.get("payload") or {})
    from_phone = (inner.get("from") or {}).get("phone_number") or ""
    to_phone   = (inner.get("to")   or {}).get("phone_number") or ""

    def _last4(p: str) -> str:
        digits = "".join(c for c in p if c.isdigit())
        return f"***{digits[-4:]}" if len(digits) >= 4 else "***"

    return {
        "event_id":   data.get("id") or payload.get("id"),
        "event_type": data.get("event_type"),
        "record_type": inner.get("record_type"),
        "message_id": inner.get("id"),  # for SMS events
        "call_control_id": inner.get("call_control_id"),  # for voice events
        "direction": inner.get("direction"),
        "status":    inner.get("status"),
        "from_last4": _last4(from_phone) if from_phone else None,
        "to_last4":   _last4(to_phone)   if to_phone   else None,
        "encoding":  inner.get("encoding"),
        "parts":     inner.get("parts"),
        # Note: text / media / full phone intentionally omitted (PII / message body)
    }


_SANITIZERS = {
    "stripe":          _stripe,
    "ghl":             _ghl,
    "ghl_inbound":     _ghl,
    "ghl_outbound":    _ghl,
    "synthflow":       _synthflow,
    "batch_data":      _batch_data,
    "batchdata":       _batch_data,
    "nws":             _nws,
    "telnyx":          _telnyx,
    "telnyx_inbound":  _telnyx,
    "telnyx_voice":    _telnyx,
}
