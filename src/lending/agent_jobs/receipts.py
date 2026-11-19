"""The receipt Cora writes to "NDL Jobs/done/{job_id}.json" (brief section 6), plus decisions D8 and D15.

Spec fields: status, action_id, provider_message_id, edited_body, timestamp. Added: job_id, rail, reason,
template_version, created_by. No phone number or email address is ever written. For an Instantly email, ``sent``
means Instantly accepted it for its next sending slot (``rail`` says which).
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping, Optional
from zoneinfo import ZoneInfo

from config.lending_compliance import DEFAULT_TZ
from packages.agent_core.pending_actions import ActionStatus, PendingAction

_OPEN = frozenset({ActionStatus.PENDING, ActionStatus.REVISING, ActionStatus.APPROVED, ActionStatus.SENDING})
_CONSENT_WORDS = ("consent", "suppressed", "do-not-contact", "opt", "do-not-disturb")
_EDIT_FIELDS = ("body", "stage", "tag", "title")


def build_receipt(row: Mapping[str, Any], status: str, *, now: datetime, action_id: Optional[int] = None,
                  provider_message_id: Optional[str] = None, edited_body: Optional[str] = None,
                  reason: Optional[str] = None) -> dict[str, Any]:
    return {"job_id": row["job_id"], "status": status, "action_id": action_id,
            "provider_message_id": provider_message_id, "edited_body": edited_body, "rail": row.get("rail"),
            "reason": reason, "template_version": row.get("template_version"), "created_by": row.get("created_by"),
            "timestamp": now.astimezone(ZoneInfo(DEFAULT_TZ)).isoformat(timespec="seconds")}


def _edited_body(action: PendingAction) -> Optional[str]:
    if not action.revisions:
        return None
    payload = action.payload
    main = next((payload[key] for key in _EDIT_FIELDS if isinstance(payload.get(key), str)), None)
    if payload.get("subject") and main is not None:
        return f"{payload['subject']}\n\n{main}"
    return main


def _blocked_status(reason: str) -> str:
    lowered = reason.lower()
    if "expired" in lowered:
        return "expired"
    if any(word in lowered for word in _CONSENT_WORDS):
        return "blocked_consent"
    return "error"


def receipt_for(row: Mapping[str, Any], action: Optional[PendingAction], *, now: datetime) -> Optional[dict[str, Any]]:
    """The receipt for a carded job, or None while its draft is still open."""
    if action is None:
        return build_receipt(row, "error", now=now, reason="the approval draft is missing")
    if action.status in _OPEN:
        return None
    common = {"now": now, "action_id": action.action_id}
    if action.status is ActionStatus.SENT:
        edited = _edited_body(action)
        return build_receipt(row, "edited_sent" if edited is not None else "sent", provider_message_id=action.provider_ref,
                             edited_body=edited, **common)
    if action.status is ActionStatus.REJECTED:
        return build_receipt(row, "rejected", **common)
    if action.status is ActionStatus.EXPIRED:
        return build_receipt(row, "expired", reason="no decision before the job expired", **common)
    if action.status is ActionStatus.BLOCKED:
        return build_receipt(row, _blocked_status(action.error or ""), reason=action.error, **common)
    return build_receipt(row, "error", reason=action.error or "the send failed", **common)
