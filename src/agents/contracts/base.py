"""
Shared plumbing for QUALITY-v2.2 Q3's four handoff contracts (Hunter->Cora,
Cora->Relay, Vera->Dev, Dev->Vera).

Every boundary module in this package defines its own Pydantic model and its
own validate/check function, but all four share:

  - HandoffRejected: the exception a boundary's validator raises (or a
    HandoffCheckResult a boundary's checker returns) when a required field
    is missing or invalid.
  - record_rejection(): writes an audit row to handoff_rejections so a
    rejected handoff is diagnosable after the fact, not just logged and lost.
  - notify_slack_rejection(): posts to Slack per decision D2 ("auto-reject
    triggers a Slack message to Josh") -- mirrors the existing WebClient
    pattern in src/services/relay/slack_post.py, deliberately with NO
    interactive buttons: an auto-reject is informational (something already
    happened and was blocked), not a decision Josh taps to make.
  - check_against_model(): a reusable "build this Pydantic model from a dict,
    tell me if it built and what's missing if not" helper for the two
    boundaries (Vera->Dev, Dev->Vera) that check a plain dict rather than
    raising synchronously inside a live code seam (contrast with
    Hunter->Cora and Cora->Relay, which DO raise synchronously because they
    sit inline in a real pipeline that must stop before doing anything with
    bad data).
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Optional, Type

from pydantic import BaseModel, ValidationError
from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

from config.settings import get_settings

logger = logging.getLogger(__name__)

BOUNDARIES = frozenset({"hunter_to_cora", "cora_to_relay", "vera_to_dev", "dev_to_vera"})


class HandoffRejected(Exception):
    """Raised (or wrapped and returned) when a handoff fails its
    required-fields contract at one of the four boundaries.

    boundary: one of BOUNDARIES.
    missing_fields: human-readable field:reason strings describing what
        failed -- not just bare field names, so the Slack message and the
        audit row are self-explanatory without re-running the validator.
    reference_id: whatever identifies the rejected item for this boundary
        (opportunity_thread_id, idempotency_key, a finding's issue title,
        a PR's commit hash...).
    """

    def __init__(self, boundary: str, missing_fields: list[str], reference_id: Optional[str] = None):
        if boundary not in BOUNDARIES:
            raise ValueError(f"HandoffRejected: unknown boundary={boundary!r} (allowed: {sorted(BOUNDARIES)})")
        self.boundary = boundary
        self.missing_fields = missing_fields
        self.reference_id = reference_id
        super().__init__(
            f"handoff rejected at boundary={boundary!r} reference_id={reference_id!r}: "
            f"missing/invalid fields={missing_fields}"
        )


def record_rejection(
    session: Session,
    *,
    boundary: str,
    missing_fields: list[str],
    reference_id: Optional[str],
    payload_snapshot: dict[str, Any],
) -> int:
    """Writes one handoff_rejections row in the caller's transaction.
    Returns the new row id."""
    row = session.execute(
        sa_text("""
            INSERT INTO handoff_rejections
                (boundary, reference_id, missing_fields, payload_snapshot)
            VALUES
                (:boundary, :reference_id, CAST(:missing_fields AS jsonb), CAST(:payload_snapshot AS jsonb))
            RETURNING id
        """),
        {
            "boundary": boundary,
            "reference_id": reference_id,
            "missing_fields": json.dumps(missing_fields),
            "payload_snapshot": json.dumps(payload_snapshot, default=str),
        },
    ).fetchone()
    return row.id


def notify_slack_rejection(exc: HandoffRejected) -> None:
    """Per decision D2: auto-reject triggers a Slack message to Josh.

    No-ops (logs and returns) if Slack isn't configured -- same convention
    as src/services/relay/slack_post.py:post_for_approval -- so this stays
    usable in local/dev/test environments without a live Slack app.
    """
    settings = get_settings()
    token = settings.slack_bot_token
    channel = settings.quality_contracts_slack_channel
    if not token or not channel:
        logger.info(
            "[QUALITY-Q3] Slack not configured (quality_contracts_slack_channel/"
            "slack_bot_token unset) — rejection at boundary=%s reference_id=%s logged only",
            exc.boundary, exc.reference_id,
        )
        return
    try:
        from slack_sdk import WebClient

        client = WebClient(token=token.get_secret_value())
        client.chat_postMessage(
            channel=channel,
            text=(
                f"*Handoff rejected* — `{exc.boundary}`\n"
                f"Reference: `{exc.reference_id}`\n"
                f"Missing/invalid: {', '.join(exc.missing_fields)}"
            ),
        )
    except Exception as slack_exc:  # noqa: BLE001
        logger.error(
            "[QUALITY-Q3] Slack post failed for boundary=%s reference_id=%s: %s",
            exc.boundary, exc.reference_id, slack_exc, exc_info=True,
        )


def reject_and_notify(
    session: Session,
    *,
    boundary: str,
    missing_fields: list[str],
    reference_id: Optional[str],
    payload_snapshot: dict[str, Any],
) -> HandoffRejected:
    """Records the rejection row, posts Slack, and returns the exception for
    the caller to raise or just log -- callers decide their own control flow
    (Cora->Relay's enqueue() raises; Hunter->Cora's target_producer.py logs
    and skips the one bad row rather than crashing the whole sweep).

    Deduplicates within 24 hours: if the same reference_id+boundary was
    already rejected recently, skips both the DB insert and the Slack post so
    a periodic sweep doesn't flood the channel with the same rejection."""
    already = session.execute(
        sa_text(
            "SELECT id FROM handoff_rejections "
            "WHERE boundary = :boundary AND reference_id = :reference_id "
            "AND rejected_at >= NOW() - INTERVAL '24 hours' "
            "LIMIT 1"
        ),
        {"boundary": boundary, "reference_id": reference_id},
    ).first()
    exc = HandoffRejected(boundary, missing_fields, reference_id)
    if already:
        return exc
    record_rejection(
        session, boundary=boundary, missing_fields=missing_fields,
        reference_id=reference_id, payload_snapshot=payload_snapshot,
    )
    notify_slack_rejection(exc)
    return exc


@dataclass
class HandoffCheckResult:
    ok: bool
    missing_fields: list[str] = field(default_factory=list)
    model: Optional[BaseModel] = None


def check_against_model(model_cls: Type[BaseModel], data: dict[str, Any]) -> HandoffCheckResult:
    """Builds model_cls(**data); returns a HandoffCheckResult instead of
    letting pydantic.ValidationError propagate. Used by the two boundaries
    (Vera->Dev, Dev->Vera) whose 'contract' is a plain dict checked against
    a template, not a live pipeline seam that must abort synchronously."""
    try:
        instance = model_cls(**data)
        return HandoffCheckResult(ok=True, missing_fields=[], model=instance)
    except ValidationError as exc:
        missing = [f"{'.'.join(str(p) for p in e['loc']) or '<model>'}: {e['msg']}" for e in exc.errors()]
        return HandoffCheckResult(ok=False, missing_fields=missing, model=None)
