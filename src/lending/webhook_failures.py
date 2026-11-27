"""Never lose an opt-out to a failed GHL webhook call.

When the opt-out or consent webhook fails after GHL delivered it (most often because the database is
unavailable), ``report_webhook_failure`` alerts the ops channel and saves the payload. The Slack alert does not
need the database, so it still goes out when the save fails too; it then says the event must be replayed by
hand. ``retry_pending`` (cron, every 5 minutes) replays saved payloads through the same handlers the webhooks
use, until they succeed or MAX_RETRY_ATTEMPTS is reached, when ops is alerted once more. A replayed consent
grant never overrides a newer refusal: if the number's consent was revoked after the grant first failed, the
grant is closed as superseded instead of applied.

Alerts carry the GHL contact id, the endpoint and the error class: never a phone number or message text.
Keyword STOPs are also covered by the 15-minute GHL DND backstop; free-text opt-outs are covered only here.

The same cron also runs ``src.lending.inbound.journal.reclaim_stuck`` (R1 0.5) while LENDING_INBOUND_JOURNAL_ENABLED.
Sources registered through the inbound journal add their own handler, label and resend hint to the dicts below.

Usage (cron, every 5 minutes):
    python -m src.lending.webhook_failures
"""
from __future__ import annotations

import json
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Optional

from sqlalchemy import text

from src.lending.after_hours_intake import record_intake
from src.lending.cora_inbox import add_to_inbox_in_own_session
from src.lending.db import lending_session
from src.lending.ghl_stage_bridge import apply_stage_payload
from src.lending.ghl_consent_events import (
    InvalidEvent,
    contact_id_of,
    granted_phone,
    record_ghl_opt_out,
    record_inbound_consent,
)
from src.lending.ops_alerts import post_ops_notice

logger = logging.getLogger(__name__)

GHL_OPT_OUT = "ghl_opt_out"
GHL_TEXT_CONSENT = "ghl_text_consent"
AFTER_HOURS_INTAKE = "after_hours_intake"
GHL_LOAN_STAGE = "ghl_loan_stage"  # R1 0.1: a GHL Funded / Paid move the loan file did not take
ENDPOINT_HANDLERS: dict[str, Callable[[Any, dict[str, Any]], Any]] = {
    GHL_LOAN_STAGE: apply_stage_payload,  # the replay key carries the file's stage, so it applies once
    GHL_OPT_OUT: record_ghl_opt_out,
    GHL_TEXT_CONSENT: record_inbound_consent,
    AFTER_HOURS_INTAKE: record_intake,  # a replay keeps the original call id, so it never pins twice
}
ENDPOINT_LABELS = {GHL_OPT_OUT: "GHL opt-out (DND changed)", GHL_TEXT_CONSENT: "GHL inbound text / consent",
                   AFTER_HOURS_INTAKE: "GHL after-hours voicemail", GHL_LOAN_STAGE: "GHL loan stage (Funded / Paid)"}
# What ops does by hand when an event cannot be retried; sources registered by the inbound journal add their own.
DEFAULT_RESEND_HINT = "re-run the GHL workflow for this contact"
ENDPOINT_RESEND_HINTS: dict[str, str] = {}
# Run in the replay's savepoint after a handler succeeds (the inbound journal marks its row processed here).
ON_RESOLVED: dict[str, Callable[[Any, int], None]] = {}
MAX_RETRY_ATTEMPTS = 5
RETRY_BATCH_SIZE = 50

_INSERT = text(
    "INSERT INTO lending.webhook_failures (endpoint, payload, ghl_contact_id, last_error, attempts, last_attempt_at) "
    "VALUES (:endpoint, CAST(:payload AS jsonb), :contact, :error, 1, now()) RETURNING id"
)
_OPEN = text(
    "SELECT id, endpoint, payload, ghl_contact_id, attempts, created_at FROM lending.webhook_failures "
    "WHERE resolved_at IS NULL AND gave_up_at IS NULL ORDER BY id LIMIT :limit FOR UPDATE SKIP LOCKED"
)
_RESOLVED = text("UPDATE lending.webhook_failures SET resolved_at = now(), last_attempt_at = now() WHERE id = :id")
# A consent grant is older than any revocation made after it failed: replaying it would undo a newer refusal.
_REVOKED_SINCE = text(
    "SELECT EXISTS (SELECT 1 FROM lending.text_consents WHERE phone = :phone AND revoked_at >= :since)"
)
_SUPERSEDED = text(
    "UPDATE lending.webhook_failures SET resolved_at = now(), last_attempt_at = now(), "
    "last_error = 'superseded_by_newer_revocation' WHERE id = :id"
)
_FAILED_AGAIN = text(
    "UPDATE lending.webhook_failures SET attempts = :attempts, last_error = :error, last_attempt_at = now(), "
    "gave_up_at = CASE WHEN :give_up THEN now() ELSE NULL END WHERE id = :id"
)


def _label(endpoint: str) -> str:
    return ENDPOINT_LABELS.get(endpoint, endpoint)


def _resend_hint(endpoint: str) -> str:
    return ENDPOINT_RESEND_HINTS.get(endpoint, DEFAULT_RESEND_HINT)


def save_failure(endpoint: str, body: dict[str, Any], error: str) -> int:
    """Store the payload for replay on a connection of its own (the request's transaction has failed)."""
    with lending_session() as db:
        return insert_failure(db, endpoint, body, error)


def insert_failure(db, endpoint: str, body: dict[str, Any], error: str) -> int:
    """Insert a retry row in the caller's transaction."""
    return int(db.execute(_INSERT, {"endpoint": endpoint, "payload": json.dumps(body),
                                    "contact": contact_id_of(body), "error": error}).scalar_one())



def report_webhook_failure(endpoint: str, body: dict[str, Any], error: BaseException, *,
                           save: Callable[[str, dict[str, Any], str], int] = save_failure,
                           notify: Callable[[str], bool] = post_ops_notice) -> Optional[int]:
    """Save the failed event for retry and alert ops. Returns the saved id, or None when it could not be saved.
    Never raises: the webhook still answers GHL with its own error."""
    error_name = type(error).__name__
    contact = contact_id_of(body) or "unknown"
    try:
        failure_id: Optional[int] = save(endpoint, body, error_name)
    except Exception as exc:  # class only: the payload carries phone numbers
        logger.error("[lending-webhook-failure] %s could not be saved for retry: %s", endpoint, type(exc).__name__)
        failure_id = None
    if failure_id is not None:
        message = (f":warning: {_label(endpoint)} webhook failed ({error_name}) for contact {contact}. "
                   f"Saved as #{failure_id}; it is retried automatically every 5 minutes.")
    else:
        message = (f":rotating_light: {_label(endpoint)} webhook failed ({error_name}) for contact {contact}, "
                   f"and it could not be saved for retry. This event is NOT recorded: {_resend_hint(endpoint)} "
                   "once the database is healthy.")
    logger.error("[lending-webhook-failure] %s failed for contact %s: %s (saved=%s)", endpoint, contact, error_name,
                 failure_id)
    notify(message)
    return failure_id


@dataclass
class RetryResult:
    resolved: int = 0
    superseded: int = 0
    still_failing: int = 0
    gave_up: int = 0
    alerts: list[str] = field(default_factory=list)
    gave_up_events: list[dict[str, Any]] = field(default_factory=list)  # one per alert, raised in Cora's inbox


def _superseded_grant(db, endpoint: str, payload: dict[str, Any], failed_at) -> bool:
    """True for a consent grant whose number had its consent revoked after this event first failed. Only grants
    are checked: an opt-out or a decline is always safe to replay, since it can only narrow permission."""
    if endpoint != GHL_TEXT_CONSENT:
        return False
    phone = granted_phone(payload)
    return bool(phone and db.execute(_REVOKED_SINCE, {"phone": phone, "since": failed_at}).scalar())


def _retry_one(db, row, result: RetryResult) -> None:
    handler = ENDPOINT_HANDLERS.get(row.endpoint)
    payload = row.payload if isinstance(row.payload, dict) else json.loads(row.payload)
    if _superseded_grant(db, row.endpoint, payload, row.created_at):
        db.execute(_SUPERSEDED, {"id": row.id})
        result.superseded += 1
        logger.info("[lending-webhook-failure] #%s not replayed: consent was revoked after it failed", row.id)
        return
    try:
        with db.begin_nested():
            if handler is None:
                raise InvalidEvent(f"no handler for endpoint {row.endpoint}")
            handler(db, payload)
            db.execute(_RESOLVED, {"id": row.id})
            on_resolved = ON_RESOLVED.get(row.endpoint)
            if on_resolved is not None:
                on_resolved(db, row.id)
        result.resolved += 1
        logger.info("[lending-webhook-failure] #%s replayed (%s)", row.id, row.endpoint)
        return
    except InvalidEvent as exc:
        error, give_up = type(exc).__name__, True
    except Exception as exc:  # class only: SQL errors embed bound parameters (phones)
        error = type(exc).__name__
        give_up = row.attempts + 1 >= MAX_RETRY_ATTEMPTS
    attempts = row.attempts + 1
    db.execute(_FAILED_AGAIN, {"id": row.id, "attempts": attempts, "error": error, "give_up": give_up})
    if give_up:
        result.gave_up += 1
        result.alerts.append(
            f":rotating_light: {_label(row.endpoint)} webhook #{row.id} for contact "
            f"{row.ghl_contact_id or 'unknown'} still fails after {attempts} attempt(s) ({error}). It is no longer "
            f"retried: this event is NOT recorded. Check it and {_resend_hint(row.endpoint)}.")
        result.gave_up_events.append({"webhook_failure_id": row.id, "endpoint": row.endpoint,
                                      "ghl_contact_id": row.ghl_contact_id, "attempts": attempts, "error": error})
    else:
        result.still_failing += 1
        logger.warning("[lending-webhook-failure] #%s retry %s failed: %s", row.id, attempts, error)


def replay_one(db, row) -> RetryResult:
    """Run one saved failure once now, exactly as the sweep would (the DLQ replay CLI)."""
    result = RetryResult()
    _retry_one(db, row, result)
    return result


def retry_pending(*, session_factory: Callable = lending_session,
                  notify: Callable[[str], bool] = post_ops_notice) -> RetryResult:
    """Replay every open failure once, each in its own savepoint; alerts go out after the commit."""
    result = RetryResult()
    with session_factory() as db:
        for row in db.execute(_OPEN, {"limit": RETRY_BATCH_SIZE}).all():
            _retry_one(db, row, result)
    for message, event in zip(result.alerts, result.gave_up_events):
        posted = notify(message)
        add_to_inbox_in_own_session(
            event_type="consent_webhook_gave_up", dedup_key=str(event["webhook_failure_id"]), target_channel="ops_notice",
            channel_posted=bool(posted), payload=event,
            summary=f"{_label(event['endpoint'])} webhook no longer retried: the change is NOT recorded",
        )
    return result


def main() -> int:
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    from src.lending.inbound import journal

    try:
        journal.load_handlers()
        result = retry_pending()
    except Exception as exc:  # class only
        logger.error("[lending-webhook-failure] retry sweep failed: %s", type(exc).__name__)
        return 1
    if result.resolved or result.superseded or result.still_failing or result.gave_up:
        logger.info("[lending-webhook-failure] resolved=%d superseded=%d still_failing=%d gave_up=%d",
                    result.resolved, result.superseded, result.still_failing, result.gave_up)
    return _reclaim_inbound_journal()


def _reclaim_inbound_journal() -> int:
    """R1 0.5: run inbound journal rows a killed worker left unprocessed (only while the journal is on)."""
    from src.lending.inbound import journal  # the journal imports this module

    if not journal.journal_enabled():
        return 0
    try:
        reclaimed = journal.reclaim_stuck()
    except Exception as exc:  # class only
        logger.error("[lending-webhook-failure] inbound journal reclaim failed: %s", type(exc).__name__)
        return 1
    if reclaimed.processed or reclaimed.failed or reclaimed.skipped:
        logger.info("[lending-webhook-failure] journal reclaimed processed=%d failed=%d skipped=%d",
                    reclaimed.processed, reclaimed.failed, reclaimed.skipped)
    return 0


if __name__ == "__main__":
    from src.lending import webhook_failures

    raise SystemExit(webhook_failures.main())
