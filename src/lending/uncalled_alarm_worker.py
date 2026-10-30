"""T-10: alarm Josh when a LendingFlow or website lead has not been reached by phone 2 and 5 minutes after it arrived.

    python -m src.lending.uncalled_alarm_worker            # loop
    python -m src.lending.uncalled_alarm_worker --once     # one cycle and exit

Each cycle copies every new, non-suppressed row of T-11's ``lending.lendingflow_leads`` and of
``lending.web_leads`` (the nextdeallending.com form, client Q53) into
``lending.uncalled_alarms`` (the stopwatch starts at the lead's ``received_at``; no age limit), then decides the
due ones. Outbound calls to the lead's phone since arrival are classified from BatchDialer CDRs
(``lending.call_dispositions``) and the lead's GHL call messages: connected -> closed, nothing sent; attempted ->
"NOT REACHED" alarms; none -> "no call" alarms. At 120 s an SMS to Josh; at 300 s an URGENT SMS plus a Slack post
to #lead-alerts. SMS go through GoHighLevel (``ghl_sms.GhlSmsSender``, single attempt).

Calls are only visible once they END, so a long call that starts before 2:00 still alarms; the wording says so.
A GHL lookup that fails counts as GHL finding nothing: a false alarm is better than a silent miss.
Each alarm is claimed (committed) before it is sent. Delivery has three outcomes per channel: sent; definitely not
sent (GHL contact/4xx rejection, Slack API rejection), retried up to MAX_SEND_ATTEMPTS times with short waits, then
'failed' with an ERROR log; and possibly sent (timeout, 5xx, unexpected error), which is never resent. A 2-minute
retry is dropped when the escalation fires or the lead has been reached. Logs carry lead ids only.
"""
from __future__ import annotations

import argparse
import logging
import signal
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Mapping, Optional

from slack_sdk.errors import SlackApiError
from sqlalchemy import text

from config.lending_alarms import (
    BATCH_SIZE,
    ESCALATION_SECONDS,
    FIRST_ALARM_SECONDS,
    GHL_CONNECTED_STATUSES,
    KIND_NO_CALL,
    KIND_NOT_REACHED,
    MAX_SEND_ATTEMPTS,
    NOT_CONNECTED_CODES,
    POLL_SECONDS,
    RETRY_DELAYS_SECONDS,
    RETRY_WINDOW_SECONDS,
    SLACK_REJECTED_ERRORS,
    SLACK_TEXT,
    SMS_TEXT,
    STALE_SEND_SECONDS,
)
from config.settings import get_settings
from src.lending.cora_inbox import add_to_inbox_safely
from src.lending.db import lending_session
from src.lending.ghl_calls import call_messages_reader, call_state, message_time
from src.lending.ghl_sms import GhlSmsError
from src.lending.slack_names import name_and_ref
from src.services.phone_utils import normalize

logger = logging.getLogger(__name__)

CONNECTED, ATTEMPTED, NONE = "connected", "attempted", "none"
GhlCallStatus = Callable[[str, datetime], str]  # (ghl_contact_id, since) -> CONNECTED / ATTEMPTED / NONE
Clock = Callable[[], datetime]

_PICKUP = text("""
    INSERT INTO lending.uncalled_alarms (lendingflow_lead_id, lead_uuid, phone, arrived_at)
    SELECT l.id, l.lead_uuid::text, l.phone, l.received_at FROM lending.lendingflow_leads l
     WHERE NOT l.suppressed
       AND NOT EXISTS (SELECT 1 FROM lending.uncalled_alarms a WHERE a.lendingflow_lead_id = l.id)
    ON CONFLICT (lendingflow_lead_id) DO NOTHING
""")


_PICKUP_WEB = text("""
    INSERT INTO lending.uncalled_alarms (web_lead_id, lead_uuid, phone, arrived_at)
    SELECT w.id, 'web-' || w.id, w.phone, w.received_at FROM lending.web_leads w
     WHERE NOT w.suppressed
       AND NOT EXISTS (SELECT 1 FROM lending.uncalled_alarms a WHERE a.web_lead_id = w.id)
    ON CONFLICT (web_lead_id) DO NOTHING
""")

# Lead facts for the message and the GHL call lookup, from whichever source the alarm was copied from.
_LEAD_FIELDS = """
           COALESCE(l.first_name, NULLIF(split_part(btrim(w.name), ' ', 1), '')) AS first_name,
           COALESCE(NULLIF(btrim(concat_ws(' ', l.first_name, l.last_name)), ''), NULLIF(btrim(w.name), '')) AS full_name,
           COALESCE(l.property_state, w.property_state) AS property_state,
           COALESCE(l.ghl_contact_id, w.ghl_contact_id) AS ghl_contact_id,
           (a.web_lead_id IS NOT NULL) AS from_web"""
_LEAD_JOINS = """
      LEFT JOIN lending.lendingflow_leads l ON l.id = a.lendingflow_lead_id
      LEFT JOIN lending.web_leads w ON w.id = a.web_lead_id"""


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def pick_up_new_leads(db) -> int:
    """Start a stopwatch for every non-suppressed LendingFlow or website lead not yet watched (no age limit)."""
    count = db.execute(_PICKUP).rowcount + db.execute(_PICKUP_WEB).rowcount
    db.commit()
    return count


_CDR_CALLS = text("""
    SELECT count(*) AS attempts,
           count(*) FILTER (WHERE CASE WHEN disposition IS NOT NULL THEN NOT (disposition = ANY(:not_connected))
                                       ELSE COALESCE(talk_duration_sec, 0) > 0 END) AS connected
      FROM lending.call_dispositions
     WHERE phone = :phone AND direction = 'outbound' AND COALESCE(call_started_at, call_ended_at) >= :since
""")


def _cdr_status(db, phone: str, since: datetime) -> str:
    counts = db.execute(_CDR_CALLS, {"phone": phone, "since": since,
                                     "not_connected": list(NOT_CONNECTED_CODES)}).mappings().one()
    if counts["connected"]:
        return CONNECTED
    return ATTEMPTED if counts["attempts"] else NONE


def call_status(db, row: Mapping[str, Any], ghl_status: Optional[GhlCallStatus]) -> str:
    """CONNECTED if any source saw a connected call since arrival, else ATTEMPTED if any saw a try, else NONE."""
    cdr = _cdr_status(db, row["phone"], row["arrived_at"])
    if cdr == CONNECTED or ghl_status is None or not row["ghl_contact_id"]:
        return cdr
    try:
        ghl = ghl_status(row["ghl_contact_id"], row["arrived_at"])
    except Exception as exc:  # class only; a failed lookup must not stop the alarm
        logger.warning("[uncalled-alarm] lead=%s GHL call lookup failed (%s); using CDR only",
                       row["lead_uuid"], type(exc).__name__)
        return cdr
    if CONNECTED in (cdr, ghl):
        return CONNECTED
    return ATTEMPTED if ATTEMPTED in (cdr, ghl) else NONE


def ghl_call_lookup(account, *, http: Optional[Callable[..., Any]] = None) -> GhlCallStatus:
    """GHL source: classify the outbound call messages dated at/after ``since`` in the contact's conversations
    (read through ``src.lending.ghl_calls``)."""
    read_calls = call_messages_reader(account, http=http)

    def status(contact_id: str, since: datetime) -> str:
        attempted = False
        for message in read_calls(contact_id):
            added = message_time(message)
            if message.get("direction") != "outbound" or added is None or added < since:
                continue
            if call_state(message) in GHL_CONNECTED_STATUSES:
                return CONNECTED
            attempted = True
        return ATTEMPTED if attempted else NONE

    return status


CHANNELS = ("sms_120", "sms_300", "slack_300")
_FIRED_COLUMN = {"sms_120": "fired_120_at", "sms_300": "fired_300_at", "slack_300": "fired_300_at"}
_KIND_COLUMN = {"sms_120": "alarm_120_kind", "sms_300": "alarm_300_kind", "slack_300": "alarm_300_kind"}

# Channel / alarm statuses. SENT and the three terminal ones below are never revisited; RETRY is the only
# status that causes another send (the provider provably did not take it); UNKNOWN is never resent.
SENT, RETRY, SENDING, UNKNOWN, FAILED = "sent", "retry", "sending", "send_unknown", "failed"
SUPERSEDED, NOT_CONFIGURED = "superseded", "not_configured"

_DUE = text(f"""
    SELECT a.id, a.lead_uuid, a.phone, a.arrived_at, a.fired_120_at,{_LEAD_FIELDS}
      FROM lending.uncalled_alarms a{_LEAD_JOINS}
     WHERE a.resolved_at IS NULL AND a.fired_300_at IS NULL AND a.arrived_at <= :first_due
       AND (a.fired_120_at IS NULL OR a.arrived_at <= :escalation_due)
     ORDER BY a.arrived_at LIMIT :limit
""")

_RESOLVE_CONNECTED = text("""
    UPDATE lending.uncalled_alarms SET resolved_at = :now, resolved_reason = 'connected'
     WHERE id = :id AND resolved_at IS NULL
""")

_CLAIM_FIRST = text("""
    UPDATE lending.uncalled_alarms SET fired_120_at = :now, alarm_120_kind = :kind
     WHERE id = :id AND fired_120_at IS NULL AND resolved_at IS NULL RETURNING id
""")

# The escalation supersedes a pending 2-minute retry; a first alarm never sent by now is recorded as skipped_late.
# The row is closed later, by _SETTLE, once every channel has reached a final status.
_CLAIM_ESCALATION = text("""
    UPDATE lending.uncalled_alarms
       SET fired_300_at = :now, alarm_300_kind = :kind,
           sms_120_status = CASE WHEN fired_120_at IS NULL THEN 'skipped_late'
                                 WHEN sms_120_status = 'retry' THEN 'superseded'
                                 ELSE sms_120_status END
     WHERE id = :id AND fired_300_at IS NULL AND resolved_at IS NULL RETURNING id
""")

_RETRY_DUE = text(f"""
    SELECT a.id, a.lead_uuid, a.phone, a.arrived_at, a.fired_120_at, a.fired_300_at, a.alarm_120_kind, a.alarm_300_kind,
           a.sms_120_status, a.sms_300_status, a.slack_300_status,
           a.sms_120_attempts, a.sms_300_attempts, a.slack_300_attempts,{_LEAD_FIELDS}
      FROM lending.uncalled_alarms a{_LEAD_JOINS}
     WHERE a.retry_at <= :now AND 'retry' IN (a.sms_120_status, a.sms_300_status, a.slack_300_status)
     ORDER BY a.retry_at LIMIT :limit
""")

# Channel names come from the closed CHANNELS tuple above, never from input; values are bind parameters.
_RECORD = {c: text(f"UPDATE lending.uncalled_alarms SET {c}_status = :status, {c}_attempts = :attempts, "
                   "retry_at = CASE WHEN :status = 'retry' THEN :retry_at ELSE retry_at END, "
                   "last_error = COALESCE(:err, last_error) WHERE id = :id") for c in CHANNELS}
_SET_FROM_RETRY = {c: text(f"UPDATE lending.uncalled_alarms SET {c}_status = :status "
                           f"WHERE id = :id AND {c}_status = 'retry'") for c in CHANNELS}
_CLAIM_RETRY = {c: text(f"UPDATE lending.uncalled_alarms SET {c}_status = 'sending', claimed_at = :now "
                        f"WHERE id = :id AND {c}_status = 'retry' RETURNING id") for c in CHANNELS}

# A retry claimed but never recorded (crash) is unknown, not retried; so is an escalation claimed but never recorded.
_STALE_SENDING = text("""
    UPDATE lending.uncalled_alarms SET
        sms_120_status = CASE WHEN sms_120_status = 'sending' THEN 'send_unknown' ELSE sms_120_status END,
        sms_300_status = CASE WHEN sms_300_status = 'sending' THEN 'send_unknown' ELSE sms_300_status END,
        slack_300_status = CASE WHEN slack_300_status = 'sending' THEN 'send_unknown' ELSE slack_300_status END
     WHERE claimed_at < :cutoff AND 'sending' IN (sms_120_status, sms_300_status, slack_300_status)
""")
_STALE_ESCALATION = text("""
    UPDATE lending.uncalled_alarms SET sms_300_status = COALESCE(sms_300_status, 'send_unknown'),
                                       slack_300_status = COALESCE(slack_300_status, 'send_unknown')
     WHERE resolved_at IS NULL AND fired_300_at < :cutoff AND (sms_300_status IS NULL OR slack_300_status IS NULL)
""")

# The escalated row closes once nothing is left to retry or in flight.
_SETTLE = text("""
    UPDATE lending.uncalled_alarms SET resolved_at = :now, resolved_reason = 'escalated'
     WHERE resolved_at IS NULL AND fired_300_at IS NOT NULL
       AND sms_300_status IS NOT NULL AND slack_300_status IS NOT NULL
       AND COALESCE(sms_120_status, '') NOT IN ('retry', 'sending')
       AND sms_300_status NOT IN ('retry', 'sending') AND slack_300_status NOT IN ('retry', 'sending')
""")


def _lead_label(row: Mapping[str, Any]) -> str:
    name = (row["first_name"] or "").strip() or "(no name)"
    return f"{name} ({row['property_state']})" if row["property_state"] else name


def _who(row: Mapping[str, Any]) -> str:
    """``Daniel Smith (lead f0084183)`` / ``TEST Sonetel (web lead 333)`` for Slack."""
    ref = str(row["lead_uuid"])
    ref = f"web lead {ref[4:]}" if row["from_web"] and ref.startswith("web-") else f"lead {ref[:8]}"
    return name_and_ref(row.get("full_name") or row["first_name"], ref)


def _fields(row: Mapping[str, Any], now: datetime) -> dict[str, Any]:
    return {"lead": _lead_label(row), "who": _who(row), "ref": str(row["lead_uuid"])[:8],
            "source": "website" if row["from_web"] else "LendingFlow",
            "minutes": int((now - row["arrived_at"]).total_seconds() // 60),
            "state": f" ({row['property_state']})" if row["property_state"] else ""}


def _failure_status(exc: Exception) -> str:
    """RETRY only when the provider provably did not take the message; everything else may have gone out."""
    if isinstance(exc, GhlSmsError):
        return UNKNOWN if exc.ambiguous else RETRY
    if isinstance(exc, SlackApiError):
        return RETRY if _slack_rejected(exc) else UNKNOWN
    return UNKNOWN  # timeouts, connection errors and unexpected exceptions: the outcome is not known


def _slack_rejected(exc: SlackApiError) -> bool:
    """True only for an error Slack documents as a rejected request. SlackApiError is also raised for server errors
    (HTTP 5xx, internal_error, fatal_error) after which part of the operation may have succeeded: those, and any
    code not on the list, are unknown."""
    response = exc.response
    status = getattr(response, "status_code", 200)
    data = getattr(response, "data", response)
    error = data.get("error") if isinstance(data, dict) else None
    return isinstance(status, int) and status < 500 and error in SLACK_REJECTED_ERRORS


def _send_sms(sms: Optional[Callable[..., str]], to: Optional[str], body: str) -> tuple[str, Optional[str]]:
    if sms is None or not to:
        return NOT_CONFIGURED, None
    try:
        sms(to, body, None)
        return SENT, None
    except Exception as exc:  # class only: GHL errors can carry request detail
        return _failure_status(exc), type(exc).__name__


def _post_slack(slack: Any, channel: str, body: str) -> tuple[str, Optional[str]]:
    if slack is None or not channel:
        return NOT_CONFIGURED, None
    try:
        slack.chat_postMessage(channel=channel, text=body)
        return SENT, None
    except Exception as exc:
        return _failure_status(exc), type(exc).__name__


def _send_channel(channel: str, row: Mapping[str, Any], kind: str, now: datetime, *, sms, slack,
                  sms_to: Optional[str], ops_channel: str) -> tuple[str, Optional[str]]:
    fields = _fields(row, now)
    if channel == "slack_300":
        return _post_slack(slack, ops_channel, SLACK_TEXT[kind].format(**fields))
    seconds = FIRST_ALARM_SECONDS if channel == "sms_120" else ESCALATION_SECONDS
    return _send_sms(sms, sms_to, SMS_TEXT[(seconds, kind)].format(**fields))


def _record(db, row: Mapping[str, Any], channel: str, status: str, err: Optional[str], attempts_before: int,
            now: datetime) -> str:
    """Store one send attempt. A definite failure becomes 'failed' once the attempts are used up."""
    attempts = attempts_before + 1
    retry_at = None
    if status == RETRY:
        if attempts >= MAX_SEND_ATTEMPTS:
            status = FAILED
            logger.error("[uncalled-alarm] lead=%s %s gave up after %d attempts (%s)", row["lead_uuid"], channel,
                         attempts, err)
        else:
            retry_at = now + timedelta(seconds=RETRY_DELAYS_SECONDS[min(attempts, len(RETRY_DELAYS_SECONDS)) - 1])
    db.execute(_RECORD[channel], {"id": row["id"], "status": status, "attempts": attempts, "retry_at": retry_at,
                                  "err": err})
    return status


def _queue_for_cora(db, row: Mapping[str, Any], kind: str, now: datetime, *, slack_sent: bool) -> None:
    """Raise the escalation in Cora's main thread too; a failed inbox write never undoes the recorded alarm."""
    fields = _fields(row, now)
    add_to_inbox_safely(
        db, event_type="lead_uncalled", dedup_key=str(row["id"]), target_channel="ops", channel_posted=slack_sent,
        summary=f"{fields['source']} lead {fields['who']}{fields['state']} still {kind.replace('_', ' ')} after "
                f"{fields['minutes']} min",
        payload={"kind": kind, "lead_ref": fields["ref"], "first_name": row["first_name"],
                 "name": row.get("full_name") or row["first_name"],
                 "property_state": row["property_state"], "minutes_since_arrival": fields["minutes"],
                 "ghl_contact_id": row["ghl_contact_id"]},
    )


def _decide(db, row: Mapping[str, Any], *, tick: Clock, sms: Optional[Callable[..., str]], slack: Any,
            ghl_status: Optional[GhlCallStatus], sms_to: Optional[str], ops_channel: str) -> str:
    calls = call_status(db, row, ghl_status)
    now = tick()  # after the slow lookup: an earlier lead in the batch may have used up minutes
    if calls == CONNECTED:
        db.execute(_RESOLVE_CONNECTED, {"id": row["id"], "now": now})
        db.commit()
        return "connected"
    kind = KIND_NOT_REACHED if calls == ATTEMPTED else KIND_NO_CALL
    escalate = row["arrived_at"] <= now - timedelta(seconds=ESCALATION_SECONDS)
    claim = _CLAIM_ESCALATION if escalate else _CLAIM_FIRST
    if db.execute(claim, {"id": row["id"], "now": now, "kind": kind}).first() is None:
        db.commit()
        return "already_claimed"
    db.commit()  # the claim is durable before anything is sent: never a second alarm

    send = dict(sms=sms, slack=slack, sms_to=sms_to, ops_channel=ops_channel)
    if not escalate:
        status, err = _send_channel("sms_120", row, kind, tick(), **send)
        status = _record(db, row, "sms_120", status, err, 0, tick())
        db.commit()
        return f"first_{kind}_sms_{status}"
    outcomes = []
    for channel in ("sms_300", "slack_300"):
        status, err = _send_channel(channel, row, kind, tick(), **send)
        outcomes.append(_record(db, row, channel, status, err, 0, tick()))
    _queue_for_cora(db, row, kind, tick(), slack_sent=outcomes[1] == SENT)
    db.commit()
    return f"escalation_{kind}_sms_{outcomes[0]}_slack_{outcomes[1]}"


def _retry_row(db, row: Mapping[str, Any], *, tick: Clock, sms, slack, ghl_status: Optional[GhlCallStatus],
               sms_to: Optional[str], ops_channel: str) -> list[str]:
    """Retry every channel of this alarm that is waiting in 'retry'; each is claimed before it is sent."""
    outcomes: list[str] = []
    reached = call_status(db, row, ghl_status) == CONNECTED
    now = tick()
    for channel in CHANNELS:
        if row[f"{channel}_status"] != RETRY:
            continue
        fired = row[_FIRED_COLUMN[channel]]
        if reached:  # the lead has been called meanwhile: the alert is stale
            db.execute(_SET_FROM_RETRY[channel], {"id": row["id"], "status": SUPERSEDED})
            outcomes.append(f"retry_{channel}_{SUPERSEDED}")
        elif fired is None or now - fired > timedelta(seconds=RETRY_WINDOW_SECONDS):
            db.execute(_SET_FROM_RETRY[channel], {"id": row["id"], "status": FAILED})
            logger.error("[uncalled-alarm] lead=%s %s gave up: retry window over", row["lead_uuid"], channel)
            outcomes.append(f"retry_{channel}_{FAILED}")
        else:
            if db.execute(_CLAIM_RETRY[channel], {"id": row["id"], "now": now}).first() is None:
                continue
            db.commit()  # claimed before the send, like the first attempt
            kind = row[_KIND_COLUMN[channel]]
            status, err = _send_channel(channel, row, kind, tick(), sms=sms, slack=slack, sms_to=sms_to,
                                        ops_channel=ops_channel)
            recorded = _record(db, row, channel, status, err, row[f"{channel}_attempts"], tick())
            outcomes.append(f"retry_{channel}_{recorded}")
        db.commit()
    return outcomes


class MentioningSlack:
    """Prefixes each alarm post with @-mentions of the approvers (Josh), so the Slack mobile app pushes it to his
    phone. Until his alarm number (R5) and 10DLC land, the 5-minute Slack post is the only alarm that reaches him."""

    def __init__(self, slack: Any, mention_ids: frozenset[str]) -> None:
        self._slack = slack
        self._prefix = " ".join(f"<@{user_id}>" for user_id in sorted(mention_ids))

    def chat_postMessage(self, *, channel: str, text: str, **kwargs: Any) -> Any:
        body = f"{self._prefix} {text}" if self._prefix else text
        return self._slack.chat_postMessage(channel=channel, text=body, **kwargs)


def process_due(db, *, now: Optional[datetime] = None, clock: Clock = _utcnow, sms: Optional[Callable[..., str]],
                slack: Any, ghl_status: Optional[GhlCallStatus], sms_to: Optional[str], ops_channel: str,
                limit: int = BATCH_SIZE) -> dict[str, int]:
    """One cycle: pick up new leads, decide the alarms that are due, retry definite failures, close settled rows.

    ``now`` freezes the clock (tests); otherwise ``clock`` is read again for every lead, because a batch of leads
    with slow providers can take minutes and a later lead must be judged by its real age, not the cycle's start.
    """
    tick: Clock = (lambda: now) if now is not None else clock
    now = tick()
    pick_up_new_leads(db)
    cutoff = now - timedelta(seconds=STALE_SEND_SECONDS)
    db.execute(_STALE_SENDING, {"cutoff": cutoff})
    db.execute(_STALE_ESCALATION, {"cutoff": cutoff})
    db.commit()
    counts: dict[str, int] = {}

    def count(outcome: str, lead_uuid: Any) -> None:
        counts[outcome] = counts.get(outcome, 0) + 1
        noisy = any(word in outcome for word in (FAILED, NOT_CONFIGURED, RETRY, "error"))
        logger.log(logging.WARNING if noisy else logging.INFO, "[uncalled-alarm] lead=%s outcome=%s", lead_uuid, outcome)

    due = db.execute(_DUE, {"first_due": now - timedelta(seconds=FIRST_ALARM_SECONDS),
                            "escalation_due": now - timedelta(seconds=ESCALATION_SECONDS),
                            "limit": limit}).mappings().all()
    for row in due:
        try:
            outcome = _decide(db, row, tick=tick, sms=sms, slack=slack, ghl_status=ghl_status,
                              sms_to=sms_to, ops_channel=ops_channel)
        except Exception as exc:  # class only: SQL errors embed bound params (phones)
            db.rollback()
            logger.error("[uncalled-alarm] lead=%s crashed (%s)", row["lead_uuid"], type(exc).__name__)
            outcome = "error"
        count(outcome, row["lead_uuid"])

    # After the decisions, so an escalation that fires this cycle supersedes a pending 2-minute retry first.
    for row in db.execute(_RETRY_DUE, {"now": now, "limit": limit}).mappings().all():
        try:
            outcomes = _retry_row(db, row, tick=tick, sms=sms, slack=slack, ghl_status=ghl_status,
                                  sms_to=sms_to, ops_channel=ops_channel)
        except Exception as exc:
            db.rollback()
            logger.error("[uncalled-alarm] lead=%s retry crashed (%s)", row["lead_uuid"], type(exc).__name__)
            outcomes = ["error"]
        for outcome in outcomes:
            count(outcome, row["lead_uuid"])

    db.execute(_SETTLE, {"now": tick()})
    db.commit()
    return counts


def run_cycle(*, now: Optional[datetime] = None) -> dict[str, int]:
    """One pass with real settings and its own session; does nothing while the flag is off."""
    settings = get_settings()
    if not settings.lending_uncalled_alarms_enabled:
        return {}
    from src.lending.disposition_delivery import _slack_client
    from src.lending.ghl_account import lending_ghl_account
    from src.lending.ghl_sms import get_sender

    account = lending_ghl_account()
    from packages.agent_core.config import parse_user_ids

    slack = _slack_client() if settings.lending_slack_bot_token else None
    if slack is not None:
        slack = MentioningSlack(slack, parse_user_ids(settings.lending_cora_approver_user_ids))
    with lending_session() as db:
        return process_due(db, now=now, sms=get_sender(internal_alert=True), slack=slack,
                           ghl_status=ghl_call_lookup(account) if account else None,
                           sms_to=normalize(settings.lending_alarm_sms_to), ops_channel=settings.lending_ops_channel)


_running = True


def _stop(signum, _frame) -> None:
    global _running
    logger.info("[uncalled-alarm] signal %s received, stopping after this cycle", signum)
    _running = False


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="T-10 uncalled-lead alarm worker")
    parser.add_argument("--once", action="store_true", help="run a single cycle and exit")
    args = parser.parse_args(argv)
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    logger.info("[uncalled-alarm] starting (enabled=%s)", get_settings().lending_uncalled_alarms_enabled)
    while _running:
        try:
            counts = run_cycle()
            if counts:
                logger.info("[uncalled-alarm] cycle %s", counts)
        except Exception as exc:  # class only
            logger.error("[uncalled-alarm] cycle failed (%s); retrying next interval", type(exc).__name__)
        if args.once:
            break
        time.sleep(POLL_SECONDS)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
