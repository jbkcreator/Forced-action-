"""T-10: alarm Josh when a LendingFlow lead has not been reached by phone 2 and 5 minutes after it arrived.

    python -m src.lending.uncalled_alarm_worker            # loop
    python -m src.lending.uncalled_alarm_worker --once     # one cycle and exit

Each cycle copies every new, non-suppressed row of T-11's ``lending.lendingflow_leads`` into
``lending.uncalled_alarms`` (the stopwatch starts at the lead's ``received_at``; no age limit), then decides the
due ones. Outbound calls to the lead's phone since arrival are classified from BatchDialer CDRs
(``lending.call_dispositions``) and the lead's GHL call messages: connected -> closed, nothing sent; attempted ->
"NOT REACHED" alarms; none -> "no call" alarms. At 120 s an SMS to Josh; at 300 s an URGENT SMS plus a Slack post
to #lendingops. SMS go through GoHighLevel (``ghl_sms.GhlSmsSender``, single attempt).

Calls are only visible once they END, so a long call that starts before 2:00 still alarms; the wording says so.
A GHL lookup that fails counts as GHL finding nothing: a false alarm is better than a silent miss.
Each alarm is claimed (committed) before it is sent and is never re-sent. Logs carry lead ids only.
"""
from __future__ import annotations

import argparse
import logging
import signal
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Mapping, Optional

from sqlalchemy import text

from config.lending_alarms import (
    BATCH_SIZE,
    ESCALATION_SECONDS,
    FIRST_ALARM_SECONDS,
    GHL_CONNECTED_STATUSES,
    GHL_TIMEOUT_SECONDS,
    KIND_NO_CALL,
    KIND_NOT_REACHED,
    NOT_CONNECTED_CODES,
    POLL_SECONDS,
    SLACK_TEXT,
    SMS_TEXT,
)
from config.settings import get_settings
from src.lending.db import lending_session
from src.services.phone_utils import normalize

logger = logging.getLogger(__name__)

CONNECTED, ATTEMPTED, NONE = "connected", "attempted", "none"
GhlCallStatus = Callable[[str, datetime], str]  # (ghl_contact_id, since) -> CONNECTED / ATTEMPTED / NONE

_PICKUP = text("""
    INSERT INTO lending.uncalled_alarms (lendingflow_lead_id, lead_uuid, phone, arrived_at)
    SELECT l.id, l.lead_uuid::text, l.phone, l.received_at FROM lending.lendingflow_leads l
     WHERE NOT l.suppressed
       AND NOT EXISTS (SELECT 1 FROM lending.uncalled_alarms a WHERE a.lendingflow_lead_id = l.id)
    ON CONFLICT (lendingflow_lead_id) DO NOTHING
""")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def pick_up_new_leads(db) -> int:
    """Start a stopwatch for every non-suppressed LendingFlow lead not yet watched (no age limit)."""
    count = db.execute(_PICKUP).rowcount
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


def _message_time(message: Mapping[str, Any]) -> Optional[datetime]:
    """``dateAdded`` as an aware datetime, or None when missing, unparseable or without a timezone."""
    try:
        added = datetime.fromisoformat(message.get("dateAdded") or "")
    except ValueError:
        return None
    return added if added.tzinfo else None


def _ghl_call_state(message: Mapping[str, Any]) -> Optional[str]:
    return (message.get("meta") or {}).get("callStatus") or message.get("callStatus")


def ghl_call_lookup(account, *, http: Optional[Callable[..., Any]] = None) -> GhlCallStatus:
    """GHL source: classify the outbound call messages dated at/after ``since`` in the contact's conversations.

    GET /conversations/search?contactId=… then GET /conversations/{id}/messages?type=TYPE_CALL (GHL API v2,
    Version 2021-04-15; scopes conversations.readonly + conversations/message.readonly). Field names
    (``conversations``, ``messages``, ``direction``, ``dateAdded``, ``meta.callStatus``) follow GHL's published
    OpenAPI spec and are UNVERIFIED against a live response; an unexpected shape finds no call.
    """
    from src.lending.ghl_account import ghl_headers
    from src.services.ghl_webhook import _GHL_BASE
    from src.utils.http_helpers import requests_get_with_retry

    get = http or (lambda url, **kw: requests_get_with_retry(url, max_retries=1, retry_delay=1,
                                                             timeout=GHL_TIMEOUT_SECONDS, **kw))
    headers = ghl_headers(account.api_key, "2021-04-15")

    def status(contact_id: str, since: datetime) -> str:
        found = get(f"{_GHL_BASE}/conversations/search", headers=headers,
                    params={"locationId": account.location_id, "contactId": contact_id})
        found.raise_for_status()
        attempted = False
        for conversation in (found.json() or {}).get("conversations") or []:
            page = get(f"{_GHL_BASE}/conversations/{conversation['id']}/messages", headers=headers,
                       params={"type": "TYPE_CALL"})
            page.raise_for_status()
            messages = (page.json() or {}).get("messages") or []
            if isinstance(messages, dict):  # the spec nests the list: {"messages": {"messages": [...]}}
                messages = messages.get("messages") or []
            for message in messages:
                added = _message_time(message)
                if message.get("direction") != "outbound" or added is None or added < since:
                    continue
                if _ghl_call_state(message) in GHL_CONNECTED_STATUSES:
                    return CONNECTED
                attempted = True
        return ATTEMPTED if attempted else NONE

    return status


_DUE = text("""
    SELECT a.id, a.lead_uuid, a.phone, a.arrived_at, a.fired_120_at, l.first_name, l.property_state, l.ghl_contact_id
      FROM lending.uncalled_alarms a
      LEFT JOIN lending.lendingflow_leads l ON l.id = a.lendingflow_lead_id
     WHERE a.resolved_at IS NULL AND a.arrived_at <= :first_due
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

# The escalation also closes the row; a first alarm never sent by now is recorded as skipped_late.
_CLAIM_ESCALATION = text("""
    UPDATE lending.uncalled_alarms
       SET fired_300_at = :now, alarm_300_kind = :kind, resolved_at = :now, resolved_reason = 'escalated',
           sms_120_status = CASE WHEN fired_120_at IS NULL THEN 'skipped_late' ELSE sms_120_status END
     WHERE id = :id AND fired_300_at IS NULL AND resolved_at IS NULL RETURNING id
""")

_RECORD_FIRST = text("UPDATE lending.uncalled_alarms SET sms_120_status = :sms, last_error = COALESCE(:err, last_error) "
                     "WHERE id = :id")
_RECORD_ESCALATION = text("UPDATE lending.uncalled_alarms SET sms_300_status = :sms, slack_300_status = :slack, "
                          "last_error = COALESCE(:err, last_error) WHERE id = :id")


def _lead_label(row: Mapping[str, Any]) -> str:
    name = (row["first_name"] or "").strip() or "(no name)"
    return f"{name} ({row['property_state']})" if row["property_state"] else name


def _send_sms(sms: Optional[Callable[..., str]], to: Optional[str], body: str) -> tuple[str, Optional[str]]:
    if sms is None or not to:
        return "not_configured", None
    try:
        sms(to, body, None)
        return "sent", None
    except Exception as exc:  # class only: GHL errors can carry request detail
        return "failed", type(exc).__name__


def _post_slack(slack: Any, channel: str, body: str) -> tuple[str, Optional[str]]:
    if slack is None or not channel:
        return "not_configured", None
    try:
        slack.chat_postMessage(channel=channel, text=body)
        return "sent", None
    except Exception as exc:
        return "failed", type(exc).__name__


def _decide(db, row: Mapping[str, Any], *, now: datetime, sms: Optional[Callable[..., str]], slack: Any,
            ghl_status: Optional[GhlCallStatus], sms_to: Optional[str], ops_channel: str) -> str:
    calls = call_status(db, row, ghl_status)
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

    fields = {"lead": _lead_label(row), "ref": str(row["lead_uuid"])[:8],
              "minutes": int((now - row["arrived_at"]).total_seconds() // 60),
              "state": f" ({row['property_state']})" if row["property_state"] else ""}
    if not escalate:
        status, err = _send_sms(sms, sms_to, SMS_TEXT[(FIRST_ALARM_SECONDS, kind)].format(**fields))
        db.execute(_RECORD_FIRST, {"id": row["id"], "sms": status, "err": err})
        db.commit()
        return f"first_{kind}_sms_{status}"
    sms_status, sms_err = _send_sms(sms, sms_to, SMS_TEXT[(ESCALATION_SECONDS, kind)].format(**fields))
    slack_status, slack_err = _post_slack(slack, ops_channel, SLACK_TEXT[kind].format(**fields))
    db.execute(_RECORD_ESCALATION, {"id": row["id"], "sms": sms_status, "slack": slack_status,
                                    "err": sms_err or slack_err})
    db.commit()
    return f"escalation_{kind}_sms_{sms_status}_slack_{slack_status}"


def process_due(db, *, now: Optional[datetime] = None, sms: Optional[Callable[..., str]], slack: Any,
                ghl_status: Optional[GhlCallStatus], sms_to: Optional[str], ops_channel: str,
                limit: int = BATCH_SIZE) -> dict[str, int]:
    """One cycle: pick up new leads, then decide every alarm that is due."""
    now = now or _utcnow()
    pick_up_new_leads(db)
    rows = db.execute(_DUE, {"first_due": now - timedelta(seconds=FIRST_ALARM_SECONDS),
                             "escalation_due": now - timedelta(seconds=ESCALATION_SECONDS),
                             "limit": limit}).mappings().all()
    counts: dict[str, int] = {}
    for row in rows:
        try:
            outcome = _decide(db, row, now=now, sms=sms, slack=slack, ghl_status=ghl_status,
                              sms_to=sms_to, ops_channel=ops_channel)
        except Exception as exc:  # class only: SQL errors embed bound params (phones)
            db.rollback()
            logger.error("[uncalled-alarm] lead=%s crashed (%s)", row["lead_uuid"], type(exc).__name__)
            outcome = "error"
        counts[outcome] = counts.get(outcome, 0) + 1
        level = logging.WARNING if "failed" in outcome or "not_configured" in outcome else logging.INFO
        logger.log(level, "[uncalled-alarm] lead=%s outcome=%s", row["lead_uuid"], outcome)
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
    slack = _slack_client() if settings.lending_slack_bot_token else None
    with lending_session() as db:
        return process_due(db, now=now, sms=get_sender(), slack=slack,
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
