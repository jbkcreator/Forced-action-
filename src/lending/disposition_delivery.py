"""Deliver a logged disposition to the Google Sheet and Slack #dial-tasks (spec §4.4).

Each sink is independent: one failing never blocks the other, and a failure is
left for the retry task (a sink is "behind" while its synced disposition
differs from the row's disposition). Sheet and Slack are updated in place when
a caller changes the result, so a call is never counted twice.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Callable, Optional
from zoneinfo import ZoneInfo

from sqlalchemy import text

from config.lending_dispositions import (
    BOOKED_CODE,
    DELIVERY_LATENCY_TARGET_SECONDS,
    DNC_CODE,
    SHEET_COLUMNS,
    SHEET_TIMEZONE,
)
from config.settings import get_settings
from src.lending.db import lending_session
from src.lending.dispositions import last4, lookup_load_record

logger = logging.getLogger(__name__)

RECORDING_STATUS_TEXT = {
    "readable": "recording ready",
    "pending": "recording pending",
    "forbidden": "recording pending (permission)",
    "missing": "recording not found",
}

DELIVERY_LOCK_NAMESPACE = 3190  # first key of the per-row advisory lock; the second is the row id

SHEETS_SCOPE = "https://www.googleapis.com/auth/spreadsheets"


def _et(dt: Optional[datetime]) -> str:
    return dt.astimezone(ZoneInfo(SHEET_TIMEZONE)).strftime("%Y-%m-%d %H:%M:%S") if dt else ""


def _yn(flag: bool) -> str:
    return "Y" if flag else "N"


def build_sheet_row(row: dict, now: datetime) -> list[str]:
    """One Sheet row, in SHEET_COLUMNS order."""
    disposition = row["disposition"]
    booked = disposition == BOOKED_CODE and not row.get("booking_blocked")
    cause = row.get("unfunded_cause") or ""
    return [
        row["dialer_call_id"],
        _et(row["call_started_at"] or row["call_ended_at"]),
        row["caller_name"] or "",
        row["caller_seat"] or "",
        row["queue"] or row["campaign_tag"] or "",
        row["phone"] or "",
        disposition or row.get("disposition_raw") or "",
        str(row["talk_duration_sec"] if row["talk_duration_sec"] is not None else ""),
        _yn(booked),
        _yn(disposition == DNC_CODE),
        cause,
        row.get("disposition_list_version") or "",
        _et(now),
    ]


def build_slack_message(row: dict, record: Optional[dict]) -> tuple[str, list[dict]]:
    """(fallback text, Block Kit blocks) for one disposition."""
    disposition = row["disposition"]
    record = record or {}
    who = record.get("borrower_name") or record.get("entity_name") or "—"
    lines = [
        f"*Borrower:* {who}",
        f"*Entity:* {record.get('entity_name') or '—'}",
        f"*Property:* {record.get('property_address') or '—'}",
        f"*Phone:* {row['phone'] or '—'}",
        f"*Queue:* {row['queue'] or row['campaign_tag'] or '—'}",
        f"*Caller:* {row['caller_name'] or row['caller_seat'] or '—'}",
        f"*Talk time:* {row['talk_duration_sec'] if row['talk_duration_sec'] is not None else '—'} s",
    ]
    if row.get("recording_ref"):
        status = RECORDING_STATUS_TEXT.get(row.get("recording_status"), "recording pending")
        about = " | ".join(p for p in (record.get("borrower_name") or record.get("entity_name"),
                                       record.get("property_address")) if p)
        lines.append(f"<{row['recording_ref']}|Open call recording>" + (f" ({about})" if about else "") + f" — {status}")

    if disposition is None:
        title = ":arrows_counterclockwise: Result removed — waiting for a new result"
    elif disposition == BOOKED_CODE and row.get("booking_blocked"):
        title = ":warning: BOOKED on a nurture-only list — not counted as a booking"
    elif disposition == BOOKED_CODE:
        title = ":star2: Booked (caller-reported)"
    elif disposition == DNC_CODE:
        title = ":no_entry: DNC request — contact blocked on all channels"
    else:
        title = f"Call result: {disposition}"

    blocks: list[dict] = [
        {"type": "header", "text": {"type": "plain_text", "text": title, "emoji": True}},
        {"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(lines)}},
    ]
    return f"{title} — {who}", blocks


def _sheets_service():
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    creds = service_account.Credentials.from_service_account_file(
        get_settings().lending_sheets_service_account_key_path, scopes=[SHEETS_SCOPE])
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def _slack_client():
    from slack_sdk import WebClient

    token = get_settings().lending_slack_bot_token
    return WebClient(token=token.get_secret_value() if token else None)


def sync_sheet(row: dict, service: Any, now: datetime) -> None:
    """Update the call's Sheet row in place, or append it (and the header) if new."""
    settings = get_settings()
    sheet_id, tab = settings.lending_disposition_sheet_id, settings.lending_disposition_sheet_tab
    values = service.spreadsheets().values()
    width = chr(ord("A") + len(SHEET_COLUMNS) - 1)
    existing = values.get(spreadsheetId=sheet_id, range=f"{tab}!A:A").execute().get("values", [])
    if not existing:
        values.append(
            spreadsheetId=sheet_id, range=f"{tab}!A:{width}", valueInputOption="RAW",
            insertDataOption="INSERT_ROWS", body={"values": [list(SHEET_COLUMNS)]},
        ).execute()
        existing = [["Call ID"]]
    line = build_sheet_row(row, now)
    position = next((i + 1 for i, cell in enumerate(existing) if cell and cell[0] == row["dialer_call_id"]), None)
    if position:
        values.update(
            spreadsheetId=sheet_id, range=f"{tab}!A{position}:{width}{position}",
            valueInputOption="RAW", body={"values": [line]},
        ).execute()
    else:
        values.append(
            spreadsheetId=sheet_id, range=f"{tab}!A:{width}", valueInputOption="RAW",
            insertDataOption="INSERT_ROWS", body={"values": [line]},
        ).execute()


def post_slack(row: dict, record: Optional[dict], client: Any) -> str:
    """Post (first time) or update (result changed) the call's message. Returns the message ts."""
    channel = get_settings().lending_dial_tasks_channel
    fallback, blocks = build_slack_message(row, record)
    if row["slack_ts"]:
        client.chat_update(channel=channel, ts=row["slack_ts"], text=fallback, blocks=blocks)
        return row["slack_ts"]
    return client.chat_postMessage(channel=channel, text=fallback, blocks=blocks)["ts"]


def _configured_sheet() -> bool:
    s = get_settings()
    return bool(s.lending_sheets_service_account_key_path and s.lending_disposition_sheet_id)


def _configured_slack() -> bool:
    s = get_settings()
    return bool(s.lending_slack_bot_token and s.lending_dial_tasks_channel)


def _latency(row: dict, done_at: datetime, sink: str) -> None:
    if not row["disposition_at"]:
        return
    seconds = (done_at - row["disposition_at"]).total_seconds()
    level = logging.WARNING if seconds > DELIVERY_LATENCY_TARGET_SECONDS else logging.INFO
    logger.log(level, "[lending] call %s %s delivered %.1fs after disposition (target %ds)",
               row["dialer_call_id"], sink, seconds, DELIVERY_LATENCY_TARGET_SECONDS)


def deliver_disposition(
    row_id: int,
    *,
    session_factory: Callable = lending_session,
    slack_client: Any = None,
    sheets_service: Any = None,
) -> None:
    """Bring the Sheet and Slack up to date with the row's disposition. Never raises.

    One delivery per row at a time: the webhook task, the retry cron and other workers
    would otherwise each see "no Slack message yet" and each create one. The lock lives
    on its own session because the delivery session commits (which would end a
    transaction-level lock) and returns its connection to the pool.
    """
    try:
        with session_factory() as lock_db:
            key = {"ns": DELIVERY_LOCK_NAMESPACE, "id": row_id}
            if not lock_db.execute(text("SELECT pg_try_advisory_lock(:ns, :id)"), key).scalar():
                logger.info("[lending] delivery for row %s already running; skipping", row_id)
                return
            try:
                _deliver_locked(row_id, session_factory, slack_client, sheets_service)
            finally:
                lock_db.execute(text("SELECT pg_advisory_unlock(:ns, :id)"), key)
    except Exception as exc:
        logger.error("[lending] delivery for row %s failed: %s", row_id, type(exc).__name__)


def _deliver_locked(row_id: int, session_factory: Callable, slack_client: Any, sheets_service: Any) -> None:
    with session_factory() as db:
        row = db.execute(
            text("SELECT * FROM lending.call_dispositions WHERE id = :id"), {"id": row_id}
        ).mappings().first()
        if not row:
            return
        row = dict(row)
        if row["sheet_synced_disposition"] != row["disposition"] and (sheets_service or _configured_sheet()):
            _deliver_sheet(db, row, sheets_service)
        # A removed result only needs Slack when an earlier post exists to update.
        slack_behind = row["slack_posted_disposition"] != row["disposition"]
        slack_needed = slack_behind and (row["disposition"] or row["slack_ts"])
        if slack_needed and (slack_client or _configured_slack()):
            _deliver_slack(db, row, slack_client)


def _deliver_sheet(db, row: dict, service: Any) -> None:
    try:
        now = datetime.now(timezone.utc)
        sync_sheet(row, service or _sheets_service(), now)
        db.execute(
            text("UPDATE lending.call_dispositions SET sheet_synced_at = :at, "
                 "sheet_synced_disposition = :d WHERE id = :id"),
            {"at": now, "d": row["disposition"], "id": row["id"]},
        )
        db.commit()
        _latency(row, now, "sheet")
    except Exception as exc:
        db.rollback()
        logger.warning("[lending] call %s sheet sync failed: %s", row["dialer_call_id"], type(exc).__name__)


def _deliver_slack(db, row: dict, client: Any) -> None:
    try:
        now = datetime.now(timezone.utc)
        record = lookup_load_record(db, row["dialer_contact_id"], row["phone"], row["call_started_at"])
        ts = post_slack(row, record, client or _slack_client())
        db.execute(
            text("UPDATE lending.call_dispositions SET slack_posted_at = :at, "
                 "slack_posted_disposition = :d, slack_ts = :ts WHERE id = :id"),
            {"at": now, "d": row["disposition"], "ts": ts, "id": row["id"]},
        )
        db.commit()
        _latency(row, now, "slack")
    except Exception as exc:
        db.rollback()
        logger.warning("[lending] call %s (%s) slack post failed: %s",
                       row["dialer_call_id"], last4(row["phone"]), type(exc).__name__)


def alert_unpropagated_dnc(call_id: str, caller_seat: Optional[str], client: Any = None) -> None:
    """Tell #dial-tasks a DNC request could not be propagated (no usable phone number). Never raises."""
    if not (client or _configured_slack()):
        logger.error("[lending] call %s: DNC request could not be propagated and Slack is not configured", call_id)
        return
    try:
        (client or _slack_client()).chat_postMessage(
            channel=get_settings().lending_dial_tasks_channel,
            text=(f":rotating_light: DNC request NOT propagated — call {call_id} (caller seat {caller_seat or 'unknown'}) "
                  "has no usable phone number. Add the contact to the suppression list manually."),
        )
    except Exception as exc:
        logger.error("[lending] call %s: DNC alert failed: %s", call_id, type(exc).__name__)


def alert_unknown_code(call_id: str, raw_code: str, caller_seat: Optional[str], client: Any = None) -> None:
    """Tell #dial-tasks the dialer sent a disposition our list does not know. Never raises."""
    if not (client or _configured_slack()):
        logger.error("[lending] call %s: unknown disposition %r and Slack is not configured", call_id, raw_code)
        return
    try:
        (client or _slack_client()).chat_postMessage(
            channel=get_settings().lending_dial_tasks_channel,
            text=(f":warning: Unknown disposition `{raw_code}` on call {call_id} (seat {caller_seat or 'unknown'}). "
                  "The dialer's Call Results do not match the lending list — check the setup."),
        )
    except Exception as exc:
        logger.error("[lending] call %s: unknown-code alert failed: %s", call_id, type(exc).__name__)


def alert_dnc_removal_pending(call_id: str, caller_seat: Optional[str], client: Any = None) -> None:
    """Tell #dial-tasks a DNC request is recorded everywhere except the dialer, where the
    removal is not confirmed: the person may still be dialable. Never raises."""
    if not (client or _configured_slack()):
        logger.error("[lending] call %s: DNC removal from the dialer is pending and Slack is not configured", call_id)
        return
    try:
        (client or _slack_client()).chat_postMessage(
            channel=get_settings().lending_dial_tasks_channel,
            text=(f":rotating_light: DNC request recorded but NOT yet removed from the dialer — call {call_id} "
                  f"(caller seat {caller_seat or 'unknown'}). Remove the contact in the dialer manually until the "
                  "automatic removal is confirmed."),
        )
    except Exception as exc:
        logger.error("[lending] call %s: DNC removal-pending alert failed: %s", call_id, type(exc).__name__)
