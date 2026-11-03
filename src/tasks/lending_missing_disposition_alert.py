"""Warn #dial-tasks about connected calls that never got a disposition (brief §3:
"a skipped disposition is a missed shift"). One warning per call, cron every 5 minutes.

* BatchDialer calls: no disposition and no raw result after LENDING_DISPOSITION_MISSING_ALERT_MINUTES
* GHL calls (G3): outbound, connected, no outcome attached after GHL_MISSING_DISPOSITION_GRACE_MINUTES (the GHL
  import runs every 15 minutes, so the grace is longer). Only calls ended at/after LENDING_GHL_OUTCOMES_LIVE_SINCE:
  unset = GHL calls are not flagged, so calls from before the GHL disposition workflow existed are never reported.
  ``disposition_raw`` is always ``ghl:<status>`` on a GHL row, so it is not required to be NULL
* GHL outcomes that never found their call (lending.ghl_disposition_events, unmatched after
  GHL_DISPOSITION_UNMATCHED_ALERT_HOURS) are posted once; a missing table is logged and skipped

    python -m src.tasks.lending_missing_disposition_alert
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from sqlalchemy import text

from config.lending_dispositions import GHL_DISPOSITION_UNMATCHED_ALERT_HOURS, GHL_MISSING_DISPOSITION_GRACE_MINUTES
from config.settings import get_settings
from src.lending.db import lending_session
from src.lending.disposition_delivery import _configured_slack, _slack_client
from src.lending.dispositions import last4
from src.lending.slack_names import name_and_ref, names_by_phone
from src.lending.slack_test_filter import log_skipped, hidden_phones

logger = logging.getLogger(__name__)

_BATCHDIALER = ("(call_source = 'batchdialer' AND disposition IS NULL AND disposition_raw IS NULL "
                "AND call_ended_at < now() - make_interval(mins => :m))")
_GHL = ("(call_source = 'ghl' AND direction = 'outbound' AND disposition IS NULL "
        "AND call_ended_at >= :ghl_since AND call_ended_at < now() - make_interval(mins => :ghl_m))")

_CLAIM_UNMATCHED = text(
    "UPDATE lending.ghl_disposition_events SET alerted_at = now() "
    "WHERE id IN (SELECT id FROM lending.ghl_disposition_events "
    "  WHERE matched_call_id IS NULL AND alerted_at IS NULL "
    "  AND received_at < now() - make_interval(hours => :h) FOR UPDATE SKIP LOCKED) "
    "RETURNING id, phone, code, caller_name"
)


def _claim_missing(minutes: int) -> list[tuple]:
    ghl_since = getattr(get_settings(), "lending_ghl_outcomes_live_since", None)
    sources = f"({_BATCHDIALER} OR {_GHL})" if ghl_since else _BATCHDIALER
    params: dict[str, Any] = {"m": minutes}
    if ghl_since:
        params.update(ghl_m=GHL_MISSING_DISPOSITION_GRACE_MINUTES, ghl_since=ghl_since)
    with lending_session() as db:
        rows = db.execute(
            text(
                "UPDATE lending.call_dispositions SET disposition_missing_alerted_at = now() "
                "WHERE id IN (SELECT id FROM lending.call_dispositions "
                f"  WHERE {sources} "
                "  AND disposition_missing_alerted_at IS NULL "
                "  AND call_ended_at IS NOT NULL AND coalesce(talk_duration_sec, 0) > 0 "
                "  FOR UPDATE SKIP LOCKED) "
                "RETURNING dialer_call_id, caller_seat, caller_name, phone"
            ),
            params,
        ).all()
        db.commit()
        borrowers = names_by_phone(db, [row[3] for row in rows]) if rows else {}
        test = hidden_phones(db, [row[3] for row in rows]) if rows else set()
    for row in rows:  # claimed with no post, so they are not picked up again
        if row[3] in test:
            log_skipped("missing-disposition alert", f"call {row[0]}")
    return [(*row, borrowers.get(row[3])) for row in rows if row[3] not in test]


def _alert_missing(client: Any, channel: str, minutes: int) -> int:
    rows = _claim_missing(minutes)
    if not rows:
        return 0
    by_seat: dict[str, list[str]] = {}
    labels: dict[str, str] = {}
    for call_id, seat, name, _phone, borrower in rows:
        by_seat.setdefault(name or seat or "unknown seat", []).append(call_id)
        labels[call_id] = name_and_ref(borrower, f"call {call_id}")
    unsent: list[str] = []
    for seat, ids in by_seat.items():
        try:
            client.chat_postMessage(
                channel=channel,
                text=f":warning: {seat}: {len(ids)} connected call(s) have no disposition after {minutes} min "
                     f"({', '.join(labels[i] for i in ids[:5])}{'…' if len(ids) > 5 else ''}). "
                     "A skipped disposition is a missed shift.",
            )
        except Exception as exc:
            logger.error("[lending] missing-disposition alert failed: %s", type(exc).__name__)
            unsent += ids
    if unsent:
        with lending_session() as db:  # un-claim so the next run retries the post
            db.execute(text("UPDATE lending.call_dispositions SET disposition_missing_alerted_at = NULL "
                            "WHERE dialer_call_id = ANY(:ids)"), {"ids": unsent})
            db.commit()
    return len(rows)


def _alert_unmatched(client: Any, channel: str) -> int:
    """One post listing GHL outcomes that never found their call. Phones are shown as the last 4 digits only."""
    try:
        with lending_session() as db:
            with db.begin_nested():  # savepoint: a missing table must not abort the caller's transaction
                rows = db.execute(_CLAIM_UNMATCHED, {"h": GHL_DISPOSITION_UNMATCHED_ALERT_HOURS}).all()
            db.commit()
    except Exception as exc:  # the table may not be migrated yet; class only
        logger.warning("[lending] unmatched GHL outcome check skipped: %s", type(exc).__name__)
        return 0
    if not rows:
        return 0
    items = [f"{code} for {last4(phone)}" + (f" ({caller})" if caller else "") for _id, phone, code, caller in rows]
    try:
        client.chat_postMessage(
            channel=channel,
            text=f":warning: {len(rows)} GHL call outcome(s) found no GHL call after "
                 f"{GHL_DISPOSITION_UNMATCHED_ALERT_HOURS} h ({', '.join(items[:5])}{'…' if len(items) > 5 else ''}). "
                 "Check the call was made from GHL on that number, or the workflow's call id field.",
        )
    except Exception as exc:
        logger.error("[lending] unmatched GHL outcome alert failed: %s", type(exc).__name__)
        with lending_session() as db:  # un-claim so the next run retries the post
            db.execute(text("UPDATE lending.ghl_disposition_events SET alerted_at = NULL WHERE id = ANY(:ids)"),
                       {"ids": [r[0] for r in rows]})
            db.commit()
    return len(rows)


def run(slack_client: Any = None, minutes: Optional[int] = None) -> int:
    """Post one warning per seat for calls past the grace period, plus unmatched GHL outcomes; returns items flagged."""
    settings = get_settings()
    minutes = minutes if minutes is not None else settings.lending_disposition_missing_alert_minutes
    if not (slack_client or _configured_slack()):
        logger.error("[lending] missing-disposition alert skipped: Slack is not configured")
        return 0
    client = slack_client or _slack_client()
    channel = settings.lending_dial_tasks_channel
    return _alert_missing(client, channel, minutes) + _alert_unmatched(client, channel)


if __name__ == "__main__":
    run()
