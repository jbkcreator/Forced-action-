"""SUPERSEDED (WP-GL-9): the missed-call text now runs inside src.lending.cdr_poller. Kept for its tests; do not run.

Sends only when MISSED_CALL_TEXT_ENABLED=true; otherwise every no-answer is logged as
``dry_run``. One cycle at a time (advisory lock), one transaction per cycle.

Usage:
    python -m src.lending.missed_call_poller            # loop every POLL_SECONDS
    python -m src.lending.missed_call_poller --once
"""
from __future__ import annotations

import argparse
import logging
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any, Optional

from sqlalchemy import text

from config.lending_missed_call import (
    CDR_DISPOSITION_FIELDS, CDR_MAX_PAGES, CDR_PAGE_LENGTH, CDR_POLL_PATH, DNC_DISPOSITIONS, POLL_LOCK_KEY,
    POLL_SECONDS,
)
from config.lending_compliance import AGENT_SHIFT_GROUPS, SHIFT_GROUPS
from config.settings import get_settings
from src.lending.call_log import record_call_attempts
from src.lending.compliance import on_attempt_recorded, propagate_opt_out
from src.lending.missed_call_text import (
    parse_time, call_record_fields, consent_gated_sender, parse_cdr, process_missed_calls,
)

logger = logging.getLogger(__name__)


def _records(body: Any) -> list[dict]:
    return list(body.get("items", [])) if isinstance(body, dict) else list(body or [])


def _next_page(body: Any) -> Optional[str]:
    return (body.get("nextPage") or body.get("next_page")) if isinstance(body, dict) else None


def _known_call_ids(db, ids: list[str]) -> set[str]:
    if not ids:
        return set()
    return set(db.execute(text("SELECT dialer_call_id FROM lending.call_dispositions WHERE dialer_call_id = ANY(:ids)"),
                          {"ids": ids}).scalars())


def _fetch_new_records(db, http) -> list[dict]:
    """Page the recent-calls list, newest first, until a page holds a call already in the
    call log (our own marker), the list ends, or the per-cycle page bound is hit."""
    records: list[dict] = []
    cursor: Optional[str] = None
    for _ in range(CDR_MAX_PAGES):
        path = f"{CDR_POLL_PATH}?pagelength={CDR_PAGE_LENGTH}" + (f"&next_page={cursor}" if cursor else "")
        body = http("GET", path)
        page = _records(body)
        records.extend(page)
        ids = [str(r["id"]) for r in page if r.get("id") is not None]
        cursor = _next_page(body)
        if not page or not cursor or _known_call_ids(db, ids):
            break
    return records


def _disposition(record: dict) -> str:
    for name in CDR_DISPOSITION_FIELDS:
        if record.get(name):
            return str(record[name]).strip().lower()
    return ""


def _agent_id(record: dict) -> Optional[str]:
    agent = record.get("agent")
    return str(agent.get("id")) if isinstance(agent, dict) and agent.get("id") is not None else None


def _record_opt_outs(db, records: list[dict]) -> int:
    """A call dispositioned "do not call" opts the number out everywhere (idempotent per call)."""
    opted = 0
    for record in records:
        if _disposition(record) not in DNC_DISPOSITIONS:
            continue
        fields = call_record_fields(record)
        if fields is None:
            continue
        propagate_opt_out(db, phone=fields["phone"], source_ref=fields["call_id"], actor=_agent_id(record))
        opted += 1
    return opted


_ET = ZoneInfo("America/New_York")


def _check_shifts(db, records: list[dict]) -> None:
    """Allow and warn: record each call's shift group, and warn when the agent has no group
    or called outside it. The call itself is already made; the hard rails are elsewhere."""
    grouped = []
    for record in records:
        agent = _agent_id(record)
        if agent is None or record.get("id") is None:
            continue  # a dropped multi-line call has no agent
        group = AGENT_SHIFT_GROUPS.get(agent)
        if group is None:
            logger.warning("[missed-call-poller] agent %s has no shift group; call allowed", agent)
            continue
        grouped.append({"c": str(record["id"]), "g": group})
        started = parse_time(record.get("callStartTime"))
        if started is not None and group in SHIFT_GROUPS:
            start, end = SHIFT_GROUPS[group]
            if not (start <= started.astimezone(_ET).time() <= end):
                logger.warning("[missed-call-poller] agent %s called outside shift group %s", agent, group)
    if grouped:
        db.execute(text("UPDATE lending.call_dispositions SET seat_group = :g "
                        "WHERE dialer_call_id = :c AND seat_group IS NULL"), grouped)


def run_cycle(db, *, http, enabled: bool, now: Optional[datetime] = None) -> Optional[dict[str, int]]:
    """None when another poller holds the lock. Does not commit."""
    if not db.execute(text("SELECT pg_try_advisory_xact_lock(:k)"), {"k": POLL_LOCK_KEY}).scalar():
        return None
    records = _fetch_new_records(db, http)
    # Every finished call is an attempt: write it to the call log and run the cap hook,
    # so the 3-attempt rail works even if the dialer never pushes a call event.
    for phone in dict.fromkeys(record_call_attempts(db, records)):
        on_attempt_recorded(db, phone, now=now)
    _record_opt_outs(db, records)
    _check_shifts(db, records)
    calls = [c for c in (parse_cdr(r) for r in records) if c is not None]
    return process_missed_calls(db, calls, sender=consent_gated_sender(db), enabled=enabled, now=now)


def main(argv: list[str] | None = None) -> None:
    logger.error("[missed-call-poller] superseded by the text-back step in src.lending.cdr_poller (WP-GL-9, GHL sender); "
                 "not starting. Running two readers of the call feed would double-count attempts.")
    return
    from src.core.database import get_db_context
    from src.lending.dialer_port import _requests_http

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--once", action="store_true", help="run a single cycle and exit")
    args = parser.parse_args(argv)
    settings = get_settings()
    if settings.batchdialer_api_key is None:
        logger.error("[missed-call-poller] BATCHDIALER_API_KEY is not set")
        return
    http = _requests_http(settings.batchdialer_api_key.get_secret_value())
    while True:
        try:
            with get_db_context() as db:
                counts = run_cycle(db, http=http, enabled=settings.missed_call_text_enabled)
                db.commit()
            if counts:
                logger.info("[missed-call-poller] %s", counts)
        except Exception as exc:
            logger.error("[missed-call-poller] cycle failed (%s); retrying next interval", type(exc).__name__)
            if args.once:
                raise
        if args.once:
            return
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
