"""R1 1.6: David's 6 AM dialer queue readiness check.

Counts the contacts David can actually call today and names every dialer pool that ran dry. It reads only: the
loaders that push contacts into BatchDialer are unchanged. Nothing is padded, so a short day shows a short count.

A source is read in its own savepoint. One that cannot be read is reported as not available, never as dry.
"""
from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Callable, Mapping, Optional

from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session

from config.lending_compliance import ReasonCode
from config.lending_dialer_queue import (
    CALLBACKS, IN_USE_DAYS, LF_LEADS, QUEUE_SOURCES, QUEUE_TARGET, SOURCE_LABELS, TRANSIENT_BLOCKS, WEB_LEADS,
)
from config.lending_morning_list import JOSH_CALLER_PREFIXES
from config.lending_queues import DIAL_ORDER, LENDINGFLOW_POOL, WEBSITE_POOL
from src.lending import compliance
from src.lending.morning_list.read import read_callbacks, read_warm_leads
from src.services.phone_utils import normalize
from src.tasks.lending_pr_dialer_autoload import staged_pr_records

logger = logging.getLogger(__name__)

Blocks = Callable[[Session, list[str], datetime], dict[str, str]]  # phone -> reason code value

POOLS: tuple[str, ...] = (*DIAL_ORDER, LENDINGFLOW_POOL, WEBSITE_POOL)
SUPPRESSED = "SUPPRESSED"
_REASONS_SHOWN = 2

_POOL_PHONES = text(
    "SELECT pool, phone FROM lending.dialer_load_records WHERE active AND pool IN :pools AND phone IS NOT NULL"
).bindparams(bindparam("pools", expanding=True))
_POOL_RECENT_LOADS = text(
    "SELECT pool, count(*) AS n FROM lending.dialer_load_records WHERE loaded_at >= :since GROUP BY pool")


@dataclass(frozen=True)
class SourceCount:
    source: str
    loaded: int
    eligible: int
    blocked: Mapping[str, int]  # reason -> count
    available: bool = True
    staged: int = 0  # PropertyRadar records staged for the 9:05 ET autoload that are not already loaded (pools only)
    staged_known: bool = True  # False when the staged records could not be read: the pool is then never called dry
    in_use: bool = True  # pools: an active row, a load in the last IN_USE_DAYS, or staged records
    pool_eligible: Optional[int] = None  # pools: eligible active members, including phones counted in an earlier source

    @property
    def ready(self) -> int:
        return self.eligible if self.pool_eligible is None else self.pool_eligible

    @property
    def is_pool(self) -> bool:
        return self.source in POOLS

    @property
    def dry(self) -> bool:
        return (self.is_pool and self.available and self.in_use and self.staged_known
                and self.ready == 0 and self.staged == 0)


@dataclass(frozen=True)
class QueueReport:
    day: date
    target: int
    sources: tuple[SourceCount, ...]

    @property
    def eligible_total(self) -> int:
        return sum(s.eligible for s in self.sources if s.available)

    @property
    def dry_sources(self) -> tuple[str, ...]:
        return tuple(s.source for s in self.sources if s.dry)

    @property
    def short(self) -> bool:
        return self.eligible_total < self.target

    @property
    def complete(self) -> bool:
        return all(s.available for s in self.sources)


def compliance_blocks(db: Session, phones: list[str], now: datetime) -> dict[str, str]:
    """phone -> the reason it cannot be called today. The call window and the 24-hour cap clear up within the
    day, so they are left out. LendingFlow leads need no DNC scrub (client decision)."""
    if not phones:
        return {}
    blocks = {p: r.value for p, r in compliance.dial_blocks(db, phones, now=now, check_scrub=False).items()
              if r.value not in TRANSIENT_BLOCKS}
    # dial_blocks reports the 24-hour cap before the attempt history, so a phone at both would read as eligible.
    for phone in compliance._attempt_history_exhausted_phones(db, phones, now):
        blocks.setdefault(phone, ReasonCode.ATTEMPT_HISTORY_EXCEEDED.value)
    # dial_blocks checks the call window before the scrub, and at 6 AM every phone is outside the window, so the
    # scrub is asked for separately.
    unscrubbed = compliance._stale_scrub_phones(db, phones, now)
    if unscrubbed:
        unscrubbed -= compliance.lendingflow_lead_phones(db, sorted(unscrubbed))
    for phone in unscrubbed:
        blocks.setdefault(phone, ReasonCode.NO_FRESH_SCRUB.value)
    for phone in compliance.suppressed_phones(db, phones):
        blocks.setdefault(phone, SUPPRESSED)
    for phone, reason in compliance.scrub_verdict_blocks(db, phones).items():
        blocks.setdefault(phone, reason.value)
    return blocks


def _phones(items) -> list[str]:
    return [p for p in (normalize(i.phone) for i in items) if p]


def _read_sources(db: Session, now: datetime, lead_days: int) -> dict[str, Optional[list[str]]]:
    """source -> its phones in read order, or None when it could not be read."""
    found: dict[str, Optional[list[str]]] = {}

    def attempt(names: tuple[str, ...], read: Callable[[], dict[str, list[str]]]) -> None:
        try:
            with db.begin_nested():
                found.update(read())
        except Exception as exc:  # class only: SQL errors embed bound parameters
            logger.warning("[queue-check] %s not available (%s)", "/".join(names), type(exc).__name__)
            found.update({name: None for name in names})

    def callbacks() -> dict[str, list[str]]:
        mine = [i for i in read_callbacks(db, now)
                if not (i.caller_name or "").strip().lower().startswith(JOSH_CALLER_PREFIXES)]
        return {CALLBACKS: _phones(mine)}

    def warm() -> dict[str, list[str]]:
        items = read_warm_leads(db, now, lead_days=lead_days)
        return {LF_LEADS: _phones(i for i in items if i.ref.startswith("lf:")),
                WEB_LEADS: _phones(i for i in items if i.ref.startswith("web:"))}

    def pools() -> dict[str, list[str]]:
        by_pool: dict[str, list[str]] = {pool: [] for pool in POOLS}
        for pool, phone in db.execute(_POOL_PHONES, {"pools": list(POOLS)}):
            if normalize(phone):
                by_pool[pool].append(normalize(phone))
        return by_pool

    attempt((CALLBACKS,), callbacks)
    attempt((LF_LEADS, WEB_LEADS), warm)  # 1.3 reads both in one call: a failure marks both
    attempt(POOLS, pools)
    return found


def _recent_loads(db: Session, now: datetime) -> Optional[Mapping[str, int]]:
    try:
        with db.begin_nested():
            return {row.pool: row.n for row in db.execute(
                _POOL_RECENT_LOADS, {"since": now - timedelta(days=IN_USE_DAYS)})}
    except Exception as exc:
        logger.warning("[queue-check] pool usage not available (%s)", type(exc).__name__)
        return None


def _staged_by_pool(db: Session, loaded: set[str]) -> Optional[Mapping[str, int]]:
    """Staged records per pool that the 9:05 autoload could still load: it loads phones not already loaded only.
    None when they could not be read. The autoload's other gates run at load time, so this is an upper bound."""
    try:
        with db.begin_nested():
            records = staged_pr_records(db)
    except Exception as exc:
        logger.warning("[queue-check] staged records not available (%s)", type(exc).__name__)
        return None
    return Counter(r["pool"] for r in records if normalize(r.get("phone")) and normalize(r.get("phone")) not in loaded)


def read_queue(db: Session, now: datetime, *, lead_days: int, autoload_enabled: bool,
               blocks: Blocks = compliance_blocks, target: int = QUEUE_TARGET) -> QueueReport:
    """``autoload_enabled``: LENDING_PR_DIALER_AUTOLOAD_ENABLED. While it is off, staged records never load, so they
    do not keep a pool from being called dry."""
    found = _read_sources(db, now, lead_days)
    seen: set[str] = set()
    kept: dict[str, list[str]] = {}
    for source in QUEUE_SOURCES:  # a phone counts once, in the first source that has it
        fresh = [p for p in dict.fromkeys(found.get(source) or []) if p not in seen]
        seen.update(fresh)
        kept[source] = fresh
    blocked_by_phone = blocks(db, sorted(seen), now) if seen else {}
    recent = _recent_loads(db, now)
    staged = _staged_by_pool(db, seen) if autoload_enabled else {}

    sources = []
    for source in QUEUE_SOURCES:
        phones = kept[source]
        reasons = Counter(blocked_by_phone[p] for p in phones if p in blocked_by_phone)
        count = dict(source=source, loaded=len(phones), eligible=len(phones) - sum(reasons.values()),
                     blocked=dict(reasons), available=found.get(source) is not None)
        if source in POOLS:  # readiness counts every active member, also those credited to an earlier source
            members = list(dict.fromkeys(found.get(source) or []))
            count["pool_eligible"] = sum(p not in blocked_by_phone for p in members)
            count["staged"] = (staged or {}).get(source, 0)
            count["staged_known"] = staged is not None
            count["in_use"] = bool(members or count["staged"] or (recent or {}).get(source))
        sources.append(SourceCount(**count))
    return QueueReport(day=now.date(), target=target, sources=tuple(sources))


def _line(s: SourceCount) -> str:
    if not s.available:
        return f"• {SOURCE_LABELS[s.source]}: not available"
    parts = [f"{s.eligible} of {s.loaded} eligible"]
    if s.blocked:
        top = Counter(s.blocked).most_common(_REASONS_SHOWN)
        parts.append("blocked: " + ", ".join(f"{n} {reason}" for reason, n in top))
    if s.ready > s.eligible:
        parts.append(f"{s.ready - s.eligible} more counted above")
    if s.staged:
        parts.append(f"+{s.staged} may load at 9:05")
    if s.is_pool and not s.staged_known:
        parts.append("staged count not available")
    if s.dry:
        parts.append("DRY")
    elif s.is_pool and not s.in_use:
        parts.append("not in use")
    return f"• {SOURCE_LABELS[s.source]}: " + " · ".join(parts)


def render_queue_post(report: QueueReport) -> str:
    total = report.eligible_total
    if not any(s.available for s in report.sources):
        headline = ":warning: Dialer queue could not be read; no count today"
    elif not report.complete:
        headline = (f":warning: Partial count: at least {total} of {report.target} eligible; "
                    "some sources could not be read")
    elif total == 0:
        headline = ":no_entry: No eligible work today"
    elif report.short:
        headline = f":warning: Only {total} of {report.target} eligible — replenish:"
    else:
        headline = f"Dialer queue ready: {total} eligible"
    return "\n".join([headline, *(_line(s) for s in report.sources)])
