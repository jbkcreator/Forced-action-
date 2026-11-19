"""DNC / litigator scrub of the numbers on Josh's GHL call lists (decisions D1 and D4).

The weekly scrub (``src.lending.weekly_scrub``) covers only numbers loaded in BatchDialer, and the GHL call-list gate
(``src.lending.ghl_call_gate``) blocks any non-LendingFlow number whose scrub is missing or older than
DNC_SCRUB_MAX_AGE_DAYS. This job keeps the listed numbers scrubbed: website leads within about an hour of reaching
the Web Leads list (D1), and every listed number again before its scrub expires (D4), so none drops off a list.

Targets: phones on any of the eight lists whose newest scrub is missing or at least GHL_LIST_RESCRUB_AFTER_DAYS old,
except LendingFlow leads (no scrub needed, client decision) and suppressed numbers. A scrub costs 1 Tracerfy credit
per number; ``max_credits`` is a hard cap, checked before each batch. A DNC hit is stored (``lending.dnc_scrubs``)
and the gate then keeps that number off the lists; a litigator is suppressed by ``_scrub``; DNC-blocked numbers get
the nurture flag, as in the weekly scrub. A failed Tracerfy batch spends nothing; the numbers are retried next hour.

Usage (cron, hourly; idle until LENDING_GHL_LIST_SCRUB_ENABLED=true):
    python -m src.lending.ghl_list_scrub                      # cap = LENDING_GHL_LIST_SCRUB_MAX_CREDITS
    python -m src.lending.ghl_list_scrub --max-credits 200
    python -m src.lending.ghl_list_scrub --dry-run            # count numbers due, spend nothing
"""
from __future__ import annotations

import argparse
import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import text

from config.lending_compliance import GHL_LIST_RESCRUB_AFTER_DAYS, GHL_LIST_SCRUB_LOCK_KEY
from config.settings import get_settings
from src.lending.compliance import (
    NURTURE_REASONS,
    Scrubber,
    _flag_nurture,
    _scrub,
    _stamp_contacts,
    _stored_scrubs,
    _verdict,
    lendingflow_lead_phones,
    suppressed_phones,
    tracerfy_scrub,
)

logger = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = 500
EXIT_FAILED = 1
EXIT_SCRUB_FAILED = 3
EXIT_CAP_REACHED = 4


@dataclass
class ListScrubResult:
    due: int = 0
    scrubbed: int = 0
    blocked: int = 0
    credits_used: int = 0
    aborted: bool = False
    left_unscrubbed: int = 0
    scrub_batch_failures: int = 0


def due_phones(db, phones: Sequence[str], *, now: datetime) -> list[str]:
    """Listed numbers that need a scrub now: no scrub, or one at least GHL_LIST_RESCRUB_AFTER_DAYS old.
    LendingFlow leads and suppressed numbers are never scrubbed."""
    phones = sorted(set(phones))
    if not phones:
        return []
    skip = suppressed_phones(db, phones) | lendingflow_lead_phones(db, phones)
    candidates = [p for p in phones if p not in skip]
    if not candidates:
        return []
    cutoff = now - timedelta(days=GHL_LIST_RESCRUB_AFTER_DAYS)
    scrubs = _stored_scrubs(db, candidates)
    return [p for p in candidates if p not in scrubs or scrubs[p].checked_at <= cutoff]


def list_scrub(db, phones: Sequence[str], *, scrubber: Scrubber, max_credits: int,
               batch_size: int = DEFAULT_BATCH_SIZE, now: Optional[datetime] = None) -> ListScrubResult:
    """Scrub the listed numbers that are due. Does not commit."""
    now = now or datetime.now(timezone.utc)
    targets = due_phones(db, phones, now=now)
    result = ListScrubResult(due=len(targets))
    for start in range(0, len(targets), batch_size):
        batch = targets[start:start + batch_size]
        if result.credits_used + len(batch) > max_credits:
            result.aborted = True
            result.left_unscrubbed = len(targets) - start
            logger.warning("[ghl-list-scrub] credit cap %d reached after %d credits; %d number(s) left for a later run",
                           max_credits, result.credits_used, result.left_unscrubbed)
            break
        scrubs = _scrub(db, batch, scrubber)
        if not scrubs:
            # _scrub returns {} only when the vendor call raised (logged there): nothing billed, retried next run.
            result.scrub_batch_failures += 1
            logger.error("[ghl-list-scrub] scrub batch of %d number(s) returned no results; 0 credits charged",
                         len(batch))
            continue
        result.credits_used += len(batch)
        _stamp_contacts(db, scrubs)
        result.scrubbed += len(scrubs)
        verdicts = {p: _verdict(p, s) for p, s in scrubs.items()}
        blocked = [p for p, v in verdicts.items() if not v.allowed]
        result.blocked += len(blocked)
        _flag_nurture(db, [p for p in blocked if verdicts[p].reason in NURTURE_REASONS])
    logger.info("[ghl-list-scrub] due=%d scrubbed=%d blocked=%d credits=%d aborted=%s left_unscrubbed=%d "
                "scrub_batch_failures=%d", result.due, result.scrubbed, result.blocked, result.credits_used,
                result.aborted, result.left_unscrubbed, result.scrub_batch_failures)
    return result


def _listed_phones() -> tuple[list[str], list[str]]:
    from src.lending.ghl_account import lending_ghl_account
    from src.lending.ghl_call_gate import GhlCallLists, read_listed_contacts

    account = lending_ghl_account()
    if account is None:
        raise RuntimeError("the Next Deal Lending GHL account is not configured")
    contacts, failed_lists = read_listed_contacts(GhlCallLists(account))
    return sorted({c.phone for c in contacts if c.phone}), failed_lists


def main(argv: list[str] | None = None) -> int:
    from src.lending.db import lending_session

    parser = argparse.ArgumentParser(description="DNC scrub of the numbers on Josh's GHL call lists.")
    parser.add_argument("--max-credits", type=int, help="hard Tracerfy credit cap for this run")
    parser.add_argument("--dry-run", action="store_true", help="count numbers due, spend nothing")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    args = parser.parse_args(argv)
    settings = get_settings()
    if not args.dry_run and not settings.lending_ghl_list_scrub_enabled:
        logger.info("[ghl-list-scrub] idle: LENDING_GHL_LIST_SCRUB_ENABLED is off")
        return 0
    try:
        phones, failed_lists = _listed_phones()
    except RuntimeError as exc:
        logger.error("[ghl-list-scrub] not run: %s", exc)
        return EXIT_FAILED

    with lending_session() as db:
        if args.dry_run:
            due = len(due_phones(db, phones, now=datetime.now(timezone.utc)))
            logger.info("[ghl-list-scrub] dry run: %d listed number(s), %d due (%d credits), unread lists=%s",
                        len(phones), due, due, failed_lists)
            return 0
        if not db.execute(text("SELECT pg_try_advisory_xact_lock(:k)"), {"k": GHL_LIST_SCRUB_LOCK_KEY}).scalar():
            logger.info("[ghl-list-scrub] another run holds the lock; skipped")
            return 0
        max_credits = args.max_credits if args.max_credits is not None else settings.lending_ghl_list_scrub_max_credits
        result = list_scrub(db, phones, scrubber=tracerfy_scrub, max_credits=max_credits, batch_size=args.batch_size)
    if failed_lists:
        logger.error("[ghl-list-scrub] list(s) not read, their numbers were not checked: %s", failed_lists)
        return EXIT_FAILED
    if result.aborted:
        return EXIT_CAP_REACHED
    return EXIT_SCRUB_FAILED if result.scrub_batch_failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
