"""Weekly DNC / litigator re-scrub of numbers loaded in the dialer (Go Live G10).

Loaded numbers whose newest scrub would turn stale for the dialer sweep before the next
weekly run (``WEEKLY_RESCRUB_AFTER_DAYS``; every loaded number at a 7-day cron and a
7-day freshness window) are sent to Tracerfy (1 credit each). ``max_credits`` is a hard cap:
the run stops before any batch that would exceed it. A number that now fails a scrub leaves
the dialer for good (reason ``opt_out`` = the vendor's permanent DNC list) and its load row
closes. A removal that fails (or has no dialer configured) leaves the load row open; the next
run retries it without re-scrubbing, and the run exits non-zero until every flagged number is out.

Usage:
    python -m src.lending.weekly_scrub --max-credits 5000
    python -m src.lending.weekly_scrub --dry-run          # count only, no credits
"""
from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import text

from src.services.phone_utils import normalize as normalize_phone

from config.lending_compliance import WEEKLY_RESCRUB_AFTER_DAYS, RemovalReason
from src.lending.compliance import (
    DialerRemover,
    NURTURE_REASONS,
    Scrubber,
    _flag_nurture,
    _remove_from_dialer,
    _scrub,
    _stamp_contacts,
    _stored_scrubs,
    _verdict,
    tracerfy_scrub,
)

logger = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = 500
EXIT_SCRUB_FAILED = 3
EXIT_CAP_REACHED = 4
EXIT_REMOVAL_FAILED = 5


@dataclass
class WeeklyScrubResult:
    scrubbed: int = 0
    blocked: int = 0
    credits_used: int = 0
    aborted: bool = False
    left_unscrubbed: int = 0  # not reached under the cap: scrub overdue, blocked at dial time by the sweep
    removal_failed: int = 0  # flagged numbers the dialer removal did not take out
    scrub_batch_failures: int = 0  # the vendor call itself failed; no credits spent, nothing verdicted


def _loaded_phones(db) -> list[str]:
    raw = db.execute(text(
        "SELECT DISTINCT phone FROM lending.dialer_load_records WHERE active AND phone IS NOT NULL"
    )).scalars().all()
    return sorted({p for p in (normalize_phone(r) for r in raw) if p})


def blocked_loaded_phones(db) -> list[str]:
    """Loaded numbers already known to be blocked (suppressed, or a stored scrub verdict
    fails) whose dialer removal has not landed. An earlier run flagged them but the removal
    failed; a fresh scrub would otherwise hide them for a week, so they are retried here
    without spending a Tracerfy credit."""
    loaded = _loaded_phones(db)
    if not loaded:
        return []
    suppressed = set(db.execute(
        text("SELECT phone FROM lending.suppression_list WHERE phone = ANY(:phones)"),
        {"phones": loaded},
    ).scalars().all())
    blocked = {p for p, scrub in _stored_scrubs(db, loaded).items() if not _verdict(p, scrub).allowed}
    return sorted(suppressed | blocked)


def stale_loaded_phones(db, *, now: Optional[datetime] = None) -> list[str]:
    now = now or datetime.now(timezone.utc)
    loaded = _loaded_phones(db)
    if not loaded:
        return []
    cutoff = now - timedelta(days=WEEKLY_RESCRUB_AFTER_DAYS)
    stored = _stored_scrubs(db, loaded)
    return [p for p in loaded if p not in stored or stored[p].checked_at < cutoff]


def _close_load_rows(db, phones: list[str]) -> None:
    phones = sorted({p for p in (normalize_phone(x) for x in phones) if p})
    if not phones:
        return
    db.execute(
        text(
            "UPDATE lending.dialer_load_records SET active = false, deactivated_at = now(), "
            "deactivation_reason = 'weekly_scrub_dnc' WHERE active AND phone = ANY(:phones)"
        ),
        {"phones": phones},
    )


def _take_out_of_dialer(db, blocked: list[str], dialer_remover: Optional[DialerRemover],
                        result: WeeklyScrubResult, *, retry: bool = False) -> None:
    removed = _remove_from_dialer(blocked, dialer_remover, RemovalReason.OPT_OUT, retry=retry)
    _close_load_rows(db, list(removed))
    result.removal_failed += len(blocked) - len(removed)


def weekly_scrub(
    db,
    *,
    scrubber: Scrubber,
    max_credits: int,
    batch_size: int = DEFAULT_BATCH_SIZE,
    now: Optional[datetime] = None,
    phones: Optional[list[str]] = None,
    dialer_remover: Optional[DialerRemover] = None,
) -> WeeklyScrubResult:
    """Does not commit; the caller commits after each successful run."""
    targets = phones if phones is not None else stale_loaded_phones(db, now=now)
    result = WeeklyScrubResult()
    if phones is None:
        due = set(targets)
        retry = [p for p in blocked_loaded_phones(db) if p not in due]
        if retry:
            logger.warning("[weekly-scrub] retrying the dialer removal of %d previously flagged number(s)", len(retry))
            _take_out_of_dialer(db, retry, dialer_remover, result, retry=True)
    for start in range(0, len(targets), batch_size):
        batch = targets[start:start + batch_size]
        if result.credits_used + len(batch) > max_credits:
            result.aborted = True
            result.left_unscrubbed = len(targets) - start
            logger.warning("[weekly-scrub] credit cap %d reached after %d credits; %d number(s) left "
                           "unscrubbed until a later run scrubs them",
                           max_credits, result.credits_used, result.left_unscrubbed)
            break
        scrubs = _scrub(db, batch, scrubber)
        if not scrubs:
            # _scrub returns {} only when the vendor call itself raised (already logged at
            # ERROR there); a legitimate all-miss response still returns one ScrubResult per
            # phone. Nothing was billed and these phones stay stale until a later run succeeds.
            result.scrub_batch_failures += 1
            logger.error("[weekly-scrub] scrub batch of %d phone(s) returned no results; "
                         "0 credits charged, phones remain stale", len(batch))
            continue
        result.credits_used += len(batch)
        _stamp_contacts(db, scrubs)
        result.scrubbed += len(scrubs)
        verdicts = {p: _verdict(p, s) for p, s in scrubs.items()}
        blocked = [p for p, v in verdicts.items() if not v.allowed]
        if not blocked:
            continue
        result.blocked += len(blocked)
        _flag_nurture(db, [p for p in blocked if verdicts[p].reason in NURTURE_REASONS])
        _take_out_of_dialer(db, blocked, dialer_remover, result)
    logger.info("[weekly-scrub] scrubbed=%d blocked=%d credits=%d aborted=%s left_unscrubbed=%d "
                "removal_failed=%d scrub_batch_failures=%d",
                result.scrubbed, result.blocked, result.credits_used, result.aborted, result.left_unscrubbed,
                result.removal_failed, result.scrub_batch_failures)
    return result


def main(argv: list[str] | None = None) -> int:
    from src.core.database import get_db_context

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Weekly re-scrub of loaded dialer numbers.")
    parser.add_argument("--max-credits", type=int, help="hard Tracerfy credit cap for this run")
    parser.add_argument("--dry-run", action="store_true", help="count numbers due, spend nothing")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    args = parser.parse_args(argv)
    if args.dry_run:
        with get_db_context() as session:
            due = len(stale_loaded_phones(session))
            retry = len(blocked_loaded_phones(session))
        logger.info("[weekly-scrub] dry run: %d number(s) would be scrubbed (%d credits), %d flagged number(s) "
                    "still to take out of the dialer", due, due, retry)
        return 0
    if args.max_credits is None:
        parser.error("--max-credits is required unless --dry-run")
    with get_db_context() as session:
        result = weekly_scrub(session, scrubber=tracerfy_scrub, max_credits=args.max_credits,
                              batch_size=args.batch_size)
        session.commit()
    if result.removal_failed:
        logger.error("[weekly-scrub] %d flagged number(s) are still in the dialer", result.removal_failed)
        return EXIT_REMOVAL_FAILED
    if result.aborted:
        return EXIT_CAP_REACHED
    return EXIT_SCRUB_FAILED if result.scrub_batch_failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
