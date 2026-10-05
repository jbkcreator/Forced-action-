"""Weekly DNC / litigator re-scrub of numbers loaded in the dialer (Go Live G10).

Only loaded numbers whose newest scrub is older than the freshness window are sent
to Tracerfy (1 credit each). ``max_credits`` is a hard cap: the run stops before any
batch that would exceed it. A number that now fails a scrub leaves the dialer for
good (reason ``opt_out`` = the vendor's permanent DNC list) and its load row closes.

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

from config.lending_compliance import DNC_SCRUB_MAX_AGE_DAYS, RemovalReason
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
EXIT_CAP_REACHED = 4
EXIT_SCRUB_FAILED = 3


@dataclass
class WeeklyScrubResult:
    scrubbed: int = 0
    blocked: int = 0
    credits_used: int = 0
    aborted: bool = False
    left_unscrubbed: int = 0  # not reached under the cap: blocked at dial time until rescrubbed
    scrub_batch_failures: int = 0  # the vendor call itself failed; no credits spent, nothing verdicted


def stale_loaded_phones(db, *, now: Optional[datetime] = None) -> list[str]:
    now = now or datetime.now(timezone.utc)
    raw = db.execute(text(
        "SELECT DISTINCT phone FROM lending.dialer_load_records WHERE active AND phone IS NOT NULL"
    )).scalars().all()
    loaded = sorted({p for p in (normalize_phone(r) for r in raw) if p})
    if not loaded:
        return []
    cutoff = now - timedelta(days=DNC_SCRUB_MAX_AGE_DAYS)
    stored = _stored_scrubs(db, loaded)
    return [p for p in loaded if p not in stored or stored[p].checked_at < cutoff]


def pending_removal_phones(db) -> list[str]:
    """Loaded phones already known to be blocked (suppressed, or a stored scrub verdict
    says blocked) whose dialer removal has not yet succeeded.

    A phone only reaches this state when a prior ``_remove_from_dialer`` call failed:
    success always closes its load row (``_close_load_rows``), so an active row plus a
    blocked verdict means the removal itself is still outstanding. Because the verdict
    is already known, retrying costs no Tracerfy credit and the phone would otherwise
    never resurface — ``stale_loaded_phones`` only selects phones with no fresh scrub,
    and this one's scrub (the one that found it blocked) is fresh.
    """
    raw = db.execute(text(
        "SELECT DISTINCT phone FROM lending.dialer_load_records WHERE active AND phone IS NOT NULL"
    )).scalars().all()
    loaded = sorted({p for p in (normalize_phone(r) for r in raw) if p})
    if not loaded:
        return []
    suppressed = db.execute(
        text("SELECT phone FROM lending.suppression_list WHERE phone = ANY(:phones)"),
        {"phones": loaded},
    ).scalars().all()
    pending = set(suppressed)
    for phone, scrub in _stored_scrubs(db, loaded).items():
        if not _verdict(phone, scrub).allowed:
            pending.add(phone)
    return sorted(pending)


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
    result = WeeklyScrubResult()
    # Retry removals a prior run already knows are due (no new scrub, no credit spent) —
    # these never appear in stale_loaded_phones once their blocking scrub is fresh.
    pending = pending_removal_phones(db)
    if pending:
        removed = _remove_from_dialer(pending, dialer_remover, RemovalReason.OPT_OUT, retry=True)
        if removed:
            result.blocked += len(removed)
            _close_load_rows(db, list(removed))

    targets = phones if phones is not None else stale_loaded_phones(db, now=now)
    for start in range(0, len(targets), batch_size):
        batch = targets[start:start + batch_size]
        if result.credits_used + len(batch) > max_credits:
            result.aborted = True
            result.left_unscrubbed = len(targets) - start
            logger.warning("[weekly-scrub] credit cap %d reached after %d credits; %d number(s) left "
                           "unscrubbed and blocked from dialing until rescrubbed",
                           max_credits, result.credits_used, result.left_unscrubbed)
            break
        scrubs = _scrub(db, batch, scrubber)
        if not scrubs:
            # _scrub returns {} only when the vendor call itself raised (already logged
            # at ERROR there) — a legitimate all-miss response still returns one
            # ScrubResult per phone. Nothing was actually billed, and these phones stay
            # stale (blocked at dial time by dial_blocks/_hold_reason) until a later
            # run's scrub succeeds, instead of being silently skipped forever.
            result.scrub_batch_failures += 1
            logger.error("[weekly-scrub] scrub batch of %d phone(s) returned no results; "
                         "0 credits charged, phones remain stale and blocked at dial time", len(batch))
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
        removed = _remove_from_dialer(blocked, dialer_remover, RemovalReason.OPT_OUT)
        _close_load_rows(db, list(removed))
    logger.info("[weekly-scrub] scrubbed=%d blocked=%d credits=%d aborted=%s left_unscrubbed=%d",
                result.scrubbed, result.blocked, result.credits_used, result.aborted, result.left_unscrubbed)
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
        logger.info("[weekly-scrub] dry run: %d number(s) would be scrubbed (%d credits)", due, due)
        return 0
    if args.max_credits is None:
        parser.error("--max-credits is required unless --dry-run")
    with get_db_context() as session:
        result = weekly_scrub(session, scrubber=tracerfy_scrub, max_credits=args.max_credits,
                              batch_size=args.batch_size)
        session.commit()
    if result.aborted:
        return EXIT_CAP_REACHED
    if result.scrub_batch_failures:
        return EXIT_SCRUB_FAILED
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
