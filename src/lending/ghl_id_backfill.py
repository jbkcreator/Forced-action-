"""One-off: fill ``dialer_load_records.ghl_contact_id`` for loads already in GHL before the sync stored the id.

The GHL load sync records the contact id since migration ``apply_lending_dialer_load_ghl_contact_id``; rows pushed
earlier (``ghl_sync_status`` pushed / skipped_already_in_ghl) are looked up by phone in the Next Deal Lending GHL
sub-account (``POST /contacts/search``, one call per distinct phone) and updated in batches. A phone GHL does not know
is counted and left NULL. Read-only on GHL: nothing is created or changed there.

Dry run by default (counts the rows, calls nothing). ``--apply`` looks up and writes; ``--limit`` caps the phones.

    python -m src.lending.ghl_id_backfill [--apply] [--limit N]
"""
from __future__ import annotations

import argparse
import logging
import time
from collections import Counter
from typing import Callable, Iterable, Optional, Sequence

from sqlalchemy import text

logger = logging.getLogger(__name__)

BACKFILL_STATUSES = ("pushed", "skipped_already_in_ghl")
WRITE_BATCH = 200
CALL_GAP_SECONDS = 0.15  # stays under GHL's 100 requests / 10 s burst limit

_PENDING_PHONES = text("""
    SELECT DISTINCT phone FROM lending.dialer_load_records
     WHERE ghl_contact_id IS NULL AND ghl_sync_status = ANY(:statuses)
     ORDER BY phone
     LIMIT :limit
""")
_SET = text("""
    UPDATE lending.dialer_load_records AS d SET ghl_contact_id = v.ghl_id
      FROM unnest(CAST(:phones AS text[]), CAST(:ids AS text[])) AS v(phone, ghl_id)
     WHERE d.phone = v.phone AND d.ghl_contact_id IS NULL AND d.ghl_sync_status = ANY(:statuses)
""")

Finder = Callable[[str], Optional[str]]


def pending_phones(db, limit: int) -> list[str]:
    return [phone for (phone,) in db.execute(_PENDING_PHONES, {"statuses": list(BACKFILL_STATUSES), "limit": limit})]


def look_up(phones: Iterable[str], find: Finder, *, gap: float = CALL_GAP_SECONDS,
            sleep: Callable[[float], None] = time.sleep) -> tuple[dict[str, str], Counter]:
    """Phone -> GHL id for the phones GHL knows. A failed lookup is counted and skipped (the next run retries it)."""
    found: dict[str, str] = {}
    counts: Counter = Counter()
    for index, phone in enumerate(phones):
        if index and gap:
            sleep(gap)
        try:
            ghl_id = find(phone)
        except Exception as exc:  # class only: GHL errors can embed the request
            logger.warning("[ghl-id-backfill] lookup failed (%s)", type(exc).__name__)
            counts["failed"] += 1
            continue
        if ghl_id:
            found[phone] = ghl_id
            counts["found"] += 1
        else:
            counts["not_in_ghl"] += 1
    return found, counts


def write(db, found: dict[str, str]) -> int:
    """Write in batches of WRITE_BATCH; returns rows updated (a phone can have more than one load row)."""
    items = list(found.items())
    updated = 0
    for start in range(0, len(items), WRITE_BATCH):
        batch = items[start:start + WRITE_BATCH]
        updated += db.execute(_SET, {"phones": [p for p, _ in batch], "ids": [i for _, i in batch],
                                     "statuses": list(BACKFILL_STATUSES)}).rowcount or 0
        db.commit()
    return updated


def main(argv: Optional[Sequence[str]] = None) -> int:
    from src.lending.bd_result_to_ghl import LendingGhlNoteWriter
    from src.lending.db import lending_session
    from src.lending.ghl_account import lending_ghl_account

    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description="Backfill GHL contact ids on dialer load records")
    parser.add_argument("--apply", action="store_true", help="look up in GHL and write (default: count only)")
    parser.add_argument("--limit", type=int, default=100_000, help="max distinct phones this run")
    args = parser.parse_args(argv)

    with lending_session() as db:
        phones = pending_phones(db, args.limit)
    if not args.apply:
        logger.info("[ghl-id-backfill] dry run: %d phone(s) to look up; nothing called", len(phones))
        return 0
    account = lending_ghl_account()
    if account is None:
        logger.error("[ghl-id-backfill] no Next Deal Lending GHL account configured")
        return 1
    find = LendingGhlNoteWriter(account).find_contact_id
    counts: Counter = Counter()
    updated = 0
    for start in range(0, len(phones), WRITE_BATCH):  # saved per batch: a stopped run keeps what it found
        found, batch_counts = look_up(phones[start:start + WRITE_BATCH], find)  # no transaction held meanwhile
        counts.update(batch_counts)
        with lending_session() as db:
            updated += write(db, found)
        logger.info("[ghl-id-backfill] %d/%d phones done, rows_updated=%d", min(start + WRITE_BATCH, len(phones)),
                    len(phones), updated)
    logger.info("[ghl-id-backfill] phones=%d found=%d not_in_ghl=%d failed=%d rows_updated=%d", len(phones),
                counts["found"], counts["not_in_ghl"], counts["failed"], updated)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
