"""R1 1.1 Done-when check: one borrower in GHL, BatchDialer and Instantly is one card with every touch once.

Runs on the shared database inside ONE transaction that is always rolled back (it never commits):
  1. the full sync (as the cron would run it, but uncommitted);
  2. a real contact known to all three systems (a GHL contact id, a BatchDialer call, an Instantly reply), shown with
     the live GHL and Instantly reads;
  3. if no real contact is in all three, a synthetic borrower (extra readers + stub live sources), said so plainly.
Output: the contact id and the card (phones are last 4 only). Record the run on screen with timestamps.

Usage:
    PYTHONPATH=. python scripts/harness/contact_spine_done_when.py
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Iterator, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from config.settings import get_settings
from src.lending.cora_tools import phone_flags
from src.lending.spine.card import find_contacts, finish_card, live_sources, load_card
from src.lending.spine.contact_spine import sync_contact_spine
from src.lending.spine.identity import (
    SourceRecord, email_identifier, entity_identifier, external_identifier, phone_identifier,
)
from src.lending.spine.sources import SOURCE_READERS
from src.lending.spine.touches import ContactKeys, Touch

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger("contact_spine_done_when")

_IN_ALL_THREE = text("""
    SELECT s.contact_id FROM lending.contact_sources s
     GROUP BY s.contact_id
    HAVING bool_or(s.source = 'instantly_replies')
       AND bool_or(s.source = 'call_dispositions' AND s.source_ref NOT LIKE 'ghl:%')
       AND EXISTS (SELECT 1 FROM lending.contact_identifiers i
                    WHERE i.contact_id = s.contact_id AND i.kind = 'ghl_contact')
     ORDER BY s.contact_id LIMIT 1""")

_SYNTHETIC_PHONE = "+18135550188"


def _synthetic_records(_db: Session) -> Iterator[SourceRecord]:
    phone, email = phone_identifier(_SYNTHETIC_PHONE), email_identifier("done-when@example.com")
    yield SourceRecord("lendingflow_leads", "done-when-1", "Test Borrower",
                       frozenset({phone, email, external_identifier("ghl_contact", "done-when-ghl")}))
    yield SourceRecord("dialer_load_records", "done-when-2", "TEST BORROWER",
                       frozenset({phone, external_identifier("batchdialer_contact", "done-when-bd")}))
    yield SourceRecord("instantly_replies", "done-when-3", "Test", frozenset({email}))
    yield SourceRecord("loan_files", "done-when-4", "Test Borrower",
                       frozenset({entity_identifier("Test Holdings LLC"), email}))


class _Stub:
    def __init__(self, system: str, touches: list[Touch]) -> None:
        self.system, self.configured, self._touches = system, True, touches

    def touches(self, keys: ContactKeys) -> list[Touch]:
        return self._touches


def _synthetic_sources(now: datetime) -> list:
    return [
        _Stub("ghl", [Touch(now - timedelta(hours=1), "ghl", "text", "outbound", "delivered", "ghl_msg:dw1")]),
        _Stub("batchdialer", [Touch(now - timedelta(hours=2), "batchdialer", "call", "outbound", "CONNECTED",
                                    "call:dw-call")]),
        _Stub("instantly", [Touch(now - timedelta(hours=3), "instantly", "email", "inbound", None, "instantly:dw1"),
                            Touch(now - timedelta(hours=3), "instantly", "email", "inbound", None, "instantly:dw1")]),
    ]


def _show(label: str, card: Optional[dict]) -> None:
    logger.info("=== %s ===\n%s", label, json.dumps(card, indent=2, default=str))


def main() -> int:
    engine = create_engine(get_settings().database_url, pool_pre_ping=True)
    now = datetime.now(timezone.utc)
    with engine.connect() as conn:
        transaction = conn.begin()
        db = Session(bind=conn)
        db.commit = lambda: (_ for _ in ()).throw(RuntimeError("the Done-when check never commits"))  # type: ignore[method-assign]
        try:
            dry = sync_contact_spine(db, now=now, dry_run=True)
            logger.info("dry run: %s", dry)
            result = sync_contact_spine(db, now=now)
            logger.info("sync (uncommitted): %s", result)
            if result.failed_sources:
                logger.error("sources unreadable, nothing to show: %s", result.failed_sources)
                return 1
            real = db.execute(_IN_ALL_THREE).scalar()
            if real is not None:
                loaded = load_card(db, real, flags=phone_flags)
                _show(f"real contact {real} (live GHL + Instantly)", finish_card(loaded, remote=live_sources()))
                return 0
            logger.info("no real contact is in GHL, BatchDialer and Instantly; showing a synthetic borrower")
            sync_contact_spine(db, now=now, readers=SOURCE_READERS + (("synthetic", _synthetic_records),))
            (contact_id,) = find_contacts(db, _SYNTHETIC_PHONE)
            loaded = load_card(db, contact_id, flags=lambda _db, phones: {})
            _show(f"synthetic contact {contact_id} (stub live sources)",
                  finish_card(loaded, remote=_synthetic_sources(now)))
            return 0
        finally:
            db.close()
            transaction.rollback()
            logger.info("rolled back: nothing was written")


if __name__ == "__main__":
    raise SystemExit(main())
