"""
Regression test: the borrower-ledger backfill must not commit on the same
session that owns the server-side yield_per cursor.

Before the fix, _backfill_deed_acquisitions/_backfill_via_owner_link streamed
rows via session.execute(...).execution_options(yield_per=N) (a server-side
cursor scoped to the current transaction) and then called session.commit()
from inside the same for-loop that was still consuming that cursor. Committing
ends the transaction the cursor lives in, so Postgres/psycopg2 invalidates it
-- the next fetch raises (InvalidCursorName), aborting the run past the first
batch. This only reproduces against a real DB with a real server-side cursor
and more rows than one commit batch; a MagicMock session (see
tests/scripts/test_backfill_borrower_ledger_streaming.py) can't catch it.

Run:
    pytest tests/scenarios/test_backfill_borrower_ledger_cursor.py -v -m scenario
"""
from __future__ import annotations

import uuid
from datetime import date

import pytest
from sqlalchemy import delete, text

from src.core.database import get_db_context
from src.core.models import BuyerEntity, Property
from src.services.borrower_ledger import get_summary

pytestmark = pytest.mark.scenario

_COUNTY = "ztest-backfill-cursor-hillsborough"


def _uid() -> str:
    return uuid.uuid4().hex[:10]


@pytest.fixture
def seeded_deeds():
    """Seed one buyer_entity plus N deeds (each linked via buyer_entity_links)
    -- more than one commit batch so the bug (if reintroduced) reproduces."""
    n_rows = 5
    with get_db_context() as session:
        entity = BuyerEntity(
            canonical_name=f"Cursor Test LLC {_uid()}",
            entity_type="LLC",
            confidence_score=95,
            verification_status="verified",
            county_id=_COUNTY,
        )
        session.add(entity)
        session.flush()

        deed_ids = []
        for i in range(n_rows):
            prop = Property(parcel_id=f"ztest-{_uid()}", zip="33565", county_id=_COUNTY)
            session.add(prop)
            session.flush()

            deed_id = session.execute(
                text("""
                    INSERT INTO deeds
                        (property_id, instrument_number, grantee, record_date, deed_type, sale_price)
                    VALUES (:pid, :inst, 'Cursor Test Grantee', :rdate, 'Warranty Deed', 100000)
                    RETURNING id
                """),
                {"pid": prop.id, "inst": f"ZTEST-{_uid()}", "rdate": date(2026, 1, i + 1)},
            ).scalar_one()
            deed_ids.append(deed_id)

            session.execute(
                text("""
                    INSERT INTO buyer_entity_links (buyer_entity_id, source_table, source_id, match_confidence, match_method)
                    VALUES (:eid, 'deeds', :did, 90, 'manual')
                """),
                {"eid": entity.id, "did": deed_id},
            )
        session.commit()
        entity_id = entity.id

    yield entity_id, deed_ids, n_rows

    with get_db_context() as session:
        session.execute(delete(BuyerEntity).where(BuyerEntity.county_id == _COUNTY))
        session.execute(text("DELETE FROM properties WHERE county_id = :c"), {"c": _COUNTY})
        session.commit()


def test_deed_backfill_survives_commit_past_first_batch(seeded_deeds, monkeypatch):
    """With _COMMIT_BATCH forced to 1 (so every row triggers a commit on
    write_session), all n_rows deeds must still be recorded -- the read_session
    streaming the deeds query must never be invalidated by write_session's
    commits."""
    import scripts.backfill_borrower_ledger as backfill_mod

    monkeypatch.setattr(backfill_mod, "_COMMIT_BATCH", 1)
    entity_id, deed_ids, n_rows = seeded_deeds

    with get_db_context() as read_session, get_db_context() as write_session:
        stats = backfill_mod._backfill_deed_acquisitions(read_session, write_session, dry_run=False)

    assert stats.errors == 0, stats.report()
    assert stats.attempted >= n_rows

    with get_db_context() as session:
        summary = get_summary(session, entity_id)
    assert summary["acquisitions"] == n_rows
