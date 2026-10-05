"""lending.list_catalog is documentation for Josh's List 1-9 taxonomy — it must
never drift from the code that actually assigns/consumes a list_key."""
from __future__ import annotations

import os

import pytest
from sqlalchemy import create_engine, text

from config.lending_list_catalog import LIST_CATALOG
from config.lending_queues import SOURCE_TAG_QUEUES
from src.services.lending import pool_extraction as pe


def test_every_catalog_key_with_a_pool_matches_source_tag_for():
    """Whatever source_tag_for() actually assigns must be exactly what the
    catalog documents for that key, so a reader can trust the table."""
    stalled_pools = {"list_9": True}
    for key, entry in LIST_CATALOG.items():
        if entry.pool_name is None:
            continue  # F1 gap: no extractor yet
        stalled = stalled_pools.get(key, False)
        assert pe.source_tag_for(entry.pool_name, "irrelevant", stalled=stalled) == key, (
            f"{key} documents pool_name={entry.pool_name!r} but source_tag_for() "
            f"doesn't return {key!r} for it"
        )


def test_every_catalog_queue_matches_the_real_queue_mapping():
    for key, entry in LIST_CATALOG.items():
        assert SOURCE_TAG_QUEUES.get(key) == entry.queue, (
            f"{key} documents queue={entry.queue!r} but SOURCE_TAG_QUEUES says "
            f"{SOURCE_TAG_QUEUES.get(key)!r}"
        )


def test_catalog_covers_every_queue_mapped_source_tag():
    assert set(SOURCE_TAG_QUEUES) <= set(LIST_CATALOG)


@pytest.mark.skipif(not os.environ.get("DATABASE_URL"), reason="requires a live Postgres DATABASE_URL")
def test_migration_upserts_every_catalog_entry_and_is_idempotent():
    from migrations.apply_lending_list_catalog import run

    engine = create_engine(os.environ["DATABASE_URL"])
    conn = engine.connect()
    tx = conn.begin()
    try:
        from sqlalchemy.orm import Session
        session = Session(bind=conn)

        class _Ctx:
            def __enter__(self):
                return session

            def __exit__(self, *exc):
                return False

        import migrations.apply_lending_list_catalog as mod
        import pytest as _pytest

        orig_get_db_context = mod.get_db_context
        mod.get_db_context = lambda: _Ctx()
        try:
            run()
            run()  # idempotent: same 9 rows, no duplicate-key error
        finally:
            mod.get_db_context = orig_get_db_context

        rows = session.execute(text(
            "SELECT list_key, display_name, pool_name, queue FROM lending.list_catalog ORDER BY list_key"
        )).fetchall()
        assert len(rows) == len(LIST_CATALOG)
        for row in rows:
            entry = LIST_CATALOG[row.list_key]
            assert (row.display_name, row.pool_name, row.queue) == (entry.display_name, entry.pool_name, entry.queue)
    finally:
        tx.rollback()
        conn.close()
        engine.dispose()
