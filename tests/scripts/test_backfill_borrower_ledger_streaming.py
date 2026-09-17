"""
Regression test: the borrower-ledger backfill streams query results instead
of loading the full source table into memory before batching.

Before the fix both source-query paths called .mappings().all() up front,
so batching only limited writes -- it did nothing to bound memory or let
processing start before the whole result set had been fetched.

Reads and writes now go through separate sessions (read_session owns the
yield_per cursor, write_session issues commits) so periodic commits never
invalidate the streaming cursor -- see
tests/scenarios/test_backfill_borrower_ledger_cursor.py for the real-DB
regression test that a MagicMock session can't express.
"""
from __future__ import annotations

from unittest.mock import MagicMock

from scripts.backfill_borrower_ledger import _backfill_deed_acquisitions, _backfill_via_owner_link


def test_deed_backfill_uses_server_side_streaming_not_fetchall():
    read_session = MagicMock()
    read_session.execute.return_value.mappings.return_value = []
    write_session = MagicMock()

    _backfill_deed_acquisitions(read_session, write_session, dry_run=True)

    text_clause = read_session.execute.call_args[0][0]
    assert text_clause._execution_options.get("yield_per"), (
        "expected the deed query to set yield_per for server-side streaming"
    )
    write_session.commit.assert_not_called()


def test_owner_link_backfill_uses_server_side_streaming_not_fetchall():
    read_session = MagicMock()
    read_session.execute.return_value.mappings.return_value = []
    write_session = MagicMock()

    _backfill_via_owner_link(
        read_session, write_session, dry_run=True,
        source_table="foreclosures", event_type="foreclosure_filed",
        query="SELECT 1 AS id, 1 AS property_id, NULL AS buyer_entity_id",
    )

    text_clause = read_session.execute.call_args[0][0]
    assert text_clause._execution_options.get("yield_per"), (
        "expected the generic owner-link query to set yield_per for server-side streaming"
    )
    write_session.commit.assert_not_called()
