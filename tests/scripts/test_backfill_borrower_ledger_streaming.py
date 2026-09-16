"""
Regression test: the borrower-ledger backfill streams query results instead
of loading the full source table into memory before batching.

Before the fix both source-query paths called .mappings().all() up front,
so batching only limited writes -- it did nothing to bound memory or let
processing start before the whole result set had been fetched.
"""
from __future__ import annotations

from unittest.mock import MagicMock

from scripts.backfill_borrower_ledger import _backfill_deed_acquisitions, _backfill_via_owner_link


def test_deed_backfill_uses_server_side_streaming_not_fetchall():
    session = MagicMock()
    session.execute.return_value.mappings.return_value = []

    _backfill_deed_acquisitions(session, dry_run=True)

    text_clause = session.execute.call_args[0][0]
    assert text_clause._execution_options.get("yield_per"), (
        "expected the deed query to set yield_per for server-side streaming"
    )


def test_owner_link_backfill_uses_server_side_streaming_not_fetchall():
    session = MagicMock()
    session.execute.return_value.mappings.return_value = []

    _backfill_via_owner_link(
        session, dry_run=True,
        source_table="foreclosures", event_type="foreclosure_filed",
        query="SELECT 1 AS id, 1 AS property_id, NULL AS buyer_entity_id",
    )

    text_clause = session.execute.call_args[0][0]
    assert text_clause._execution_options.get("yield_per"), (
        "expected the generic owner-link query to set yield_per for server-side streaming"
    )
