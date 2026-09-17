"""
Regression tests for the buyer_entity merge/unmerge data-loss bug (WP-4, WI-1).

Before the fix, unmerge_entity() identified links to restore via
`linked_at <= merged_at`, which is ALSO true of the surviving entity's own
pre-existing links (they predate the merge too) -- so an unmerge could
reassign the survivor's original links to the restored entity, corrupting
both sides. The fix records moved_link_ids (the exact link IDs reassigned
at merge time) on buyer_entity_merge_log and restores by ID.

Runs against the real Postgres DB (get_db_context / DATABASE_URL), same
convention as test_hunter_resolution_fixes.py: every test seeds its own
rows under a sentinel county_id/name token and deletes everything in
teardown.

Coverage:
  - merge -> unmerge round trip: surviving entity's own pre-existing links
    are untouched by the unmerge; absorbed entity is restored with exactly
    its original links.
  - a merge_log row with moved_link_ids IS NULL (pre-fix data) raises
    ValueError on unmerge rather than guessing.
  - unmerging an already-reversed merge_log raises ValueError.

Run:
    pytest tests/scenarios/test_buyer_entity_merge.py -v -m scenario
"""
from __future__ import annotations

import uuid

import pytest
from sqlalchemy import delete, text

from src.core.database import get_db_context
from src.core.models import BuyerEntity, BuyerEntityLink, BuyerEntityMergeLog
from src.services.buyer_entity_merge import merge_entities, unmerge_entity

pytestmark = pytest.mark.scenario

_COUNTY = "ztest-merge-hillsborough"


def _uid() -> str:
    return uuid.uuid4().hex[:10]


def _make_entity(session, *, canonical_name: str, n_links: int) -> tuple[int, list[int]]:
    """Create a buyer_entities row with n_links buyer_entity_links pointed at
    distinct fake owners source_ids (never joined back to a real owners row --
    only source_table/source_id uniqueness matters here). Returns (entity_id,
    link_ids)."""
    entity = BuyerEntity(
        canonical_name=canonical_name,
        entity_type="Individual",
        confidence_score=95,
        verification_status="verified",
        county_id=_COUNTY,
    )
    session.add(entity)
    session.flush()

    link_ids: list[int] = []
    for _ in range(n_links):
        result = session.execute(
            text("""
                INSERT INTO buyer_entity_links
                    (buyer_entity_id, source_table, source_id, match_confidence, match_method)
                VALUES (:eid, 'owners', :sid, 90, 'manual')
                RETURNING id
            """),
            {"eid": entity.id, "sid": int(uuid.uuid4().int % 2_000_000_000)},
        )
        link_ids.append(result.scalar_one())
    session.commit()
    return entity.id, link_ids


def _cleanup(entity_ids: list[int], merge_log_ids: list[int]) -> None:
    with get_db_context() as session:
        if merge_log_ids:
            session.execute(delete(BuyerEntityMergeLog).where(BuyerEntityMergeLog.id.in_(merge_log_ids)))
        if entity_ids:
            session.execute(delete(BuyerEntityLink).where(BuyerEntityLink.buyer_entity_id.in_(entity_ids)))
            session.execute(delete(BuyerEntity).where(BuyerEntity.id.in_(entity_ids)))
        session.commit()


def test_merge_then_unmerge_preserves_survivor_own_links():
    """
    The core regression: entity A (survivor) has 2 of its own links, entity
    B (absorbed) has 3. Merge B into A -> A has 5 links. Unmerge -> A must
    have EXACTLY its original 2 links back (not fewer, not the 3 that came
    from B), and the restored entity must have exactly the 3 that were B's.
    """
    token = f"ZMERGE{_uid()}".upper()
    entity_ids: list[int] = []
    merge_log_ids: list[int] = []
    try:
        with get_db_context() as session:
            survivor_id, survivor_links = _make_entity(
                session, canonical_name=f"{token} SURVIVOR", n_links=2,
            )
            absorbed_id, absorbed_links = _make_entity(
                session, canonical_name=f"{token} ABSORBED", n_links=3,
            )
        entity_ids.extend([survivor_id, absorbed_id])

        with get_db_context() as session:
            log = merge_entities(
                session, surviving_id=survivor_id, absorbed_id=absorbed_id,
                merged_by="test:wp4", reason="regression test",
            )
            merge_log_id = log.id
            session.commit()
        merge_log_ids.append(merge_log_id)

        with get_db_context() as session:
            log_row = session.execute(
                text("SELECT links_moved, moved_link_ids FROM buyer_entity_merge_log WHERE id = :id"),
                {"id": merge_log_id},
            ).mappings().one()
            assert log_row["links_moved"] == 3
            assert set(log_row["moved_link_ids"]) == set(absorbed_links)

            post_merge_links = session.execute(
                text("SELECT id FROM buyer_entity_links WHERE buyer_entity_id = :id"),
                {"id": survivor_id},
            ).scalars().all()
            assert set(post_merge_links) == set(survivor_links) | set(absorbed_links)

            assert session.execute(
                text("SELECT 1 FROM buyer_entities WHERE id = :id"),
                {"id": absorbed_id},
            ).one_or_none() is None

        with get_db_context() as session:
            restored = unmerge_entity(session, merge_log_id=merge_log_id, reversed_by="test:wp4")
            restored_id = restored.id
            session.commit()
        entity_ids.append(restored_id)

        with get_db_context() as session:
            survivor_after = session.execute(
                text("SELECT id FROM buyer_entity_links WHERE buyer_entity_id = :id"),
                {"id": survivor_id},
            ).scalars().all()
            assert set(survivor_after) == set(survivor_links), (
                "unmerge must return the survivor to EXACTLY its own original links"
            )

            restored_links = session.execute(
                text("SELECT id FROM buyer_entity_links WHERE buyer_entity_id = :id"),
                {"id": restored_id},
            ).scalars().all()
            assert set(restored_links) == set(absorbed_links), (
                "restored entity must hold exactly the links that were moved"
            )

            log_row = session.execute(
                text("SELECT reversed_at, restored_id FROM buyer_entity_merge_log WHERE id = :id"),
                {"id": merge_log_id},
            ).mappings().one()
            assert log_row["reversed_at"] is not None
            assert log_row["restored_id"] == restored_id
    finally:
        _cleanup(entity_ids, merge_log_ids)


def test_merge_then_unmerge_moves_ledger_events_and_monitor_log():
    """
    Regression for the ledger/monitor data-loss bug: borrower_ledger_events
    and borrower_monitor_log both carry buyer_entity_id ON DELETE CASCADE,
    so merge_entities() must move those rows to the survivor BEFORE deleting
    the absorbed buyer_entities row, or the absorbed entity's whole history
    (and its monitor idempotency log) is destroyed instead of transferred.
    unmerge_entity() must move them back onto the restored entity.
    """
    token = f"ZMERGELEDGER{_uid()}".upper()
    entity_ids: list[int] = []
    merge_log_ids: list[int] = []
    ledger_event_ids: list[int] = []
    monitor_log_ids: list[int] = []
    try:
        with get_db_context() as session:
            survivor_id, _ = _make_entity(session, canonical_name=f"{token} SURVIVOR", n_links=1)
            absorbed_id, _ = _make_entity(session, canonical_name=f"{token} ABSORBED", n_links=1)
        entity_ids.extend([survivor_id, absorbed_id])

        with get_db_context() as session:
            ledger_event_id = session.execute(
                text("""
                    INSERT INTO borrower_ledger_events
                        (buyer_entity_id, event_type, event_date, source_table, source_id, summary)
                    VALUES (:eid, 'deed_acquisition', CURRENT_DATE, 'deeds', :sid, 'test event')
                    RETURNING id
                """),
                {"eid": absorbed_id, "sid": int(uuid.uuid4().int % 2_000_000_000)},
            ).scalar_one()
            monitor_log_id = session.execute(
                text("""
                    INSERT INTO borrower_monitor_log
                        (buyer_entity_id, monitor_type, source_event_id)
                    VALUES (:eid, 'loan_maturity', :sid)
                    RETURNING id
                """),
                {"eid": absorbed_id, "sid": ledger_event_id},
            ).scalar_one()
            session.commit()
        ledger_event_ids.append(ledger_event_id)
        monitor_log_ids.append(monitor_log_id)

        with get_db_context() as session:
            log = merge_entities(
                session, surviving_id=survivor_id, absorbed_id=absorbed_id,
                merged_by="test:wp4", reason="ledger regression test",
            )
            merge_log_id = log.id
            session.commit()
        merge_log_ids.append(merge_log_id)

        with get_db_context() as session:
            log_row = session.execute(
                text("""
                    SELECT moved_ledger_event_ids, moved_monitor_log_ids
                      FROM buyer_entity_merge_log WHERE id = :id
                """),
                {"id": merge_log_id},
            ).mappings().one()
            assert log_row["moved_ledger_event_ids"] == [ledger_event_id]
            assert log_row["moved_monitor_log_ids"] == [monitor_log_id]

            owner = session.execute(
                text("SELECT buyer_entity_id FROM borrower_ledger_events WHERE id = :id"),
                {"id": ledger_event_id},
            ).scalar_one()
            assert owner == survivor_id, "ledger event must move to survivor, not be cascade-deleted"

            monitor_owner = session.execute(
                text("SELECT buyer_entity_id FROM borrower_monitor_log WHERE id = :id"),
                {"id": monitor_log_id},
            ).scalar_one()
            assert monitor_owner == survivor_id, "monitor log must move to survivor, not be cascade-deleted"

        with get_db_context() as session:
            restored = unmerge_entity(session, merge_log_id=merge_log_id, reversed_by="test:wp4")
            restored_id = restored.id
            session.commit()
        entity_ids.append(restored_id)

        with get_db_context() as session:
            owner_after = session.execute(
                text("SELECT buyer_entity_id FROM borrower_ledger_events WHERE id = :id"),
                {"id": ledger_event_id},
            ).scalar_one()
            assert owner_after == restored_id, "unmerge must return the ledger event to the restored entity"

            monitor_owner_after = session.execute(
                text("SELECT buyer_entity_id FROM borrower_monitor_log WHERE id = :id"),
                {"id": monitor_log_id},
            ).scalar_one()
            assert monitor_owner_after == restored_id, "unmerge must return the monitor log row to the restored entity"
    finally:
        with get_db_context() as session:
            if monitor_log_ids:
                session.execute(
                    text("DELETE FROM borrower_monitor_log WHERE id = ANY(:ids)"),
                    {"ids": monitor_log_ids},
                )
            if ledger_event_ids:
                session.execute(
                    text("DELETE FROM borrower_ledger_events WHERE id = ANY(:ids)"),
                    {"ids": ledger_event_ids},
                )
            session.commit()
        _cleanup(entity_ids, merge_log_ids)


def test_merge_then_unmerge_moves_closer_calls():
    """
    Regression: closer_calls.buyer_entity_id has NO ondelete clause at all
    (Postgres default NO ACTION, unlike the CASCADE FKs above) -- before the
    fix, merging an absorbed entity with call history didn't move its
    closer_calls rows first, so DELETE FROM buyer_entities raised a raw FK
    violation and the merge failed outright (surfaced to the admin as a
    generic 500). unmerge_entity() must move the row back to the restored
    entity.
    """
    token = f"ZMERGECALL{_uid()}".upper()
    entity_ids: list[int] = []
    merge_log_ids: list[int] = []
    closer_call_ids: list[int] = []
    try:
        with get_db_context() as session:
            survivor_id, _ = _make_entity(session, canonical_name=f"{token} SURVIVOR", n_links=1)
            absorbed_id, _ = _make_entity(session, canonical_name=f"{token} ABSORBED", n_links=1)
        entity_ids.extend([survivor_id, absorbed_id])

        with get_db_context() as session:
            closer_call_id = session.execute(
                text("""
                    INSERT INTO closer_calls (aircall_call_id, buyer_entity_id)
                    VALUES (:call_id, :eid)
                    RETURNING id
                """),
                {"call_id": f"ztest-{_uid()}", "eid": absorbed_id},
            ).scalar_one()
            session.commit()
        closer_call_ids.append(closer_call_id)

        with get_db_context() as session:
            # Before the fix this raised sqlalchemy.exc.IntegrityError (FK
            # violation on closer_calls_buyer_entity_id_fkey) instead of
            # returning a log row.
            log = merge_entities(
                session, surviving_id=survivor_id, absorbed_id=absorbed_id,
                merged_by="test:wp4", reason="closer_calls regression test",
            )
            merge_log_id = log.id
            session.commit()
        merge_log_ids.append(merge_log_id)

        with get_db_context() as session:
            log_row = session.execute(
                text("SELECT moved_closer_call_ids FROM buyer_entity_merge_log WHERE id = :id"),
                {"id": merge_log_id},
            ).mappings().one()
            assert log_row["moved_closer_call_ids"] == [closer_call_id]

            owner = session.execute(
                text("SELECT buyer_entity_id FROM closer_calls WHERE id = :id"),
                {"id": closer_call_id},
            ).scalar_one()
            assert owner == survivor_id, "closer call must move to survivor, not block the merge"

        with get_db_context() as session:
            restored = unmerge_entity(session, merge_log_id=merge_log_id, reversed_by="test:wp4")
            restored_id = restored.id
            session.commit()
        entity_ids.append(restored_id)

        with get_db_context() as session:
            owner_after = session.execute(
                text("SELECT buyer_entity_id FROM closer_calls WHERE id = :id"),
                {"id": closer_call_id},
            ).scalar_one()
            assert owner_after == restored_id, "unmerge must return the closer call to the restored entity"
    finally:
        with get_db_context() as session:
            if closer_call_ids:
                session.execute(
                    text("DELETE FROM closer_calls WHERE id = ANY(:ids)"),
                    {"ids": closer_call_ids},
                )
            session.commit()
        _cleanup(entity_ids, merge_log_ids)


def test_unmerge_raises_when_moved_link_ids_missing():
    """A merge_log row logged before moved_link_ids existed (NULL) must refuse
    to unmerge rather than fall back to a timestamp guess that could steal
    the survivor's own links."""
    token = f"ZMERGENULL{_uid()}".upper()
    entity_ids: list[int] = []
    merge_log_ids: list[int] = []
    try:
        with get_db_context() as session:
            survivor_id, _ = _make_entity(session, canonical_name=f"{token} SURVIVOR", n_links=1)
        entity_ids.append(survivor_id)

        with get_db_context() as session:
            log = BuyerEntityMergeLog(
                surviving_id=survivor_id,
                absorbed_id=999_999_999,
                absorbed_snapshot={"id": 999_999_999, "canonical_name": "legacy absorbed"},
                links_moved=0,
                moved_link_ids=None,
                merged_by="test:legacy",
            )
            session.add(log)
            session.commit()
            merge_log_id = log.id
        merge_log_ids.append(merge_log_id)

        with get_db_context() as session:
            with pytest.raises(ValueError, match="moved_link_ids"):
                unmerge_entity(session, merge_log_id=merge_log_id, reversed_by="test:wp4")
    finally:
        _cleanup(entity_ids, merge_log_ids)


def test_unmerge_raises_on_already_reversed_merge():
    """A second unmerge attempt on an already-reversed merge_log must raise,
    never silently no-op or double-restore."""
    token = f"ZMERGETWICE{_uid()}".upper()
    entity_ids: list[int] = []
    merge_log_ids: list[int] = []
    try:
        with get_db_context() as session:
            survivor_id, _ = _make_entity(session, canonical_name=f"{token} SURVIVOR", n_links=1)
            absorbed_id, _ = _make_entity(session, canonical_name=f"{token} ABSORBED", n_links=1)
        entity_ids.extend([survivor_id, absorbed_id])

        with get_db_context() as session:
            log = merge_entities(
                session, surviving_id=survivor_id, absorbed_id=absorbed_id,
                merged_by="test:wp4",
            )
            merge_log_id = log.id
            session.commit()
        merge_log_ids.append(merge_log_id)

        with get_db_context() as session:
            restored = unmerge_entity(session, merge_log_id=merge_log_id, reversed_by="test:wp4")
            restored_id = restored.id
            session.commit()
        entity_ids.append(restored_id)

        with get_db_context() as session:
            with pytest.raises(ValueError, match="already reversed"):
                unmerge_entity(session, merge_log_id=merge_log_id, reversed_by="test:wp4")
    finally:
        _cleanup(entity_ids, merge_log_ids)
