"""
Manual merge and unmerge operations for buyer entities.

merge_entities()   — collapses two buyer_entities rows into one, moving all
                     buyer_entity_links from the absorbed entity to the surviving
                     entity, snapshotting the absorbed row, then deleting it.

unmerge_entity()   — reverses a logged merge: restores the absorbed entity from
                     its snapshot, moves its links back, and marks the log row
                     reversed.

These functions are for manual corrections and admin-driven operations only.
The nightly resolver (buyer_entity_resolution.run_incremental) never calls them —
the resolver only creates new entities and attaches links.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session

from src.core.models import BuyerEntity, BuyerEntityMergeLog

logger = logging.getLogger(__name__)


def merge_entities(
    session: Session,
    surviving_id: int,
    absorbed_id: int,
    merged_by: str,
    reason: Optional[str] = None,
) -> BuyerEntityMergeLog:
    """
    Merge absorbed_id into surviving_id.

    All buyer_entity_links rows pointing at absorbed_id are reassigned to
    surviving_id. The absorbed buyer_entities row is snapshotted into
    buyer_entity_merge_log.absorbed_snapshot, then deleted. The surviving
    entity's last_updated_at is stamped.

    Raises ValueError if either ID does not exist or if they are the same row.
    Does not commit — caller controls the transaction boundary.
    """
    if surviving_id == absorbed_id:
        raise ValueError("surviving_id and absorbed_id must be different")

    surviving = session.execute(
        text("SELECT * FROM buyer_entities WHERE id = :id FOR UPDATE"),
        {"id": surviving_id},
    ).mappings().one_or_none()
    if surviving is None:
        raise ValueError(f"surviving buyer_entity id={surviving_id} not found")

    absorbed = session.execute(
        text("SELECT * FROM buyer_entities WHERE id = :id FOR UPDATE"),
        {"id": absorbed_id},
    ).mappings().one_or_none()
    if absorbed is None:
        raise ValueError(f"absorbed buyer_entity id={absorbed_id} not found")

    absorbed_snapshot = {
        k: (v.isoformat() if isinstance(v, datetime) else v)
        for k, v in dict(absorbed).items()
    }

    result = session.execute(
        text("""
            UPDATE buyer_entity_links
               SET buyer_entity_id = :surviving_id
             WHERE buyer_entity_id = :absorbed_id
        """),
        {"surviving_id": surviving_id, "absorbed_id": absorbed_id},
    )
    links_moved = result.rowcount

    # Reassign the absorbed entity's append-only history BEFORE deleting it —
    # borrower_ledger_events and borrower_monitor_log both FK buyer_entities with
    # ON DELETE CASCADE, so a bare delete would silently wipe the audit trail the
    # module is meant to preserve. Capture the moved ids so unmerge can reverse.
    moved_ledger_ids = [
        r[0] for r in session.execute(
            text("SELECT id FROM borrower_ledger_events WHERE buyer_entity_id = :absorbed_id"),
            {"absorbed_id": absorbed_id},
        ).all()
    ]
    if moved_ledger_ids:
        session.execute(
            text("""
                UPDATE borrower_ledger_events
                   SET buyer_entity_id = :surviving_id
                 WHERE buyer_entity_id = :absorbed_id
            """),
            {"surviving_id": surviving_id, "absorbed_id": absorbed_id},
        )

    moved_monitor_ids = [
        r[0] for r in session.execute(
            text("SELECT id FROM borrower_monitor_log WHERE buyer_entity_id = :absorbed_id"),
            {"absorbed_id": absorbed_id},
        ).all()
    ]
    if moved_monitor_ids:
        session.execute(
            text("""
                UPDATE borrower_monitor_log
                   SET buyer_entity_id = :surviving_id
                 WHERE buyer_entity_id = :absorbed_id
            """),
            {"surviving_id": surviving_id, "absorbed_id": absorbed_id},
        )

    session.execute(
        text("DELETE FROM buyer_entities WHERE id = :id"),
        {"id": absorbed_id},
    )

    session.execute(
        text("UPDATE buyer_entities SET last_updated_at = now() WHERE id = :id"),
        {"id": surviving_id},
    )

    log = BuyerEntityMergeLog(
        surviving_id=surviving_id,
        absorbed_id=absorbed_id,
        absorbed_snapshot=absorbed_snapshot,
        links_moved=links_moved,
        merged_by=merged_by,
        merge_reason=reason,
        moved_ledger_event_ids=moved_ledger_ids,
        moved_monitor_log_ids=moved_monitor_ids,
    )
    session.add(log)
    session.flush()

    logger.info(
        "merge_entities: absorbed entity %d into %d (%d links, %d ledger events, "
        "%d monitor rows moved) by %s",
        absorbed_id, surviving_id, links_moved,
        len(moved_ledger_ids), len(moved_monitor_ids), merged_by,
    )
    return log


def unmerge_entity(
    session: Session,
    merge_log_id: int,
    reversed_by: str,
) -> BuyerEntity:
    """
    Reverse a logged merge, restoring the absorbed entity from its snapshot.

    The absorbed entity is re-inserted with a new PK (the old ID was deleted
    and may have been reused). The new ID is stored in merge_log.restored_id.
    buyer_entity_links rows that were moved during the merge are identified by
    their linked_at timestamp (≤ merged_at) and reassigned back to the restored
    entity. The merge log row is stamped reversed_at/reversed_by.

    Raises ValueError if the log row does not exist or was already reversed.
    Does not commit — caller controls the transaction boundary.
    """
    log = session.execute(
        text("SELECT * FROM buyer_entity_merge_log WHERE id = :id FOR UPDATE"),
        {"id": merge_log_id},
    ).mappings().one_or_none()
    if log is None:
        raise ValueError(f"merge_log id={merge_log_id} not found")
    if log["reversed_at"] is not None:
        raise ValueError(f"merge_log id={merge_log_id} was already reversed at {log['reversed_at']}")

    snap: dict = dict(log["absorbed_snapshot"])
    snap.pop("id", None)

    for ts_field in ("first_seen_at", "last_updated_at", "whale_flagged_at",
                     "buyer_type_classified_at", "portfolio_profiled_at"):
        raw = snap.get(ts_field)
        if isinstance(raw, str):
            try:
                snap[ts_field] = datetime.fromisoformat(raw)
            except ValueError:
                snap[ts_field] = None

    restored = BuyerEntity(**{k: v for k, v in snap.items() if hasattr(BuyerEntity, k)})
    session.add(restored)
    session.flush()

    merged_at: datetime = log["merged_at"]
    if merged_at.tzinfo is None:
        merged_at = merged_at.replace(tzinfo=timezone.utc)

    result = session.execute(
        text("""
            UPDATE buyer_entity_links
               SET buyer_entity_id = :restored_id
             WHERE buyer_entity_id = :surviving_id
               AND linked_at <= :merged_at
        """),
        {
            "restored_id": restored.id,
            "surviving_id": log["surviving_id"],
            "merged_at": merged_at,
        },
    )
    links_returned = result.rowcount

    # Move the exact append-only history rows that were reassigned during the
    # merge back onto the restored entity.
    moved_ledger_ids = list(log["moved_ledger_event_ids"] or [])
    if moved_ledger_ids:
        session.execute(
            text("""
                UPDATE borrower_ledger_events
                   SET buyer_entity_id = :restored_id
                 WHERE id IN :ids
            """).bindparams(bindparam("ids", expanding=True)),
            {"restored_id": restored.id, "ids": moved_ledger_ids},
        )
    moved_monitor_ids = list(log["moved_monitor_log_ids"] or [])
    if moved_monitor_ids:
        session.execute(
            text("""
                UPDATE borrower_monitor_log
                   SET buyer_entity_id = :restored_id
                 WHERE id IN :ids
            """).bindparams(bindparam("ids", expanding=True)),
            {"restored_id": restored.id, "ids": moved_monitor_ids},
        )

    session.execute(
        text("""
            UPDATE buyer_entity_merge_log
               SET reversed_at  = now(),
                   reversed_by  = :reversed_by,
                   restored_id  = :restored_id
             WHERE id = :id
        """),
        {"reversed_by": reversed_by, "restored_id": restored.id, "id": merge_log_id},
    )

    session.execute(
        text("UPDATE buyer_entities SET last_updated_at = now() WHERE id = :id"),
        {"id": log["surviving_id"]},
    )

    logger.info(
        "unmerge_entity: restored absorbed entity (was %d, now %d) from merge_log %d "
        "(%d links returned) by %s",
        log["absorbed_id"], restored.id, merge_log_id, links_returned, reversed_by,
    )
    return restored
