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
from datetime import datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.core.models import BuyerEntity, BuyerEntityMergeLog

logger = logging.getLogger(__name__)

# BuyerEntity columns typed Numeric/Decimal -- absorbed_snapshot is stored as
# JSONB, which has no native Decimal type, so these round-trip through str()
# on the way in and Decimal() on the way back out (unmerge_entity). Keep this
# in sync with any new Numeric column added to BuyerEntity.
_DECIMAL_FIELDS = ("total_cash_volume", "cadence_purchases_per_year")


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
        k: (v.isoformat() if isinstance(v, datetime) else str(v) if isinstance(v, Decimal) else v)
        for k, v in dict(absorbed).items()
    }

    result = session.execute(
        text("""
            UPDATE buyer_entity_links
               SET buyer_entity_id = :surviving_id
             WHERE buyer_entity_id = :absorbed_id
             RETURNING id
        """),
        {"surviving_id": surviving_id, "absorbed_id": absorbed_id},
    )
    moved_link_ids = [row[0] for row in result.fetchall()]
    links_moved = len(moved_link_ids)

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
        moved_link_ids=moved_link_ids,
        merged_by=merged_by,
        merge_reason=reason,
    )
    session.add(log)
    session.flush()

    logger.info(
        "merge_entities: absorbed entity %d into %d (%d links moved) by %s",
        absorbed_id, surviving_id, links_moved, merged_by,
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
    buyer_entity_links rows that were moved during the merge are restored by
    the EXACT ids recorded in moved_link_ids at merge time -- never by a
    timestamp heuristic, which cannot distinguish links moved by this merge
    from the survivor's own pre-existing links (both predate merged_at) and
    would silently steal the survivor's original links on unmerge. The merge
    log row is stamped reversed_at/reversed_by.

    Raises ValueError if the log row does not exist, was already reversed, or
    has no moved_link_ids (a merge logged before that column existed --
    restoring it safely is not possible without guessing, so this refuses
    rather than risk corrupting the surviving entity's own links).
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
    moved_link_ids = log["moved_link_ids"]
    if moved_link_ids is None:
        raise ValueError(
            f"merge_log id={merge_log_id} has no moved_link_ids recorded -- "
            f"cannot safely unmerge without risking the surviving entity's own links",
        )

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

    for decimal_field in _DECIMAL_FIELDS:
        raw = snap.get(decimal_field)
        if isinstance(raw, str):
            try:
                snap[decimal_field] = Decimal(raw)
            except (ValueError, ArithmeticError):
                snap[decimal_field] = None

    restored = BuyerEntity(**{k: v for k, v in snap.items() if hasattr(BuyerEntity, k)})
    session.add(restored)
    session.flush()

    result = session.execute(
        text("""
            UPDATE buyer_entity_links
               SET buyer_entity_id = :restored_id
             WHERE id = ANY(:moved_link_ids)
               AND buyer_entity_id = :surviving_id
             RETURNING id
        """),
        {
            "restored_id": restored.id,
            "moved_link_ids": moved_link_ids,
            "surviving_id": log["surviving_id"],
        },
    )
    links_returned = len(result.fetchall())
    if links_returned != len(moved_link_ids):
        logger.warning(
            "unmerge_entity: merge_log %d recorded %d moved links but only %d were "
            "found still on surviving entity %d -- some may have been re-merged or "
            "reassigned since",
            merge_log_id, len(moved_link_ids), links_returned, log["surviving_id"],
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
