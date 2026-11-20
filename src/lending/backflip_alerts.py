"""Backflip block alerts: every draft or agent job the Backflip check refused is raised once in Cora's inbox.

Cora's minute sweep posts the inbox digest to her channel (#cora), so a block is seen there, not only in a SECURITY
log line. Two kinds of block are found:

  * a draft in ``lending.pending_actions`` blocked by the lane deny list, at submit (no card) or at send time;
  * an agent job refused by the job check before any draft was made (``result_status`` ``blocked_lane`` with a
    Backflip reason and no ``action_id``). A job blocked by the send gate has a draft, so it is raised as that draft.

Only blocks from the last ``LOOKBACK_DAYS`` are read, and a block already in the inbox is skipped, so each one is
raised exactly once. The payload carries references only (action id, job id, contact ref), never the draft text.
"""
from __future__ import annotations

import logging
from typing import Any, Mapping

from sqlalchemy import text
from sqlalchemy.orm import Session

from packages.agent_core.lane_guard import LANE_BLOCK_PREFIX
from src.lending.cora_inbox import add_to_inbox

logger = logging.getLogger(__name__)

EVENT_TYPE = "backflip_block"
LOOKBACK_DAYS = 7

_NEW_BLOCKS = text("""
    SELECT kind, ref, source, channel, contact_ref, blocked_at FROM (
        SELECT 'draft' AS kind, action_id::text AS ref, tool_name AS source, channel, contact_ref,
               updated_at AS blocked_at
        FROM lending.pending_actions
        WHERE status = 'blocked' AND error LIKE :lane_prefix
          AND updated_at > now() - make_interval(days => :days)
        UNION ALL
        SELECT 'job', job_id, created_by, job_type, ghl_contact_id, updated_at
        FROM lending.agent_jobs
        WHERE result_status = 'blocked_lane' AND action_id IS NULL AND result->>'reason' ILIKE '%backflip%'
          AND updated_at > now() - make_interval(days => :days)
    ) blocks
    WHERE NOT EXISTS (SELECT 1 FROM lending.cora_inbox i
                      WHERE i.event_type = :event_type AND i.dedup_key = blocks.kind || ':' || blocks.ref)
    ORDER BY blocked_at
""")


def _event(block: Mapping[str, Any]) -> dict[str, Any]:
    kind, ref = block["kind"], block["ref"]
    if kind == "draft":
        summary = (f"The Backflip check blocked draft #{ref} ({block['source']}, {block['channel']}). "
                   "Nothing was sent and no card was posted.")
        payload = {"action_id": int(ref), "tool": block["source"], "channel": block["channel"]}
    else:
        summary = (f"The Backflip check blocked agent job {ref} from {block['source'] or 'an agent'}. "
                   "No draft was made and nothing was sent.")
        payload = {"job_id": ref, "created_by": block["source"], "job_type": block["channel"]}
    if block.get("contact_ref"):
        payload["contact_ref"] = block["contact_ref"]
    return {"event_type": EVENT_TYPE, "dedup_key": f"{kind}:{ref}", "summary": summary, "payload": payload}


def raise_backflip_blocks(db: Session) -> int:
    """Queue every new Backflip block in Cora's inbox. Does not commit. Returns how many were queued."""
    blocks = db.execute(_NEW_BLOCKS, {"lane_prefix": f"{LANE_BLOCK_PREFIX}%", "days": LOOKBACK_DAYS,
                                      "event_type": EVENT_TYPE}).mappings().all()
    queued = sum(1 for block in blocks if add_to_inbox(db, **_event(block)) is not None)
    if queued:
        logger.warning("[backflip_alerts] raised %s Backflip block(s) in Cora's inbox", queued)
    return queued
