"""
Golden CLOSE library writer (CLONE-v2.2 CL2).

Assembles and stores one golden_close_chains row per closed deal — the full
winning chain (first signal -> enrichment -> first outreach -> objections
handled -> call -> proposal -> payment -> account expansion) as a portable
snapshot. See GoldenCloseChain in src/core/models.py for the schema
rationale, including why this is CL2's own working shape rather than an
agreed LEARN-v2.2 / L4 schema (L4 hadn't landed a golden-close data model in
this repo at the time this was built; built ahead of it per lead guidance
rather than blocking on coordination).

All DB I/O is raw SQL via sa_text — repo convention.
"""

from __future__ import annotations

import json
import logging
from typing import Optional

from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

VALID_STATUSES = ("draft", "verified", "promoted_to_playbook", "retired")


def upsert_golden_close_chain(
    session: Session,
    *,
    deal_id: int,
    chain_stages: list,
    authored_by: str,
    venture: str = "hillsborough_distress",
    subscriber_id: Optional[int] = None,
    property_id: Optional[int] = None,
    county_id: Optional[str] = None,
    distress_type: Optional[str] = None,
    buyer_vertical: Optional[str] = None,
    offer_step: Optional[str] = None,
    deal_amount: Optional[float] = None,
    days_to_close: Optional[int] = None,
    status: str = "draft",
    schema_version: int = 1,
) -> int:
    """INSERT or overwrite one golden_close_chains row for (deal_id, venture).

    Unlike playbook_writer's ON CONFLICT DO NOTHING (a proven pattern must
    not be silently overwritten by a re-run authored by someone else), a
    golden-close chain is a snapshot of one specific deal's own history —
    re-assembling it (e.g. a late-arriving closer_calls transcript) should
    refresh the row, not skip it. Returns the row id either way.

    chain_stages: ordered list of
        {"stage": str, "occurred_at": iso8601 str, "source_table": str,
         "source_id": int, "summary": str}
    dicts. Stage names: 'first_signal', 'enrichment', 'first_outreach',
    'objection_handled', 'call', 'proposal', 'payment', 'account_expansion'.
    Not every deal has every stage.
    """
    if status not in VALID_STATUSES:
        raise ValueError(f"status must be one of {VALID_STATUSES}, got {status!r}")

    row = session.execute(sa_text("""
        INSERT INTO golden_close_chains (
            deal_id, venture, schema_version,
            subscriber_id, property_id, county_id, distress_type,
            buyer_vertical, offer_step, deal_amount, days_to_close,
            chain_stages, status, authored_by, created_at, updated_at
        ) VALUES (
            :deal_id, :venture, :schema_version,
            :subscriber_id, :property_id, :county_id, :distress_type,
            :buyer_vertical, :offer_step, :deal_amount, :days_to_close,
            CAST(:chain_stages AS jsonb), :status, :authored_by, NOW(), NOW()
        )
        ON CONFLICT (deal_id, venture) DO UPDATE SET
            schema_version = EXCLUDED.schema_version,
            subscriber_id = EXCLUDED.subscriber_id,
            property_id = EXCLUDED.property_id,
            county_id = EXCLUDED.county_id,
            distress_type = EXCLUDED.distress_type,
            buyer_vertical = EXCLUDED.buyer_vertical,
            offer_step = EXCLUDED.offer_step,
            deal_amount = EXCLUDED.deal_amount,
            days_to_close = EXCLUDED.days_to_close,
            chain_stages = EXCLUDED.chain_stages,
            status = EXCLUDED.status,
            authored_by = EXCLUDED.authored_by,
            updated_at = NOW()
        RETURNING id
    """), {
        "deal_id": deal_id,
        "venture": venture,
        "schema_version": schema_version,
        "subscriber_id": subscriber_id,
        "property_id": property_id,
        "county_id": county_id,
        "distress_type": distress_type,
        "buyer_vertical": buyer_vertical,
        "offer_step": offer_step,
        "deal_amount": deal_amount,
        "days_to_close": days_to_close,
        "chain_stages": json.dumps(chain_stages),
        "status": status,
        "authored_by": authored_by,
    }).first()

    chain_id = int(row.id)
    logger.info(
        "[golden_close] deal_id=%d venture=%s status=%s chain_id=%d stages=%d",
        deal_id, venture, status, chain_id, len(chain_stages),
    )
    return chain_id


def transition_status(
    session: Session,
    chain_id: int,
    *,
    to_status: str,
) -> bool:
    """Transition a golden-close chain's curation status.

    Returns True if the row was updated, False if no such row exists.
    Any (from, to) pair is allowed except leaving 'retired' — a retired
    chain is a settled call, re-activate by writing a fresh chain instead.
    """
    if to_status not in VALID_STATUSES:
        raise ValueError(f"to_status must be one of {VALID_STATUSES}, got {to_status!r}")

    result = session.execute(sa_text("""
        UPDATE golden_close_chains
        SET status = :to_status, updated_at = NOW()
        WHERE id = :id AND status != 'retired'
    """), {"id": chain_id, "to_status": to_status})

    return result.rowcount > 0
