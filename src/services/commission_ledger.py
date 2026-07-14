"""Commission ledger service — Layer 3E.

Owns commission entry lifecycle for closed_won broker transitions.
All DB access via sa_text. Idempotent post_commission (one entry per transition_id).
"""
from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

_SOURCE = "commission_ledger"


def resolve_split_config(
    session: Session,
    gross_amount_cents: int,
) -> str | None:
    """Return the split_config_id for a deal of this size, or None if no tier matches.

    A tier matches when min_gross_cents <= gross < max_gross_cents (max NULL =
    unbounded). When tiers overlap, the most specific wins: highest floor first,
    then narrowest ceiling — so a real band beats the catch-all [0, +inf) default.
    Returns None when no active tier covers the amount, letting the caller fall
    back to an explicit split_config_id.
    """
    row = session.execute(
        sa_text(
            "SELECT split_config_id FROM commission_splits "
            "WHERE is_active = true "
            "AND min_gross_cents <= :gross "
            "AND (max_gross_cents IS NULL OR :gross < max_gross_cents) "
            "ORDER BY min_gross_cents DESC, max_gross_cents ASC NULLS LAST "
            "LIMIT 1"
        ),
        {"gross": gross_amount_cents},
    ).fetchone()
    return row.split_config_id if row else None


def compute_net_lines(
    session: Session,
    gross_amount_cents: int,
    split_config_id: str,
) -> list[dict]:
    """Return per-party net line amounts using the given split config.

    Remainder cents (from integer division) go to the first party.
    """
    row = session.execute(
        sa_text(
            "SELECT parties FROM commission_splits "
            "WHERE split_config_id = :sid AND is_active = true"
        ),
        {"sid": split_config_id},
    ).fetchone()
    if row is None:
        raise ValueError(f"Split config not found or inactive: {split_config_id!r}")

    parties = row.parties
    amounts = [int(gross_amount_cents * p["pct"] / 100) for p in parties]
    remainder = gross_amount_cents - sum(amounts)
    amounts[0] += remainder  # remainder goes to first party (deterministic)
    return [
        {"party": p["party"], "amount_cents": amt}
        for p, amt in zip(parties, amounts)
    ]


def post_commission(
    session: Session,
    trigger_transition_id: str,
    gross_amount_cents: int,
    split_config_id: str | None = None,
) -> str | None:
    """Post a commission entry for a closed_won transition. Idempotent.

    Returns the entry_id string on first call; returns None on replay.
    Looks up the transition to get lane_id, prospect_id, broker_id.

    When split_config_id is None, the split is resolved from gross_amount_cents
    via the deal-size fee tiers (Task 3.2). This is the authoritative resolution
    point — the split is chosen here, at the money-write, so the ledger always
    reflects the tier in force when the entry is posted.
    """
    existing = session.execute(
        sa_text(
            "SELECT entry_id FROM commission_ledger "
            "WHERE trigger_transition_id = CAST(:tid AS uuid)"
        ),
        {"tid": str(trigger_transition_id)},
    ).fetchone()
    if existing is not None:
        return None

    if split_config_id is None:
        split_config_id = resolve_split_config(session, gross_amount_cents)
    if split_config_id is None:
        raise ValueError(
            f"No split_config_id supplied and no fee tier matches "
            f"gross_amount_cents={gross_amount_cents}."
        )

    tr = session.execute(
        sa_text(
            "SELECT lane_id, broker_id "
            "FROM broker_transitions "
            "WHERE transition_id = CAST(:tid AS uuid)"
        ),
        {"tid": str(trigger_transition_id)},
    ).fetchone()
    if tr is None:
        raise ValueError(f"Transition not found: {trigger_transition_id!r}")

    net_lines = compute_net_lines(session, gross_amount_cents, split_config_id)

    row = session.execute(
        sa_text("""
            INSERT INTO commission_ledger
                (lane_id, broker_id, trigger_transition_id,
                 gross_amount_cents, split_config_id, net_lines, status)
            VALUES
                (CAST(:lid AS uuid), CAST(:bid AS uuid),
                 CAST(:tid AS uuid), :gross, :split, CAST(:nl AS jsonb), 'posted')
            RETURNING entry_id
        """),
        {
            "lid": str(tr.lane_id),
            "bid": str(tr.broker_id),
            "tid": str(trigger_transition_id),
            "gross": gross_amount_cents,
            "split": split_config_id,
            "nl": __import__("json").dumps(net_lines),
        },
    ).fetchone()
    entry_id = str(row.entry_id)
    logger.info(
        "[CommissionLedger] posted entry_id=%s transition_id=%s gross=%d",
        entry_id, trigger_transition_id, gross_amount_cents,
    )
    return entry_id


def dispute_entry(session: Session, entry_id: str, actor: str = "admin") -> None:
    """Mark a commission entry as disputed."""
    session.execute(
        sa_text(
            "UPDATE commission_ledger SET status = 'disputed' "
            "WHERE entry_id = CAST(:eid AS uuid)"
        ),
        {"eid": str(entry_id)},
    )
    logger.info("[CommissionLedger] disputed entry_id=%s actor=%s", entry_id, actor)


def post_offset(session: Session, original_entry_id: str, actor: str = "admin") -> str:
    """Post a reversal offset entry negating the original net_lines.

    Returns the new offset entry_id.
    """
    orig = session.execute(
        sa_text(
            "SELECT lane_id, broker_id, trigger_transition_id, "
            "gross_amount_cents, split_config_id, net_lines "
            "FROM commission_ledger WHERE entry_id = CAST(:eid AS uuid)"
        ),
        {"eid": str(original_entry_id)},
    ).fetchone()
    if orig is None:
        raise ValueError(f"Original entry not found: {original_entry_id!r}")

    neg_lines = [
        {"party": l["party"], "amount_cents": -l["amount_cents"]}
        for l in orig.net_lines
    ]

    row = session.execute(
        sa_text("""
            INSERT INTO commission_ledger
                (lane_id, broker_id,
                 gross_amount_cents, split_config_id, net_lines, status)
            VALUES
                (CAST(:lid AS uuid), CAST(:bid AS uuid),
                 :gross, :split, CAST(:nl AS jsonb), 'posted')
            RETURNING entry_id
        """),
        {
            "lid": str(orig.lane_id),
            "bid": str(orig.broker_id),
            "gross": orig.gross_amount_cents,
            "split": orig.split_config_id,
            "nl": __import__("json").dumps(neg_lines),
        },
    ).fetchone()
    offset_id = str(row.entry_id)
    logger.info(
        "[CommissionLedger] offset entry_id=%s original=%s actor=%s",
        offset_id, original_entry_id, actor,
    )
    return offset_id
