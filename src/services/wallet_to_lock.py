"""
Wallet-to-Lock upgrade detection service.

Identifies wallet subscribers who have spent >= LOCK_THRESHOLD_CREDITS credits
in a single ZIP over the past LOCK_WINDOW_DAYS days and are not already on a
Lock-or-above tier.

Called by: src/tasks/wallet_to_lock_sweep.py (daily cron 0 9 * * *)
"""

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from config.settings import settings
from config.wallet_to_lock import (
    LOCK_IDEMPOTENCY_WINDOW,
    LOCK_OR_ABOVE_TIERS,
    LOCK_THRESHOLD_CREDITS,
    LOCK_WINDOW_DAYS,
)
from src.core.models import Subscriber, WalletTransaction, ZipTerritory

logger = logging.getLogger(__name__)


LOCK_MIN_UNCONTACTED_LEADS = 10


@dataclass
class WalletToLockCandidate:
    subscriber_id: int
    zip_code: str
    credits_used: int
    vertical: str
    county_id: str
    uncontacted_count: int = 0
    tier_breakdown: dict = None  # {"gold": N, "silver": N, "bronze": N}


def find_candidates(db: Session) -> List[WalletToLockCandidate]:
    """
    Group wallet debits by (subscriber, zip) over last LOCK_WINDOW_DAYS.
    Return rows where credits_used >= LOCK_THRESHOLD_CREDITS.
    Only wallet-tier subscribers (not already on Lock+).
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=LOCK_WINDOW_DAYS)

    rows = db.execute(
        select(
            WalletTransaction.subscriber_id,
            WalletTransaction.zip_code,
            func.sum(func.abs(WalletTransaction.amount)).label("credits"),
        )
        .where(
            WalletTransaction.txn_type == "debit",
            WalletTransaction.zip_code.is_not(None),
            WalletTransaction.created_at >= cutoff,
        )
        .group_by(WalletTransaction.subscriber_id, WalletTransaction.zip_code)
        .having(func.sum(func.abs(WalletTransaction.amount)) >= LOCK_THRESHOLD_CREDITS)
    ).all()

    candidates = []
    for row in rows:
        sub = db.get(Subscriber, row.subscriber_id)
        if not sub:
            continue
        if sub.tier in LOCK_OR_ABOVE_TIERS:
            continue

        # Gate: require ≥10 scorable leads in this ZIP
        try:
            from src.agents.tools.read_tools import get_lead_pool
            leads = get_lead_pool(zip_code=row.zip_code, vertical=sub.vertical, min_score=60, limit=50)
            # leads with no contacted field are treated as uncontacted
            uncontacted = [l for l in leads if not l.get("contacted")]
            if len(uncontacted) < LOCK_MIN_UNCONTACTED_LEADS:
                logger.debug(
                    "wallet_to_lock: sub=%s zip=%s only %d uncontacted leads, skipping",
                    row.subscriber_id, row.zip_code, len(uncontacted),
                )
                continue
            tier_counts = {"gold": 0, "silver": 0, "bronze": 0}
            for lead in uncontacted:
                t = (lead.get("tier") or "").lower()
                if t in tier_counts:
                    tier_counts[t] += 1
        except Exception as exc:
            logger.warning("wallet_to_lock lead-pool gate failed sub=%s: %s", row.subscriber_id, exc)
            uncontacted = []
            tier_counts = {"gold": 0, "silver": 0, "bronze": 0}

        candidates.append(WalletToLockCandidate(
            subscriber_id=row.subscriber_id,
            zip_code=row.zip_code,
            credits_used=int(row.credits),
            vertical=sub.vertical,
            county_id=sub.county_id,
            uncontacted_count=len(uncontacted),
            tier_breakdown=tier_counts,
        ))

    return candidates


def is_zip_locked(db: Session, zip_code: str, vertical: str, county_id: str) -> bool:
    """Return True if ZIP is already locked by any subscriber."""
    zt = db.execute(
        select(ZipTerritory).where(
            ZipTerritory.zip_code == zip_code,
            ZipTerritory.vertical == vertical,
            ZipTerritory.county_id == county_id,
            ZipTerritory.status == "locked",
        )
    ).scalar_one_or_none()
    return zt is not None


def mark_lock_candidate(
    db: Session,
    subscriber_id: int,
    zip_code: str,
) -> None:
    sub = db.get(Subscriber, subscriber_id)
    if not sub:
        return
    sub.lock_candidate_zip = zip_code
    sub.lock_candidate_at = datetime.now(timezone.utc)
    db.flush()


def build_lock_cta_url(subscriber_id: int, zip_code: str) -> str:
    """Build pre-filled Territory Lock checkout URL for SMS CTA."""
    base = settings.app_base_url.rstrip("/")
    return f"{base}/checkout?tier=annual_lock&zip={zip_code}&sub={subscriber_id}&utm=cora_lock_close"


def emit_event(
    subscriber_id: int,
    zip_code: str,
    credits_used: int,
    vertical: str,
    uncontacted_count: int = 0,
    tier_breakdown: dict = None,
) -> None:
    """Emit subscriber_crossed_lock_threshold event to Cora supervisor."""
    from src.agents.events.types import Event
    from src.agents.supervisor import dispatch_event

    yyyymm = datetime.now(timezone.utc).strftime(LOCK_IDEMPOTENCY_WINDOW)
    decision_id = str(uuid.uuid4())

    evt = Event(
        event_type="subscriber_crossed_lock_threshold",
        subscriber_id=subscriber_id,
        payload={
            "zip_code": zip_code,
            "credits_used": credits_used,
            "credits_spent": credits_used,
            "window_days": LOCK_WINDOW_DAYS,
            "vertical": vertical,
            "lock_cta_url": build_lock_cta_url(subscriber_id, zip_code),
            "cta_url": build_lock_cta_url(subscriber_id, zip_code),
            "uncontacted_count": uncontacted_count,
            "tier_breakdown": tier_breakdown or {"gold": 0, "silver": 0, "bronze": 0},
        },
        source="cron",
        decision_id=decision_id,
        idempotency_key=f"wal2lock:{subscriber_id}:{zip_code}:{yyyymm}",
    )
    try:
        dispatch_event(evt.to_dispatch_dict())
    except Exception as exc:
        logger.error(
            "wallet_to_lock emit_event failed sub=%s zip=%s: %s",
            subscriber_id, zip_code, exc,
        )
