"""
Dynamic Flash Scarcity production trigger.

Detects Gold lead spikes (>= 3 new Gold-scored leads in one ZIP within 60 minutes)
and opens urgency windows + emits FOMO events for non-locked ZIPs.

Called from: src/services/cds_engine.py after new Gold lead scores saved.
"""

import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from src.core.models import DistressScore, Property, ZipTerritory
from src.services.urgency_engine import create_window

logger = logging.getLogger(__name__)

GOLD_SCORE_THRESHOLD = 80
SPIKE_LEAD_COUNT = 3
SPIKE_WINDOW_MINUTES = 60
DEDUP_WINDOW_SECONDS = 1800  # 30-min Redis dedup key


def detect_spike(db: Session, zip_code: str, vertical: str) -> bool:
    """Return True if >= SPIKE_LEAD_COUNT new Gold leads in ZIP in last 60 min."""
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=SPIKE_WINDOW_MINUTES)
    count = db.execute(
        select(func.count()).select_from(DistressScore).join(
            Property, DistressScore.property_id == Property.id
        ).where(
            Property.zip == zip_code,
            DistressScore.final_cds_score >= GOLD_SCORE_THRESHOLD,
            DistressScore.score_date >= cutoff,
        )
    ).scalar() or 0
    return count >= SPIKE_LEAD_COUNT


def _is_zip_locked(db: Session, zip_code: str, vertical: str) -> bool:
    zt = db.execute(
        select(ZipTerritory).where(
            ZipTerritory.zip_code == zip_code,
            ZipTerritory.vertical == vertical,
            ZipTerritory.status == "locked",
        )
    ).scalar_one_or_none()
    return zt is not None


def _dedup_key(zip_code: str) -> str:
    return f"flash_scar_lock:{zip_code}"


def open_window_if_spike(
    db: Session,
    lead_id: int,
    zip_code: str,
    vertical: str,
) -> bool:
    """
    Called after a new Gold lead is scored. Opens urgency window and emits
    FOMO event if spike detected and ZIP not locked.

    Returns True if window was opened.
    """
    if not zip_code or not vertical:
        return False

    try:
        from src.core.redis_client import redis_available, rget, rset

        # Redis dedup: skip if window opened in last 30 min for this ZIP
        if redis_available():
            key = _dedup_key(zip_code)
            if rget(key):
                logger.debug("flash_scarcity: dedup hit for ZIP %s", zip_code)
                return False

        if not detect_spike(db, zip_code, vertical):
            return False

        if _is_zip_locked(db, zip_code, vertical):
            logger.debug("flash_scarcity: ZIP %s locked, skipping", zip_code)
            return False

        # Set dedup key before creating window to prevent races
        if redis_available():
            rset(key, "1", ttl_seconds=DEDUP_WINDOW_SECONDS)

        # Create full urgency window (the missing production call site)
        window = create_window(lead_id, zip_code, vertical)

        logger.info(
            "flash_scarcity: window opened zip=%s vertical=%s expires_at=%s",
            zip_code, vertical, window.get("expires_at"),
        )

        _emit_event(db, zip_code, vertical, lead_id, window)
        return True

    except Exception as exc:
        logger.error("flash_scarcity.open_window_if_spike error: %s", exc)
        return False


def _emit_event(
    db: Session,
    zip_code: str,
    vertical: str,
    lead_id: int,
    window: dict,
) -> None:
    """Emit flash_scarcity_window_open once per eligible subscriber in the vertical."""
    from src.agents.events.types import Event
    from src.agents.supervisor import dispatch_event
    from src.core.models import Subscriber

    try:
        subscriber_ids = db.execute(
            select(Subscriber.id).where(
                Subscriber.vertical == vertical,
                Subscriber.status.in_(("active", "grace")),
            )
        ).scalars().all()
    except Exception as exc:
        logger.error("flash_scarcity: subscriber fan-out query failed: %s", exc)
        return

    if not subscriber_ids:
        logger.debug(
            "flash_scarcity: no eligible subscribers zip=%s vertical=%s", zip_code, vertical
        )
        return

    now = datetime.now(timezone.utc)
    bucket = now.strftime("%Y%m%d%H") + str(now.minute // 10)

    for sub_id in subscriber_ids:
        idem_key = f"flashscar:{zip_code}:{vertical}:{bucket}:{sub_id}"
        evt = Event(
            event_type="flash_scarcity_window_open",
            subscriber_id=sub_id,
            payload={
                "zip_code": zip_code,
                "vertical": vertical,
                "lead_id": lead_id,
                "expires_at": window.get("expires_at"),
                "window_minutes": window.get("window_minutes"),
                "event_type": "flash_scarcity_window_open",
            },
            source="cds_engine",
            decision_id=str(uuid.uuid4()),
            idempotency_key=idem_key,
        )
        try:
            dispatch_event(evt.to_dispatch_dict())
        except Exception as exc:
            logger.error("flash_scarcity emit_event failed sub=%s: %s", sub_id, exc)
