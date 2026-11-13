"""P2.4 closing blocker card: deals stuck in an active GHL stage, who holds them up, and a drafted chase.

``upsert_current_stage`` keeps ``lending.ghl_opportunity_stage`` from the GHL stage webhook.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from sqlalchemy import text

logger = logging.getLogger(__name__)

_UPSERT_STAGE = text("""
    INSERT INTO lending.ghl_opportunity_stage (ghl_opportunity_id, stage_name, stage_key, entered_at, phone, updated_at)
    VALUES (:opp, :stage, :key, :now, :phone, :now)
    ON CONFLICT (ghl_opportunity_id) DO UPDATE SET
        stage_name = EXCLUDED.stage_name,
        stage_key = EXCLUDED.stage_key,
        phone = COALESCE(EXCLUDED.phone, lending.ghl_opportunity_stage.phone),
        updated_at = EXCLUDED.updated_at,
        entered_at = CASE WHEN lending.ghl_opportunity_stage.stage_key = EXCLUDED.stage_key
                          THEN lending.ghl_opportunity_stage.entered_at ELSE EXCLUDED.entered_at END
""")


def upsert_current_stage(db, *, opportunity_id: str, stage_name: str, phone: Optional[str], now: datetime) -> None:
    """Record that the opportunity is in ``stage_name``; a repeat of its current stage keeps the original entry time."""
    db.execute(_UPSERT_STAGE, {"opp": opportunity_id, "stage": stage_name, "key": stage_name.strip().lower(),
                               "now": now, "phone": phone})
