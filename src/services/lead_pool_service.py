"""
Lead pool service — thin wrappers over read_tools queries for use outside the agents layer.

api/main.py and services/wallet_to_lock.py need lead-pool and ZIP-activity data
but should not import directly from src.agents.tools.read_tools (crosses the
process boundary). This module exposes the same queries via service functions.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional


def get_lead_pool(
    zip_code: str,
    vertical: Optional[str] = None,
    min_score: int = 0,
    limit: int = 25,
) -> List[Dict[str, Any]]:
    """
    Return scored leads available in a ZIP.
    Delegates to read_tools.get_lead_pool — same query, same return shape.
    """
    from src.agents.tools.read_tools import get_lead_pool as _get_lead_pool
    return _get_lead_pool(zip_code=zip_code, vertical=vertical, min_score=min_score, limit=limit)


def get_zip_activity(
    zip_code: str,
    vertical: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Live activity snapshot for a ZIP — active urgency-window count + 24h message volume.
    Delegates to read_tools.get_zip_activity — same query, same return shape.
    """
    from src.agents.tools.read_tools import get_zip_activity as _get_zip_activity
    return _get_zip_activity(zip_code=zip_code, vertical=vertical)
