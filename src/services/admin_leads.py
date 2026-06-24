"""Admin lead-delivery visibility (B4).

Read-only listing of delivery records across ALL accounts, for the admin panel.
Customers see only their own leads through their own surface; this service is the
admin counterpart — full visibility, filterable and paginated.

The response shape is a stable contract the admin frontend depends on:

    {
      "total":  <int>,            # total matching rows (for paging)
      "limit":  <int>,
      "offset": <int>,
      "items": [
        {
          "delivery_id":        <int>,
          "property_id":        <int>,
          "account_id":         "<uuid>",
          "company_name":       <str|null>,
          "grade":              <str>,
          "vertical":           <str>,
          "status":             "delivered" | "rejected",
          "rejection_reason":   <str|null>,
          "rejected_at":        <iso8601|null>,
          "billing_period_end": <iso8601|null>,
          "delivered_at":       <iso8601|null>,
          "source":             <str>
        }, ...
      ]
    }
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def _iso(value):
    return value.isoformat() if value else None


def list_deliveries(
    db: Session,
    *,
    account_id: Optional[str] = None,
    grade: Optional[str] = None,
    status: Optional[str] = None,
    property_id: Optional[int] = None,
    frm: Optional[datetime] = None,
    to: Optional[datetime] = None,
    limit: int = 50,
    offset: int = 0,
) -> dict:
    """List delivery records (newest first) with optional filters + pagination.
    All filters are AND-combined; any omitted filter is ignored. Returns the
    stable admin contract documented in the module docstring."""
    where: list[str] = []
    params: dict = {}
    if account_id is not None:
        where.append("d.account_id = :account_id"); params["account_id"] = str(account_id)
    if grade is not None:
        where.append("d.grade = :grade"); params["grade"] = grade
    if status is not None:
        where.append("d.status = :status"); params["status"] = status
    if property_id is not None:
        where.append("d.property_id = :property_id"); params["property_id"] = property_id
    if frm is not None:
        where.append("d.delivered_at >= :frm"); params["frm"] = frm
    if to is not None:
        where.append("d.delivered_at < :to"); params["to"] = to
    clause = ("WHERE " + " AND ".join(where)) if where else ""

    total = db.execute(
        text(f"SELECT count(*) FROM deliveries d {clause}"), params
    ).scalar() or 0

    rows = db.execute(text(f"""
        SELECT d.id AS delivery_id, d.property_id, d.account_id, ca.company_name,
               d.grade, d.vertical, d.status, d.rejection_reason, d.rejected_at,
               d.billing_period_end, d.delivered_at, d.source
        FROM deliveries d
        LEFT JOIN customer_accounts ca ON ca.account_id = d.account_id
        {clause}
        ORDER BY d.delivered_at DESC, d.id DESC
        LIMIT :limit OFFSET :offset
    """), {**params, "limit": limit, "offset": offset}).mappings().fetchall()

    items = [{
        "delivery_id": r["delivery_id"],
        "property_id": r["property_id"],
        "account_id": str(r["account_id"]),
        "company_name": r["company_name"],
        "grade": r["grade"],
        "vertical": r["vertical"],
        "status": r["status"],
        "rejection_reason": r["rejection_reason"],
        "rejected_at": _iso(r["rejected_at"]),
        "billing_period_end": _iso(r["billing_period_end"]),
        "delivered_at": _iso(r["delivered_at"]),
        "source": r["source"],
    } for r in rows]

    return {"total": int(total), "limit": limit, "offset": offset, "items": items}
