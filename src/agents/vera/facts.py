"""
Vera's facts store — read/write helpers for vera_facts.

write_fact() is the ONLY place in the codebase that writes vera_facts, and
vera_facts is the ONLY table Vera ever writes. It goes through the normal
app DB role (src.core.database), not the vera_readonly connection —
vera_readonly holds no write grants at all, on any table, including this one
(see migrations/apply_vera_readonly_role.py). This keeps "Vera cannot write
business data" and "Vera can only write her own facts" as two separately
enforced guarantees rather than one role trying to do both.

read_facts() goes through the read-only vera_readonly connection (src.agents
.vera.db), consistent with every other check Vera runs.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional

from sqlalchemy import text

from src.agents.vera.config import FRESHNESS_MAX_AGE_HOURS
from src.agents.vera.db import vera_db
from src.core.database import get_db_context
from src.core.models import VeraFact


def write_fact(
    fact_key: str,
    fact_value: str,
    *,
    source: str,
    method: str,
    freshness_class: str,
    value_numeric: Optional[Decimal] = None,
    county_id: Optional[str] = None,
    confidence: Optional[int] = None,
) -> VeraFact:
    """Append one verified fact. Never updates an existing row."""
    if freshness_class not in FRESHNESS_MAX_AGE_HOURS:
        raise ValueError(f"unknown freshness_class: {freshness_class!r}")

    with get_db_context() as session:
        fact = VeraFact(
            fact_key=fact_key,
            fact_value=fact_value,
            value_numeric=value_numeric,
            county_id=county_id,
            source=source,
            method=method,
            freshness_class=freshness_class,
            confidence=confidence,
        )
        session.add(fact)
        session.flush()
        session.refresh(fact)
        return fact


def is_stale(freshness_class: str, observed_at: datetime) -> bool:
    """True if a fact of this freshness_class, observed at observed_at, has expired."""
    max_age_hours = FRESHNESS_MAX_AGE_HOURS.get(freshness_class)
    if max_age_hours is None:
        return False
    if observed_at.tzinfo is None:
        observed_at = observed_at.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) - observed_at > timedelta(hours=max_age_hours)


def read_facts(
    fact_key: str,
    *,
    county_id: Optional[str] = None,
    fresh_only: bool = True,
    limit: int = 1,
):
    """
    Latest fact row(s) for a key, newest first.

    fresh_only=True (default) drops rows whose freshness_class has expired
    relative to observed_at — callers get None/empty rather than a stale
    number silently read as current.
    """
    query = "SELECT * FROM vera_facts WHERE fact_key = :fact_key"
    params = {"fact_key": fact_key, "limit": limit}
    if county_id is not None:
        query += " AND county_id = :county_id"
        params["county_id"] = county_id
    query += " ORDER BY observed_at DESC LIMIT :limit"

    with vera_db.session_scope() as session:
        rows = session.execute(text(query), params).mappings().all()

    if not fresh_only:
        return list(rows)

    return [row for row in rows if not is_stale(row["freshness_class"], row["observed_at"])]
