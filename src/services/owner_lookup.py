"""
Owner → properties reverse-lookup helpers.

Single source of truth for the v1 reverse-index queries: portfolio size, sibling
properties for an owner, and "what LLCs is this person a managing member of".

v1 design intentionally stays on the `owners` table (no entity graph). Queries
fall into two shapes:

  1. **Exact-string match on `owner_name`** — fast, indexable (`idx_owner_name`),
     used for the public lead-feed `portfolio_size` field. Misses fuzzy variants
     across assessor reloads (e.g. "ACME LLC" vs "ACME, LLC"). Accepted cost for
     subscriber-facing counters; assessor data is internally consistent enough
     that exact-match captures the multi-property signal we care about.

  2. **Python-side normalized match** — uses `BaseLoader.normalize_owner_name`
     (strips LLC/INC/CORP suffixes, noise phrases, punctuation). Used by admin
     reverse-lookup views and skip-trace prioritization where correctness
     matters more than per-row latency.

When the v2 entity graph lands, only the bodies of these functions change; the
call sites in `src/api/main.py` and `src/services/skip_trace.py` see the same
public interface.
"""

from __future__ import annotations

import re
from typing import Iterable, Optional

from sqlalchemy import and_, func, select, text
from sqlalchemy.orm import Session

from src.core.models import Owner, Property


# Keep these in sync with BaseLoader.normalize_owner_name in src/loaders/base.py.
# Duplicated here (rather than imported) so this service doesn't pull in the
# loaders package's heavy transitive deps (usaddress, pandas-via-master, etc.).
_OWNER_NAME_NOISE_PHRASES: tuple[str, ...] = (
    "AS TRUSTEE OF THE", "AS SUCCESSOR TRUSTEE", "AS TRUSTEE OF",
    "AS NOMINEE FOR", "AS NOMINEE", "AS SUCCESSOR",
    "SUCCESSOR IN INTEREST", "THROUGH UNDER", "CLAIMING BY",
    "TRUSTEE OF THE", "TRUSTEE OF",
)
_OWNER_NAME_SUFFIXES: tuple[str, ...] = (
    "LLC", "INC", "CORP", "CO", "LTD", "LP", "LLP", "PLLC",
    "TRUSTEE", "TRUST", "ESTATE", "EST",
    "REVOCABLE", "IRREVOCABLE", "REV", "IRREV",
    "INDIVIDUALLY", "AKA", "FKA", "NKA", "DBA",
    "THE", "AND", "&",
)


def _normalize(name: Optional[str]) -> str:
    if not name:
        return ""
    s = str(name).upper().strip()
    for phrase in sorted(_OWNER_NAME_NOISE_PHRASES, key=len, reverse=True):
        s = re.sub(rf"\b{re.escape(phrase)}\b", " ", s)
    for suffix in _OWNER_NAME_SUFFIXES:
        s = re.sub(rf"\b{suffix}\b\.?", "", s)
    s = re.sub(r"[^\w\s]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


# ── public surface ──────────────────────────────────────────────────────────


def portfolio_size(
    db: Session,
    owner_name: Optional[str],
    *,
    county_id: Optional[str] = None,
) -> int:
    """
    Count of properties that share this exact `owner_name` (raw, case-sensitive
    against the assessor string). Returns 0 for None / empty.

    Cheap and indexed — safe to call per-lead, but prefer
    `portfolio_sizes_for_names` when fanning out across a feed page to avoid
    the obvious N+1.
    """
    if not owner_name:
        return 0
    stmt = select(func.count(Owner.id)).where(Owner.owner_name == owner_name)
    if county_id:
        stmt = (
            stmt.select_from(Owner)
            .join(Property, Property.id == Owner.property_id)
            .where(Property.county_id == county_id)
        )
    return int(db.execute(stmt).scalar() or 0)


def portfolio_sizes_for_names(
    db: Session,
    owner_names: Iterable[Optional[str]],
    *,
    county_id: Optional[str] = None,
) -> dict[str, int]:
    """
    Bulk variant. Returns {owner_name: count}. One SQL round-trip regardless
    of how many names. Names that don't appear in the result are absent from
    the dict (caller should treat missing as 1 — the lead's own row).
    """
    distinct = {n for n in owner_names if n}
    if not distinct:
        return {}

    stmt = (
        select(Owner.owner_name, func.count(Owner.id))
        .where(Owner.owner_name.in_(distinct))
        .group_by(Owner.owner_name)
    )
    if county_id:
        stmt = (
            stmt.select_from(Owner)
            .join(Property, Property.id == Owner.property_id)
            .where(Property.county_id == county_id)
        )
    return {row[0]: int(row[1]) for row in db.execute(stmt).all()}


def properties_by_normalized_owner(
    db: Session,
    owner_name: str,
    *,
    county_id: Optional[str] = None,
    limit: int = 500,
) -> list[int]:
    """
    Return property_ids of every Owner whose normalized name matches the
    normalized form of `owner_name`. Two-step: SQL fetches candidates broadly
    via trigram similarity, Python filters by exact normalized equality.

    Bounded by `limit` to keep admin-view payloads sane.
    """
    target_norm = _normalize(owner_name)
    if not target_norm:
        return []

    # Use the existing GIN trigram index (idx_owner_name_trgm) for the broad
    # sweep; high-similarity bucket keeps the Python-side filter set small.
    # Fall back to exact-match-only if pg_trgm isn't available in this DB.
    try:
        candidates = db.execute(
            select(Owner.id, Owner.owner_name, Owner.property_id)
            .where(Owner.owner_name.op("%")(owner_name))
            .limit(limit * 4)
        ).all()
    except Exception:
        candidates = db.execute(
            select(Owner.id, Owner.owner_name, Owner.property_id)
            .where(Owner.owner_name.ilike(f"%{owner_name}%"))
            .limit(limit * 4)
        ).all()

    matches = [
        (row[0], row[2])
        for row in candidates
        if _normalize(row[1]) == target_norm
    ][:limit]

    if not matches:
        return []

    property_ids = [m[1] for m in matches]
    if not county_id:
        return property_ids

    in_county = db.execute(
        select(Property.id).where(
            and_(Property.id.in_(property_ids), Property.county_id == county_id)
        )
    ).scalars().all()
    return list(in_county)


def llcs_managed_by(
    db: Session,
    person_name: str,
    *,
    county_id: Optional[str] = None,
    limit: int = 100,
) -> list[Owner]:
    """
    Return Owner rows whose `managing_members` JSONB array contains an entry
    with `name == person_name`. Indexable via `ix_owners_managing_members` GIN.

    Caller-supplied `person_name` must match the form stored in JSONB (typically
    "LAST, FIRST" as Sunbiz returns). Address-based narrowing is intentionally
    omitted in v1 — same-name false-merges are bounded by JSONB strict equality
    on the name field, and v2's entity dedup will replace this entirely.
    """
    if not person_name:
        return []

    probe = [{"name": person_name}]
    stmt = (
        select(Owner)
        .where(Owner.managing_members.op("@>")(probe))
        .limit(limit)
    )
    if county_id:
        stmt = stmt.join(Property, Property.id == Owner.property_id).where(
            Property.county_id == county_id
        )
    return list(db.execute(stmt).scalars().all())
