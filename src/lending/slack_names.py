"""Who a lending Slack post is about: the person's full name next to the record id.

Josh's team reads these posts, so every person-facing post shows ``Full Name (ref)`` instead of a bare id. The name
comes from the record the post already points at; a phone or contact with no lead record behind it shows
``Name not on file``. When different people's records share one phone number, every name is shown
(``Ana Lopez or Ben Ray``) rather than guessing one. Phones and emails stay masked: only names are added, and every
name is Slack-escaped.

Lookups batch one query per post (never one per row) and run on the caller's session.
"""
from __future__ import annotations

import logging
from typing import Iterable, Optional

from sqlalchemy import text

from packages.agent_core.redaction import slack_escape, slack_safe

logger = logging.getLogger(__name__)

UNKNOWN = "Name not on file"
MAX_NAMES_PER_PHONE = 2

# Every distinct name on the phone, the most relevant first: LendingFlow, then website, then partner files, then the
# dialer pool, newest first within a source.
_NAMES_BY_PHONE = text("""
    SELECT phone, name, min(rank) AS rank, max(at) AS at FROM (
        SELECT phone, NULLIF(btrim(concat_ws(' ', first_name, last_name)), '') AS name, 1 AS rank, received_at AS at
          FROM lending.lendingflow_leads WHERE phone = ANY(:phones)
        UNION ALL
        SELECT phone, NULLIF(btrim(name), ''), 2, received_at FROM lending.web_leads WHERE phone = ANY(:phones)
        UNION ALL
        SELECT borrower_phone, NULLIF(btrim(concat_ws(' ', borrower_first_name, borrower_last_name)), ''), 3, created_at
          FROM lending.loan_files WHERE borrower_phone = ANY(:phones)
        UNION ALL
        SELECT phone, NULLIF(btrim(COALESCE(borrower_name, entity_name)), ''), 4, loaded_at
          FROM lending.dialer_load_records WHERE phone = ANY(:phones)
    ) s
     WHERE name IS NOT NULL
     GROUP BY phone, name
     ORDER BY phone, rank, at DESC
""")

_NAMES_BY_GHL_CONTACT = text("""
    SELECT DISTINCT ON (ghl_contact_id) ghl_contact_id, name FROM (
        SELECT ghl_contact_id, NULLIF(btrim(concat_ws(' ', first_name, last_name)), '') AS name, 1 AS rank, received_at AS at
          FROM lending.lendingflow_leads WHERE ghl_contact_id = ANY(:ids)
        UNION ALL
        SELECT ghl_contact_id, NULLIF(btrim(name), ''), 2, received_at FROM lending.web_leads WHERE ghl_contact_id = ANY(:ids)
    ) s
     WHERE name IS NOT NULL
     ORDER BY ghl_contact_id, rank, at DESC
""")

_LENDINGFLOW_NAMES = text("""
    SELECT id, NULLIF(btrim(concat_ws(' ', first_name, last_name)), '') FROM lending.lendingflow_leads WHERE id = ANY(:ids)
""")


def full_name(*parts: Optional[str]) -> Optional[str]:
    """Join the non-empty parts (first, last, or a company name); None when nothing is left."""
    joined = " ".join(part.strip() for part in parts if part and part.strip())
    return joined or None


def name_and_ref(name: Optional[str], ref: Optional[str] = None, *, escape: bool = True) -> str:
    """``Daniel Smith (lead 1424)``; ``Name not on file (lead 1424)`` without a name; the name alone without a ref.
    ``escape=False`` for text that is escaped later (Cora's approval-card summary and inbox items are escaped when
    they are posted), so it is not escaped twice."""
    shown = name.strip() if name and name.strip() else UNKNOWN
    if escape:
        # The name is person-typed: masked (a phone typed into it) and escaped. The ref is our own id (a dialer
        # call id is ten digits), so it is only escaped, never masked.
        return f"{slack_safe(shown)} ({slack_escape(ref)})" if ref else slack_safe(shown)
    return f"{shown} ({ref})" if ref else shown


def shared_phone_label(names: list[str]) -> str:
    """One name as is; people sharing a phone as ``Ana Lopez or Ben Ray`` (``… (+2 more)`` past two)."""
    unique: list[str] = []
    for name in names:
        if name.casefold() not in {seen.casefold() for seen in unique}:
            unique.append(name)
    shown = " or ".join(unique[:MAX_NAMES_PER_PHONE])
    extra = len(unique) - MAX_NAMES_PER_PHONE
    return f"{shown} (+{extra} more)" if extra > 0 else shown


def _rows(db, query, params: dict) -> list:
    """Run one name lookup in a savepoint: a failure (a table missing, a timeout) only drops the names, never the
    post or the caller's transaction."""
    try:
        with db.begin_nested():
            return list(db.execute(query, params))
    except Exception as exc:  # class only
        logger.warning("[slack-names] name lookup failed (%s); posting without names", type(exc).__name__)
        return []


def names_by_phone(db, phones: Iterable[Optional[str]]) -> dict[str, str]:
    """Normalized phone -> the name(s) on the lead records that hold that phone."""
    wanted = sorted({phone for phone in phones if phone})
    if not wanted:
        return {}
    grouped: dict[str, list[str]] = {}
    for row in _rows(db, _NAMES_BY_PHONE, {"phones": wanted}):
        grouped.setdefault(row[0], []).append(row[1])
    return {phone: shared_phone_label(names) for phone, names in grouped.items()}


def names_by_ghl_contact(db, contact_ids: Iterable[Optional[str]]) -> dict[str, str]:
    wanted = sorted({contact_id for contact_id in contact_ids if contact_id})
    return {row[0]: row[1] for row in _rows(db, _NAMES_BY_GHL_CONTACT, {"ids": wanted})} if wanted else {}


def names_by_lendingflow_lead(db, lead_id: int) -> Optional[str]:
    """One LendingFlow lead's full name, or None."""
    return {row[0]: row[1] for row in _rows(db, _LENDINGFLOW_NAMES, {"ids": [lead_id]})}.get(lead_id)


def readable_ref(ref: str) -> str:
    """``contact:18008`` -> ``contact 18008``, ``web_lead:12`` -> ``web lead 12`` (how the client reads an id)."""
    head, sep, tail = ref.partition(":")
    return f"{head.replace('_', ' ')} {tail}" if sep else ref
