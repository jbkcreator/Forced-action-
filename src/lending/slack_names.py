"""Who a lending Slack post is about: the person's full name next to the record id.

Josh's team reads these posts, so every person-facing post shows ``Full Name (ref)`` instead of a bare id. The name
comes from the record the post already points at; a phone or contact with no lead record behind it falls back to
``Unknown caller``. Phones and emails stay masked: only names are added, and every name is Slack-escaped.

Lookups batch one query per post (never one per row) and run on the caller's session.
"""
from __future__ import annotations

import logging
from typing import Iterable, Optional

from sqlalchemy import text

from packages.agent_core.redaction import slack_escape, slack_safe

logger = logging.getLogger(__name__)

UNKNOWN = "Unknown caller"

# Newest record wins within a source; LendingFlow, then website, then partner files, then the dialer pool.
_NAMES_BY_PHONE = text("""
    SELECT DISTINCT ON (phone) phone, name FROM (
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

_CONTACT_PHONES = text("SELECT id, phone FROM lending.contacts WHERE id = ANY(:ids)")
_WEB_LEAD_NAMES = text("SELECT id, NULLIF(btrim(name), '') FROM lending.web_leads WHERE id = ANY(:ids)")
_LENDINGFLOW_NAMES = text("""
    SELECT id, NULLIF(btrim(concat_ws(' ', first_name, last_name)), '') FROM lending.lendingflow_leads WHERE id = ANY(:ids)
""")


def full_name(*parts: Optional[str]) -> Optional[str]:
    """Join the non-empty parts (first, last, or a company name); None when nothing is left."""
    joined = " ".join(part.strip() for part in parts if part and part.strip())
    return joined or None


def name_and_ref(name: Optional[str], ref: Optional[str] = None, *, escape: bool = True) -> str:
    """``Daniel Smith (lead 1424)``; ``Unknown caller (lead 1424)`` without a name; the name alone without a ref.
    ``escape=False`` for text that is escaped later (Cora's approval-card summary goes through agent_core's
    ``slack_safe``), so it is not escaped twice."""
    shown = name.strip() if name and name.strip() else UNKNOWN
    if escape:
        # The name is person-typed: masked (a phone typed into it) and escaped. The ref is our own id (a dialer
        # call id is ten digits), so it is only escaped, never masked.
        return f"{slack_safe(shown)} ({slack_escape(ref)})" if ref else slack_safe(shown)
    return f"{shown} ({ref})" if ref else shown


def _lookup(db, query, params: dict) -> dict:
    """Run one name lookup in a savepoint: a failure (a table missing, a timeout) only drops the names, never the
    post or the caller's transaction."""
    try:
        with db.begin_nested():
            return {row[0]: row[1] for row in db.execute(query, params)}
    except Exception as exc:  # class only
        logger.warning("[slack-names] name lookup failed (%s); posting without names", type(exc).__name__)
        return {}


def names_by_phone(db, phones: Iterable[Optional[str]]) -> dict[str, str]:
    """Normalized phone -> full name, from whichever lead record holds that phone."""
    wanted = sorted({phone for phone in phones if phone})
    return _lookup(db, _NAMES_BY_PHONE, {"phones": wanted}) if wanted else {}


def names_by_ghl_contact(db, contact_ids: Iterable[Optional[str]]) -> dict[str, str]:
    wanted = sorted({contact_id for contact_id in contact_ids if contact_id})
    return _lookup(db, _NAMES_BY_GHL_CONTACT, {"ids": wanted}) if wanted else {}


def _ref_ids(refs: Iterable[str], prefix: str) -> list[int]:
    ids = []
    for ref in refs:
        head, _, tail = ref.partition(":")
        if head == prefix and tail.isdigit():
            ids.append(int(tail))
    return ids


def names_by_lead_ref(db, refs: Iterable[Optional[str]]) -> dict[str, str]:
    """``contact:18008`` / ``web_lead:12`` / ``lendingflow_lead:5`` -> full name (refs with no name are left out)."""
    refs = [ref for ref in refs if ref]
    names: dict[str, str] = {}
    web_ids, lf_ids, contact_ids = (_ref_ids(refs, "web_lead"), _ref_ids(refs, "lendingflow_lead"),
                                    _ref_ids(refs, "contact"))
    if web_ids:
        names.update({f"web_lead:{k}": v for k, v in _lookup(db, _WEB_LEAD_NAMES, {"ids": web_ids}).items() if v})
    if lf_ids:
        names.update({f"lendingflow_lead:{k}": v
                      for k, v in _lookup(db, _LENDINGFLOW_NAMES, {"ids": lf_ids}).items() if v})
    if contact_ids:
        phones = _lookup(db, _CONTACT_PHONES, {"ids": contact_ids})
        by_phone = names_by_phone(db, phones.values())
        names.update({f"contact:{cid}": by_phone[phone] for cid, phone in phones.items() if phone in by_phone})
    return names


def names_by_lendingflow_lead(db, lead_id: int) -> Optional[str]:
    """One LendingFlow lead's full name, or None."""
    return _lookup(db, _LENDINGFLOW_NAMES, {"ids": [lead_id]}).get(lead_id)


def readable_ref(ref: str) -> str:
    """``contact:18008`` -> ``contact 18008``, ``web_lead:12`` -> ``web lead 12`` (how the client reads an id)."""
    head, sep, tail = ref.partition(":")
    return f"{head.replace('_', ' ')} {tail}" if sep else ref
