"""R1 0.2: an admin finding a borrower's uploaded documents and opening one through a 15-minute link.

Shared by both front doors: Cora's ``send_document_link`` tool (Slack) and the staff API on ``lending-api``. Both
check the role first; :func:`issue_view_link` checks it again, and :func:`resolve_view_token` checks the issuer's
role once more when the link is opened.

A borrower is never guessed: :func:`find_document_borrowers` returns every match (up to :data:`MAX_MATCHES`), and
the caller lists them for the person to pick. Only borrowers who have a document chase are searched. Results carry a
masked name (``Mark S. …0101``), never the phone, file path or the borrower's own file name.
"""
from __future__ import annotations

import logging
import os
import re
import secrets
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Optional

from sqlalchemy import text

from config.lending_closing import DOC_VIEW_LINK_PATH, DOC_VIEW_LINK_TTL
from src.lending.partner_service import PartnerLinkNotConfigured, link_base_url
from src.lending.partners import hash_token
from src.lending.security.rbac import SLACK_ISSUER, TOKEN_ISSUER, DocumentAccessDenied, Role, role_of_issuer
from src.services.phone_utils import normalize

logger = logging.getLogger(__name__)

MAX_MATCHES = 10
TOKEN_BYTES = 32
_LEAD_REF = re.compile(r"^(web_lead|lendingflow_lead|contact):(\d+)$")

# One row per borrower (lead_ref) with a chase: their newest chase's name and phone, the full name from the lead
# record when there is one, and every item they uploaded across their chases.
_BORROWERS = """
    WITH borrowers AS (
        SELECT DISTINCT ON (c.lead_ref) c.lead_ref, c.first_name, c.phone, c.accepted_at,
               COALESCE(NULLIF(w.name, ''), NULLIF(concat_ws(' ', l.first_name, l.last_name), ''), c.first_name)
                   AS full_name
          FROM lending.doc_chases c
          LEFT JOIN lending.web_leads w
                 ON w.id = CAST(substring(c.lead_ref FROM '^web_lead:([0-9]+)$') AS bigint)
          LEFT JOIN lending.lendingflow_leads l
                 ON l.id = CAST(substring(c.lead_ref FROM '^lendingflow_lead:([0-9]+)$') AS bigint)
         ORDER BY c.lead_ref, c.accepted_at DESC, c.id DESC
    )
    SELECT b.lead_ref, b.first_name, b.phone, b.accepted_at, b.full_name,
           COALESCE((SELECT array_agg(DISTINCT u.item_code ORDER BY u.item_code)
                       FROM lending.doc_chase_uploads u JOIN lending.doc_chases c2 ON c2.id = u.chase_id
                      WHERE c2.lead_ref = b.lead_ref), '{}') AS uploaded
      FROM borrowers b
"""
_SEARCH = text(_BORROWERS + """
     WHERE (CAST(:ref AS text) IS NOT NULL AND b.lead_ref = :ref)
        OR (CAST(:phone AS text) IS NOT NULL AND b.phone = :phone)
        OR (CAST(:last4 AS text) IS NOT NULL AND right(b.phone, 4) = :last4)
        OR (CAST(:name AS text) IS NOT NULL AND ((' ' || b.full_name) ILIKE :name ESCAPE '\\'
                                                 OR (' ' || b.first_name) ILIKE :name ESCAPE '\\'))
     ORDER BY b.accepted_at DESC, b.lead_ref
     LIMIT :limit
""")
_CHASES = text("SELECT id, status, accepted_at FROM lending.doc_chases WHERE lead_ref = :ref "
               "ORDER BY accepted_at DESC, id DESC")
_ITEMS = text("SELECT chase_id, item_code, label, received_at FROM lending.doc_chase_items "
              "WHERE chase_id = ANY(:ids) ORDER BY chase_id, id")
_UPLOADS = text("SELECT id, chase_id, item_code, uploaded_at, content_type, size_bytes FROM lending.doc_chase_uploads "
                "WHERE chase_id = ANY(:ids) ORDER BY uploaded_at DESC, id DESC")
_LATEST = text("""
    SELECT u.id, i.label, u.uploaded_at
      FROM lending.doc_chase_uploads u
      JOIN lending.doc_chases c ON c.id = u.chase_id
      JOIN lending.doc_chase_items i ON i.chase_id = u.chase_id AND i.item_code = u.item_code
     WHERE c.lead_ref = :ref AND u.item_code = :code
     ORDER BY u.uploaded_at DESC, u.id DESC
     LIMIT 1
""")
_UPLOAD = text("SELECT stored_path, quarantine_status FROM lending.doc_chase_uploads WHERE id = :id")
_PACKAGES_TABLE = text("SELECT to_regclass('lending.lender_packages')")
_IN_OPEN_PACKAGE = text("""
    SELECT 1 FROM lending.lender_packages
     WHERE status NOT IN ('submitted', 'dismissed')
       AND validator_verdict -> 'per_doc' @> CAST(:doc AS jsonb)
     LIMIT 1
""")
_INSERT_LINK = text("INSERT INTO lending.doc_view_links (token_hash, upload_id, issued_to, expires_at, created_at) "
                    "VALUES (:hash, :upload, :issued_to, :expires, :now) RETURNING id")
# Live Slack-issued links whose issuer is not in the approver list Cora holds now.
_REVOKE_SLACK_LINKS = text("""
    UPDATE lending.doc_view_links SET revoked_at = :now
     WHERE revoked_at IS NULL AND expires_at > :now AND issued_to LIKE 'slack:%'
       AND NOT (issued_to = ANY(CAST(:allowed AS text[])))
""")
_LINKS_TABLE = text("SELECT to_regclass('lending.doc_view_links')")
_LINK = text("""
    SELECT l.id, l.upload_id, l.issued_to, l.expires_at, l.revoked_at, u.stored_path, u.content_type, u.item_code,
           u.quarantine_status
      FROM lending.doc_view_links l JOIN lending.doc_chase_uploads u ON u.id = l.upload_id
     WHERE l.token_hash = :hash
""")


@dataclass(frozen=True)
class BorrowerMatch:
    lead_ref: str
    display: str
    chase_started: date
    uploaded_items: tuple[str, ...]


@dataclass(frozen=True)
class BorrowerSearch:
    matches: tuple[BorrowerMatch, ...]
    too_many: bool


@dataclass(frozen=True)
class IssuedLink:
    url: str
    expires_at: datetime


@dataclass(frozen=True)
class ViewableFile:
    path: str
    content_type: str
    download_name: str


class DocumentQuarantined(Exception):
    """The upload is quarantined (hidden or injected content): no link to it is made or opened."""


class ViewRefused(Exception):
    """A view link that cannot be opened: 403 (unknown, or its issuer is no longer admin), 404 (file gone), 410
    (expired)."""

    def __init__(self, status: int) -> None:
        super().__init__(status)
        self.status = status


def masked_name(full_name: Optional[str], first_name: Optional[str], phone: Optional[str]) -> str:
    """``Mark Smith`` + ``+18135550101`` -> ``Mark S. …0101``."""
    parts = (full_name or "").split()
    first = (first_name or (parts[0] if parts else "") or "Borrower").strip()
    initial = f" {parts[-1][0].upper()}." if len(parts) > 1 else ""
    last4 = f" …{phone[-4:]}" if phone else ""
    return f"{first}{initial}{last4}"


def _match(row: Any) -> BorrowerMatch:
    return BorrowerMatch(row.lead_ref, masked_name(row.full_name, row.first_name, row.phone),
                         row.accepted_at.date(), tuple(row.uploaded or ()))


def _like(value: str) -> str:
    """Matches at the start of a word: "ann" finds "Ann Lee", never "Joanne"."""
    return "% " + re.sub(r"([\\%_])", r"\\\1", value) + "%"


def _search_params(query: str) -> dict[str, Optional[str]]:
    value = query.strip()
    digits = re.sub(r"\D", "", value)
    if _LEAD_REF.match(value):
        return {"ref": value, "phone": None, "last4": None, "name": None}
    if len(digits) >= 7 and not re.search(r"[A-Za-z]", value):
        return {"ref": None, "phone": normalize(value), "last4": None, "name": None}
    if len(digits) == 4 and digits == value:
        return {"ref": None, "phone": None, "last4": digits, "name": None}
    return {"ref": None, "phone": None, "last4": None, "name": _like(value)}


def find_document_borrowers(db, query: str) -> BorrowerSearch:
    """Every borrower with a document chase matching a name, the last 4 digits, a phone number or a lead_ref.
    More than :data:`MAX_MATCHES` returns ``too_many`` and no matches: the person must narrow it down."""
    rows = db.execute(_SEARCH, {**_search_params(query), "limit": MAX_MATCHES + 1}).all()
    if len(rows) > MAX_MATCHES:
        return BorrowerSearch((), True)
    return BorrowerSearch(tuple(_match(row) for row in rows), False)


def document_list(db, lead_ref: str) -> Optional[dict[str, Any]]:
    """A borrower's chases, newest first, with each required document, whether it arrived and its uploads (ids and
    metadata only: never a path or the borrower's file name). None when the borrower has no chase."""
    found = db.execute(_SEARCH, {"ref": lead_ref, "phone": None, "last4": None, "name": None, "limit": 1}).first()
    if found is None:
        return None
    chases = db.execute(_CHASES, {"ref": lead_ref}).mappings().all()
    ids = [chase["id"] for chase in chases]
    uploads: dict[tuple[int, str], list[dict[str, Any]]] = {}
    for row in db.execute(_UPLOADS, {"ids": ids}).mappings():
        uploads.setdefault((row["chase_id"], row["item_code"]), []).append(
            {"upload_id": row["id"], "uploaded_at": row["uploaded_at"].isoformat(),
             "content_type": row["content_type"], "size_bytes": row["size_bytes"]})
    items: dict[int, list[dict[str, Any]]] = {}
    for row in db.execute(_ITEMS, {"ids": ids}).mappings():
        items.setdefault(row["chase_id"], []).append(
            {"item_code": row["item_code"], "label": row["label"], "received": row["received_at"] is not None,
             "uploads": uploads.get((row["chase_id"], row["item_code"]), [])})
    return {"lead_ref": lead_ref, "borrower": _match(found).display,
            "chases": [{"chase_id": chase["id"], "status": chase["status"],
                        "started": chase["accepted_at"].date().isoformat(), "items": items.get(chase["id"], [])}
                       for chase in chases]}


def latest_upload(db, lead_ref: str, item_code: str) -> Optional[tuple[int, str, datetime]]:
    """(upload id, checklist label, uploaded at) of the newest upload of that item across the borrower's chases."""
    row = db.execute(_LATEST, {"ref": lead_ref, "code": item_code}).first()
    return (row.id, row.label, row.uploaded_at) if row is not None else None


def issue_view_link(db, upload_id: int, issued_to: str, now: datetime) -> IssuedLink:
    """A 15-minute link to one upload for an admin (``slack:<user id>`` or ``token:<id>``). Only the token's hash is
    stored. Raises DocumentAccessDenied, LookupError (no such upload), DocumentQuarantined or
    PartnerLinkNotConfigured."""
    if role_of_issuer(db, issued_to, now) is not Role.ADMIN:
        raise DocumentAccessDenied(issued_to)
    upload = db.execute(_UPLOAD, {"id": upload_id}).first()
    if upload is None:
        raise LookupError(upload_id)
    if upload.quarantine_status == "quarantined" or _quarantined_on_rescan(db, upload_id, upload.stored_path):
        raise DocumentQuarantined(upload_id)
    base = link_base_url()
    if base is None:
        raise PartnerLinkNotConfigured("LENDING_PARTNER_LINK_BASE_URL is not set: document links cannot be made")
    token = secrets.token_urlsafe(TOKEN_BYTES)
    expires_at = now + DOC_VIEW_LINK_TTL
    link_id = db.execute(_INSERT_LINK, {"hash": hash_token(token), "upload": upload_id, "issued_to": issued_to,
                                        "expires": expires_at, "now": now}).scalar_one()
    logger.info("[doc-access] view link %s issued for upload %s", link_id, upload_id)
    return IssuedLink(base + DOC_VIEW_LINK_PATH.format(token=token), expires_at)


def _quarantined_on_rescan(db, upload_id: int, stored_path: str) -> bool:
    """An upload in an open lender package is scanned again before a link to it is made."""
    import json

    from src.lending.packaging.packet_validator import DOC_CHASE_UPLOADS
    from src.lending.packaging.quarantine import scan_upload

    if db.execute(_PACKAGES_TABLE).scalar() is None:
        return False
    doc = json.dumps([{"upload_table": DOC_CHASE_UPLOADS, "upload_id": upload_id}])
    if db.execute(_IN_OPEN_PACKAGE, {"doc": doc}).first() is None:
        return False
    return scan_upload(db, DOC_CHASE_UPLOADS, upload_id, stored_path)


def resolve_view_token(db, token: str, now: datetime) -> ViewableFile:
    """The file a view link opens, or ViewRefused. Checked in order: known, not expired, issuer still admin, file
    still on disk."""
    row = db.execute(_LINK, {"hash": hash_token(token)}).first() if token and len(token) <= 200 else None
    if row is None:
        raise ViewRefused(403)
    if row.revoked_at is not None:
        logger.warning("[doc-access] view link %s refused: revoked (its issuer is no longer admin)", row.id)
        raise ViewRefused(403)
    if row.expires_at <= now:
        raise ViewRefused(410)
    # A token link is re-checked against its token here. A Slack link is not checked against this process's
    # approver list, which is only as fresh as its last restart: Cora revokes it (revoked_at, above) instead.
    if row.issued_to.startswith(TOKEN_ISSUER) and role_of_issuer(db, row.issued_to, now) is not Role.ADMIN:
        logger.warning("[doc-access] view link %s refused: its token is no longer an admin token", row.id)
        raise ViewRefused(403)
    if not row.issued_to.startswith((TOKEN_ISSUER, SLACK_ISSUER)):
        raise ViewRefused(403)
    if row.quarantine_status == "quarantined":
        logger.warning("[doc-access] view link %s refused: upload %s is quarantined", row.id, row.upload_id)
        raise ViewRefused(403)
    if not os.path.isfile(row.stored_path):
        logger.error("[doc-access] view link %s: upload %s is missing on disk", row.id, row.upload_id)
        raise ViewRefused(404)
    logger.info("[doc-access] view link %s opened (upload %s)", row.id, row.upload_id)
    extension = os.path.splitext(row.stored_path)[1].lower()
    return ViewableFile(row.stored_path, row.content_type, f"{row.item_code}{extension}")


def revoke_links_of_removed_approvers(db, approver_ids: frozenset[str], now: datetime) -> int:
    """Revoke every live Slack-issued link whose issuer is not in ``approver_ids``. Run by Cora at start and on each
    minute sweep, from the approver list it was started with, so removing an approver (and restarting Cora) stops
    their open links within a minute, whatever lending-api's cached settings say. Returns how many were revoked;
    0 while the table does not exist yet (migration not run)."""
    if db.execute(_LINKS_TABLE).scalar() is None:
        return 0
    allowed = [f"{SLACK_ISSUER}{user_id}" for user_id in sorted(approver_ids)]
    revoked = db.execute(_REVOKE_SLACK_LINKS, {"now": now, "allowed": allowed}).rowcount
    if revoked:
        logger.warning("[doc-access] revoked %s live document link(s) of former approvers", revoked)
    return revoked
