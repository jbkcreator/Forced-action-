"""Partner Door (P-1): partner links, partner-submitted files, consent evidence, status and attribution.

A partner (wholesaler, broker, referral source) gets a private link with a random token; only the
token's SHA-256 is stored. A submission through that link creates one shared ``loan_files`` row
(``source = 'partner'``, ``partner_id`` set), the partner's consent evidence and any uploads, in one
transaction. Partner consent is never borrower consent: nothing here records consent for, or
contacts, the borrower.

CLI (Josh creates partners by hand in v1):
    PYTHONPATH=. python -m src.lending.partners create --name "Maria Lopez" --type wholesaler --email m@x.com
    PYTHONPATH=. python -m src.lending.partners new-link --partner-id 3
    PYTHONPATH=. python -m src.lending.partners revoke --partner-id 3
    PYTHONPATH=. python -m src.lending.partners set-status --file-id 12 --status funded
    PYTHONPATH=. python -m src.lending.partners report [--month 2026-10]
"""
from __future__ import annotations

import argparse
import hashlib
import logging
import re
import secrets
import sys
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Optional, Sequence

from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import text

from config.lending_partners import (
    AGREEMENT_STATUSES,
    CREDIT_BANDS,
    FILE_STATUSES,
    MAX_MONEY,
    MAX_UPLOAD_BYTES,
    MAX_UPLOADS,
    PARTNER_NOTIFY_STATUSES,
    PARTNER_TYPES,
    PRODUCTS,
    SOURCE_PARTNER,
    TIMELINES,
    TOKEN_BYTES,
    UPLOAD_TYPES,
)
from src.services.phone_utils import normalize

logger = logging.getLogger(__name__)

LINK_PATH = "/api/lending/partners/{token}"
_STATE = re.compile(r"^[A-Z]{2}$")
_SAFE_NAME = re.compile(r"[^A-Za-z0-9._-]+")


class InvalidSubmission(ValueError):
    """The submission cannot be saved; the message is safe to show on the form."""


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _new_token() -> str:
    return secrets.token_urlsafe(TOKEN_BYTES)


# -- partners ----------------------------------------------------------------------------------

@dataclass(frozen=True)
class Partner:
    id: int
    name: str
    company: Optional[str]
    partner_type: str
    email: Optional[str]
    phone: Optional[str]
    sms_consent: bool


def create_partner(db, *, name: str, partner_type: str, company: Optional[str] = None, phone: Optional[str] = None,
                   email: Optional[str] = None, states: Optional[str] = None, agreement_status: str = "pending",
                   referral_terms: Optional[str] = None, sms_consent: bool = False, created_by: Optional[str] = None,
                   now: Optional[datetime] = None) -> tuple[int, str]:
    """Create a partner and return ``(partner_id, raw_token)``. The raw token is never stored."""
    if partner_type not in PARTNER_TYPES:
        raise ValueError(f"partner_type must be one of {PARTNER_TYPES}")
    if agreement_status not in AGREEMENT_STATUSES:
        raise ValueError(f"agreement_status must be one of {AGREEMENT_STATUSES}")
    if not name.strip():
        raise ValueError("name is required")
    token, now = _new_token(), now or _utcnow()
    partner_id = db.execute(
        text("INSERT INTO lending.partners (name, company, partner_type, phone, email, states, agreement_status, "
             "referral_terms, sms_consent, active, created_by, token_hash, created_at, updated_at) "
             "VALUES (:name, :company, :type, :phone, :email, :states, :agreement, :terms, :sms, :active, :by, "
             ":hash, :now, :now) RETURNING id"),
        {"name": name.strip(), "company": company, "type": partner_type, "phone": normalize(phone) if phone else None,
         "email": (email or "").strip().lower() or None, "states": states, "agreement": agreement_status,
         "terms": referral_terms, "sms": sms_consent, "active": True, "by": created_by, "hash": hash_token(token),
         "now": now},
    ).scalar_one()
    return int(partner_id), token


def issue_new_link(db, partner_id: int, now: Optional[datetime] = None) -> Optional[str]:
    """Replace the partner's link with a fresh one (the old link stops working). None if no such partner."""
    token, now = _new_token(), now or _utcnow()
    result = db.execute(
        text("UPDATE lending.partners SET token_hash = :hash, token_revoked_at = NULL, updated_at = :now "
             "WHERE id = :id"),
        {"hash": hash_token(token), "now": now, "id": partner_id},
    )
    return token if result.rowcount == 1 else None


def revoke_link(db, partner_id: int, now: Optional[datetime] = None) -> bool:
    """Kill the partner's link. The partner record and their files are untouched."""
    now = now or _utcnow()
    result = db.execute(
        text("UPDATE lending.partners SET token_revoked_at = :now, updated_at = :now "
             "WHERE id = :id AND token_revoked_at IS NULL"),
        {"now": now, "id": partner_id},
    )
    return result.rowcount == 1


def partner_for_token(db, token: str) -> Optional[Partner]:
    """The partner whose live link this is; None for an unknown, revoked or inactive link."""
    if not token or len(token) > 200:
        return None
    row = db.execute(
        text("SELECT id, name, company, partner_type, email, phone, sms_consent FROM lending.partners "
             "WHERE token_hash = :hash AND token_revoked_at IS NULL AND active"),
        {"hash": hash_token(token)},
    ).mappings().first()
    if row is None:
        return None
    return Partner(int(row["id"]), row["name"], row["company"], row["partner_type"], row["email"], row["phone"],
                   bool(row["sms_consent"]))


def partner_by_id(db, partner_id: int) -> Optional[Partner]:
    row = db.execute(
        text("SELECT id, name, company, partner_type, email, phone, sms_consent FROM lending.partners WHERE id = :id"),
        {"id": partner_id},
    ).mappings().first()
    if row is None:
        return None
    return Partner(int(row["id"]), row["name"], row["company"], row["partner_type"], row["email"], row["phone"],
                   bool(row["sms_consent"]))


# -- submissions -------------------------------------------------------------------------------

def _blank_to_none(value: Any) -> Any:
    return None if isinstance(value, str) and not value.strip() else value


class SubmissionInput(BaseModel):
    """What a partner may submit. Field names match the form."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    borrower_first_name: str = Field(..., min_length=1, max_length=80)
    borrower_last_name: str = Field(..., min_length=1, max_length=80)
    borrower_phone: str = Field(..., min_length=7, max_length=30)
    borrower_email: Optional[str] = Field(None, max_length=255)
    property_address: str = Field(..., min_length=3, max_length=200)
    property_city: Optional[str] = Field(None, max_length=80)
    property_state: str
    property_zip: Optional[str] = Field(None, max_length=10)
    product: str
    purchase_price: Optional[int] = Field(None, gt=0, le=MAX_MONEY)
    rehab_budget: Optional[int] = Field(None, ge=0, le=MAX_MONEY)
    loan_amount: int = Field(..., gt=0, le=MAX_MONEY)
    arv: Optional[int] = Field(None, gt=0, le=MAX_MONEY)
    completed_projects: Optional[int] = Field(None, ge=0, le=1000)
    credit_band: str
    timeline: Optional[str] = None
    notes: Optional[str] = Field(None, max_length=2000)

    @field_validator("borrower_email", "property_city", "property_zip", "purchase_price", "rehab_budget", "arv",
                     "completed_projects", "timeline", "notes", mode="before")
    @classmethod
    def _optional_blank(cls, value: Any) -> Any:
        return _blank_to_none(value)

    @field_validator("purchase_price", "rehab_budget", "loan_amount", "arv", mode="before")
    @classmethod
    def _money(cls, value: Any) -> Any:
        if isinstance(value, str):
            cleaned = value.replace("$", "").replace(",", "").strip()
            return cleaned or None
        return value

    @field_validator("borrower_phone")
    @classmethod
    def _phone(cls, value: str) -> str:
        phone = normalize(value)
        if not phone:
            raise ValueError("enter a valid US phone number")
        return phone

    @field_validator("borrower_email")
    @classmethod
    def _email(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        if "@" not in value or "." not in value.split("@")[-1]:
            raise ValueError("enter a valid email address")
        return value.lower()

    @field_validator("property_state")
    @classmethod
    def _state(cls, value: str) -> str:
        state = value.upper()
        if not _STATE.match(state):
            raise ValueError("use the two-letter state code")
        return state

    @field_validator("product")
    @classmethod
    def _product(cls, value: str) -> str:
        if value not in PRODUCTS:
            raise ValueError("choose a loan type")
        return value

    @field_validator("credit_band")
    @classmethod
    def _credit_band(cls, value: str) -> str:
        if value not in CREDIT_BANDS:
            raise ValueError("choose a credit range")
        return value

    @field_validator("timeline")
    @classmethod
    def _timeline(cls, value: Optional[str]) -> Optional[str]:
        if value is not None and value not in TIMELINES:
            raise ValueError("choose a timeline")
        return value


@dataclass(frozen=True)
class Upload:
    original_name: str
    content_type: str
    data: bytes


def check_uploads(uploads: Sequence[Upload]) -> list[Upload]:
    """Drop empty file inputs, then enforce count, size and type. Raises InvalidSubmission."""
    present = [upload for upload in uploads if upload.original_name and upload.data]
    if len(present) > MAX_UPLOADS:
        raise InvalidSubmission(f"attach at most {MAX_UPLOADS} files")
    for upload in present:
        extension = Path(upload.original_name).suffix.lower()
        allowed = UPLOAD_TYPES.get(extension)
        if allowed is None or (upload.content_type or "").lower() not in allowed:
            raise InvalidSubmission(f"{upload.original_name}: only PDF, JPG, PNG or HEIC files can be attached")
        if len(upload.data) > MAX_UPLOAD_BYTES:
            raise InvalidSubmission(f"{upload.original_name}: each file must be {MAX_UPLOAD_BYTES // (1024 * 1024)} MB or less")
    return present


@dataclass(frozen=True)
class ConsentEvidence:
    consent_text: str
    ip_address: Optional[str]
    page_url: Optional[str]
    user_agent: Optional[str]


def save_submission(db, partner: Partner, data: SubmissionInput, uploads: Sequence[Upload], *,
                    consent: ConsentEvidence, upload_dir: str, now: Optional[datetime] = None) -> int:
    """Store the file, the partner's consent evidence and the uploads. Returns the loan file id.

    The caller commits. Upload bytes are written to disk first under the new file's folder and
    removed again if the database writes fail, so a failed save leaves nothing behind.
    """
    if not consent.consent_text.strip():
        raise InvalidSubmission("the consent wording is not configured")
    present = check_uploads(uploads)
    now = now or _utcnow()
    file_id = int(db.execute(
        text("INSERT INTO lending.loan_files (source, partner_id, borrower_first_name, borrower_last_name, "
             "borrower_phone, borrower_email, product, loan_amount, property_address, property_city, property_state, "
             "property_zip, purchase_price, rehab_budget, arv, completed_projects, credit_band, timeline, notes, "
             "status, status_at, created_at, updated_at) VALUES (:source, :partner_id, :first, :last, :phone, :email, "
             ":product, :amount, :address, :city, :state, :zip, :price, :rehab, :arv, :projects, :band, :timeline, "
             ":notes, 'received', :now, :now, :now) RETURNING id"),
        {"source": SOURCE_PARTNER, "partner_id": partner.id, "first": data.borrower_first_name,
         "last": data.borrower_last_name, "phone": data.borrower_phone, "email": data.borrower_email,
         "product": data.product, "amount": data.loan_amount, "address": data.property_address,
         "city": data.property_city, "state": data.property_state, "zip": data.property_zip,
         "price": data.purchase_price, "rehab": data.rehab_budget, "arv": data.arv,
         "projects": data.completed_projects, "band": data.credit_band, "timeline": data.timeline,
         "notes": data.notes, "now": now},
    ).scalar_one())
    db.execute(
        text("INSERT INTO lending.partner_submissions (file_id, partner_id, consent_checked, consent_text, "
             "consented_at, ip_address, page_url, user_agent, created_at) VALUES (:file_id, :partner_id, :checked, "
             ":consent_text, :now, :ip, :url, :agent, :now)"),
        {"file_id": file_id, "partner_id": partner.id, "checked": True, "consent_text": consent.consent_text,
         "now": now, "ip": (consent.ip_address or "")[:45] or None, "url": (consent.page_url or "")[:300] or None,
         "agent": (consent.user_agent or "")[:300] or None},
    )

    folder = Path(upload_dir) / str(file_id)
    written: list[Path] = []
    try:
        for upload in present:
            folder.mkdir(parents=True, exist_ok=True)
            stored = folder / f"{uuid.uuid4().hex}{Path(upload.original_name).suffix.lower()}"
            stored.write_bytes(upload.data)
            written.append(stored)
            db.execute(
                text("INSERT INTO lending.partner_file_uploads (file_id, original_name, stored_path, content_type, "
                     "size_bytes, created_at) VALUES (:file_id, :name, :path, :type, :size, :now)"),
                {"file_id": file_id, "name": _SAFE_NAME.sub("_", upload.original_name)[:200], "path": str(stored),
                 "type": upload.content_type[:80], "size": len(upload.data), "now": now},
            )
    except Exception:
        for path in written:
            path.unlink(missing_ok=True)
        raise
    logger.info("[partner-door] file=%s saved for partner=%s uploads=%s", file_id, partner.id, len(present))
    return file_id


# -- status and dashboard ------------------------------------------------------------------------

@dataclass(frozen=True)
class StatusChange:
    """A status a partner should hear about (routed / declined / approved / funded)."""

    file_id: int
    partner_id: int
    status: str
    reason: Optional[str]
    property_address: Optional[str]
    borrower_last_name: str


def record_status_change(db, file_id: int, status: str, reason: Optional[str] = None,
                         now: Optional[datetime] = None) -> Optional[StatusChange]:
    """Move a file to ``status``. The router (R-1) calls this on every route, decline, approval and funding.

    Returns a :class:`StatusChange` when the partner should be told (pass it to
    ``partner_pipeline.notify_partner_status`` after committing); None when nothing changed or the
    status is internal. The caller commits.
    """
    if status not in FILE_STATUSES:
        raise ValueError(f"status must be one of {FILE_STATUSES}")
    now = now or _utcnow()
    result = db.execute(
        text("UPDATE lending.loan_files SET status = :status, status_reason = :reason, status_at = :now, "
             "funded_at = CASE WHEN :status = 'funded' THEN :now ELSE funded_at END, updated_at = :now "
             "WHERE id = :id AND (status <> :status OR COALESCE(status_reason, '') <> COALESCE(:reason, ''))"),
        {"status": status, "reason": (reason or None) and reason[:200], "now": now, "id": file_id},
    )
    if result.rowcount != 1 or status not in PARTNER_NOTIFY_STATUSES:
        return None
    row = db.execute(
        text("SELECT partner_id, property_address, borrower_last_name FROM lending.loan_files "
             "WHERE id = :id AND source = :source AND partner_id IS NOT NULL"),
        {"id": file_id, "source": SOURCE_PARTNER},
    ).mappings().first()
    if row is None:
        return None
    return StatusChange(file_id, int(row["partner_id"]), status, reason, row["property_address"],
                        row["borrower_last_name"])


def partner_files(db, partner_id: int, limit: int = 200) -> list[dict[str, Any]]:
    """The partner's own files, newest first, for their read-only dashboard."""
    rows = db.execute(
        text("SELECT id, created_at, borrower_first_name, borrower_last_name, property_address, property_state, "
             "product, loan_amount, status, status_at FROM lending.loan_files "
             "WHERE partner_id = :partner_id AND source = :source ORDER BY created_at DESC, id DESC LIMIT :limit"),
        {"partner_id": partner_id, "source": SOURCE_PARTNER, "limit": limit},
    ).mappings().all()
    return [dict(row) for row in rows]


def funded_by_partner(db, month_start: date, month_end: date) -> list[tuple[str, int]]:
    """``(partner name, funded files)`` for every partner, funded within [month_start, month_end)."""
    rows = db.execute(
        text("SELECT p.name, COUNT(f.id) AS funded FROM lending.partners p "
             "LEFT JOIN lending.loan_files f ON f.partner_id = p.id AND f.status = 'funded' "
             "AND f.funded_at >= :start AND f.funded_at < :end "
             "GROUP BY p.id, p.name ORDER BY funded DESC, p.name"),
        {"start": datetime.combine(month_start, datetime.min.time(), tzinfo=timezone.utc),
         "end": datetime.combine(month_end, datetime.min.time(), tzinfo=timezone.utc)},
    ).all()
    return [(name, int(funded)) for name, funded in rows]


def _month_bounds(month: Optional[str], today: Optional[date] = None) -> tuple[date, date]:
    today = today or _utcnow().date()
    start = date.fromisoformat(f"{month}-01") if month else today.replace(day=1)
    end = date(start.year + (start.month == 12), start.month % 12 + 1, 1)
    return start, end


# -- CLI -----------------------------------------------------------------------------------------

def _show_link_once(token: str) -> None:
    """The link is a credential: written to the terminal only, never to the logs, and not stored anywhere."""
    sys.stdout.write(f"Partner link (shown once, keep it private): {LINK_PATH.format(token=token)}\n")



def main(argv: Optional[list[str]] = None) -> int:
    from src.lending.db import lending_session

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Partner Door: manage partner links and see attribution.")
    commands = parser.add_subparsers(dest="command", required=True)
    create = commands.add_parser("create", help="create a partner and print their private link")
    create.add_argument("--name", required=True)
    create.add_argument("--type", required=True, choices=PARTNER_TYPES)
    create.add_argument("--company")
    create.add_argument("--email")
    create.add_argument("--phone")
    create.add_argument("--states", help="comma-separated, e.g. FL,GA")
    create.add_argument("--agreement", default="pending", choices=AGREEMENT_STATUSES)
    create.add_argument("--terms", help="referral terms, free text")
    create.add_argument("--sms-consent", action="store_true", help="the partner agreed to status texts")
    create.add_argument("--created-by")
    for name, helptext in (("new-link", "replace a partner's link"), ("revoke", "kill a partner's link")):
        sub = commands.add_parser(name, help=helptext)
        sub.add_argument("--partner-id", type=int, required=True)
    status = commands.add_parser("set-status", help="move a file to a status (until the router does it)")
    status.add_argument("--file-id", type=int, required=True)
    status.add_argument("--status", required=True, choices=FILE_STATUSES)
    status.add_argument("--reason")
    report = commands.add_parser("report", help="funded files per partner for a month")
    report.add_argument("--month", help="YYYY-MM, default this month")
    args = parser.parse_args(argv)

    if args.command == "create":
        with lending_session() as db:
            partner_id, token = create_partner(
                db, name=args.name, partner_type=args.type, company=args.company, phone=args.phone, email=args.email,
                states=args.states, agreement_status=args.agreement, referral_terms=args.terms,
                sms_consent=args.sms_consent, created_by=args.created_by)
        logger.info("partner %s created", partner_id)
        _show_link_once(token)
        return 0
    if args.command == "new-link":
        with lending_session() as db:
            token = issue_new_link(db, args.partner_id)
        if token is None:
            logger.error("no partner %s", args.partner_id)
            return 1
        logger.info("partner %s has a new link; the old one no longer works", args.partner_id)
        _show_link_once(token)
        return 0
    if args.command == "revoke":
        with lending_session() as db:
            revoked = revoke_link(db, args.partner_id)
        if not revoked:
            logger.error("partner %s has no live link to revoke", args.partner_id)
            return 1
        logger.info("partner %s link revoked", args.partner_id)
        return 0
    if args.command == "set-status":
        from src.lending.partner_pipeline import notify_partner_status

        with lending_session() as db:
            change = record_status_change(db, args.file_id, args.status, args.reason)
        if change is not None:
            notify_partner_status(change)
        logger.info("file %s -> %s", args.file_id, args.status)
        return 0
    start, end = _month_bounds(args.month)
    with lending_session() as db:
        rows = funded_by_partner(db, start, end)
    logger.info("Funded files by partner, %s:", f"{start:%Y-%m}")
    for name, funded in rows:
        logger.info("  %s: %s", name, funded)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
