"""
Cross-channel Do-Not-Contact enforcement.

Suppression is all-or-nothing: an opt-out on any channel blocks every
channel for the same contact, with no exemption for transactional mail
(receipts, payment-failed, login links). See
docs/adr/0028-cross-channel-suppression-block-all.md.

email_opt_outs and sms_opt_outs are independent per-channel tables. The link
between them is the contact record — resolved across both the Subscriber
(post-conversion) and DBPRContact (pre-conversion marketing) populations, so
the cascade reaches the whole audience, not just paying subscribers.

suppress_contact() does NOT commit — the caller owns the transaction, so a
cascade fired mid-loop (e.g. from the Instantly sync or an SMS STOP handler)
stays atomic with the surrounding unit of work.
"""
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import text

from src.services.phone_utils import normalize as normalize_phone
from src.utils.logger import get_logger

logger = get_logger(__name__)


def is_email_suppressed(db, email: str) -> bool:
    if not email:
        return False
    row = db.execute(
        text("SELECT 1 FROM email_opt_outs WHERE email = :email LIMIT 1"),
        {"email": email.strip().lower()},
    ).fetchone()
    return row is not None


def _resolve_phone_for_email(db, email: str) -> Optional[str]:
    """Find a phone belonging to the same contact as `email`, across the
    subscriber and DBPR-contact populations."""
    row = db.execute(
        text("SELECT phone FROM subscribers "
             "WHERE lower(email) = :e AND phone IS NOT NULL LIMIT 1"),
        {"e": email},
    ).fetchone()
    if row and row[0]:
        return row[0]
    row = db.execute(
        text("SELECT phone FROM dbpr_contacts "
             "WHERE (lower(email) = :e OR lower(work_email) = :e) "
             "AND phone IS NOT NULL AND phone <> '' LIMIT 1"),
        {"e": email},
    ).fetchone()
    return row[0] if row and row[0] else None


def _resolve_email_for_phone(db, phone: str) -> Optional[str]:
    """Find an email belonging to the same contact as `phone`, across the
    subscriber and DBPR-contact populations."""
    row = db.execute(
        text("SELECT email FROM subscribers "
             "WHERE phone = :p AND email IS NOT NULL LIMIT 1"),
        {"p": phone},
    ).fetchone()
    if row and row[0]:
        return row[0]
    row = db.execute(
        text("SELECT COALESCE(NULLIF(email, ''), work_email) AS addr FROM dbpr_contacts "
             "WHERE phone = :p AND COALESCE(NULLIF(email, ''), work_email) IS NOT NULL LIMIT 1"),
        {"p": phone},
    ).fetchone()
    return row[0] if row and row[0] else None


def suppress_contact(
    db,
    email: Optional[str] = None,
    phone: Optional[str] = None,
    source: str = "manual",
) -> None:
    """Write an opt-out row for every identifier on this contact.

    Resolves the sibling identifier (if any) via the owning Subscriber /
    DBPRContact record and writes an opt-out to both email_opt_outs and
    sms_opt_outs when both identifiers exist. Does not commit.
    """
    email = email.strip().lower() if email else None
    phone = normalize_phone(phone) if phone else None
    if not email and not phone:
        return

    if email and not phone:
        phone = _resolve_phone_for_email(db, email)
    elif phone and not email:
        email = _resolve_email_for_phone(db, phone)

    now = datetime.now(timezone.utc)

    if email:
        db.execute(
            text(
                "INSERT INTO email_opt_outs (email, source, opted_out_at) "
                "VALUES (:email, :source, :now) ON CONFLICT (email) DO NOTHING"
            ),
            {"email": email, "source": source, "now": now},
        )
    if phone:
        db.execute(
            text(
                "INSERT INTO sms_opt_outs (phone, source, opted_out_at) "
                "VALUES (:phone, :source, :now) ON CONFLICT (phone) DO NOTHING"
            ),
            {"phone": phone, "source": source, "now": now},
        )

    logger.info(
        "[Suppression] opt-out written email=%s phone=%s source=%s",
        bool(email), bool(phone), source,
    )
