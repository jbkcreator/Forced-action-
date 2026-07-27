"""Contractor notification for a newly delivered lead.

A `deliveries` row on its own is silent — the contractor only learns about the
lead if they happen to open the portal. Speed to contact is the product in this
category, so every claimed lead also gets an email the moment it lands.

Best-effort by design: the delivery is already committed and its entitlement
already spent by the time we notify. A send failure must never roll back the
delivery or abort the sweep, so every failure here is logged and swallowed.
"""

from __future__ import annotations

import logging
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.settings import get_settings
from src.services.email import send_email

logger = logging.getLogger(__name__)

_RECIPIENT_SQL = text("""
    SELECT d.id            AS delivery_id,
           d.grade         AS grade,
           d.vertical      AS vertical,
           p.address       AS address,
           p.city          AS city,
           p.zip           AS zip,
           s.email         AS email,
           s.name          AS name
    FROM deliveries d
    JOIN properties p         ON p.id = d.property_id
    JOIN customer_accounts ca ON ca.account_id = d.account_id
    LEFT JOIN subscribers s   ON s.id = ca.subscriber_id
    WHERE d.id = :delivery_id
""")


def _format_location(address: Optional[str], city: Optional[str], zip_code: Optional[str]) -> str:
    """Human-readable property location, tolerating missing address parts."""
    parts = [p for p in (address, city, zip_code) if p]
    return ", ".join(parts) if parts else "your territory"


def _build_bodies(name: Optional[str], grade: str, vertical: str, location: str) -> tuple[str, str]:
    """Plain-text and HTML bodies for the new-lead notification."""
    greeting = f"Hi {name}," if name else "Hi,"
    portal_url = f"{get_settings().app_base_url.rstrip('/')}/leads"

    body_text = (
        f"{greeting}\n\n"
        f"A new {grade} {vertical} lead just landed in your territory:\n\n"
        f"    {location}\n\n"
        f"Speed matters on these — the sooner you make contact, the better the "
        f"conversion. View the full details and owner contact info here:\n\n"
        f"{portal_url}\n"
    )

    body_html = f"""<html>
<body style="font-family: Arial, sans-serif; color: #222; line-height: 1.5;">
  <p>{greeting}</p>
  <p>A new <strong>{grade} {vertical}</strong> lead just landed in your territory:</p>
  <p style="font-size: 16px; padding: 12px; background: #f4f6f9; border-left: 3px solid #1F3352;">
    {location}
  </p>
  <p>Speed matters on these &mdash; the sooner you make contact, the better the conversion.</p>
  <p>
    <a href="{portal_url}"
       style="display: inline-block; padding: 10px 18px; background: #1F3352;
              color: #fff; text-decoration: none; border-radius: 4px;">
      View the lead
    </a>
  </p>
</body>
</html>"""

    return body_text, body_html


def notify_delivery(db: Session, delivery_id: int) -> bool:
    """Email the contractor that a new lead has been delivered to them.

    Reads the delivery fresh so it is safe to call after the delivery's own
    transaction has committed. Returns True when an email was accepted by SMTP,
    False when there was nothing to send (no recipient, suppressed address,
    SMTP not configured) or the send failed.
    """
    try:
        row = db.execute(_RECIPIENT_SQL, {"delivery_id": delivery_id}).mappings().first()
    except Exception:
        logger.error("notify_delivery: lookup failed for delivery %s", delivery_id, exc_info=True)
        return False

    if row is None:
        logger.warning("notify_delivery: no delivery %s", delivery_id)
        return False

    if not row["email"]:
        logger.info(
            "notify_delivery: delivery %s has no contractor email on file — skipping",
            delivery_id,
        )
        return False

    location = _format_location(row["address"], row["city"], row["zip"])
    body_text, body_html = _build_bodies(row["name"], row["grade"], row["vertical"], location)
    subject = f"New {row['grade']} {row['vertical']} lead — {row['zip'] or 'your territory'}"

    try:
        sent = send_email(
            to=row["email"],
            subject=subject,
            body_text=body_text,
            body_html=body_html,
            db=db,
        )
    except Exception:
        logger.error("notify_delivery: send failed for delivery %s", delivery_id, exc_info=True)
        return False

    if sent:
        logger.info("notify_delivery: notified contractor of delivery %s", delivery_id)
    return sent
