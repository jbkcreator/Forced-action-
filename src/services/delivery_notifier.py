"""Contractor notification for newly delivered leads.

A `deliveries` row on its own is silent — the contractor only learns about the
lead if they happen to open the portal. Speed to contact is the product in this
category, so delivered leads get an email the moment they land.

Batched, not per-lead: when a sweep claims many leads for one contractor (a
just-locked ZIP dumps its whole backlog), sending one email per lead reads like
spam. `notify_pending_deliveries` groups un-notified deliveries by recipient and
sends ONE summary email each, then stamps `deliveries.notified_at`. The watermark
makes it crash-safe and idempotent: a failed/partial run leaves rows NULL and the
next run retries them; a sent email marks its rows so they are never re-sent.

Best-effort by design: the delivery is already committed and its entitlement
already spent by the time we notify. A send failure must never roll back the
delivery or abort the sweep, so every failure here is logged and swallowed.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.settings import get_settings
from src.services.email import send_email

logger = logging.getLogger(__name__)

# Cap leads listed inline per email; the rest are covered by the dashboard link.
_MAX_ROWS_PER_EMAIL = 25

# Brand accent — matches the distress-digest email design.
_ACCENT = "#d4a040"

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


# ---------------------------------------------------------------------------
# Batched notification — one summary email per recipient (preferred path)
# ---------------------------------------------------------------------------

_PENDING_SQL = text("""
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
    JOIN subscribers s        ON s.id = ca.subscriber_id
    WHERE d.notified_at IS NULL
      AND d.status = 'delivered'
      AND s.email IS NOT NULL
    ORDER BY s.email, d.delivered_at DESC
""")


def _build_batch_bodies(name: Optional[str], leads: list[dict]) -> tuple[str, str]:
    """Plain-text and HTML bodies for a multi-lead summary email.

    HTML matches the Forced Action distress-digest design (dark shell, gold
    accent) so both notification channels look like one brand.
    """
    greeting = f"Hi {name}," if name else "Hi,"
    portal_url = f"{get_settings().app_base_url.rstrip('/')}/leads"
    n = len(leads)
    shown = leads[:_MAX_ROWS_PER_EMAIL]
    extra = n - len(shown)
    noun = "lead" if n == 1 else "leads"

    text_lines = [greeting, "", f"{n} new {noun} just landed in your territory:", ""]
    for l in shown:
        loc = _format_location(l["address"], l["city"], l["zip"])
        text_lines.append(f"    [{l['grade']} {l['vertical']}] {loc}")
    if extra > 0:
        text_lines.append(f"    …and {extra} more.")
    text_lines += [
        "",
        "Speed matters on these — the sooner you make contact, the better the "
        "conversion. View the full details and owner contact info here:",
        "",
        portal_url,
    ]
    body_text = "\n".join(text_lines)

    rows_html = "".join(
        f'<tr><td style="padding:16px 28px;border-bottom:1px solid #ffffff12;border-left:3px solid {_ACCENT};">'
        f'<div style="font-size:16px;font-weight:700;color:#f0f2f5;letter-spacing:0.02em;">'
        f'{l["address"] or "Address unavailable"} '
        f'<span style="font-size:13px;font-weight:400;color:#6b7280;">'
        f'{(l["city"] + ", ") if l["city"] else ""}{l["zip"] or ""}</span></div>'
        f'<div style="margin-top:5px;font-size:12px;color:#7a8396;">'
        f'<span style="font-weight:600;color:{_ACCENT};letter-spacing:0.04em;text-transform:uppercase;">'
        f'{l["grade"]}</span>'
        f'<span style="color:#ffffff18;"> &nbsp;|&nbsp; </span>{l["vertical"]}</div>'
        f'</td></tr>'
        for l in shown
    )
    extra_html = (
        f'<tr><td style="padding:14px 28px;color:#6b7280;font-size:13px;">'
        f'&hellip;and {extra} more in your territory.</td></tr>' if extra > 0 else ""
    )

    body_html = f"""<html><head><meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1"></head>
    <body style="margin:0;background:#1a1f2e;font-family:Arial,Helvetica,sans-serif;">
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#1a1f2e;padding:32px 16px;">
      <tr><td align="center">
        <table role="presentation" width="680" cellpadding="0" cellspacing="0" style="max-width:680px;width:100%;background:#0c1221;border:1px solid #ffffff14;">
          <tr><td style="padding:28px 32px 24px;border-bottom:1px solid #ffffff0f;">
            <div style="font-size:18px;font-weight:700;color:#f0f2f5;">Forced <span style="color:{_ACCENT};">Action</span></div>
            <div style="margin-top:16px;font-size:26px;font-weight:700;color:#f0f2f5;line-height:1.15;">
              {n} new {noun} just landed in your territory</div>
            <div style="margin-top:8px;font-size:13px;color:#6b7280;">{greeting} Speed matters &mdash; the sooner you make contact, the better the conversion.</div>
          </td></tr>
          <tr><td><table role="presentation" width="100%" cellpadding="0" cellspacing="0">{rows_html}{extra_html}</table></td></tr>
          <tr><td style="padding:24px 32px 28px;border-top:1px solid #ffffff0a;text-align:center;">
            <a href="{portal_url}" style="display:block;padding:16px 36px;background:{_ACCENT};color:#0c1221;
            font-size:14px;font-weight:700;letter-spacing:0.08em;text-transform:uppercase;text-decoration:none;">
              View Your Leads &rarr;</a>
          </td></tr>
        </table>
        <div style="margin-top:16px;font-size:11px;color:#2d3344;">ForcedActionLeads.com &middot; noreply@forcedactionleads.com</div>
      </td></tr>
    </table></body></html>"""
    return body_text, body_html


def _mark_notified(db: Session, delivery_ids: list[int]) -> None:
    db.execute(
        text("UPDATE deliveries SET notified_at = :now WHERE id = ANY(:ids)"),
        {"now": datetime.now(timezone.utc), "ids": delivery_ids},
    )
    db.commit()


def notify_pending_deliveries(db: Session) -> dict:
    """Send one summary email per recipient for all un-notified deliveries.

    Groups by recipient email, sends a single email listing that recipient's new
    leads, then stamps `notified_at` on the emailed rows. Best-effort per
    recipient: a send failure leaves that recipient's rows NULL for the next run,
    so nobody is skipped and nobody is double-emailed. Returns counts.
    """
    stats = {"recipients": 0, "emailed": 0, "deliveries": 0, "errors": 0}
    try:
        rows = db.execute(_PENDING_SQL).mappings().all()
    except Exception:
        logger.error("notify_pending_deliveries: lookup failed", exc_info=True)
        stats["errors"] += 1
        return stats

    by_email: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        by_email[r["email"]].append(dict(r))

    for email, leads in by_email.items():
        stats["recipients"] += 1
        delivery_ids = [l["delivery_id"] for l in leads]
        name = leads[0]["name"]
        try:
            body_text, body_html = _build_batch_bodies(name, leads)
            n = len(leads)
            subject = (
                f"{n} new lead{'s' if n != 1 else ''} in your territory"
                if n != 1
                else f"New {leads[0]['grade']} {leads[0]['vertical']} lead — "
                f"{leads[0]['zip'] or 'your territory'}"
            )
            sent = send_email(to=email, subject=subject, body_text=body_text,
                              body_html=body_html, db=db)
            if sent:
                _mark_notified(db, delivery_ids)
                stats["emailed"] += 1
                stats["deliveries"] += len(delivery_ids)
                logger.info("notify_pending_deliveries: emailed %d leads to recipient",
                            len(delivery_ids))
        except Exception:
            logger.error("notify_pending_deliveries: failed for a recipient (%d leads)",
                         len(delivery_ids), exc_info=True)
            stats["errors"] += 1

    return stats
