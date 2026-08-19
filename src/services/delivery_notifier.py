"""Contractor notification for newly delivered leads.

A `deliveries` row on its own is silent — the contractor only learns about the
lead if they happen to open the portal. Speed to contact is the product in this
category, so delivered leads get an email the moment they land.

Batched, not per-lead: when a sweep claims many leads for one contractor (a
just-locked ZIP dumps its whole backlog), sending one email per lead reads like
spam. `notify_pending_deliveries` groups un-notified deliveries by recipient and
sends ONE summary email each, then stamps `deliveries.notified_at`.

Watermark + row locks give the safety guarantees:
  - Crash-safe: a failed/partial run leaves rows NULL and the next run retries.
  - No lost sends: the email is sent before the timestamp is stamped.
  - No cross-run double-sends: each recipient's rows are selected `FOR UPDATE
    SKIP LOCKED` inside that recipient's own transaction, so a second concurrent
    sweep skips rows already being processed. (A crash in the narrow window after
    SMTP accepts but before commit can still re-send on the next run — send-then-
    stamp deliberately favours never-lost over never-doubled.)

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
from src.services.email_shell import ACCENT, lead_row, render_email_shell

logger = logging.getLogger(__name__)

# Cap leads listed inline per email; the rest are summarised as "and N more".
_MAX_ROWS_PER_EMAIL = 25

# Safety cap on how many of one recipient's pending rows a single run locks and
# stamps. Bounds the query and the lock set; any overflow rolls to the next run.
_MAX_LOCK_PER_EMAIL = 200


def _format_location(address: Optional[str], city: Optional[str], zip_code: Optional[str]) -> str:
    """Human-readable property location, tolerating missing address parts."""
    parts = [p for p in (address, city, zip_code) if p]
    return ", ".join(parts) if parts else "your territory"


_PENDING_EMAILS_SQL = text("""
    SELECT DISTINCT s.email
    FROM deliveries d
    JOIN customer_accounts ca ON ca.account_id = d.account_id
    JOIN subscribers s        ON s.id = ca.subscriber_id
    WHERE d.notified_at IS NULL
      AND d.status = 'delivered'
      AND s.email IS NOT NULL
""")

# One recipient's pending rows, locked so a concurrent run skips them.
_PENDING_FOR_EMAIL_SQL = text("""
    SELECT d.id      AS delivery_id,
           d.grade   AS grade,
           d.vertical AS vertical,
           p.address AS address,
           p.city    AS city,
           p.zip     AS zip,
           s.name    AS name
    FROM deliveries d
    JOIN properties p         ON p.id = d.property_id
    JOIN customer_accounts ca ON ca.account_id = d.account_id
    JOIN subscribers s        ON s.id = ca.subscriber_id
    WHERE d.notified_at IS NULL
      AND d.status = 'delivered'
      AND s.email = :email
    ORDER BY d.delivered_at DESC
    LIMIT :cap
    FOR UPDATE OF d SKIP LOCKED
""")


def _build_batch_bodies(name: Optional[str], leads: list[dict]) -> tuple[str, str]:
    """Plain-text and HTML bodies for a multi-lead summary email, branded with the
    shared Forced Action email shell (dark + gold)."""
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
        lead_row(
            title=(f'{l["address"] or "Address unavailable"} '
                   f'<span style="font-size:13px;font-weight:400;color:#6b7280;">'
                   f'{(l["city"] + ", ") if l["city"] else ""}{l["zip"] or ""}</span>'),
            sub=(f'<span style="font-weight:600;color:{ACCENT};letter-spacing:0.04em;'
                 f'text-transform:uppercase;">{l["grade"]}</span>'
                 f'<span style="color:#ffffff18;"> &nbsp;|&nbsp; </span>{l["vertical"]}'),
        )
        for l in shown
    )
    footer_note = f"&hellip;and {extra} more in your territory." if extra > 0 else None

    body_html = render_email_shell(
        headline=f"{n} new {noun} just landed in your territory",
        subhead=f"{greeting} Speed matters — the sooner you make contact, the better the conversion.",
        inner_html=rows_html,
        cta_text="View Your Leads",
        cta_url=portal_url,
        footer_note=footer_note,
    )
    return body_text, body_html


def _pending_recipient_emails(db: Session) -> list[str]:
    try:
        return list(db.execute(_PENDING_EMAILS_SQL).scalars().all())
    except Exception:
        logger.error("notify_pending_deliveries: recipient lookup failed", exc_info=True)
        return []


def _notify_one_recipient(db: Session, email: str) -> int:
    """Send one summary email to `email` and stamp its rows. Returns the number of
    deliveries notified (0 on nothing-to-do, suppressed, or failure). Runs in the
    caller's session; commits on success, rolls back on failure so the locked rows
    stay NULL and are retried next run."""
    try:
        rows = db.execute(
            _PENDING_FOR_EMAIL_SQL, {"email": email, "cap": _MAX_LOCK_PER_EMAIL}
        ).mappings().all()
    except Exception:
        logger.error("notify_pending_deliveries: row lock failed for a recipient", exc_info=True)
        db.rollback()
        return 0

    if not rows:
        db.rollback()  # release the (empty) transaction another run may have won
        return 0

    leads = [dict(r) for r in rows]
    delivery_ids = [l["delivery_id"] for l in leads]
    n = len(leads)
    try:
        body_text, body_html = _build_batch_bodies(leads[0]["name"], leads)
        subject = (
            f"{n} new leads in your territory"
            if n != 1
            else f"New {leads[0]['grade']} {leads[0]['vertical']} lead — "
            f"{leads[0]['zip'] or 'your territory'}"
        )
        sent = send_email(to=email, subject=subject, body_text=body_text,
                          body_html=body_html, db=db)
        if not sent:
            db.rollback()  # suppressed / SMTP off / failed — leave rows NULL
            return 0
        db.execute(
            text("UPDATE deliveries SET notified_at = now() WHERE id = ANY(:ids)"),
            {"ids": delivery_ids},
        )
        db.commit()
        logger.info("notify_pending_deliveries: emailed %d leads to a recipient", n)
        return n
    except Exception:
        db.rollback()
        logger.error("notify_pending_deliveries: send failed for a recipient (%d leads)",
                     n, exc_info=True)
        return 0


def notify_pending_deliveries(db: Session) -> dict:
    """Send one summary email per recipient for all un-notified deliveries.

    Discovers recipients with pending deliveries, then processes each in its own
    transaction (`FOR UPDATE SKIP LOCKED`) so overlapping runs never double-send.
    Best-effort per recipient: a failure leaves that recipient's rows NULL for the
    next run, so nobody is skipped. Returns counts.
    """
    stats = {"recipients": 0, "emailed": 0, "deliveries": 0}
    for email in _pending_recipient_emails(db):
        stats["recipients"] += 1
        n = _notify_one_recipient(db, email)
        if n > 0:
            stats["emailed"] += 1
            stats["deliveries"] += n
    return stats
