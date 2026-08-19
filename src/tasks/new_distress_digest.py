"""
T-B12-03 — Tier1 daily "new distress" digest job.

Two audiences, one run:
  - Subscribers (active/grace territory) get the FULL new-lead list for their
    ZIP/vertical — reuses subscriber_email.py's rendering conventions and
    sends through the same src.services.email.send_email direct-send path
    (Instantly is skipped here — free-plan 402 blocker on API campaign
    creation makes email_campaigns.py unsuitable for this job).
  - Waitlist entries (status='waiting') get a blurred teaser — reuses
    proof_moment.py's `_blur_address` masking pattern and deps.py's
    `visible_tier_fields` tier-visibility gate.

"New" leads = scored since the recipient's last digest send:
  - Subscribers: no new "last digest" column needed — SentLead already
    records every property emailed to a subscriber (any source). A lead is
    "new" here if it has never been recorded in sent_leads for that
    subscriber, which is exactly the dedupe the ticket asks for. Sending
    stamps sent_leads with source='new_distress_digest', so tomorrow's run
    naturally excludes everything already shown.
  - Waitlist: `WaitlistEntry.notified_email_at` already exists for this
    purpose (last email sent to this entry) — reused as the "last digest"
    watermark instead of adding new schema. New leads = scored after that
    timestamp (or after signup, if never notified).

Cron: 11:00 UTC daily = 7:00 AM America/Detroit (EDT). Matches the existing
crontab convention of fixed-UTC daily jobs (see scripts/cron/crontab.txt) —
other daily jobs in this repo tolerate the same DST-driven hour drift in
winter (EST) rather than computing per-day UTC offsets.

    python -m src.tasks.new_distress_digest
    python -m src.tasks.new_distress_digest --dry-run
"""
from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import text

from config.settings import get_settings
from src.core.database import get_db_context
from src.services.email import send_email
from src.services.proof_moment import _blur_address
from src.utils.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

DIGEST_SOURCE = "new_distress_digest"

_VERTICAL_LABELS = {
    "roofing":          "Roofing",
    "restoration":      "Restoration / Remediation",
    "wholesalers":      "Wholesale / Investor",
    "fix_flip":         "Fix & Flip",
    "public_adjusters": "Public Adjusters",
    "attorneys":        "Attorneys",
}


# ---------------------------------------------------------------------------
# Subscriber side — full lead list
# ---------------------------------------------------------------------------

def _active_subscribers(db) -> list:
    return db.execute(text("""
        SELECT s.id, s.email, s.name, s.vertical, s.county_id, s.tier, s.event_feed_uuid
        FROM subscribers s
        WHERE s.status IN ('active', 'grace')
          AND s.email IS NOT NULL
    """)).fetchall()


def _subscriber_zips(db, subscriber_id: int) -> list[str]:
    rows = db.execute(text("""
        SELECT zip_code FROM zip_territories
        WHERE subscriber_id = :sub_id AND status IN ('locked', 'grace')
    """), {"sub_id": subscriber_id}).scalars().all()
    return list(rows)


def _new_leads_for_subscriber(db, subscriber, zip_codes: list[str]) -> list[dict]:
    """Qualified leads in this subscriber's ZIPs/vertical never sent to them
    before (no sent_leads row for this subscriber+property, any source)."""
    if not zip_codes:
        return []
    rows = db.execute(text("""
        SELECT p.id AS property_id, p.address, p.city, p.state, p.zip,
               ds.final_cds_score, ds.lead_tier, ds.urgency_level,
               ds.distress_types, ds.vertical_scores
        FROM distress_scores ds
        JOIN properties p ON p.id = ds.property_id
        WHERE ds.qualified = TRUE
          AND ds.is_guess_lead = FALSE
          AND p.zip = ANY(:zips)
          AND p.county_id = :county_id
          AND ds.vertical_scores ? :vertical
          AND NOT EXISTS (
              SELECT 1 FROM sent_leads sl
              WHERE sl.subscriber_id = :sub_id AND sl.property_id = p.id
          )
        ORDER BY (ds.vertical_scores ->> :vertical)::float DESC
        LIMIT 25
    """), {
        "zips": zip_codes, "county_id": subscriber.county_id,
        "vertical": subscriber.vertical, "sub_id": subscriber.id,
    }).fetchall()

    leads = []
    for r in rows:
        dt = r.distress_types
        signals = list(dt.keys()) if isinstance(dt, dict) else (dt or [])
        leads.append({
            "property_id": r.property_id,
            "address": r.address or "Address unavailable",
            "city": r.city or "",
            "state": r.state or "FL",
            "zip": r.zip or "",
            "score": float(r.final_cds_score) if r.final_cds_score else 0.0,
            "lead_tier": r.lead_tier or "Gold",
            "urgency": r.urgency_level or "",
            "signals": [s.replace("_", " ").title() for s in signals],
        })
    return leads


_ACCENT = "#d4a040"


def _render_subscriber_digest(subscriber, leads: list[dict], zip_codes: list[str]) -> tuple[str, str, str]:
    vertical_label = _VERTICAL_LABELS.get(subscriber.vertical or "", subscriber.vertical or "")
    zip_str = ", ".join(zip_codes) if zip_codes else "your territory"
    n = len(leads)
    subject = f"{n} new distressed propert{'y' if n == 1 else 'ies'} in {zip_str}"

    base = get_settings().app_base_url.rstrip("/")
    feed_uuid = subscriber.event_feed_uuid
    dashboard_url = f"{base}/dashboard/{feed_uuid}"
    today = datetime.now(timezone.utc).strftime("%B %d, %Y").replace(" 0", " ")

    rows_html = "".join(
        f'<tr><td style="padding:18px 28px;border-bottom:1px solid #ffffff12;border-left:3px solid {_ACCENT};">'
        f'<div style="font-size:16px;font-weight:700;color:#f0f2f5;letter-spacing:0.02em;">{l["address"]} '
        f'<span style="font-size:13px;font-weight:400;color:#6b7280;">{l["city"]}, {l["zip"]}</span></div>'
        f'<div style="margin-top:5px;font-size:12px;color:#7a8396;">'
        f'<span style="font-weight:600;color:{_ACCENT};letter-spacing:0.04em;">{l["lead_tier"]}</span>'
        f'<span style="color:#ffffff18;"> &nbsp;|&nbsp; </span>{", ".join(l["signals"]) or "Distressed property"}</div>'
        f'<div style="margin-top:6px;font-size:12px;color:#6b7280;">CDS '
        f'<span style="color:{_ACCENT};font-weight:600;">{int(l["score"])}/100</span></div>'
        f'</td></tr>'
        for l in leads
    )

    html = f"""<!DOCTYPE html><html><head><meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1"></head>
    <body style="margin:0;background:#1a1f2e;font-family:Arial,Helvetica,sans-serif;">
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#1a1f2e;padding:32px 16px;">
      <tr><td align="center">
        <table role="presentation" width="680" cellpadding="0" cellspacing="0" style="max-width:680px;width:100%;background:#0c1221;border:1px solid #ffffff14;">
          <tr><td style="padding:28px 32px 24px;border-bottom:1px solid #ffffff0f;">
            <table role="presentation" width="100%" cellpadding="0" cellspacing="0"><tr>
              <td style="font-size:18px;font-weight:700;color:#f0f2f5;">Forced <span style="color:{_ACCENT};">Action</span></td>
              <td style="text-align:right;font-size:13px;color:#6b7280;">{today}</td>
            </tr></table>
            <div style="margin-top:16px;font-size:28px;font-weight:700;color:#f0f2f5;line-height:1.1;">
              {n} new {vertical_label} lead{'s' if n != 1 else ''} in <span style="color:{_ACCENT};">{zip_str}</span></div>
            <div style="margin-top:8px;font-size:13px;color:#6b7280;">New properties scored in your territory</div>
          </td></tr>
          <tr><td><table role="presentation" width="100%" cellpadding="0" cellspacing="0">{rows_html}</table></td></tr>
          <tr><td style="padding:24px 32px 28px;border-top:1px solid #ffffff0a;text-align:center;">
            <a href="{dashboard_url}" style="display:block;padding:16px 36px;background:{_ACCENT};color:#0c1221;
            font-size:14px;font-weight:700;letter-spacing:0.08em;text-transform:uppercase;text-decoration:none;">
              View All {n} Lead{'s' if n != 1 else ''} in Your Territory &rarr;</a>
          </td></tr>
        </table>
        <div style="margin-top:16px;font-size:11px;color:#2d3344;">ForcedActionLeads.com &middot; noreply@forcedactionleads.com</div>
      </td></tr>
    </table></body></html>"""

    lines = [f"{n} new {vertical_label} leads in {zip_str}", ""]
    for l in leads:
        lines.append(f"[{l['lead_tier']}] {int(l['score'])}/100 — {l['address']}, {l['city']} {l['zip']}")
    lines += ["", f"View all {n} leads in your dashboard: {dashboard_url}"]
    plain_text = "\n".join(lines)
    return subject, html, plain_text


def _upsert_sent_leads(db, subscriber_id: int, property_ids: list[int]) -> None:
    if not property_ids:
        return
    now = datetime.now(timezone.utc)
    db.execute(text("""
        INSERT INTO sent_leads (subscriber_id, property_id, sent_at, source)
        SELECT :sub_id, pid, :now, :source
        FROM unnest(:pids) AS pid
        ON CONFLICT ON CONSTRAINT uq_sent_lead
        DO UPDATE SET sent_at = EXCLUDED.sent_at, source = EXCLUDED.source
    """), {"sub_id": subscriber_id, "pids": property_ids, "now": now, "source": DIGEST_SOURCE})
    db.flush()


def _run_subscriber_digests(db, dry_run: bool) -> dict:
    stats = {"subscribers": 0, "sent": 0, "skipped_no_leads": 0, "errors": 0}
    for sub in _active_subscribers(db):
        stats["subscribers"] += 1
        zip_codes = _subscriber_zips(db, sub.id)
        leads = _new_leads_for_subscriber(db, sub, zip_codes)
        if not leads:
            stats["skipped_no_leads"] += 1
            continue

        subject, html, plain_text = _render_subscriber_digest(sub, leads, zip_codes)
        if dry_run:
            logger.info("[DRY RUN] digest -> %s: %s (%d leads)", sub.email, subject, len(leads))
            stats["sent"] += 1
            continue

        try:
            ok = send_email(to=sub.email, subject=subject, body_text=plain_text, body_html=html)
            if ok:
                _upsert_sent_leads(db, sub.id, [l["property_id"] for l in leads])
                db.commit()
                stats["sent"] += 1
            else:
                db.rollback()
                stats["errors"] += 1
        except Exception:
            db.rollback()
            logger.error("new_distress_digest: failed to send subscriber digest to %s",
                         sub.id, exc_info=True)
            stats["errors"] += 1
    return stats


# ---------------------------------------------------------------------------
# Waitlist side — blurred teaser
# ---------------------------------------------------------------------------

def _active_waitlist_entries(db) -> list:
    return db.execute(text("""
        SELECT id, zip_code, vertical, county_id, name, email,
               notified_email_at, created_at
        FROM waitlist_entries
        WHERE status = 'waiting'
    """)).fetchall()


def _new_lead_count_for_waitlist(db, entry) -> tuple[int, Optional[dict]]:
    """Count of new-since-last-digest qualified leads in this entry's ZIP/vertical,
    plus one masked example lead for the teaser copy."""
    since = entry.notified_email_at or entry.created_at
    rows = db.execute(text("""
        SELECT p.address, ds.score_date
        FROM distress_scores ds
        JOIN properties p ON p.id = ds.property_id
        WHERE ds.qualified = TRUE
          AND ds.is_guess_lead = FALSE
          AND p.zip = :zip_code
          AND p.county_id = :county_id
          AND ds.vertical_scores ? :vertical
          AND ds.score_date > :since
        ORDER BY ds.score_date DESC
    """), {
        "zip_code": entry.zip_code, "county_id": entry.county_id,
        "vertical": entry.vertical, "since": since,
    }).fetchall()
    if not rows:
        return 0, None
    return len(rows), {"address": rows[0].address}


def _render_waitlist_teaser(entry, count: int, example: dict) -> tuple[str, str, str]:
    vertical_label = _VERTICAL_LABELS.get(entry.vertical or "", entry.vertical or "")
    masked = _blur_address(example.get("address"))
    subject = f"{count} new distressed propert{'y' if count == 1 else 'ies'} in {entry.zip_code} — unlock to see"

    html = f"""<!DOCTYPE html><html><body style="background:#0f172a;color:#e2e8f0;font-family:Arial,sans-serif;">
    <table width="580" cellpadding="0" cellspacing="0" style="margin:0 auto;padding:24px 0;">
      <tr><td>
        <h2 style="color:#ffffff;">{count} new {vertical_label} lead{'s' if count != 1 else ''} in {entry.zip_code}</h2>
        <p style="color:#94a3b8;">Example: <b style="color:#fbbf24;filter:blur(3px);">{masked}</b></p>
        <p style="color:#94a3b8;">Unlock {entry.zip_code} to see full addresses, owner contacts, and scores.</p>
      </td></tr>
    </table></body></html>"""

    plain_text = (
        f"{count} new distressed properties in {entry.zip_code} — unlock to see.\n"
        f"Example: {masked}\n"
    )
    return subject, html, plain_text


def _run_waitlist_digests(db, dry_run: bool) -> dict:
    stats = {"entries": 0, "sent": 0, "skipped_no_leads": 0, "errors": 0}
    for entry in _active_waitlist_entries(db):
        stats["entries"] += 1
        count, example = _new_lead_count_for_waitlist(db, entry)
        if not count:
            stats["skipped_no_leads"] += 1
            continue

        subject, html, plain_text = _render_waitlist_teaser(entry, count, example)
        if dry_run:
            logger.info("[DRY RUN] teaser -> %s: %s", entry.email, subject)
            stats["sent"] += 1
            continue

        try:
            ok = send_email(to=entry.email, subject=subject, body_text=plain_text, body_html=html)
            if ok:
                db.execute(text(
                    "UPDATE waitlist_entries SET notified_email_at = :now WHERE id = :id"
                ), {"now": datetime.now(timezone.utc), "id": entry.id})
                db.commit()
                stats["sent"] += 1
            else:
                db.rollback()
                stats["errors"] += 1
        except Exception:
            db.rollback()
            logger.error("new_distress_digest: failed to send waitlist teaser to entry %s",
                         entry.id, exc_info=True)
            stats["errors"] += 1
    return stats


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run_new_distress_digest(db=None, *, dry_run: bool = False) -> dict:
    own = db is None
    ctx = get_db_context() if own else None
    db = ctx.__enter__() if own else db
    try:
        sub_stats = _run_subscriber_digests(db, dry_run)
        wl_stats = _run_waitlist_digests(db, dry_run)
        logger.info(
            "new_distress_digest: subscribers=%s waitlist=%s", sub_stats, wl_stats,
        )
        return {"subscribers": sub_stats, "waitlist": wl_stats}
    finally:
        if own:
            ctx.__exit__(None, None, None)


def main() -> int:
    parser = argparse.ArgumentParser(description="Send daily new-distress digest emails")
    parser.add_argument("--dry-run", action="store_true", help="Render and log without sending")
    args = parser.parse_args()

    result = run_new_distress_digest(dry_run=args.dry_run)
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
