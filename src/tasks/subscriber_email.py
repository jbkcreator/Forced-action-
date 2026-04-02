"""
Daily subscriber lead email.

Queries each active subscriber's top Gold+ leads for their territory and vertical,
renders a per-vertical HTML email, and sends it.

Cron: 0 10 * * 1-5   (10:00 AM UTC / 6:00 AM EDT / 3:30 PM IST)
      Runs after CDS scoring (07:00 UTC) completes.

Usage:
    python -m src.tasks.subscriber_email                  # all subscribers, today
    python -m src.tasks.subscriber_email --dry-run        # render + log, no send
    python -m src.tasks.subscriber_email --feed-uuid X    # single subscriber only
"""
import argparse
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import select, desc, and_
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from src.core.database import get_db_context
from src.core.models import (
    DistressScore, Owner, Property, ScraperRunStats, SentLead, Subscriber, ZipTerritory,
)
from src.services.email import send_alert, send_email
from src.utils.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

GOLD_PLUS_TIERS = {"Ultra Platinum", "Platinum", "Gold"}

# Leads delivered per tier
TIER_LEAD_LIMIT = {
    "starter":    5,
    "pro":       10,
    "dominator": 20,
    "agency":    20,
}
DEFAULT_LEAD_LIMIT = 10

SENT_LEAD_DEDUP_DAYS = 7   # suppress re-sending same property within this window
LEAD_FRESHNESS_DAYS  = 14  # only include leads scored within this many days

# Human-readable distress signal labels
_SIGNAL_LABELS = {
    "foreclosure":      "Foreclosure",
    "tax_lien":         "Tax Lien",
    "tax_delinquency":  "Tax Delinquency",
    "code_violation":   "Code Violation",
    "lis_pendens":      "Lis Pendens",
    "lien":             "Lien",
    "mechanic_lien":    "Mechanic's Lien",
    "hoa_lien":         "HOA Lien",
    "judgment":         "Judgment",
    "eviction":         "Eviction",
    "probate":          "Probate",
    "bankruptcy":       "Bankruptcy",
    "flood_damage":     "Flood Damage",
    "fire_incident":    "Fire Incident",
    "storm_damage":     "Storm Damage",
    "insurance_claim":  "Insurance Claim",
    "enforcement_permit": "Enforcement Permit",
}

_TIER_STYLES = {
    "Ultra Platinum": {"bg": "#2e1065", "color": "#c4b5fd", "border": "#7c3aed"},
    "Platinum":       {"bg": "#451a03", "color": "#fbbf24", "border": "#92400e"},
    "Gold":           {"bg": "#1c1917", "color": "#fde68a", "border": "#78716c"},
}

_VERTICAL_LABELS = {
    "roofing":          "Roofing",
    "restoration":      "Restoration / Remediation",
    "wholesalers":      "Wholesale / Investor",
    "fix_flip":         "Fix & Flip",
    "public_adjusters": "Public Adjusters",
    "attorneys":        "Attorneys",
}


# ---------------------------------------------------------------------------
# Lead query
# ---------------------------------------------------------------------------

def query_top_leads(
    db: Session,
    subscriber: Subscriber,
    zip_codes: list[str],
    limit: int = DEFAULT_LEAD_LIMIT,
) -> list[dict]:
    """
    Return the top `limit` Gold+ leads for this subscriber's ZIPs and vertical,
    ordered by vertical score descending.

    Each lead dict contains property fields, score fields, and owner contact.
    """
    if not zip_codes:
        return []

    vertical = subscriber.vertical
    try:
        score_col = DistressScore.vertical_scores[vertical].as_float()
    except KeyError:
        logger.error("Unknown vertical '%s' on subscriber %s", vertical, subscriber.id)
        return []

    # Dedup: exclude properties already sent to this subscriber within the window
    cutoff_dedup = datetime.now(timezone.utc) - timedelta(days=SENT_LEAD_DEDUP_DAYS)
    recently_sent_subq = (
        select(SentLead.property_id)
        .where(SentLead.subscriber_id == subscriber.id, SentLead.sent_at >= cutoff_dedup)
        .scalar_subquery()
    )

    # Freshness: only include leads scored within LEAD_FRESHNESS_DAYS
    cutoff_freshness = datetime.now(timezone.utc) - timedelta(days=LEAD_FRESHNESS_DAYS)

    rows = db.execute(
        select(Property, DistressScore, Owner)
        .join(DistressScore, DistressScore.property_id == Property.id)
        .outerjoin(Owner, Owner.property_id == Property.id)
        .where(and_(
            Property.zip.in_(zip_codes),
            Property.county_id == subscriber.county_id,
            DistressScore.lead_tier.in_(GOLD_PLUS_TIERS),
            DistressScore.qualified == True,
            score_col > 0,
            DistressScore.score_date >= cutoff_freshness,
            ~Property.id.in_(recently_sent_subq),
        ))
        .order_by(desc(score_col))
        .limit(limit)
    ).all()

    leads = []
    for prop, score, owner in rows:
        signals = score.distress_types or []
        signal_labels = [_SIGNAL_LABELS.get(s, s.replace("_", " ").title()) for s in signals]

        leads.append({
            "property_id":    prop.id,
            "address":        prop.address or "Address unavailable",
            "city":           prop.city or "",
            "state":          prop.state or "FL",
            "zip":            prop.zip or "",
            "property_type":  prop.property_type or "",
            "year_built":     prop.year_built,
            "sq_ft":          prop.sq_ft,
            "cds_score":      float(score.final_cds_score) if score.final_cds_score else 0,
            "vertical_score": score.vertical_scores.get(vertical, 0) if score.vertical_scores else 0,
            "lead_tier":      score.lead_tier or "Gold",
            "urgency":        score.urgency_level or "",
            "signals":        signal_labels,
            "owner_name":     owner.owner_name if owner else None,
            "phone":          (owner.phone_1 or owner.phone_2 or owner.phone_3) if owner else None,
            "email":          (owner.email_1 or owner.email_2) if owner else None,
            "absentee":       (owner.absentee_status == "absentee") if owner else False,
        })

    return leads


# ---------------------------------------------------------------------------
# Email rendering
# ---------------------------------------------------------------------------

def _tier_badge_html(tier: str) -> str:
    s = _TIER_STYLES.get(tier, _TIER_STYLES["Gold"])
    return (
        f'<span style="display:inline-block;padding:3px 10px;border-radius:999px;'
        f'background:{s["bg"]};border:1px solid {s["border"]};'
        f'color:{s["color"]};font-size:11px;font-weight:700;'
        f'letter-spacing:0.5px;text-transform:uppercase;">{tier}</span>'
    )


def _lead_card_html(lead: dict, n: int) -> str:
    badge      = _tier_badge_html(lead["lead_tier"])
    address    = lead["address"]
    city_line  = f"{lead['city']}, {lead['state']} {lead['zip']}".strip(", ")
    score      = int(lead["vertical_score"])
    signals    = " &middot; ".join(lead["signals"]) if lead["signals"] else "Distressed property"
    year_built = f"Built {lead['year_built']}" if lead["year_built"] else ""
    sq_ft      = f"{lead['sq_ft']:,} sq ft" if lead["sq_ft"] else ""
    prop_meta  = " &middot; ".join(filter(None, [year_built, sq_ft]))

    owner_line = ""
    if lead["owner_name"]:
        absentee_tag = (
            ' <span style="color:#f87171;font-size:11px;">(absentee)</span>'
            if lead["absentee"] else ""
        )
        owner_line += f'<p style="margin:0 0 4px;font-size:13px;color:#e2e8f0;">{lead["owner_name"]}{absentee_tag}</p>'
    if lead["phone"]:
        owner_line += f'<p style="margin:0 0 4px;font-size:13px;color:#94a3b8;">{lead["phone"]}</p>'
    if lead["email"]:
        owner_line += f'<p style="margin:0;font-size:13px;color:#94a3b8;">{lead["email"]}</p>'

    urgency_color = {
        "Immediate": "#f87171",
        "High":      "#fb923c",
        "Medium":    "#fbbf24",
    }.get(lead["urgency"], "#94a3b8")

    return f"""
    <table width="100%" cellpadding="0" cellspacing="0"
           style="background:#1e293b;border:1px solid rgba(255,255,255,0.08);
                  border-radius:12px;margin-bottom:12px;overflow:hidden;">
      <tr>
        <td style="padding:16px 20px;">

          <!-- Row 1: number + badge + score -->
          <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:8px;">
            <tr>
              <td>
                <span style="color:#475569;font-size:13px;font-weight:600;margin-right:8px;">#{n}</span>
                {badge}
              </td>
              <td align="right">
                <span style="font-size:22px;font-weight:800;color:#fbbf24;">{score}</span>
                <span style="font-size:11px;color:#64748b;"> / 100</span>
              </td>
            </tr>
          </table>

          <!-- Row 2: address -->
          <p style="margin:0 0 2px;font-size:16px;font-weight:700;color:#ffffff;">{address}</p>
          <p style="margin:0 0 8px;font-size:13px;color:#64748b;">{city_line}
            {f'&nbsp;&middot;&nbsp;<span style="color:{urgency_color};font-weight:600;">{lead["urgency"]}</span>' if lead["urgency"] else ""}
          </p>

          <!-- Row 3: property meta -->
          {f'<p style="margin:0 0 8px;font-size:12px;color:#475569;">{prop_meta}</p>' if prop_meta else ""}

          <!-- Row 4: signals -->
          <p style="margin:0 0 12px;font-size:12px;color:#94a3b8;
                    padding:6px 10px;background:rgba(255,255,255,0.04);
                    border-radius:6px;border-left:3px solid #fbbf24;">
            {signals}
          </p>

          <!-- Row 5: owner contact -->
          {f'<div style="border-top:1px solid rgba(255,255,255,0.06);padding-top:10px;">{owner_line}</div>' if owner_line else ""}

        </td>
      </tr>
    </table>"""


def render_lead_email(
    subscriber: Subscriber,
    leads: list[dict],
    subject_prefix: str = "Your leads are ready",
    zip_codes: Optional[list[str]] = None,
) -> tuple[str, str, str]:
    """
    Returns (subject, html_body, plain_text_body).
    """
    vertical_label = _VERTICAL_LABELS.get(subscriber.vertical or "", subscriber.vertical or "")
    name           = subscriber.name or "there"
    n_leads        = len(leads)
    today_str      = datetime.now(timezone.utc).strftime("%A, %B %-d")
    zip_str        = ", ".join(zip_codes or []) if zip_codes else "your territory"
    tier_label     = (subscriber.tier or "").title()

    subject = f"{subject_prefix} — {n_leads} {vertical_label} leads in {zip_str}"

    # Lead cards HTML
    cards_html = "".join(_lead_card_html(lead, i + 1) for i, lead in enumerate(leads))

    # Plain text fallback
    lines = [
        f"Forced Action — {vertical_label} Lead Report",
        f"{today_str} · {n_leads} leads in {zip_str}",
        "",
    ]
    for i, lead in enumerate(leads, 1):
        lines.append(f"#{i} [{lead['lead_tier']}] Score: {int(lead['vertical_score'])}")
        lines.append(f"   {lead['address']}, {lead['city']} {lead['zip']}")
        if lead["signals"]:
            lines.append(f"   Signals: {', '.join(lead['signals'])}")
        if lead["owner_name"]:
            lines.append(f"   Owner: {lead['owner_name']}")
        if lead["phone"]:
            lines.append(f"   Phone: {lead['phone']}")
        lines.append("")
    plain_text = "\n".join(lines)

    from config.settings import get_settings
    _settings = get_settings()
    feed_url = (
        f"{_settings.app_base_url}/dashboard/{subscriber.event_feed_uuid}"
        if subscriber.event_feed_uuid else _settings.app_base_url
    )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
</head>
<body style="margin:0;padding:0;background:#0f172a;font-family:Inter,Arial,sans-serif;color:#e2e8f0;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#0f172a;padding:32px 0;">
    <tr><td align="center">
      <table width="580" cellpadding="0" cellspacing="0"
             style="max-width:580px;width:100%;">

        <!-- Header -->
        <tr>
          <td style="padding:0 0 20px;">
            <table width="100%" cellpadding="0" cellspacing="0">
              <tr>
                <td>
                  <p style="margin:0;font-size:20px;font-weight:800;color:#ffffff;">
                    Forced <span style="color:#fbbf24;">Action</span>
                  </p>
                </td>
                <td align="right">
                  <span style="font-size:12px;color:#475569;">{today_str}</span>
                </td>
              </tr>
            </table>
          </td>
        </tr>

        <!-- Headline -->
        <tr>
          <td style="padding:0 0 20px;">
            <h1 style="margin:0 0 6px;font-size:24px;font-weight:800;color:#ffffff;">
              {n_leads} {vertical_label} lead{'s' if n_leads != 1 else ''} in your territory
            </h1>
            <p style="margin:0;font-size:14px;color:#64748b;">
              {tier_label} plan &middot; {zip_str}
              {' &middot; <span style="color:#fbbf24;">Founding Member</span>' if subscriber.founding_member else ''}
            </p>
          </td>
        </tr>

        <!-- Lead cards -->
        <tr>
          <td>
            {cards_html}
          </td>
        </tr>

        <!-- CTA -->
        <tr>
          <td style="padding:20px 0 0;">
            <table cellpadding="0" cellspacing="0">
              <tr>
                <td style="background:#fbbf24;border-radius:8px;">
                  <a href="{feed_url}"
                     style="display:inline-block;padding:13px 28px;color:#0f172a;
                            font-size:14px;font-weight:700;text-decoration:none;">
                    View All Leads &rarr;
                  </a>
                </td>
              </tr>
            </table>
          </td>
        </tr>

        <!-- Footer -->
        <tr>
          <td style="padding:32px 0 0;font-size:12px;color:#334155;border-top:1px solid rgba(255,255,255,0.06);margin-top:32px;">
            <p style="margin:0 0 4px;">
              Forced Action &mdash; Hillsborough County Distressed Property Intelligence
            </p>
            <p style="margin:0;">
              You're receiving this because you're subscribed to the {vertical_label} vertical.
              <a href="{_settings.app_base_url}" style="color:#475569;text-decoration:none;">forcedaction.io</a>
            </p>
          </td>
        </tr>

      </table>
    </td></tr>
  </table>
</body>
</html>"""

    return subject, html, plain_text


# ---------------------------------------------------------------------------
# Send
# ---------------------------------------------------------------------------

def send_subscriber_lead_email(
    subscriber: Subscriber,
    leads: list[dict],
    subject_prefix: str = "Your leads are ready",
    zip_codes: Optional[list[str]] = None,
) -> bool:
    if not subscriber.email:
        logger.warning("Subscriber %s has no email — skipping", subscriber.id)
        return False
    if not leads:
        logger.info("No leads for subscriber %s — skipping", subscriber.id)
        return False

    subject, html, text = render_lead_email(subscriber, leads, subject_prefix, zip_codes)
    try:
        send_email(to=subscriber.email, subject=subject, body_text=text, body_html=html)
        logger.info(
            "Lead email sent → %s (%d leads, vertical=%s, zips=%s)",
            subscriber.email, len(leads), subscriber.vertical, zip_codes,
        )
        return True
    except Exception:
        logger.error("Failed to send lead email to subscriber %s", subscriber.id, exc_info=True)
        return False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_HEALTH_CHECK_SOURCES = frozenset({
    "insurance_claims", "storm_damage", "fire_incidents",
    "flood_damage", "roofing_permits",
})


def _check_scraper_health(db: Session) -> bool:
    """
    Check whether yesterday's scraper runs completed successfully.
    Sends an ops alert if any runs failed or are missing.
    Returns True (healthy) or False (stale/failed).
    Advisory only — never blocks email delivery.
    """
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date()
    try:
        rows = db.execute(
            select(ScraperRunStats).where(ScraperRunStats.run_date == yesterday)
        ).scalars().all()
    except Exception as exc:
        logger.error("Could not query ScraperRunStats for health check: %s", exc)
        return False

    if not rows:
        send_alert(
            subject=f"[Forced Action] WARNING: No scraper data for {yesterday}",
            body=(
                f"No ScraperRunStats rows found for {yesterday}.\n"
                f"Scrapers may not have run. Lead emails will proceed with potentially stale data.\n\n"
                f"Check: python -m src.tasks.run_scrapers hillsborough"
            ),
        )
        logger.warning("No scraper run stats for %s — alerting ops", yesterday)
        return False

    ran_sources = {r.source_type for r in rows}
    failed = [r for r in rows if not r.run_success]
    missing = _HEALTH_CHECK_SOURCES - ran_sources

    if failed or missing:
        lines = (
            [f"  {r.source_type}: {r.error_message or 'run_success=False'}" for r in failed]
            + [f"  {s}: no row found" for s in sorted(missing)]
        )
        send_alert(
            subject=f"[Forced Action] WARNING: Scraper issues on {yesterday} — leads may be stale",
            body=(
                f"The following scraper issues were detected for {yesterday}:\n\n"
                + "\n".join(lines)
                + "\n\nLead emails will proceed. Verify data freshness."
            ),
        )
        logger.warning(
            "Scraper health issues for %s: %d failed, %d missing",
            yesterday, len(failed), len(missing),
        )
        return False

    return True


def _upsert_sent_leads(db: Session, subscriber_id: int, property_ids: list[int]) -> None:
    """
    Bulk-upsert SentLead rows for a just-delivered email.
    ON CONFLICT DO UPDATE refreshes sent_at so the 7-day window slides forward on resend.
    """
    if not property_ids:
        return
    now = datetime.now(timezone.utc)
    rows = [
        {"subscriber_id": subscriber_id, "property_id": pid, "sent_at": now}
        for pid in property_ids
    ]
    stmt = (
        pg_insert(SentLead)
        .values(rows)
        .on_conflict_do_update(constraint="uq_sent_lead", set_={"sent_at": now})
    )
    db.execute(stmt)
    db.flush()


# ---------------------------------------------------------------------------
# Daily batch runner
# ---------------------------------------------------------------------------

def run_daily_emails(dry_run: bool = False, feed_uuid: Optional[str] = None) -> dict:
    stats = {"subscribers": 0, "sent": 0, "skipped_no_leads": 0, "skipped_no_email": 0, "errors": 0}

    with get_db_context() as db:
        # Pre-send health check — advisory only, never blocks delivery
        _stale_data = not _check_scraper_health(db)

        query = select(Subscriber).where(Subscriber.status.in_(["active", "grace"]))
        if feed_uuid:
            query = query.where(Subscriber.event_feed_uuid == feed_uuid)
        subscribers = db.execute(query).scalars().all()

        for sub in subscribers:
            stats["subscribers"] += 1

            if not sub.email:
                stats["skipped_no_email"] += 1
                continue

            # Get locked ZIPs
            zip_codes = db.execute(
                select(ZipTerritory.zip_code).where(
                    ZipTerritory.subscriber_id == sub.id,
                    ZipTerritory.status.in_(["locked", "grace"]),
                )
            ).scalars().all()

            limit = TIER_LEAD_LIMIT.get(sub.tier or "", DEFAULT_LEAD_LIMIT)
            leads = query_top_leads(db, sub, list(zip_codes), limit=limit)

            if not leads:
                stats["skipped_no_leads"] += 1
                logger.info("No Gold+ leads for subscriber %s (zips=%s) — skipping", sub.id, list(zip_codes))
                continue

            if dry_run:
                subject, _, _ = render_lead_email(sub, leads, zip_codes=list(zip_codes))
                logger.info("[DRY RUN] Would send to %s: %s (%d leads)", sub.email, subject, len(leads))
                stats["sent"] += 1
                continue

            subject_prefix = (
                "[STALE DATA] Your leads are ready" if _stale_data
                else "Your leads are ready"
            )
            try:
                ok = send_subscriber_lead_email(
                    sub, leads,
                    subject_prefix=subject_prefix,
                    zip_codes=list(zip_codes),
                )
                if ok:
                    stats["sent"] += 1
                    _upsert_sent_leads(db, sub.id, [lead["property_id"] for lead in leads])
                else:
                    stats["errors"] += 1
            except Exception:
                logger.error("Unexpected error sending to subscriber %s", sub.id, exc_info=True)
                stats["errors"] += 1

    logger.info(
        "Daily subscriber emails: %d subscribers, %d sent, %d no-leads, %d no-email, %d errors",
        stats["subscribers"], stats["sent"], stats["skipped_no_leads"],
        stats["skipped_no_email"], stats["errors"],
    )
    return stats


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Send daily lead emails to subscribers")
    parser.add_argument("--dry-run", action="store_true", help="Render and log without sending")
    parser.add_argument("--feed-uuid", default=None, help="Send to a single subscriber only")
    args = parser.parse_args()

    result = run_daily_emails(dry_run=args.dry_run, feed_uuid=args.feed_uuid)
    print(result)
