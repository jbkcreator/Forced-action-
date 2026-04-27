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

from sqlalchemy import select, desc, and_, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from src.core.database import get_db_context
from src.core.models import (
    BuildingPermit, Deed, DistressScore, Owner, Property, ScraperRunStats, SentLead,
    Subscriber, ZipTerritory,
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

LEAD_FRESHNESS_DAYS = 14  # only include leads scored within this many days


def _stale_sent_property_ids(db: Session, subscriber_id: int) -> list[int]:
    """
    Return property IDs that were sent to this subscriber AND have no new distress
    signals loaded since the send date.  These are excluded from the next email.

    A property becomes eligible again only when a new signal record is added to any
    source table (building_permits, code_violations, legal_and_liens, foreclosures,
    legal_proceedings, tax_delinquencies, incidents, deeds) after the last send_at.
    This ensures subscribers only see leads where something materially changed —
    never the same stale lead repeated week after week.
    """
    rows = db.execute(
        text("""
            SELECT sl.property_id
            FROM sent_leads sl
            WHERE sl.subscriber_id = :sub_id
              AND NOT (
                  EXISTS (SELECT 1 FROM building_permits    WHERE property_id = sl.property_id AND date_added > sl.sent_at)
                  OR EXISTS (SELECT 1 FROM code_violations  WHERE property_id = sl.property_id AND date_added > sl.sent_at)
                  OR EXISTS (SELECT 1 FROM legal_and_liens  WHERE property_id = sl.property_id AND date_added > sl.sent_at)
                  OR EXISTS (SELECT 1 FROM foreclosures     WHERE property_id = sl.property_id AND date_added > sl.sent_at)
                  OR EXISTS (SELECT 1 FROM legal_proceedings WHERE property_id = sl.property_id AND date_added > sl.sent_at)
                  OR EXISTS (SELECT 1 FROM tax_delinquencies WHERE property_id = sl.property_id AND date_added > sl.sent_at)
                  OR EXISTS (SELECT 1 FROM incidents        WHERE property_id = sl.property_id AND date_added > sl.sent_at)
                  OR EXISTS (SELECT 1 FROM deeds            WHERE property_id = sl.property_id AND date_added > sl.sent_at)
              )
        """),
        {"sub_id": subscriber_id},
    ).scalars().all()
    return list(rows)

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

    # Dedup: exclude properties already sent to this subscriber where no new signal
    # has been added since the send.  Properties with new signals are eligible again —
    # something materially changed for the owner since we last told the subscriber.
    stale_sent_ids = _stale_sent_property_ids(db, subscriber.id)

    # Freshness: only include leads scored within LEAD_FRESHNESS_DAYS
    cutoff_freshness = datetime.now(timezone.utc) - timedelta(days=LEAD_FRESHNESS_DAYS)

    from config.settings import get_settings
    _settings = get_settings()

    filters = [
        Property.zip.in_(zip_codes),
        Property.county_id == subscriber.county_id,
        DistressScore.lead_tier.in_(GOLD_PLUS_TIERS),
        DistressScore.qualified == True,
        score_col > 0,
        DistressScore.score_date >= cutoff_freshness,
    ]
    if stale_sent_ids:
        filters.append(~Property.id.in_(stale_sent_ids))

    # Dead lead filter: skip properties sold in the last 60 days.
    # A recent deed transfer means the property changed hands — the new owner
    # is not a motivated seller and outreach is wasted.
    dead_sale_cutoff = date.today() - timedelta(days=60)
    recent_sold_subq = select(Deed.property_id).where(
        Deed.record_date >= dead_sale_cutoff
    ).scalar_subquery()
    filters.append(~Property.id.in_(recent_sold_subq))

    # Owner-occupied suppression for wholesalers: in-county individuals are
    # almost certainly homeowners living in the property — not absentee/investor
    # targets. LLCs, Trusts, and out-of-county owners are kept regardless.
    if vertical == "wholesalers":
        filters.append(
            ~and_(
                Owner.absentee_status == "In-County",
                Owner.owner_type == "Individual",
            )
        )

    # Production only: exclude leads with no owner phone number
    if not _settings.debug:
        filters.append(Owner.phone.isnot(None))

    rows = db.execute(
        select(Property, DistressScore, Owner)
        .join(DistressScore, DistressScore.property_id == Property.id)
        .outerjoin(Owner, Owner.property_id == Property.id)
        .where(and_(*filters))
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
            "distress_types": signals,
            "owner_name":     owner.owner_name if owner else None,
            "owner_type":     owner.owner_type if owner else None,
            "phone":          (owner.phone_1 or owner.phone_2 or owner.phone_3) if owner else None,
            "email":          (owner.email_1 or owner.email_2) if owner else None,
            "absentee":       (owner.absentee_status in ("Out-of-County", "Out-of-State")) if owner else False,
            "permit_status":  None,
            "permit_type_str": None,
        })

    # Bulk-fetch permit details for any lead whose signals include a permit signal.
    # Shows subscribers the actual permit type/status so they can qualify the lead.
    permit_prop_ids = [
        l["property_id"] for l in leads
        if any(s in ("building_permits", "enforcement_permit") for s in l["distress_types"])
    ]
    if permit_prop_ids:
        permit_rows = db.execute(
            select(
                BuildingPermit.property_id,
                BuildingPermit.status,
                BuildingPermit.permit_type,
                BuildingPermit.is_enforcement_permit,
            )
            .where(BuildingPermit.property_id.in_(permit_prop_ids))
            .order_by(
                BuildingPermit.property_id,
                BuildingPermit.is_enforcement_permit.desc(),  # enforcement first
                BuildingPermit.issue_date.desc(),
            )
        ).all()
        # Keep only the most relevant permit per property (first = enforcement or most recent)
        permit_map: dict[int, tuple[str | None, str | None]] = {}
        for pid, status, ptype, _ in permit_rows:
            if pid not in permit_map:
                permit_map[pid] = (status, ptype)

        for lead in leads:
            if lead["property_id"] in permit_map:
                lead["permit_status"], lead["permit_type_str"] = permit_map[lead["property_id"]]

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


def _permit_detail_html(lead: dict) -> str:
    """Return a small inline permit detail line if this lead has permit signal data."""
    pstatus = lead.get("permit_status")
    ptype   = lead.get("permit_type_str")
    if not pstatus and not ptype:
        return ""
    parts = []
    if ptype:
        parts.append(ptype)
    if pstatus:
        parts.append(f"Status: {pstatus}")
    return (
        f'<br/><span style="font-size:11px;color:#64748b;">Permit: {" · ".join(parts)}</span>'
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
        owner_type = lead.get("owner_type") or ""
        owner_type_tag = (
            f' <span style="color:#7dd3fc;font-size:11px;">({owner_type})</span>'
            if owner_type and owner_type != "Individual" else ""
        )
        owner_line += f'<p style="margin:0 0 4px;font-size:13px;color:#e2e8f0;">{lead["owner_name"]}{absentee_tag}{owner_type_tag}</p>'
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
            {_permit_detail_html(lead)}
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


def _check_scraper_health(db: Session, county_id: str = "hillsborough") -> bool:
    """
    Check whether yesterday's scraper runs completed successfully.
    Sends an ops alert if any runs failed or are missing.
    Returns True (healthy) or False (stale/failed).
    Advisory only — never blocks email delivery.

    Suppresses the failed-run alert if load_validator already sent a
    'scraper_error' alert within ALERT_COOLDOWN_HOURS — avoids duplicate emails
    for the same failures from two independent cron jobs.
    """
    from src.core.models import ScraperAlertLog
    from config.settings import get_settings

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
                f"Check individual scraper logs in logs/cron/ and re-run failing modules."
            ),
        )
        logger.warning("No scraper run stats for %s — alerting ops", yesterday)
        return False

    ran_sources = {r.source_type for r in rows}
    # Exclude no_data rows from failure count — those are expected empty days, not errors
    failed = [r for r in rows if not r.run_success and r.error_type != 'no_data']
    missing = _HEALTH_CHECK_SOURCES - ran_sources

    if failed or missing:
        # Suppress if load_validator already alerted for the same scraper failures
        cooldown = timedelta(hours=get_settings().alert_cooldown_hours)
        cutoff = datetime.now(timezone.utc) - cooldown
        try:
            recent_alert = db.execute(
                select(ScraperAlertLog).where(
                    ScraperAlertLog.source_type == '_batch',
                    ScraperAlertLog.county_id == county_id,
                    ScraperAlertLog.alert_type == 'scraper_error',
                    ScraperAlertLog.alerted_at >= cutoff,
                )
            ).scalar_one_or_none()
        except Exception:
            recent_alert = None

        if recent_alert:
            logger.info(
                "Health check: scraper failures already alerted by load_validator "
                "within cooldown — suppressing duplicate email"
            )
            return False

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
        # Log this alert so future calls within cooldown are suppressed
        try:
            db.add(ScraperAlertLog(
                source_type='_batch',
                county_id=county_id,
                alert_type='health_check',
                alerted_at=datetime.now(timezone.utc),
            ))
            db.flush()
        except Exception as exc:
            logger.warning("Could not write health check alert log: %s", exc)

        logger.warning(
            "Scraper health issues for %s: %d failed, %d missing",
            yesterday, len(failed), len(missing),
        )
        return False

    return True


def _upsert_sent_leads(db: Session, subscriber_id: int, property_ids: list[int]) -> None:
    """
    Bulk-upsert SentLead rows for a just-delivered email.
    ON CONFLICT DO UPDATE refreshes sent_at to now, so _stale_sent_property_ids will
    look for signals added AFTER this send — not after the original first send.
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

        # ── Duplicate safety net ───────────────────────────────────────────
        # The DB partial unique index + upsert logic should prevent duplicates,
        # but this catches any that slip through (direct SQL inserts, migration
        # bugs, etc.).  Deduplicates the send loop and fires an ops alert so
        # the underlying cause can be investigated.
        seen_keys: set[tuple] = set()
        deduped: list[Subscriber] = []
        for s in subscribers:
            key = (s.email.lower().strip() if s.email else None, s.vertical, s.county_id)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            deduped.append(s)

        if len(deduped) < len(subscribers):
            n_dupes = len(subscribers) - len(deduped)
            logger.error(
                "Duplicate active/grace subscribers detected before send loop — %d duplicate(s) skipped. "
                "Investigate how they bypassed the unique index.",
                n_dupes,
            )
            send_alert(
                subject=f"[Forced Action] ALERT: {n_dupes} duplicate subscriber(s) detected before email send",
                body=(
                    f"{n_dupes} active/grace subscriber row(s) share a (email, vertical, county_id) key "
                    f"with another active row.\n\n"
                    f"Only the first occurrence per key was emailed. Duplicates were skipped.\n\n"
                    f"This should not happen — the DB partial unique index (uq_subscriber_email_vertical_active) "
                    f"enforces uniqueness. Investigate how these rows were created:\n\n"
                    f"  SELECT lower(email), vertical, county_id, count(*), array_agg(id)\n"
                    f"  FROM subscribers\n"
                    f"  WHERE status IN ('active','grace')\n"
                    f"  GROUP BY 1,2,3 HAVING count(*) > 1;\n"
                ),
            )
        subscribers = deduped

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
