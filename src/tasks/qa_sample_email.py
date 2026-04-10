"""
Daily QA sample email — 5 new Gold+ leads per vertical.

Sent to ALERT_EMAIL each morning after scoring so sales can manually QA
lead quality and feed back signal-level feedback.

"New" = Gold+ today that were NOT Gold+ yesterday.

Usage:
    python -m src.tasks.qa_sample_email                  # today
    python -m src.tasks.qa_sample_email --dry-run        # log only, no send
    python -m src.tasks.qa_sample_email --date 2026-04-07

Cron (after CDS scoring, e.g. 08:30 UTC):
    30 8 * * 1-5 cd /path/to/app && python -m src.tasks.qa_sample_email
"""

import argparse
import logging
from collections import defaultdict
from datetime import date, datetime, timezone
from typing import Optional

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from src.core.database import get_db_context
from src.core.models import DistressScore, Owner, Property
from src.services.email import send_alert, send_email
from src.utils.logger import setup_logging
from config.settings import get_settings

setup_logging()
logger = logging.getLogger(__name__)

GOLD_PLUS_TIERS = {"Ultra Platinum", "Platinum", "Gold"}
LEADS_PER_VERTICAL = 5

_VERTICAL_LABELS = {
    "roofing":          "Roofing",
    "restoration":      "Restoration / Remediation",
    "wholesalers":      "Wholesale / Investor",
    "fix_flip":         "Fix & Flip",
    "public_adjusters": "Public Adjusters",
    "attorneys":        "Attorneys",
}

_SIGNAL_LABELS = {
    "foreclosures":       "Foreclosure",
    "tax_delinquencies":  "Tax Delinquency",
    "code_violations":    "Code Violation",
    "judgment_liens":     "Judgment Lien",
    "irs_tax_liens":      "IRS Tax Lien",
    "hoa_liens":          "HOA Lien",
    "mechanics_liens":    "Mechanic's Lien",
    "tampa_code_liens":   "Tampa Code Lien",
    "county_code_liens":  "County Code Lien",
    "deed_transfers":     "Deed Transfer",
    "probate":            "Probate",
    "evictions":          "Eviction",
    "bankruptcy":         "Bankruptcy",
    "building_permits":   "Building Permit",
    "enforcement_permit": "Enforcement Permit",
    "insurance_claim":    "Insurance Claim",
    "Fire":               "Fire Incident",
    "storm_damage":       "Storm Damage",
    "flood_damage":       "Flood Damage",
}


def _query_new_gold_leads(db: Session, run_date: date) -> dict[str, list[dict]]:
    """
    Return up to LEADS_PER_VERTICAL new Gold+ leads per vertical.
    "New" = Gold+ today whose most recent prior score (before today) was NOT Gold+,
    or who have never been scored before.

    Comparing against yesterday's batch is wrong: the CDS engine only writes a new
    DistressScore row when the score changes, so yesterday's set is always tiny and
    nearly everything appears "new."  We use DISTINCT ON to find each property's
    actual last known tier instead.
    """
    # Today's Gold+ scores
    rows = db.execute(
        select(DistressScore, Property, Owner)
        .join(Property, Property.id == DistressScore.property_id)
        .outerjoin(Owner, Owner.property_id == Property.id)
        .where(
            func.date(DistressScore.score_date) == run_date,
            DistressScore.lead_tier.in_(GOLD_PLUS_TIERS),
        )
        .order_by(DistressScore.final_cds_score.desc())
    ).all()

    if not rows:
        return {}

    today_pids = [score.property_id for score, _, _ in rows]

    # Property IDs whose most recent prior score was already Gold+
    prior_rows = db.execute(
        text("""
            SELECT DISTINCT ON (property_id) property_id, lead_tier
            FROM distress_scores
            WHERE property_id = ANY(:pids)
              AND date(score_date) < :run_date
            ORDER BY property_id, score_date DESC
        """),
        {"pids": today_pids, "run_date": run_date},
    ).fetchall()
    already_gold_plus = {row[0] for row in prior_rows if row[1] in GOLD_PLUS_TIERS}

    by_vertical: dict[str, list[dict]] = defaultdict(list)

    for score, prop, owner in rows:
        if score.property_id in already_gold_plus:
            continue  # not new today

        vs = score.vertical_scores or {}
        if not vs:
            continue
        best_vertical = max(vs, key=lambda k: vs.get(k, 0))
        if best_vertical not in _VERTICAL_LABELS:
            continue
        if len(by_vertical[best_vertical]) >= LEADS_PER_VERTICAL:
            continue

        signals = score.distress_types or []
        signal_labels = [_SIGNAL_LABELS.get(s, s.replace("_", " ").title()) for s in signals]

        by_vertical[best_vertical].append({
            "address":        prop.address or "—",
            "city":           prop.city or "",
            "zip":            prop.zip or "",
            "cds_score":      float(score.final_cds_score or 0),
            "vertical_score": float(vs.get(best_vertical, 0)),
            "lead_tier":      score.lead_tier or "Gold",
            "signals":        signal_labels,
            "distress_types": signals,
            "owner_name":     owner.owner_name if owner else None,
            "phone":          (owner.phone_1 or owner.phone_2 or owner.phone_3) if owner else None,
            "absentee":       (owner.absentee_status or "") if owner else "",
        })

    return dict(by_vertical)


def _render_html(run_date: date, by_vertical: dict[str, list[dict]]) -> tuple[str, str]:
    """Returns (subject, html_body)."""
    total = sum(len(v) for v in by_vertical.values())
    subject = f"[FA QA] {total} new Gold+ leads today — {run_date} ({len(by_vertical)} verticals)"

    vert_sections = []
    for key, label in _VERTICAL_LABELS.items():
        leads = by_vertical.get(key, [])
        if not leads:
            vert_sections.append(
                f'<tr><td style="padding:8px 0;color:#64748b;font-size:13px;">'
                f'<strong style="color:#94a3b8;">{label}</strong> — no new leads today</td></tr>'
            )
            continue

        cards = ""
        for i, lead in enumerate(leads, 1):
            sig_str = " · ".join(lead["signals"]) if lead["signals"] else "—"
            owner_line = ""
            if lead["owner_name"]:
                owner_line += f'<span style="color:#e2e8f0;">{lead["owner_name"]}</span>'
                if "absentee" in lead["absentee"].lower():
                    owner_line += ' <span style="color:#f87171;font-size:11px;">(absentee)</span>'
            if lead["phone"]:
                owner_line += f' &nbsp;·&nbsp; <span style="color:#94a3b8;">{lead["phone"]}</span>'

            tier_colors = {
                "Ultra Platinum": "#c4b5fd",
                "Platinum":       "#fbbf24",
                "Gold":           "#fde68a",
            }
            tier_color = tier_colors.get(lead["lead_tier"], "#fde68a")

            cards += f"""
            <tr>
              <td style="padding:6px 0 6px 12px;border-left:3px solid {tier_color};margin-bottom:6px;">
                <span style="font-size:13px;font-weight:700;color:#ffffff;">
                  #{i} {lead['address']}, {lead['city']} {lead['zip']}
                </span>
                &nbsp;
                <span style="font-size:12px;color:{tier_color};font-weight:600;">
                  {lead['lead_tier']} · {int(lead['vertical_score'])}/100
                </span>
                <br/>
                <span style="font-size:11px;color:#94a3b8;">{sig_str}</span>
                {'<br/><span style="font-size:11px;color:#64748b;">' + owner_line + '</span>' if owner_line else ''}
              </td>
            </tr>"""

        vert_sections.append(f"""
        <tr>
          <td style="padding:16px 0 4px;">
            <p style="margin:0 0 8px;font-size:14px;font-weight:700;color:#fbbf24;">
              {label} — {len(leads)} new lead{'s' if len(leads) != 1 else ''}
            </p>
            <table width="100%" cellpadding="0" cellspacing="0">{cards}</table>
          </td>
        </tr>""")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"/></head>
<body style="margin:0;padding:0;background:#0f172a;font-family:Inter,Arial,sans-serif;color:#e2e8f0;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#0f172a;padding:24px 0;">
    <tr><td align="center">
      <table width="600" cellpadding="0" cellspacing="0" style="max-width:600px;width:100%;">
        <tr>
          <td style="padding:0 0 16px;">
            <p style="margin:0;font-size:18px;font-weight:800;color:#ffffff;">
              Forced <span style="color:#fbbf24;">Action</span>
              <span style="font-size:13px;font-weight:400;color:#64748b;margin-left:8px;">QA Sample — {run_date}</span>
            </p>
            <p style="margin:4px 0 0;font-size:13px;color:#475569;">
              {total} new Gold+ leads across {len(by_vertical)} vertical(s) — review signal quality below
            </p>
          </td>
        </tr>
        {''.join(vert_sections)}
        <tr>
          <td style="padding:24px 0 0;font-size:11px;color:#334155;border-top:1px solid rgba(255,255,255,0.06);">
            Forced Action QA digest — ops use only. Reply with feedback on lead quality.
          </td>
        </tr>
      </table>
    </td></tr>
  </table>
</body>
</html>"""

    return subject, html


def run_qa_sample(run_date: Optional[date] = None, dry_run: bool = False) -> dict:
    run_date = run_date or date.today()
    stats = {"date": str(run_date), "verticals": 0, "total_leads": 0, "sent": False}

    with get_db_context() as db:
        by_vertical = _query_new_gold_leads(db, run_date)

    stats["verticals"] = len(by_vertical)
    stats["total_leads"] = sum(len(v) for v in by_vertical.values())

    if not by_vertical:
        logger.info("[qa_sample] No new Gold+ leads for %s — skipping email", run_date)
        return stats

    subject, html = _render_html(run_date, by_vertical)

    if dry_run:
        logger.info("[qa_sample][DRY RUN] Would send: %s (%d leads)", subject, stats["total_leads"])
        for key, label in _VERTICAL_LABELS.items():
            leads = by_vertical.get(key, [])
            logger.info("  %-25s %d leads", label, len(leads))
        stats["sent"] = False
        return stats

    settings = get_settings()
    if not settings.alert_email:
        logger.warning("[qa_sample] ALERT_EMAIL not configured — skipping send")
        return stats

    plain = f"Forced Action QA Sample — {run_date}\n{stats['total_leads']} new Gold+ leads\n\n"
    for key, label in _VERTICAL_LABELS.items():
        leads = by_vertical.get(key, [])
        plain += f"{label}: {len(leads)}\n"
        for i, lead in enumerate(leads, 1):
            plain += f"  #{i} {lead['address']}, {lead['city']} {lead['zip']} | {lead['lead_tier']} {int(lead['vertical_score'])}/100\n"
            plain += f"       {', '.join(lead['signals'])}\n"

    ok = send_email(to=settings.alert_email, subject=subject, body_text=plain, body_html=html)
    stats["sent"] = ok
    if ok:
        logger.info("[qa_sample] Sent to %s: %s", settings.alert_email, subject)
    else:
        logger.error("[qa_sample] Failed to send QA email")

    return stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Send daily QA lead sample to ops")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--date", type=lambda s: date.fromisoformat(s), default=None)
    args = parser.parse_args()

    result = run_qa_sample(run_date=args.date, dry_run=args.dry_run)
    print(f"  Verticals : {result['verticals']}")
    print(f"  Leads     : {result['total_leads']}")
    print(f"  Sent      : {result['sent']}")
