"""
Vendor cost reporting layer — prepares structured data for Revenue Pulse SMS
and the daily HTML vendor cost email.
"""

import logging
from datetime import date, datetime, timezone
from typing import Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from src.core.models import ApiUsageLog, VendorCostPause
from src.services.vendor_cost_pause_service import (
    count_skipped_actions,
    list_active_pauses,
)

logger = logging.getLogger(__name__)


def build_vendor_cost_summary(db: Session) -> dict:
    """
    Build the full vendor cost summary dict used by both Revenue Pulse SMS
    and the daily HTML email.

    Returns:
        {
            "vendor_totals": { "claude": float, "telnyx": float, "stripe": float },
            "anomalies": [ { pause_target, cost_usd, threshold, vendor }, ... ],
            "active_pauses": [ { pause_target, vendor, cost_usd, skipped, ... }, ... ],
            "digital_ocean": "pending",
        }
    """
    today = datetime.now(timezone.utc).date()
    day_start = datetime.combine(today, datetime.min.time(), tzinfo=timezone.utc)

    # Vendor totals
    vendor_totals: dict[str, float] = {}
    rows = db.execute(
        select(
            ApiUsageLog.service,
            func.sum(ApiUsageLog.cost_usd),
        ).where(
            ApiUsageLog.created_at >= day_start,
            ApiUsageLog.cost_usd > 0,
            ApiUsageLog.blocked_by_pause == False,
        ).group_by(ApiUsageLog.service)
    ).all()
    for svc, cost in rows:
        vendor_totals[svc] = float(cost or 0.0)

    # Active pauses with skipped action counts
    active_pauses = list_active_pauses(db)
    pause_summaries = []
    for p in active_pauses:
        skipped = count_skipped_actions(
            db, p.vendor, p.pause_target,
            since=p.paused_at,
        )
        severity = float(p.anomaly_score or 0) if p.anomaly_score else 0
        pause_summaries.append({
            "id": p.id,
            "vendor": p.vendor,
            "pause_target": p.pause_target,
            "cost_usd": float(p.today_cost_usd or 0),
            "threshold_usd": float(p.threshold_usd or 0),
            "skipped_actions": skipped,
            "severity": severity,
            "paused_at": p.paused_at.isoformat(),
            "auto_resume_at": p.auto_resume_at.isoformat() if p.auto_resume_at else None,
        })

    # Sort by severity descending for top-pause selection
    pause_summaries.sort(key=lambda x: x["severity"], reverse=True)

    return {
        "vendor_totals": vendor_totals,
        "active_pauses": pause_summaries,
        "digital_ocean": "pending",
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def format_sms_cost_summary(summary: dict) -> str | None:
    """
    Format a compact vendor cost line for Revenue Pulse SMS.
    Returns None if there's nothing noteworthy.
    """
    totals = summary.get("vendor_totals", {})
    claude = totals.get("claude", 0)
    telnyx = totals.get("telnyx", 0)
    stripe_cost = totals.get("stripe", 0)
    total = claude + telnyx + stripe_cost

    if total == 0 and not summary["active_pauses"]:
        return None

    parts = [f"💰 ${total:.2f} vendor"]

    if claude > 0:
        parts.append(f"Claude=${claude:.2f}")
    if telnyx > 0:
        parts.append(f"Telnyx=${telnyx:.2f}")

    pauses = summary.get("active_pauses", [])
    if pauses:
        top = pauses[0]
        parts.append(f"⚠️ pause:{top['pause_target']}")
        parts.append(f"skipped:{top['skipped_actions']}")
        if len(pauses) > 1:
            parts.append(f"+{len(pauses) - 1} more")

    return " | ".join(parts)


def format_html_vendor_cost_report(summary: dict) -> str:
    """
    Build the HTML block for the daily vendor cost email.
    Returns a full <div> ready to inject into the emailer.
    """
    totals = summary.get("vendor_totals", {})
    claude = totals.get("claude", 0)
    telnyx = totals.get("telnyx", 0)
    stripe_cost = totals.get("stripe", 0)
    total = claude + telnyx + stripe_cost

    # Vendor total rows
    vendor_rows = ""
    for label, cost in [("Claude/Anthropic", claude), ("Telnyx", telnyx), ("Stripe", stripe_cost)]:
        color = "#4ade80" if cost < 50 else ("#fbbf24" if cost < 200 else "#ef4444")
        vendor_rows += f"""
        <tr>
            <td style="padding:6px 12px;border-bottom:1px solid #2a2a3a;text-align:left;color:#e2e8f0;">{label}</td>
            <td style="padding:6px 12px;border-bottom:1px solid #2a2a3a;text-align:right;color:{color};font-weight:600;">${cost:.2f}</td>
        </tr>"""

    # Anomalies / active pauses rows
    pause_rows = ""
    for p in summary.get("active_pauses", []):
        pause_rows += f"""
        <tr>
            <td style="padding:6px 12px;border-bottom:1px solid #2a2a3a;text-align:left;color:#e2e8f0;">{p['vendor']}</td>
            <td style="padding:6px 12px;border-bottom:1px solid #2a2a3a;text-align:left;color:#fbbf24;">{p['pause_target']}</td>
            <td style="padding:6px 12px;border-bottom:1px solid #2a2a3a;text-align:right;color:#ef4444;">${p['cost_usd']:.2f}</td>
            <td style="padding:6px 12px;border-bottom:1px solid #2a2a3a;text-align:right;color:#94a3b8;">{p['skipped_actions']}</td>
            <td style="padding:6px 12px;border-bottom:1px solid #2a2a3a;text-align:right;color:#94a3b8;">{p.get('auto_resume_at', '')[:16] if p.get('auto_resume_at') else ''}</td>
        </tr>"""

    if not pause_rows:
        pause_rows = """
        <tr>
            <td colspan="5" style="padding:12px;text-align:center;color:#64748b;">No active pauses</td>
        </tr>"""

    # DigitalOcean placeholder
    do_status = summary.get("digital_ocean", "pending")
    do_color = "#64748b" if do_status == "pending" else "#4ade80"

    html = f"""
    <div style="margin-top:20px;background:#0f172a;border:1px solid #2a2a3a;border-radius:8px;padding:16px;">
        <h2 style="color:#fbbf24;font-size:15px;margin:0 0 12px;border-bottom:1px solid #2a2a3a;padding-bottom:6px;">
            Daily Vendor Cost Summary &mdash; ${total:.2f} total
        </h2>

        <table style="width:100%;border-collapse:collapse;margin-bottom:12px;">
            <tr>
                <th style="padding:6px 12px;border-bottom:2px solid #fbbf24;text-align:left;color:#fbbf24;font-weight:600;">Vendor</th>
                <th style="padding:6px 12px;border-bottom:2px solid #fbbf24;text-align:right;color:#fbbf24;font-weight:600;">Today</th>
            </tr>
            {vendor_rows}
            <tr>
                <td style="padding:6px 12px;text-align:left;color:#64748b;">DigitalOcean</td>
                <td style="padding:6px 12px;text-align:right;color:{do_color};">{do_status}</td>
            </tr>
        </table>

        <h3 style="color:#fbbf24;font-size:13px;margin:12px 0 6px;">Active Cost Pauses</h3>
        <table style="width:100%;border-collapse:collapse;">
            <tr>
                <th style="padding:4px 8px;border-bottom:1px solid #2a2a3a;text-align:left;color:#94a3b8;font-size:11px;">Vendor</th>
                <th style="padding:4px 8px;border-bottom:1px solid #2a2a3a;text-align:left;color:#94a3b8;font-size:11px;">Target</th>
                <th style="padding:4px 8px;border-bottom:1px solid #2a2a3a;text-align:right;color:#94a3b8;font-size:11px;">Cost</th>
                <th style="padding:4px 8px;border-bottom:1px solid #2a2a3a;text-align:right;color:#94a3b8;font-size:11px;">Skipped</th>
                <th style="padding:4px 8px;border-bottom:1px solid #2a2a3a;text-align:right;color:#94a3b8;font-size:11px;">Auto-resume</th>
            </tr>
            {pause_rows}
        </table>
    </div>
    """
    return html