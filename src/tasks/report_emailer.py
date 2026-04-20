"""
Email sender for daily and weekly operations reports.

Renders the structured report dict (from daily_report.build_report or
weekly_report.build_report) into an HTML email and sends it to all
addresses in REPORT_RECIPIENTS.

Usage:
    # Called automatically by daily_report and weekly_report when
    # REPORT_RECIPIENTS is configured.

    # Manual send:
    python -m src.tasks.report_emailer --type daily
    python -m src.tasks.report_emailer --type weekly
"""

import logging
from datetime import date
from typing import Optional

from config.settings import get_settings
from src.services.email import send_email

logger = logging.getLogger(__name__)

VERTICAL_DISPLAY = {
    "roofing":          "Roofing",
    "restoration":      "Restoration / Remediation",
    "wholesalers":      "Wholesalers",
    "fix_flip":         "Fix & Flip",
    "public_adjusters": "Public Adjusters",
    "attorneys":        "Attorneys",
}


def _td(val, align="right"):
    return f'<td style="padding:6px 12px;border-bottom:1px solid #2a2a3a;text-align:{align};color:#e2e8f0;">{val}</td>'

def _th(val, align="right"):
    return f'<th style="padding:6px 12px;border-bottom:2px solid #fbbf24;text-align:{align};color:#fbbf24;font-weight:600;">{val}</th>'


def render_daily_html(report: dict) -> str:
    """Render a daily report dict into an HTML email body."""
    run_date = report["run_date"]
    s = report["scoring"]
    t = report["tiers"]
    vb = report.get("vertical_breakdown", {})
    total_vb = vb.pop("_total", 0) if "_total" in vb else sum(d["count"] for d in vb.values())
    crosstab = report.get("vertical_tier_crosstab", {})
    gd = report.get("gold_delta", {})

    gold_total = t.get("Ultra Platinum", 0) + t.get("Platinum", 0) + t.get("Gold", 0)

    # Scraper table rows
    scraper_rows = ""
    for row in report["scraper_data"]:
        status = "OK" if row.get("ok") else ("FAIL" if row.get("ok") is False else "--")
        color = "#4ade80" if row.get("ok") else ("#ef4444" if row.get("ok") is False else "#64748b")
        scraper_rows += f"""<tr>
            {_td(row['label'], 'left')}
            {_td(f"{row['scraped']:,}")}
            {_td(f'<span style="color:{color}">{status}</span>')}
        </tr>"""

    # Vertical breakdown rows
    vert_rows = ""
    for key, label in VERTICAL_DISPLAY.items():
        stats = vb.get(label, {"count": 0, "pct": 0.0})
        tiers_d = crosstab.get(label, {})
        new_today = sum(tiers_d.get(tier, {}).get("new_today", 0)
                        for tier in ("Ultra Platinum", "Platinum", "Gold"))
        vert_rows += f"""<tr>
            {_td(label, 'left')}
            {_td(f"{stats['count']:,}")}
            {_td(f"+{new_today}" if new_today else "0")}
            {_td(f"{stats['pct']:.1f}%")}
        </tr>"""

    # Delta rows
    delta_rows = ""
    for label, d in gd.get("by_vertical", {}).items():
        delta = d["delta"]
        color = "#4ade80" if delta > 0 else ("#ef4444" if delta < 0 else "#64748b")
        delta_str = f"+{delta}" if delta > 0 else str(delta)
        delta_rows += f"""<tr>
            {_td(label, 'left')}
            {_td(d['today'])}
            {_td(d['avg_7d'])}
            {_td(f'<span style="color:{color}">{delta_str}</span>')}
        </tr>"""

    # Top ZIPs
    zip_rows = ""
    for z in report.get("zip_breakdown", [])[:10]:
        zip_rows += f"""<tr>
            {_td(z['zip'], 'left')}
            {_td(z['ultra_platinum'])}
            {_td(z['platinum'])}
            {_td(z['gold'])}
            {_td(f"<strong>{z['total']}</strong>")}
        </tr>"""

    # Alerts
    alerts_html = ""
    if report.get("errors"):
        alerts_html = '<div style="background:#450a0a;border:1px solid #ef4444;border-radius:8px;padding:12px;margin-top:16px;">'
        for err in report["errors"]:
            alerts_html += f'<div style="color:#fca5a5;font-size:13px;">WARNING: {err}</div>'
        alerts_html += "</div>"

    html = f"""
    <div style="max-width:640px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#0f172a;color:#e2e8f0;padding:24px;border-radius:12px;">

        <div style="text-align:center;margin-bottom:24px;">
            <h1 style="color:#fbbf24;font-size:22px;margin:0;">Forced Action</h1>
            <p style="color:#94a3b8;font-size:14px;margin:4px 0 0;">Daily Report &mdash; {run_date}</p>
        </div>

        <!-- Headline KPIs -->
        <div style="display:flex;gap:12px;margin-bottom:20px;">
            <div style="flex:1;background:#1e293b;border-radius:8px;padding:14px;text-align:center;">
                <div style="color:#94a3b8;font-size:11px;text-transform:uppercase;letter-spacing:0.1em;">Scraped</div>
                <div style="color:#ffffff;font-size:24px;font-weight:700;">{report['total_scraped']:,}</div>
            </div>
            <div style="flex:1;background:#1e293b;border-radius:8px;padding:14px;text-align:center;">
                <div style="color:#94a3b8;font-size:11px;text-transform:uppercase;letter-spacing:0.1em;">Match Rate</div>
                <div style="color:#ffffff;font-size:24px;font-weight:700;">{report['match_pct']:.1f}%</div>
            </div>
            <div style="flex:1;background:#1e293b;border-radius:8px;padding:14px;text-align:center;">
                <div style="color:#94a3b8;font-size:11px;text-transform:uppercase;letter-spacing:0.1em;">Gold+ Leads</div>
                <div style="color:#fbbf24;font-size:24px;font-weight:700;">{gold_total:,}</div>
            </div>
            <div style="flex:1;background:#1e293b;border-radius:8px;padding:14px;text-align:center;">
                <div style="color:#94a3b8;font-size:11px;text-transform:uppercase;letter-spacing:0.1em;">New Today</div>
                <div style="color:#4ade80;font-size:24px;font-weight:700;">{s.get('leads_new', 0):,}</div>
            </div>
        </div>

        <!-- Tier Breakdown -->
        <h2 style="color:#fbbf24;font-size:15px;margin:20px 0 8px;border-bottom:1px solid #2a2a3a;padding-bottom:6px;">Tier Breakdown</h2>
        <table style="width:100%;border-collapse:collapse;">
            <tr>{_th('Tier', 'left')}{_th('Count')}</tr>
            <tr>{_td('Ultra Platinum', 'left')}{_td(f"{t.get('Ultra Platinum', 0):,}")}</tr>
            <tr>{_td('Platinum', 'left')}{_td(f"{t.get('Platinum', 0):,}")}</tr>
            <tr>{_td('Gold', 'left')}{_td(f"{t.get('Gold', 0):,}")}</tr>
            <tr>{_td('Silver', 'left')}{_td(f"{t.get('Silver', 0):,}")}</tr>
        </table>

        <!-- Gold+ by Vertical -->
        <h2 style="color:#fbbf24;font-size:15px;margin:20px 0 8px;border-bottom:1px solid #2a2a3a;padding-bottom:6px;">Gold+ by Vertical ({total_vb:,} total)</h2>
        <table style="width:100%;border-collapse:collapse;">
            <tr>{_th('Vertical', 'left')}{_th('Gold+')}{_th('New Today')}{_th('% of Total')}</tr>
            {vert_rows}
        </table>

        <!-- Today vs 7-Day Average -->
        <h2 style="color:#fbbf24;font-size:15px;margin:20px 0 8px;border-bottom:1px solid #2a2a3a;padding-bottom:6px;">Today vs 7-Day Average</h2>
        <table style="width:100%;border-collapse:collapse;">
            <tr>{_th('Vertical', 'left')}{_th('Today')}{_th('7d Avg')}{_th('Delta')}</tr>
            {delta_rows}
        </table>

        <!-- Top ZIPs -->
        <h2 style="color:#fbbf24;font-size:15px;margin:20px 0 8px;border-bottom:1px solid #2a2a3a;padding-bottom:6px;">Top 10 ZIPs by Gold+ Leads</h2>
        <table style="width:100%;border-collapse:collapse;">
            <tr>{_th('ZIP', 'left')}{_th('Ultra Plat')}{_th('Platinum')}{_th('Gold')}{_th('Total')}</tr>
            {zip_rows}
        </table>

        <!-- Scraper Status -->
        <h2 style="color:#fbbf24;font-size:15px;margin:20px 0 8px;border-bottom:1px solid #2a2a3a;padding-bottom:6px;">Scraper Ingest</h2>
        <table style="width:100%;border-collapse:collapse;">
            <tr>{_th('Source', 'left')}{_th('Scraped')}{_th('Status')}</tr>
            {scraper_rows}
        </table>

        {alerts_html}

        <div style="text-align:center;margin-top:24px;padding-top:16px;border-top:1px solid #2a2a3a;">
            <span style="color:#64748b;font-size:11px;">Forced Action &mdash; Distressed Property Intelligence</span>
        </div>
    </div>
    """
    return html


def render_weekly_html(report: dict) -> str:
    """Render a weekly report dict into an HTML email body."""
    week_start = report["week_start"]
    week_end = report["week_end"]
    s = report["scoring"]
    t = report["tiers"]
    vb = report.get("vertical_breakdown", {})
    total_vb = vb.pop("_total", 0) if "_total" in vb else sum(d["count"] for d in vb.values())

    gold_total = t.get("Ultra Platinum", 0) + t.get("Platinum", 0) + t.get("Gold", 0)

    # Scraper table
    scraper_rows = ""
    for row in report["scraper_data"]:
        fail_text = f' ({row["failures"]} fail)' if row.get("failures") else ""
        scraper_rows += f"""<tr>
            {_td(row['label'], 'left')}
            {_td(f"{row['scraped']:,}")}
            {_td(row['matched'] if row['matched'] is not None else '--')}
            {_td(f"{row['runs']} runs{fail_text}")}
        </tr>"""

    # Daily breakdown
    daily_rows = ""
    for d in report.get("daily_totals", []):
        daily_rows += f"""<tr>
            {_td(d['date'], 'left')}
            {_td(f"{d['scraped']:,}")}
            {_td(f"{d['matched']:,}")}
            {_td(f"{d['pct']:.1f}%")}
        </tr>"""

    # Vertical breakdown
    vert_rows = ""
    for label, stats in vb.items():
        vert_rows += f"""<tr>
            {_td(label, 'left')}
            {_td(f"{stats['count']:,}")}
            {_td(f"{stats['pct']:.1f}%")}
        </tr>"""

    # Alerts
    alerts_html = ""
    if report.get("errors"):
        alerts_html = '<div style="background:#450a0a;border:1px solid #ef4444;border-radius:8px;padding:12px;margin-top:16px;">'
        for err in report["errors"]:
            alerts_html += f'<div style="color:#fca5a5;font-size:13px;">WARNING: {err}</div>'
        alerts_html += "</div>"

    html = f"""
    <div style="max-width:640px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#0f172a;color:#e2e8f0;padding:24px;border-radius:12px;">

        <div style="text-align:center;margin-bottom:24px;">
            <h1 style="color:#fbbf24;font-size:22px;margin:0;">Forced Action</h1>
            <p style="color:#94a3b8;font-size:14px;margin:4px 0 0;">Weekly Report &mdash; {week_start} to {week_end}</p>
        </div>

        <!-- Headline KPIs -->
        <div style="display:flex;gap:12px;margin-bottom:20px;">
            <div style="flex:1;background:#1e293b;border-radius:8px;padding:14px;text-align:center;">
                <div style="color:#94a3b8;font-size:11px;text-transform:uppercase;letter-spacing:0.1em;">Week Scraped</div>
                <div style="color:#ffffff;font-size:24px;font-weight:700;">{report['total_scraped']:,}</div>
            </div>
            <div style="flex:1;background:#1e293b;border-radius:8px;padding:14px;text-align:center;">
                <div style="color:#94a3b8;font-size:11px;text-transform:uppercase;letter-spacing:0.1em;">Match Rate</div>
                <div style="color:#ffffff;font-size:24px;font-weight:700;">{report['match_pct']:.1f}%</div>
            </div>
            <div style="flex:1;background:#1e293b;border-radius:8px;padding:14px;text-align:center;">
                <div style="color:#94a3b8;font-size:11px;text-transform:uppercase;letter-spacing:0.1em;">Gold+ Leads</div>
                <div style="color:#fbbf24;font-size:24px;font-weight:700;">{gold_total:,}</div>
            </div>
            <div style="flex:1;background:#1e293b;border-radius:8px;padding:14px;text-align:center;">
                <div style="color:#94a3b8;font-size:11px;text-transform:uppercase;letter-spacing:0.1em;">New This Week</div>
                <div style="color:#4ade80;font-size:24px;font-weight:700;">{s.get('leads_new', 0):,}</div>
            </div>
        </div>

        <!-- Tier Breakdown -->
        <h2 style="color:#fbbf24;font-size:15px;margin:20px 0 8px;border-bottom:1px solid #2a2a3a;padding-bottom:6px;">Tier Breakdown (End of Week)</h2>
        <table style="width:100%;border-collapse:collapse;">
            <tr>{_th('Tier', 'left')}{_th('Count')}</tr>
            <tr>{_td('Ultra Platinum', 'left')}{_td(f"{t.get('Ultra Platinum', 0):,}")}</tr>
            <tr>{_td('Platinum', 'left')}{_td(f"{t.get('Platinum', 0):,}")}</tr>
            <tr>{_td('Gold', 'left')}{_td(f"{t.get('Gold', 0):,}")}</tr>
            <tr>{_td('Silver', 'left')}{_td(f"{t.get('Silver', 0):,}")}</tr>
        </table>

        <!-- Gold+ by Vertical -->
        <h2 style="color:#fbbf24;font-size:15px;margin:20px 0 8px;border-bottom:1px solid #2a2a3a;padding-bottom:6px;">Gold+ by Vertical ({total_vb:,} total)</h2>
        <table style="width:100%;border-collapse:collapse;">
            <tr>{_th('Vertical', 'left')}{_th('Leads')}{_th('% of Total')}</tr>
            {vert_rows}
        </table>

        <!-- Daily Breakdown -->
        <h2 style="color:#fbbf24;font-size:15px;margin:20px 0 8px;border-bottom:1px solid #2a2a3a;padding-bottom:6px;">Daily Ingest Breakdown</h2>
        <table style="width:100%;border-collapse:collapse;">
            <tr>{_th('Date', 'left')}{_th('Scraped')}{_th('Matched')}{_th('Match %')}</tr>
            {daily_rows}
        </table>

        <!-- Scraper Totals -->
        <h2 style="color:#fbbf24;font-size:15px;margin:20px 0 8px;border-bottom:1px solid #2a2a3a;padding-bottom:6px;">Scraper Totals (Week)</h2>
        <table style="width:100%;border-collapse:collapse;">
            <tr>{_th('Source', 'left')}{_th('Scraped')}{_th('Matched')}{_th('Runs')}</tr>
            {scraper_rows}
        </table>

        {alerts_html}

        <div style="text-align:center;margin-top:24px;padding-top:16px;border-top:1px solid #2a2a3a;">
            <span style="color:#64748b;font-size:11px;">Forced Action &mdash; Distressed Property Intelligence</span>
        </div>
    </div>
    """
    return html


def _get_recipients() -> list:
    """Return list of report recipient emails from settings."""
    settings = get_settings()
    raw = settings.report_recipients
    if not raw:
        return []
    return [e.strip() for e in raw.split(",") if e.strip()]


def send_daily_report(report: dict) -> int:
    """Send the daily report to all REPORT_RECIPIENTS. Returns count sent."""
    recipients = _get_recipients()
    if not recipients:
        logger.info("[report_emailer] No REPORT_RECIPIENTS configured — skipping email")
        return 0

    run_date = report["run_date"]
    gold_total = sum(report["tiers"].get(t, 0) for t in ("Ultra Platinum", "Platinum", "Gold"))

    subject = f"[Forced Action] Daily Report {run_date} — {gold_total:,} Gold+ leads"
    html = render_daily_html(report)

    # Plain-text fallback
    s = report["scoring"]
    t = report["tiers"]
    text = (
        f"Forced Action Daily Report — {run_date}\n\n"
        f"Scraped: {report['total_scraped']:,} | Matched: {report['match_pct']:.1f}%\n"
        f"Gold+: Ultra Plat {t.get('Ultra Platinum', 0):,} | Plat {t.get('Platinum', 0):,} | Gold {t.get('Gold', 0):,}\n"
        f"New today: {s.get('leads_new', 0):,} | Updated: {s.get('leads_updated', 0):,}\n"
    )
    if report.get("errors"):
        text += "\nAlerts:\n" + "\n".join(f"  - {e}" for e in report["errors"])

    sent = 0
    for addr in recipients:
        if send_email(to=addr, subject=subject, body_text=text, body_html=html):
            sent += 1
        else:
            logger.warning("[report_emailer] Failed to send daily report to %s", addr)

    logger.info("[report_emailer] Daily report sent to %d/%d recipients", sent, len(recipients))
    return sent


def send_weekly_report(report: dict) -> int:
    """Send the weekly report to all REPORT_RECIPIENTS. Returns count sent."""
    recipients = _get_recipients()
    if not recipients:
        logger.info("[report_emailer] No REPORT_RECIPIENTS configured — skipping email")
        return 0

    week_start = report["week_start"]
    week_end = report["week_end"]
    gold_total = sum(report["tiers"].get(t, 0) for t in ("Ultra Platinum", "Platinum", "Gold"))

    subject = f"[Forced Action] Weekly Report {week_start} to {week_end} — {gold_total:,} Gold+"
    html = render_weekly_html(report)

    s = report["scoring"]
    t = report["tiers"]
    text = (
        f"Forced Action Weekly Report — {week_start} to {week_end}\n\n"
        f"Scraped: {report['total_scraped']:,} | Matched: {report['match_pct']:.1f}%\n"
        f"Gold+: Ultra Plat {t.get('Ultra Platinum', 0):,} | Plat {t.get('Platinum', 0):,} | Gold {t.get('Gold', 0):,}\n"
        f"New this week: {s.get('leads_new', 0):,}\n"
    )
    if report.get("errors"):
        text += "\nAlerts:\n" + "\n".join(f"  - {e}" for e in report["errors"])

    sent = 0
    for addr in recipients:
        if send_email(to=addr, subject=subject, body_text=text, body_html=html):
            sent += 1
        else:
            logger.warning("[report_emailer] Failed to send weekly report to %s", addr)

    logger.info("[report_emailer] Weekly report sent to %d/%d recipients", sent, len(recipients))
    return sent


if __name__ == "__main__":
    import argparse
    import sys
    from datetime import timedelta
    from src.utils.logger import setup_logging

    setup_logging()

    parser = argparse.ArgumentParser(description="Send report email")
    parser.add_argument("--type", choices=["daily", "weekly"], required=True)
    parser.add_argument("--date", type=lambda s: date.fromisoformat(s), default=date.today())
    parser.add_argument("--county", default="hillsborough")
    args = parser.parse_args()

    if args.type == "daily":
        from src.tasks.daily_report import build_report
        report = build_report(args.date, args.county)
        count = send_daily_report(report)
    else:
        from src.tasks.weekly_report import build_report
        report = build_report(args.date, args.county)
        count = send_weekly_report(report)

    print(f"Sent {args.type} report to {count} recipient(s)")
    sys.exit(0 if count > 0 else 1)
