"""
Supplier Intelligence Foundation — monthly report generation task (fa067).

Runs on the 1st of every month at 08:00 UTC.
For each active/trialing supplier account + their counties:
  1. Generates a report via report_engine.generate_report()
  2. Renders PDF via pdf_export.render_pdf()
  3. Emails the PDF to the contact email via send_email()
  4. Logs the export to supplier_report_exports

Idempotent: skips accounts already sent a report this calendar month.

Usage:
    python -m src.tasks.supplier_report_monthly
    python -m src.tasks.supplier_report_monthly --dry-run
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import text as sa_text

from config.supplier_intel_config import ALERT_ELIGIBLE_STATUSES
from src.core.database import get_db_context
from src.services.email import send_email
from src.services.supplier_intel.report_engine import generate_report
from src.services.supplier_intel.pdf_export import render_pdf

logger = logging.getLogger(__name__)


def _already_reported_this_month(db, account_id: int, county_id: str) -> bool:
    """Return True if a generated report exists for this account+county in the current month."""
    row = db.execute(sa_text("""
        SELECT 1 FROM supplier_reports
        WHERE account_id = :aid AND county_id = :county
          AND status = 'generated'
          AND DATE_TRUNC('month', created_at) = DATE_TRUNC('month', NOW())
        LIMIT 1
    """), {"aid": account_id, "county": county_id}).first()
    return row is not None


def _email_report(account, pdf_path, month_label: str) -> bool:
    """Email the PDF report. Returns True on success."""
    try:
        send_email(
            to=account.contact_email,
            subject=f"Your Forced Action Supplier Intelligence Report — {month_label}",
            body_text=(
                f"Hi {account.contact_name or account.company_name},\n\n"
                f"Your monthly Supplier Intelligence report is attached.\n\n"
                f"This report reflects activity data for your covered territories. "
                f"Sections marked 'Insufficient data' or 'Phase 2' will be available "
                f"when more market data has been collected.\n\n"
                f"— Forced Action Intelligence"
            ),
            attachments=[pdf_path],
        )
        return True
    except Exception:
        logger.warning("[si-monthly] email failed for account=%s", account.id, exc_info=True)
        return False


def run_monthly_reports(dry_run: bool = False) -> dict:
    month_label = datetime.now(timezone.utc).strftime("%B %Y")
    results = {"month": month_label, "dry_run": dry_run, "processed": 0, "skipped": 0, "errors": 0}

    with get_db_context() as db:
        accounts = db.execute(sa_text("""
            SELECT sa.id, sa.company_name, sa.contact_name, sa.contact_email,
                   sa.counties, sa.verticals, ss.plan_tier
            FROM supplier_accounts sa
            JOIN supplier_subscriptions ss ON ss.account_id = sa.id
            WHERE sa.status = 'active'
              AND ss.status = ANY(:statuses)
        """), {"statuses": list(ALERT_ELIGIBLE_STATUSES)}).fetchall()

        for acc in accounts:
            counties = acc.counties or ["hillsborough"]
            for county_id in counties:
                if _already_reported_this_month(db, acc.id, county_id):
                    results["skipped"] += 1
                    continue

                if dry_run:
                    logger.info("[si-monthly] dry-run: would generate report account=%s county=%s", acc.id, county_id)
                    results["processed"] += 1
                    continue

                try:
                    # Generate report
                    rpt = db.execute(sa_text("""
                        INSERT INTO supplier_reports
                            (account_id, county_id, status, created_at)
                        VALUES (:aid, :county, 'pending', NOW())
                        RETURNING id
                    """), {"aid": acc.id, "county": county_id}).first()
                    report_id = rpt.id
                    db.flush()

                    data = generate_report(acc.id, county_id, counties, acc.verticals or [], db)

                    db.execute(sa_text("""
                        UPDATE supplier_reports
                        SET status = 'generated',
                            sections_json = CAST(:sections AS jsonb),
                            data_readiness_snapshot = CAST(:readiness AS jsonb),
                            generated_at = NOW(),
                            report_period_start = :pstart,
                            report_period_end = :pend
                        WHERE id = :id
                    """), {
                        "sections": json.dumps(data["sections"]),
                        "readiness": json.dumps(data["data_readiness_snapshot"]),
                        "pstart": data["period_start"],
                        "pend": data["period_end"],
                        "id": report_id,
                    })
                    db.flush()

                    # Render PDF
                    from types import SimpleNamespace
                    account_ns = SimpleNamespace(
                        id=acc.id, company_name=acc.company_name,
                        counties=acc.counties, verticals=acc.verticals,
                        contact_email=acc.contact_email, contact_name=acc.contact_name,
                    )
                    pdf_path = render_pdf(data, account_ns)

                    # Email
                    emailed = _email_report(account_ns, pdf_path, month_label)

                    db.execute(sa_text("""
                        INSERT INTO supplier_report_exports
                            (report_id, format, file_path, exported_at, emailed_at, created_at)
                        VALUES (:rid, 'pdf', :path, NOW(),
                                CASE WHEN :emailed THEN NOW() ELSE NULL END,
                                NOW())
                    """), {"rid": report_id, "path": str(pdf_path), "emailed": emailed})
                    db.execute(sa_text("""
                        UPDATE supplier_reports SET status = 'exported' WHERE id = :id
                    """), {"id": report_id})
                    db.flush()

                    results["processed"] += 1
                except Exception:
                    logger.exception("[si-monthly] failed account=%s county=%s", acc.id, county_id)
                    results["errors"] += 1

    logger.info("[si-monthly] %s", results)
    return results


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")
    dry_run = "--dry-run" in set(argv or sys.argv[1:])
    import json as _json
    print(_json.dumps(run_monthly_reports(dry_run=dry_run), indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
