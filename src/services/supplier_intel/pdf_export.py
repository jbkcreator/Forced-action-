"""
Supplier Intelligence Foundation — PDF and CSV export (fa067).

Mirrors src/tasks/contractor_benchmark_report.py:
  - Jinja2 renders src/templates/supplier_report.html
  - Playwright sync API converts HTML → PDF
  - CSV flattens available (non-N/A) sections to a spreadsheet

Output: reports/supplier_intel/{account_id}_{date}.{pdf|csv}
"""

from __future__ import annotations

import csv
import io
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from jinja2 import Environment, FileSystemLoader

from config.supplier_intel_config import REPORT_OUTPUT_DIR

logger = logging.getLogger(__name__)

_TEMPLATES_DIR = Path(__file__).resolve().parents[3] / "src" / "templates"
_OUTPUT_DIR = Path(REPORT_OUTPUT_DIR)


def _ensure_output_dir() -> None:
    _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def _output_path(account_id: int, format: str) -> Path:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return _OUTPUT_DIR / f"supplier_{account_id}_{today}.{format}"


def render_pdf(
    report_data: dict,
    account: object,
    output_path: Optional[Path] = None,
) -> Path:
    """Render the supplier report to PDF using Jinja2 + Playwright."""
    _ensure_output_dir()
    if output_path is None:
        output_path = _output_path(account.id, "pdf")

    env = Environment(loader=FileSystemLoader(str(_TEMPLATES_DIR)), autoescape=True)
    template = env.get_template("supplier_report.html")
    html = template.render(
        company_name=account.company_name,
        report=report_data,
        generated_date=datetime.now(timezone.utc).strftime("%B %d, %Y"),
        counties=account.counties or [],
        verticals=account.verticals or [],
    )

    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page()
            page.set_content(html, wait_until="networkidle")
            page.pdf(path=str(output_path), format="A4",
                     print_background=True,
                     margin={"top": "12mm", "bottom": "12mm", "left": "10mm", "right": "10mm"})
            browser.close()
    except ImportError:
        # Playwright not installed — write HTML as fallback
        html_path = output_path.with_suffix(".html")
        html_path.write_text(html, encoding="utf-8")
        logger.warning("[supplier-pdf] Playwright not available — wrote HTML to %s", html_path)
        raise RuntimeError(f"Playwright not installed. HTML written to {html_path}.")

    logger.info("[supplier-pdf] PDF written: %s", output_path)
    return output_path


def export_csv(
    report_data: dict,
    account: object,
    output_path: Optional[Path] = None,
) -> Path:
    """Export available sections to a flat CSV. Skips N/A / phase2 sections."""
    _ensure_output_dir()
    if output_path is None:
        output_path = _output_path(account.id, "csv")

    rows = []
    sections = report_data.get("sections", {})
    meta = {
        "company": account.company_name,
        "county_id": report_data.get("county_id", ""),
        "period_start": report_data.get("period_start", ""),
        "period_end": report_data.get("period_end", ""),
        "generated_at": report_data.get("generated_at", ""),
    }

    for section_key, data in sections.items():
        status = data.get("status", "ok")
        if status in ("insufficient_data", "phase2", "error"):
            rows.append({**meta, "section": section_key, "metric": "status", "value": status,
                         "note": data.get("message", "")})
            continue
        # Flatten dict fields as individual rows
        for k, v in data.items():
            if k == "status":
                continue
            if isinstance(v, dict):
                for sub_k, sub_v in v.items():
                    rows.append({**meta, "section": section_key, "metric": f"{k}.{sub_k}", "value": sub_v, "note": ""})
            elif isinstance(v, list):
                for i, item in enumerate(v):
                    if isinstance(item, dict):
                        for sub_k, sub_v in item.items():
                            rows.append({**meta, "section": section_key, "metric": f"{k}[{i}].{sub_k}", "value": sub_v, "note": ""})
                    else:
                        rows.append({**meta, "section": section_key, "metric": f"{k}[{i}]", "value": item, "note": ""})
            else:
                rows.append({**meta, "section": section_key, "metric": k, "value": v, "note": ""})

    fieldnames = ["company", "county_id", "period_start", "period_end", "generated_at", "section", "metric", "value", "note"]
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    logger.info("[supplier-csv] CSV written: %s", output_path)
    return output_path
