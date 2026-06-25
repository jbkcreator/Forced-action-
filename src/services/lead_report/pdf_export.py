"""
Lead report PDF renderer (Sprint 4.9).

Mirrors src/services/supplier_intel/pdf_export.py:
  Jinja2 renders src/templates/lead_report.html → Playwright chromium → PDF.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from jinja2 import Environment, FileSystemLoader

logger = logging.getLogger(__name__)

_TEMPLATES_DIR = Path(__file__).resolve().parents[3] / "src" / "templates"
_OUTPUT_DIR = Path("reports/lead_report")


def render_lead_report_pdf(
    report_data: dict,
    purchase_id: int,
    *,
    full: bool,
    output_dir: Optional[Path] = None,
) -> Path:
    """Render lead_report.html → PDF. Returns output path.

    Raises RuntimeError (with HTML fallback written) if Playwright unavailable.
    """
    out_dir = output_dir or _OUTPUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    output_path = out_dir / f"{purchase_id}.pdf"

    env = Environment(loader=FileSystemLoader(str(_TEMPLATES_DIR)), autoescape=True)
    template = env.get_template("lead_report.html")
    html = template.render(full=full, **report_data)

    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page()
            page.set_content(html, wait_until="networkidle")
            page.pdf(
                path=str(output_path),
                format="A4",
                print_background=True,
                margin={"top": "12mm", "bottom": "12mm", "left": "10mm", "right": "10mm"},
            )
            browser.close()
    except ImportError:
        html_path = output_path.with_suffix(".html")
        html_path.write_text(html, encoding="utf-8")
        logger.warning("[lead-pdf] Playwright not available — wrote HTML to %s", html_path)
        raise RuntimeError(f"Playwright not installed. HTML written to {html_path}.")

    logger.info("[lead-pdf] PDF written: %s", output_path)
    return output_path
