"""Shared lending PDF helper (T-07 Minute-5 pre-qual, T-08 Call-One soft approval).

Jinja2 HTML -> Playwright Chromium, same approach as the other report PDFs.
The watermark is a required argument so a non-binding document can never ship without it.
"""
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, StrictUndefined

_TEMPLATES_DIR = Path(__file__).parent / "templates"

NON_BINDING_PREQUAL_WATERMARK = "NON-BINDING PRE-QUALIFICATION ESTIMATE — FOR INFORMATIONAL PURPOSES ONLY"


def render_html(template_name: str, context: dict, *, watermark: str) -> str:
    if not watermark or not watermark.strip():
        raise ValueError("watermark is required")
    env = Environment(
        loader=FileSystemLoader(str(_TEMPLATES_DIR)),
        autoescape=True,
        undefined=StrictUndefined,
    )
    return env.get_template(template_name).render(**context, watermark=watermark)


def render_pdf(template_name: str, context: dict, *, watermark: str) -> bytes:
    """Render a lending template to PDF bytes. Storage is the caller's job."""
    html = render_html(template_name, context, watermark=watermark)
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch()
        try:
            page = browser.new_page()
            page.set_content(html, wait_until="networkidle")
            return page.pdf(
                format="Letter",
                print_background=True,
                prefer_css_page_size=True,
            )
        finally:
            browser.close()
