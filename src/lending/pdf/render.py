"""Shared lending PDF helper (T-07 Minute-5 pre-qual, T-08 Call-One soft approval).

Jinja2 HTML -> Playwright Chromium, same approach as the other report PDFs.
The watermark is a required argument so a non-binding document can never ship without it.
"""
import base64
from functools import lru_cache
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, StrictUndefined

_TEMPLATES_DIR = Path(__file__).parent / "templates"
_ARCHIVO_WOFF2 = Path(__file__).parent / "fonts" / "Archivo-latin-var.woff2"

NON_BINDING_PREQUAL_WATERMARK = "NON-BINDING PRE-QUALIFICATION ESTIMATE — FOR INFORMATIONAL PURPOSES ONLY"


@lru_cache(maxsize=1)
def _archivo_b64() -> str:
    return base64.b64encode(_ARCHIVO_WOFF2.read_bytes()).decode("ascii")


def render_html(template_name: str, context: dict, *, watermark: str) -> str:
    if not watermark or not watermark.strip():
        raise ValueError("watermark is required")
    env = Environment(
        loader=FileSystemLoader(str(_TEMPLATES_DIR)),
        autoescape=True,
        undefined=StrictUndefined,
    )
    return env.get_template(template_name).render(**context, watermark=watermark, archivo_woff2_b64=_archivo_b64())


def render_pdf(template_name: str, context: dict, *, watermark: str) -> bytes:
    """Render a lending template to PDF bytes. Storage is the caller's job."""
    html = render_html(template_name, context, watermark=watermark)
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch()
        try:
            page = browser.new_page()
            page.set_content(html, wait_until="load")
            return page.pdf(
                format="Letter",
                print_background=True,
                prefer_css_page_size=True,
            )
        finally:
            browser.close()
