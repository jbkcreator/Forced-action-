"""Render SEO pages via Jinja2 (Task 5.2)."""
import hashlib
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

_TEMPLATES_DIR = Path(__file__).resolve().parents[3] / "src" / "templates" / "seo"


def _env() -> Environment:
    return Environment(loader=FileSystemLoader(str(_TEMPLATES_DIR)), autoescape=True)


def render_page(data: dict) -> str:
    """Render city×vertical page HTML. Includes noindex meta when data['status']=='noindex'."""
    return _env().get_template("page.html").render(**data)


def content_hash(html: str) -> str:
    """16-char SHA-256 prefix — change detection for sitemap lastmod."""
    return hashlib.sha256(html.encode()).hexdigest()[:16]
