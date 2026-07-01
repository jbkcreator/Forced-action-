"""Build sitemap.xml for all live SEO pages (Task 5.2)."""
import logging
from pathlib import Path

from sqlalchemy.orm import Session
from sqlalchemy import text

from config.settings import get_settings

logger = logging.getLogger(__name__)


def build_sitemap(db: Session, output_dir: Path) -> Path:
    """Write sitemap.xml to output_dir's parent. Returns the path written.

    Only 'live' pages appear. 'noindex' and 'retired' are excluded.
    lastmod is formatted as YYYY-MM-DD.
    """
    settings = get_settings()
    base_url = settings.seo_site_base_url.rstrip("/")

    rows = db.execute(
        text("""
            SELECT url_path, lastmod
            FROM seo_pages
            WHERE status = 'live'
            ORDER BY url_path
        """)
    ).fetchall()

    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    ]
    for row in rows:
        lines.append("  <url>")
        lines.append(f"    <loc>{base_url}{row.url_path}</loc>")
        if row.lastmod:
            lines.append(f"    <lastmod>{row.lastmod.strftime('%Y-%m-%d')}</lastmod>")
        lines.append("  </url>")
    lines.append("</urlset>")

    sitemap_path = output_dir.parent / "sitemap.xml"
    sitemap_path.parent.mkdir(parents=True, exist_ok=True)
    sitemap_path.write_text("\n".join(lines), encoding="utf-8")
    logger.info("sitemap.xml: %d live pages → %s", len(rows), sitemap_path)
    return sitemap_path
