"""Weekly SEO compiler — generates city×vertical pages and sitemap (Task 5.2).

Run: python -m src.tasks.seo_compiler
Cron: 30 9 * * 0  (weekly Sunday, staggered after CDS)
"""
import logging
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy.orm import Session
from sqlalchemy import text

from config.settings import get_settings
from src.core.database import get_db_context
from src.services.seo.grid import GridCell, discover_cells, all_qualified_counts, is_eligible
from src.services.seo.stats import gather_page_data
from src.services.seo.faq import best_faq
from src.services.seo.render import render_page, content_hash
from src.services.seo.sitemap import build_sitemap
from src.services.seo import indexing_api

logger = logging.getLogger(__name__)


def compile_all(
    db: Session,
    *,
    now: datetime | None = None,
    write: bool = True,
    cells: list[GridCell] | None = None,
    output_dir: Path | None = None,
    counts: dict | None = None,
) -> dict:
    """Discover → gate → gather → render → write → sitemap → retire → notify.

    write=False is a dry-run (no files written, no sitemap, used in tests).
    cells/output_dir/counts override the discovered grid, settings dir, and bulk
    count query (targeted rebuilds, tests).
    Returns summary dict: {built, retired, skipped, changed}.
    """
    settings = get_settings()
    now = now or datetime.now(timezone.utc)
    floor = settings.seo_eligibility_floor
    hysteresis = settings.seo_retire_hysteresis_runs
    output_dir = output_dir or Path(settings.seo_output_dir)

    cells = cells if cells is not None else discover_cells(db)
    counts = counts if counts is not None else all_qualified_counts(db)
    logger.info("Grid: %d cells across %d cities", len(cells), len(cells) // 6 if cells else 0)

    # County-wide qualified total per vertical (denominator for city-vs-county %),
    # derived once from the bulk counts — avoids a county-wide scan per cell.
    county_totals: dict[str, int] = {}
    for (_city, _vertical), _cnt in counts.items():
        county_totals[_vertical] = county_totals.get(_vertical, 0) + _cnt

    changed_urls: list[str] = []
    built = retired = skipped = 0

    for cell in cells:
        count = counts.get((cell.city_raw, cell.vertical), 0)

        if not is_eligible(count, floor):
            _handle_sub_threshold(db, cell, hysteresis, now, write)
            skipped += 1
            continue

        stats = gather_page_data(
            db, cell.city_raw, cell.vertical,
            county_qualified=county_totals.get(cell.vertical),
        )
        faq = best_faq(db, cell.vertical)

        url_path = f"/florida/{cell.city_slug}/{cell.topic_slug}/"
        canonical = f"{settings.seo_site_base_url.rstrip('/')}{url_path}"

        page_data = {
            "city": cell.city_raw,
            "city_slug": cell.city_slug,
            "vertical": cell.vertical,
            "topic_slug": cell.topic_slug,
            "stats": stats,
            "faq": faq,
            "status": "live",
            "canonical_url": canonical,
        }

        html = render_page(page_data)
        new_hash = content_hash(html)
        existing = _get_page(db, url_path)

        if existing and existing["content_hash"] == new_hash:
            _touch_page(db, url_path, count, now)
            built += 1
            continue

        if write:
            out = output_dir / cell.city_slug / cell.topic_slug / "index.html"
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(html, encoding="utf-8")

        _upsert_page(db, cell, url_path, new_hash, count, now, status="live")
        changed_urls.append(url_path)
        built += 1

    if write:
        build_sitemap(db, output_dir)

    if changed_urls:
        indexing_api.notify(changed_urls)

    logger.info(
        "compile_all done: built=%d retired=%d skipped=%d changed=%d",
        built, retired, skipped, len(changed_urls),
    )
    return {"built": built, "retired": retired, "skipped": skipped, "changed": len(changed_urls)}


# ─── private helpers ──────────────────────────────────────────────────────────

def _get_page(db: Session, url_path: str) -> dict | None:
    row = db.execute(
        text("SELECT * FROM seo_pages WHERE url_path = :u"),
        {"u": url_path},
    ).mappings().fetchone()
    return dict(row) if row else None


def _handle_sub_threshold(
    db: Session, cell: GridCell, hysteresis: int, now: datetime, write: bool
) -> None:
    """Increment below_threshold_runs; retire (noindex) after hysteresis consecutive runs."""
    url_path = f"/florida/{cell.city_slug}/{cell.topic_slug}/"
    existing = _get_page(db, url_path)
    if existing is None:
        return  # never published — nothing to retire

    new_runs = (existing["below_threshold_runs"] or 0) + 1

    if new_runs >= hysteresis:
        db.execute(
            text("""
                UPDATE seo_pages
                   SET status               = 'noindex',
                       below_threshold_runs = :runs,
                       last_built_at        = :now
                 WHERE url_path = :u
            """),
            {"runs": new_runs, "u": url_path, "now": now},
        )
        logger.info("Retired (noindex): %s after %d sub-threshold runs", url_path, new_runs)
    else:
        db.execute(
            text("""
                UPDATE seo_pages
                   SET below_threshold_runs = :runs,
                       last_built_at        = :now
                 WHERE url_path = :u
            """),
            {"runs": new_runs, "u": url_path, "now": now},
        )


def _touch_page(db: Session, url_path: str, count: int, now: datetime) -> None:
    """No content change — reset strike counter and keep lastmod stable."""
    db.execute(
        text("""
            UPDATE seo_pages
               SET qualified_count      = :count,
                   last_built_at        = :now,
                   status               = 'live',
                   below_threshold_runs = 0
             WHERE url_path = :u
        """),
        {"count": count, "u": url_path, "now": now},
    )


def _upsert_page(
    db: Session, cell: GridCell, url_path: str, new_hash: str,
    count: int, now: datetime, status: str,
) -> None:
    db.execute(
        text("""
            INSERT INTO seo_pages
                (city_slug, topic_slug, city_raw, vertical, url_path,
                 content_hash, lastmod, status, below_threshold_runs,
                 qualified_count, first_published_at, last_built_at)
            VALUES
                (:cs, :ts, :cr, :v, :u,
                 :hash, :now, :status, 0,
                 :count, :now, :now)
            ON CONFLICT (url_path) DO UPDATE SET
                content_hash         = :hash,
                lastmod              = :now,
                status               = :status,
                below_threshold_runs = 0,
                qualified_count      = :count,
                last_built_at        = :now
        """),
        {
            "cs": cell.city_slug, "ts": cell.topic_slug,
            "cr": cell.city_raw,  "v": cell.vertical,
            "u": url_path,         "hash": new_hash,
            "now": now,            "status": status,
            "count": count,
        },
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    with get_db_context() as db:
        result = compile_all(db)
    logger.info("SEO compiler finished: %s", result)


if __name__ == "__main__":
    main()
