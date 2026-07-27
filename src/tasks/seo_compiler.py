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
from src.services.seo.faq import build_faq
from src.services.seo.render import render_page, content_hash
from src.services.seo.sitemap import build_sitemap

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
    """Discover → gate → gather → render → write → sitemap → retire.

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

    # All page state in one query instead of one SELECT per cell.
    pages = _load_pages(db)

    changed_urls: list[str] = []
    visited_urls: set[str] = set()
    built = retired = skipped = 0

    for cell in cells:
        count = counts.get((cell.city_slug, cell.vertical), 0)
        url_path = f"/florida/{cell.city_slug}/{cell.topic_slug}/"
        visited_urls.add(url_path)
        existing = pages.get(url_path)

        if not is_eligible(count, floor):
            if _handle_sub_threshold(
                db, cell, existing, hysteresis, now, write,
                settings=settings, counts=counts, floor=floor,
                county_totals=county_totals, output_dir=output_dir,
            ):
                retired += 1
            skipped += 1
            continue

        stats = gather_page_data(
            db, cell.variants or (cell.city_raw,), cell.vertical,
            county_qualified=county_totals.get(cell.vertical, 0),
        )
        page_data = _page_data(cell, stats, "live", settings, counts, floor)

        html = render_page(page_data)
        new_hash = content_hash(html)
        out = output_dir / cell.city_slug / cell.topic_slug / "index.html"

        if existing and existing["content_hash"] == new_hash:
            # Unchanged content — but the file may be missing (fresh host,
            # cleared dist/). Rewrite it without bumping lastmod, otherwise the
            # sitemap advertises a URL that 404s.
            if write and not out.exists():
                out.parent.mkdir(parents=True, exist_ok=True)
                out.write_text(html, encoding="utf-8")
            _touch_page(db, url_path, count, now)
            built += 1
            continue

        if write:
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(html, encoding="utf-8")

        _upsert_page(db, cell, url_path, new_hash, count, now, status="live")
        changed_urls.append(url_path)
        built += 1

    # Orphaned pages: live rows whose cell vanished from the grid entirely
    # (city renamed, blocklisted, or gone from properties). They get the same
    # hysteresis treatment as a zero-count cell — otherwise they'd stay in the
    # sitemap forever with their source data gone.
    for url_path, row in pages.items():
        if url_path in visited_urls or row["status"] != "live":
            continue
        orphan_cell = GridCell(
            city_raw=row["city_raw"], city_slug=row["city_slug"],
            vertical=row["vertical"], topic_slug=row["topic_slug"],
            variants=(row["city_raw"],),
        )
        logger.warning("Orphaned page (cell no longer in grid): %s", url_path)
        if _handle_sub_threshold(
            db, orphan_cell, row, hysteresis, now, write,
            settings=settings, counts=counts, floor=floor,
            county_totals=county_totals, output_dir=output_dir,
        ):
            retired += 1
        skipped += 1

    if write:
        build_sitemap(db, output_dir)

    logger.info(
        "compile_all done: built=%d retired=%d skipped=%d changed=%d",
        built, retired, skipped, len(changed_urls),
    )
    return {"built": built, "retired": retired, "skipped": skipped, "changed": len(changed_urls)}


# ─── private helpers ──────────────────────────────────────────────────────────

def _load_pages(db: Session) -> dict[str, dict]:
    """All seo_pages rows keyed by url_path — one query for the whole run."""
    rows = db.execute(text("SELECT * FROM seo_pages")).mappings().fetchall()
    return {row["url_path"]: dict(row) for row in rows}


def _page_data(
    cell: GridCell, stats: dict, status: str, settings, counts: dict, floor: int,
) -> dict:
    """Assemble the template context for one cell (used by live and retire paths)."""
    display_city = cell.city_raw.title()
    url_path = f"/florida/{cell.city_slug}/{cell.topic_slug}/"

    # Internal links: this city's OTHER eligible verticals (never link a page
    # that doesn't exist / is below the floor).
    siblings = [
        {
            "label": v.replace("_", " ").title(),
            "href": f"/florida/{cell.city_slug}/{v.replace('_', '-')}/",
        }
        for v in _sibling_verticals(cell, counts, floor)
    ]

    return {
        "city": display_city,
        "city_slug": cell.city_slug,
        "vertical": cell.vertical,
        "topic_slug": cell.topic_slug,
        "stats": stats,
        "faq_items": build_faq(display_city, cell.vertical, stats),
        "sibling_links": siblings,
        "status": status,
        "canonical_url": f"{settings.seo_site_base_url.rstrip('/')}{url_path}",
        # Client-side retargeting pixel — no-ops in the template if unset,
        # same gating as the frontend's VITE_META_PIXEL_ID (src/utils/metaPixel.js).
        "meta_pixel_id": settings.meta_pixel_id,
    }


def _sibling_verticals(cell: GridCell, counts: dict, floor: int) -> list[str]:
    from src.services.seo.grid import VERTICALS

    return [
        v for v in VERTICALS
        if v != cell.vertical and counts.get((cell.city_slug, v), 0) >= floor
    ]


def _handle_sub_threshold(
    db: Session, cell: GridCell, existing: dict | None,
    hysteresis: int, now: datetime, write: bool,
    *, settings, counts: dict, floor: int, county_totals: dict, output_dir: Path,
) -> bool:
    """Increment below_threshold_runs; retire (noindex) after hysteresis consecutive
    runs. On retirement the served file is re-rendered WITH the noindex meta —
    the page stays reachable but tells Google to drop it. Returns True on retire."""
    if existing is None:
        return False  # never published — nothing to retire

    url_path = f"/florida/{cell.city_slug}/{cell.topic_slug}/"
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
        if write:
            stats = gather_page_data(
                db, cell.variants or (cell.city_raw,), cell.vertical,
                county_qualified=county_totals.get(cell.vertical, 0),
            )
            html = render_page(_page_data(cell, stats, "noindex", settings, counts, floor))
            out = output_dir / cell.city_slug / cell.topic_slug / "index.html"
            if out.parent.exists():
                out.write_text(html, encoding="utf-8")
        logger.info("Retired (noindex): %s after %d sub-threshold runs", url_path, new_runs)
        return True

    db.execute(
        text("""
            UPDATE seo_pages
               SET below_threshold_runs = :runs,
                   last_built_at        = :now
             WHERE url_path = :u
        """),
        {"runs": new_runs, "u": url_path, "now": now},
    )
    return False


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
    try:
        with get_db_context() as db:
            result = compile_all(db)
        logger.info("SEO compiler finished: %s", result)
    except Exception as exc:
        logger.error("SEO compiler failed: %s", exc, exc_info=True)
        _alert_ops(exc)
        raise


def _alert_ops(exc: Exception) -> None:
    """Email ops on compiler failure — weekly cron dies silently otherwise."""
    try:
        from src.services.email import send_email

        recipients = (get_settings().report_recipients or "").split(",")
        for to in [r.strip() for r in recipients if r.strip()]:
            send_email(
                to=to,
                subject="[Forced Action] SEO compiler FAILED",
                body_text=f"Weekly seo_compiler run failed: {exc}\n\nCheck cron logs.",
            )
    except Exception as mail_exc:
        logger.error("SEO compiler failure alert could not be sent: %s", mail_exc)


if __name__ == "__main__":
    main()
