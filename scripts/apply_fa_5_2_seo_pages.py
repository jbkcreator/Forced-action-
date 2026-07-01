"""Apply fa_5_2_seo_pages migration: create seo_pages table."""
from sqlalchemy import text
from src.core.database import get_db_context

_DDL = """
CREATE TABLE IF NOT EXISTS seo_pages (
    id                   SERIAL PRIMARY KEY,
    city_slug            TEXT NOT NULL,
    topic_slug           TEXT NOT NULL,
    city_raw             TEXT NOT NULL,
    vertical             TEXT NOT NULL,
    url_path             TEXT NOT NULL,
    content_hash         TEXT,
    lastmod              TIMESTAMPTZ,
    status               TEXT NOT NULL DEFAULT 'live',
    below_threshold_runs INT  NOT NULL DEFAULT 0,
    qualified_count      INT,
    first_published_at   TIMESTAMPTZ,
    last_built_at        TIMESTAMPTZ,
    CONSTRAINT uq_seo_pages_url    UNIQUE (url_path),
    CONSTRAINT uq_seo_pages_cell   UNIQUE (city_slug, topic_slug),
    CONSTRAINT ck_seo_pages_status CHECK (status IN ('live', 'noindex', 'retired'))
);

CREATE INDEX IF NOT EXISTS idx_seo_pages_status ON seo_pages (status);
"""

if __name__ == "__main__":
    with get_db_context() as db:
        db.execute(text(_DDL))
    print("fa_5_2_seo_pages applied: seo_pages table ready.")
