"""fa_5_2b — functional index on properties (TRIM(city), county_id)

Speeds the per-cell city scans in the weekly SEO compiler (Task 5.2), which
filter WHERE TRIM(city) = ANY(...) over ~522k rows ~300 times per run.

Revision ID: fa_5_2b_seo_city_index
Revises: fa_5_2_seo_pages
Create Date: 2026-07-02
"""

revision = "fa_5_2b_seo_city_index"
down_revision = "fa_5_2_seo_pages"
branch_labels = None
depends_on = None


def upgrade():
    pass  # Applied via scripts/apply_fa_5_2b_seo_city_index.py (CONCURRENTLY)


def downgrade():
    pass
