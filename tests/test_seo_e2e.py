"""End-to-end integration test for the SEO engine against real Postgres.

Exercises the real SQL (CTEs, JSONB casts, joins) that unit tests mock:
  - stats.gather_page_data (data spine aggregate, city-scoped CTE)
  - compile_all full path: render → write file → upsert seo_pages → sitemap → idempotent re-run

Uses fresh_db (real shared Postgres, rolled back after test). Skips if no DB.
Requires the seo_pages table to exist (scripts/apply_fa_5_2_seo_pages.py).
"""
import uuid
from pathlib import Path

import pytest
from sqlalchemy import text

from src.core.models import Property, DistressScore, Financial, Owner
from src.services.seo.grid import GridCell
from src.services.seo.stats import gather_page_data
from src.tasks.seo_compiler import compile_all

_CITY = "Zzz Seo Testburg"          # unique, won't collide with real cities
_CITY_SLUG = "zzz-seo-testburg"
_VERTICAL = "wholesalers"
_N = 30                              # above the 25 eligibility floor
_ABSENTEE = 8


def _seed(db, n=_N):
    """Seed n qualified properties in hillsborough for _CITY × _VERTICAL."""
    for i in range(n):
        parcel = f"SEOTEST-{uuid.uuid4().hex[:12]}"
        p = Property(parcel_id=parcel, city=_CITY, state="FL",
                     county_id="hillsborough", address=f"{i} Test St")
        db.add(p)
        db.flush()  # get p.id

        db.add(Financial(property_id=p.id, county_id="hillsborough",
                         assessed_value_mkt=200_000 + i * 1000))
        db.add(Owner(property_id=p.id, county_id="hillsborough",
                     owner_name=f"Owner {i}",
                     absentee_status="Out-of-State" if i < _ABSENTEE else "In-County"))
        db.add(DistressScore(
            property_id=p.id, county_id="hillsborough",
            vertical_scores={_VERTICAL: 70, "fix_flip": 0},
            lead_tier="Platinum" if i < 3 else "Gold",
            final_cds_score=70, qualified=True,
        ))

    # Noise rows that must NOT be counted: positive vertical score but not
    # deliverable inventory (unqualified / A2 guess lead).
    for suffix, extra in (("UNQ", {"qualified": False}),
                          ("GUESS", {"qualified": True, "is_guess_lead": True})):
        p = Property(parcel_id=f"SEOTEST-{suffix}-{uuid.uuid4().hex[:8]}", city=_CITY,
                     state="FL", county_id="hillsborough", address=f"{suffix} Noise St")
        db.add(p)
        db.flush()
        db.add(DistressScore(
            property_id=p.id, county_id="hillsborough",
            vertical_scores={_VERTICAL: 70}, lead_tier="Gold",
            final_cds_score=70, **extra,
        ))
    db.flush()


def _cell():
    return GridCell(city_raw=_CITY, city_slug=_CITY_SLUG,
                    vertical=_VERTICAL, topic_slug="wholesalers",
                    variants=(_CITY,))


def test_seo_e2e_real_sql_and_compile(fresh_db, tmp_path):
    # skip cleanly if the table isn't there yet
    exists = fresh_db.execute(
        text("SELECT to_regclass('public.seo_pages')")
    ).scalar()
    if exists is None:
        pytest.skip("seo_pages table not applied — run scripts/apply_fa_5_2_seo_pages.py")

    _seed(fresh_db)

    # ── 1. Real aggregate SQL: gather_page_data (city-scoped CTE) ──
    data = gather_page_data(fresh_db, [_CITY], _VERTICAL, county_qualified=_N)
    assert data["qualified_count"] == _N
    assert data["absentee_count"] == _ABSENTEE
    assert data["platinum_count"] == 3
    assert data["gold_count"] == _N - 3
    assert data["median_value"] > 200_000

    # ── 3. Full compile: file + seo_pages row + sitemap ──
    out_dir = tmp_path / "florida"
    counts = {(_CITY_SLUG, _VERTICAL): _N}
    result = compile_all(
        fresh_db, cells=[_cell()], counts=counts, output_dir=out_dir, write=True,
    )
    assert result["built"] == 1
    assert result["changed"] == 1

    page_file = out_dir / _CITY_SLUG / "wholesalers" / "index.html"
    assert page_file.exists(), "page HTML not written"
    html = page_file.read_text(encoding="utf-8")
    assert _CITY in html
    assert str(_N) in html

    row = fresh_db.execute(
        text("SELECT * FROM seo_pages WHERE city_slug = :cs"),
        {"cs": _CITY_SLUG},
    ).mappings().fetchone()
    assert row is not None
    assert row["url_path"] == f"/florida/{_CITY_SLUG}/wholesalers/"
    assert row["content_hash"] is not None
    assert row["status"] == "live"
    first_lastmod = row["lastmod"]

    sitemap = out_dir.parent / "sitemap.xml"
    assert sitemap.exists()
    sitemap_xml = sitemap.read_text(encoding="utf-8")
    assert f"/florida/{_CITY_SLUG}/wholesalers/" in sitemap_xml

    # ── 4. Idempotent re-run: no content change → no rewrite, lastmod stable ──
    result2 = compile_all(
        fresh_db, cells=[_cell()], counts=counts, output_dir=out_dir, write=True,
    )
    assert result2["changed"] == 0, "unchanged content should not bump lastmod"

    row2 = fresh_db.execute(
        text("SELECT lastmod FROM seo_pages WHERE city_slug = :cs"),
        {"cs": _CITY_SLUG},
    ).mappings().fetchone()
    assert row2["lastmod"] == first_lastmod, "lastmod must be stable on no-op rebuild"

    # ── 5. Missing file on unchanged hash (fresh host / cleared dist) gets
    #       rewritten — sitemap must never point at a 404 ──
    page_file.unlink()
    compile_all(fresh_db, cells=[_cell()], counts=counts, output_dir=out_dir, write=True)
    assert page_file.exists(), "missing file must be rematerialized even when hash is unchanged"
    row3 = fresh_db.execute(
        text("SELECT lastmod FROM seo_pages WHERE city_slug = :cs"),
        {"cs": _CITY_SLUG},
    ).mappings().fetchone()
    assert row3["lastmod"] == first_lastmod, "rematerializing must not bump lastmod"


def test_seo_e2e_orphaned_city_retires(fresh_db, tmp_path):
    """A live page whose cell vanishes from the grid entirely (city renamed /
    blocklisted / gone) must go through hysteresis → noindex, not live forever."""
    exists = fresh_db.execute(text("SELECT to_regclass('public.seo_pages')")).scalar()
    if exists is None:
        pytest.skip("seo_pages table not applied")

    _seed(fresh_db)
    out_dir = tmp_path / "florida"
    live_counts = {(_CITY_SLUG, _VERTICAL): _N}
    compile_all(fresh_db, cells=[_cell()], counts=live_counts, output_dir=out_dir, write=True)

    # city disappears from discovery: two runs with an empty grid
    compile_all(fresh_db, cells=[], counts={}, output_dir=out_dir, write=True)
    compile_all(fresh_db, cells=[], counts={}, output_dir=out_dir, write=True)

    row = fresh_db.execute(
        text("SELECT status, below_threshold_runs FROM seo_pages WHERE city_slug = :cs"),
        {"cs": _CITY_SLUG},
    ).mappings().fetchone()
    assert row["status"] == "noindex", "orphaned page must retire via hysteresis"
    sitemap_xml = (out_dir.parent / "sitemap.xml").read_text(encoding="utf-8")
    assert _CITY_SLUG not in sitemap_xml


def test_seo_e2e_retirement_hysteresis(fresh_db, tmp_path):
    exists = fresh_db.execute(
        text("SELECT to_regclass('public.seo_pages')")
    ).scalar()
    if exists is None:
        pytest.skip("seo_pages table not applied")

    _seed(fresh_db)
    out_dir = tmp_path / "florida"
    live_counts = {(_CITY_SLUG, _VERTICAL): _N}

    # publish it live
    compile_all(fresh_db, cells=[_cell()], counts=live_counts, output_dir=out_dir, write=True)

    # now the cell drops below floor — first sub-threshold run: NOT retired
    sub_counts = {(_CITY_SLUG, _VERTICAL): 0}
    compile_all(fresh_db, cells=[_cell()], counts=sub_counts, output_dir=out_dir, write=True)
    row = fresh_db.execute(
        text("SELECT status, below_threshold_runs FROM seo_pages WHERE city_slug = :cs"),
        {"cs": _CITY_SLUG},
    ).mappings().fetchone()
    assert row["status"] == "live", "one sub-threshold run must NOT retire"
    assert row["below_threshold_runs"] == 1

    # second consecutive sub-threshold run: retired (noindex), dropped from sitemap
    result = compile_all(fresh_db, cells=[_cell()], counts=sub_counts, output_dir=out_dir, write=True)
    # >= because the orphan sweep may retire other live rows in the shared DB
    assert result["retired"] >= 1, "summary must count the retirement"
    row = fresh_db.execute(
        text("SELECT status, below_threshold_runs FROM seo_pages WHERE city_slug = :cs"),
        {"cs": _CITY_SLUG},
    ).mappings().fetchone()
    assert row["status"] == "noindex", "two consecutive sub-threshold runs must retire"
    assert row["below_threshold_runs"] == 2

    sitemap_xml = (out_dir.parent / "sitemap.xml").read_text(encoding="utf-8")
    assert _CITY_SLUG not in sitemap_xml, "retired page must be dropped from sitemap"

    # the served file itself must now carry the noindex meta (re-rendered on retire)
    retired_html = (out_dir / _CITY_SLUG / "wholesalers" / "index.html").read_text(encoding="utf-8")
    assert 'content="noindex"' in retired_html, "retired page file must be re-rendered with noindex meta"

    # recovery: data returns → back to live, counter reset
    compile_all(fresh_db, cells=[_cell()], counts=live_counts, output_dir=out_dir, write=True)
    row = fresh_db.execute(
        text("SELECT status, below_threshold_runs FROM seo_pages WHERE city_slug = :cs"),
        {"cs": _CITY_SLUG},
    ).mappings().fetchone()
    assert row["status"] == "live", "page must recover when data returns"
    assert row["below_threshold_runs"] == 0
