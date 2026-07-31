"""
CLONE-v2.2 CL3 — src/services/venture_provisioning.py.

Like test_venture_config.py these never commit: every row lives inside
fresh_db's nested transaction and vanishes on rollback, so there is no
teardown helper and nothing can leak into a later run.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import text

from config.venture_template import COUNTY_TEMPLATE, new_venture_config
from src.services.venture_provisioning import (
    clone_column_mappings,
    clone_county_sources,
    provision_venture,
    upsert_venture,
)
from src.utils import county_config, venture_config


@pytest.fixture(autouse=True)
def clean_caches():
    venture_config.invalidate_cache()
    county_config.invalidate_cache()
    yield
    venture_config.invalidate_cache()
    county_config.invalidate_cache()


def _make_county(db, county_id: str, venture_key: str) -> None:
    """counties.is_active is NOT NULL with only a Python-side ORM default, so
    a raw INSERT has to supply it explicitly."""
    db.execute(text("""
        INSERT INTO counties (county_id, display_name, venture_key, is_active)
        VALUES (:cid, :name, :vk, true)
    """), {"cid": county_id, "name": f"County {county_id}", "vk": venture_key})


@pytest.fixture
def template_county(fresh_db):
    """A venture + template county with three sources, one of them inactive
    and one on playwright_only with cached code."""
    suffix = uuid.uuid4().hex[:8]
    venture_key = f"vp_{suffix}"
    county_id = f"tmpl_{suffix}"

    fresh_db.execute(text("""
        INSERT INTO ventures (venture_key, display_name, brand_name, state, template_county_id)
        VALUES (:vk, 'Template Venture', 'Template Brand', 'FL', :cid)
    """), {"vk": venture_key, "cid": county_id})
    fresh_db.execute(text("""
        INSERT INTO counties (county_id, display_name, venture_key, is_active)
        VALUES (:cid, 'Template County', :vk, true)
    """), {"cid": county_id, "vk": venture_key})

    fresh_db.execute(text("""
        INSERT INTO county_sources (
            county_id, signal_type, source_name, url, description, navigation_hint,
            output_format, date_range_available, frequency, special_flags,
            scrape_mode, playwright_code, playwright_code_version,
            playwright_code_approved, is_active
        )
        VALUES
            (:cid, 'foreclosures', 'Tmpl Foreclosures', 'https://tmpl.example/fc',
             'desc fc', 'nav fc', 'csv', true, 'daily', '{"prr_only": true}'::jsonb,
             'playwright_only', 'async def run_scrape(): pass', 'v1', true, true),
            (:cid, 'permits', 'Tmpl Permits', 'https://tmpl.example/permits',
             'desc permits', NULL, 'xlsx', false, 'weekly', '{}'::jsonb,
             'ai_only', NULL, NULL, false, true),
            (:cid, 'violations', 'Tmpl Violations (retired)', 'https://tmpl.example/v',
             NULL, NULL, 'csv', true, 'daily', '{}'::jsonb, 'ai_only', NULL, NULL,
             false, false)
    """), {"cid": county_id})

    return {"venture_key": venture_key, "county_id": county_id, "suffix": suffix}


def _sources(db, county_id: str) -> dict[str, dict]:
    rows = db.execute(text("""
        SELECT signal_type, source_name, url, description, navigation_hint,
               output_format, date_range_available, frequency, special_flags,
               scrape_mode, playwright_code, playwright_code_version,
               playwright_code_approved, is_active
        FROM county_sources WHERE county_id = :cid
    """), {"cid": county_id}).mappings().all()
    return {r["signal_type"]: dict(r) for r in rows}


def test_clone_copies_active_sources_only(fresh_db, template_county):
    dest = f"dest_{template_county['suffix']}"
    _make_county(fresh_db, dest, template_county["venture_key"])

    result = clone_county_sources(
        fresh_db, from_county_id=template_county["county_id"], to_county_id=dest,
    )

    assert result["cloned"] == 2
    cloned = _sources(fresh_db, dest)
    assert set(cloned) == {"foreclosures", "permits"}  # the inactive one is not cloned


def test_clone_carries_descriptive_fields_and_flags(fresh_db, template_county):
    dest = f"dest_{template_county['suffix']}"
    _make_county(fresh_db, dest, template_county["venture_key"])

    clone_county_sources(
        fresh_db, from_county_id=template_county["county_id"], to_county_id=dest,
    )

    fc = _sources(fresh_db, dest)["foreclosures"]
    assert fc["source_name"] == "Tmpl Foreclosures"
    assert fc["description"] == "desc fc"
    assert fc["navigation_hint"] == "nav fc"
    assert fc["output_format"] == "csv"
    assert fc["frequency"] == "daily"
    assert fc["special_flags"] == {"prr_only": True}
    permits = _sources(fresh_db, dest)["permits"]
    assert permits["date_range_available"] is False  # copied, not defaulted to true


def test_clone_never_copies_playwright_code(fresh_db, template_county):
    """Cached selectors are written against one portal's DOM. Carrying them to
    a different county would scrape the wrong page while looking like it
    worked, so the code is dropped and the mode downgraded to ai_only for
    regeneration."""
    dest = f"dest_{template_county['suffix']}"
    _make_county(fresh_db, dest, template_county["venture_key"])

    clone_county_sources(
        fresh_db, from_county_id=template_county["county_id"], to_county_id=dest,
    )

    fc = _sources(fresh_db, dest)["foreclosures"]
    assert fc["playwright_code"] is None
    assert fc["playwright_code_version"] is None
    assert fc["playwright_code_approved"] is False
    assert fc["scrape_mode"] == "ai_only"  # was playwright_only on the template

    # A source that was already ai_only keeps its mode untouched.
    assert _sources(fresh_db, dest)["permits"]["scrape_mode"] == "ai_only"


def test_clone_applies_url_overrides(fresh_db, template_county):
    dest = f"dest_{template_county['suffix']}"
    _make_county(fresh_db, dest, template_county["venture_key"])

    result = clone_county_sources(
        fresh_db,
        from_county_id=template_county["county_id"],
        to_county_id=dest,
        url_overrides={"foreclosures": "https://dest.example/fc"},
    )

    cloned = _sources(fresh_db, dest)
    assert cloned["foreclosures"]["url"] == "https://dest.example/fc"
    # Not overridden — inherits the template's URL, and is reported so the
    # caller knows a scraper would hit the wrong county's portal.
    assert cloned["permits"]["url"] == "https://tmpl.example/permits"
    assert result["missing_urls"] == ["permits"]


def test_clone_is_idempotent(fresh_db, template_county):
    dest = f"dest_{template_county['suffix']}"
    _make_county(fresh_db, dest, template_county["venture_key"])

    first = clone_county_sources(
        fresh_db, from_county_id=template_county["county_id"], to_county_id=dest,
    )
    second = clone_county_sources(
        fresh_db, from_county_id=template_county["county_id"], to_county_id=dest,
    )

    assert first["cloned"] == 2 and first["skipped"] == 0
    assert second["cloned"] == 0 and second["skipped"] == 2
    assert len(_sources(fresh_db, dest)) == 2  # no duplicates


def test_clone_from_a_county_with_no_sources_reports_everything_missing(fresh_db, template_county):
    empty = f"empty_{template_county['suffix']}"
    dest = f"dest_{template_county['suffix']}"
    for cid in (empty, dest):
        _make_county(fresh_db, cid, template_county["venture_key"])

    result = clone_county_sources(fresh_db, from_county_id=empty, to_county_id=dest)

    assert result["cloned"] == 0
    assert result["missing_urls"]  # every required signal is unfulfilled
    assert _sources(fresh_db, dest) == {}


def test_clone_column_mappings_lands_unapproved(fresh_db, template_county):
    dest = f"dest_{template_county['suffix']}"
    _make_county(fresh_db, dest, template_county["venture_key"])
    template_source_id = fresh_db.execute(text("""
        SELECT id FROM county_sources
        WHERE county_id = :cid AND signal_type = 'foreclosures'
    """), {"cid": template_county["county_id"]}).scalar_one()
    fresh_db.execute(text("""
        INSERT INTO county_column_mappings (source_id, source_columns, mapping, is_approved, mapped_by)
        VALUES (:sid, '["Case #"]'::jsonb, '{"Case #": "case_number"}'::jsonb, true, 'human')
    """), {"sid": template_source_id})

    clone_county_sources(
        fresh_db, from_county_id=template_county["county_id"], to_county_id=dest,
    )
    written = clone_column_mappings(
        fresh_db, from_county_id=template_county["county_id"], to_county_id=dest,
    )

    assert written == 1
    row = fresh_db.execute(text("""
        SELECT m.is_approved, m.mapping
        FROM county_column_mappings m
        JOIN county_sources s ON s.id = m.source_id
        WHERE s.county_id = :cid
    """), {"cid": dest}).one()
    assert row.is_approved is False  # must be re-reviewed for the new portal
    assert row.mapping == {"Case #": "case_number"}


def test_clone_column_mappings_is_idempotent(fresh_db, template_county):
    dest = f"dest_{template_county['suffix']}"
    _make_county(fresh_db, dest, template_county["venture_key"])
    template_source_id = fresh_db.execute(text("""
        SELECT id FROM county_sources
        WHERE county_id = :cid AND signal_type = 'foreclosures'
    """), {"cid": template_county["county_id"]}).scalar_one()
    fresh_db.execute(text("""
        INSERT INTO county_column_mappings (source_id, source_columns, mapping, is_approved, mapped_by)
        VALUES (:sid, '["Case #"]'::jsonb, '{"Case #": "case_number"}'::jsonb, true, 'human')
    """), {"sid": template_source_id})
    clone_county_sources(
        fresh_db, from_county_id=template_county["county_id"], to_county_id=dest,
    )

    assert clone_column_mappings(
        fresh_db, from_county_id=template_county["county_id"], to_county_id=dest) == 1
    assert clone_column_mappings(
        fresh_db, from_county_id=template_county["county_id"], to_county_id=dest) == 0


def test_upsert_venture_rejects_an_invalid_config(fresh_db):
    cfg = new_venture_config(venture_key=f"vp_{uuid.uuid4().hex[:8]}", state="Florida")
    with pytest.raises(ValueError, match="two-letter"):
        upsert_venture(fresh_db, cfg)


def test_upsert_venture_overwrites_on_second_call(fresh_db):
    key = f"vp_{uuid.uuid4().hex[:8]}"
    first_id = upsert_venture(fresh_db, new_venture_config(
        venture_key=key, display_name="First", brand_name="First", relay_daily_ceiling=5,
    ))
    second_id = upsert_venture(fresh_db, new_venture_config(
        venture_key=key, display_name="Second", brand_name="Second", relay_daily_ceiling=9,
    ))

    assert first_id == second_id
    row = fresh_db.execute(text(
        "SELECT display_name, relay_daily_ceiling FROM ventures WHERE id = :id"
    ), {"id": first_id}).one()
    assert row.display_name == "Second"
    assert row.relay_daily_ceiling == 9


def test_provision_venture_end_to_end(fresh_db, template_county):
    """A filled-in config becomes a venture, a county, and that county's
    cloned source set — and county_config then reports the new venture's
    geography rather than Florida's."""
    key = f"vp_{uuid.uuid4().hex[:8]}"
    new_county = f"newc_{uuid.uuid4().hex[:8]}"
    cfg = new_venture_config(
        venture_key=key,
        display_name="Venture Two",
        brand_name="Venture Two LLC",
        state="TX",
        bankruptcy_court_code="txnb",
        default_bankruptcy_division="4:",
        template_county_id=template_county["county_id"],
    )

    report = provision_venture(
        fresh_db,
        venture_cfg=cfg,
        counties=[{
            "county_id": new_county,
            "display_name": "New County",
            "zip_prefixes": ["750", "751"],
            "source_url_overrides": {
                "foreclosures": "https://new.example/fc",
                "permits": "https://new.example/permits",
            },
        }],
    )

    assert report["venture_key"] == key
    county_report = report["counties"][new_county]
    assert county_report["county_created"] is True
    assert county_report["cloned"] == 2
    assert county_report["missing_urls"] == []

    cloned = _sources(fresh_db, new_county)
    assert cloned["foreclosures"]["url"] == "https://new.example/fc"
    assert cloned["permits"]["url"] == "https://new.example/permits"

    resolved = county_config._load_from_db(new_county, session=fresh_db)
    assert resolved["venture_key"] == key
    assert resolved["state"] == "TX"
    assert resolved["court"]["bankruptcy_code"] == "txnb"
    assert resolved["court"]["division_prefix"] == "4:"
    assert resolved["zip_prefixes"] == ["750", "751"]
    assert resolved["urls"]["foreclosure"] == "https://new.example/fc"


def test_provision_venture_is_idempotent(fresh_db, template_county):
    key = f"vp_{uuid.uuid4().hex[:8]}"
    new_county = f"newc_{uuid.uuid4().hex[:8]}"
    cfg = new_venture_config(
        venture_key=key, display_name="V2", brand_name="V2",
        template_county_id=template_county["county_id"],
    )
    counties = [{"county_id": new_county, "display_name": "New County"}]

    first = provision_venture(fresh_db, venture_cfg=cfg, counties=counties)
    second = provision_venture(fresh_db, venture_cfg=cfg, counties=counties)

    assert first["counties"][new_county]["county_created"] is True
    assert second["counties"][new_county]["county_created"] is False
    assert second["counties"][new_county]["cloned"] == 0
    assert len(_sources(fresh_db, new_county)) == 2
    assert fresh_db.execute(text(
        "SELECT count(*) FROM counties WHERE county_id = :cid"
    ), {"cid": new_county}).scalar() == 1


def test_provision_venture_rejects_unedited_template_placeholders(fresh_db):
    """The emitted template ships `county_id: "CHANGE_ME"`. Running it
    unedited must fail rather than create a real county with that name."""
    cfg = new_venture_config(
        venture_key=f"vp_{uuid.uuid4().hex[:8]}", display_name="V2", brand_name="V2",
    )
    with pytest.raises(ValueError, match="template placeholder"):
        provision_venture(fresh_db, venture_cfg=cfg, counties=[dict(COUNTY_TEMPLATE)])


def test_provision_venture_rejects_a_county_with_no_id(fresh_db):
    cfg = new_venture_config(
        venture_key=f"vp_{uuid.uuid4().hex[:8]}", display_name="V2", brand_name="V2",
    )
    with pytest.raises(ValueError, match="county_id is required"):
        provision_venture(fresh_db, venture_cfg=cfg, counties=[{"display_name": "No Id County"}])


def test_provision_venture_rejects_duplicate_county_ids(fresh_db):
    cfg = new_venture_config(
        venture_key=f"vp_{uuid.uuid4().hex[:8]}", display_name="V2", brand_name="V2",
    )
    county = {"county_id": "dupco", "display_name": "Dup County"}
    with pytest.raises(ValueError, match="duplicate county_id"):
        provision_venture(fresh_db, venture_cfg=cfg, counties=[county, dict(county)])


def test_provision_venture_without_a_template_county_says_so(fresh_db):
    key = f"vp_{uuid.uuid4().hex[:8]}"
    new_county = f"newc_{uuid.uuid4().hex[:8]}"
    cfg = new_venture_config(
        venture_key=key, display_name="V2", brand_name="V2", template_county_id=None,
    )

    report = provision_venture(
        fresh_db, venture_cfg=cfg,
        counties=[{"county_id": new_county, "display_name": "New County"}],
    )

    assert "sources_skipped" in report["counties"][new_county]
    assert _sources(fresh_db, new_county) == {}
