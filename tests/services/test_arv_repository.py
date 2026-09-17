"""
WP-8B comparable-sales retrieval adapter tests.

Pure mapper tests need no DB. Integration tests use the in-memory SQLite
schema fixture and seed rows via the ORM, rolling back after each test.
"""
from decimal import Decimal

import pytest
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import sessionmaker

from sqlalchemy.exc import OperationalError

from src.core.models import DorSale, Property
from src.services.quote_ready.arv_repository import (
    _condition_with_flag,
    _row_to_candidate,
    _row_to_subject,
    _sqft_of,
    compute_arv_for_property,
    condition_to_int,
    fetch_candidate_sales,
)


# ---------------------------------------------------------------------------
# Pure mapper tests
# ---------------------------------------------------------------------------

def test_condition_to_int_mapping():
    assert condition_to_int("Good") == 4
    assert condition_to_int("excellent") == 5
    assert condition_to_int("  fair ") == 2
    assert condition_to_int(None) == 3
    assert condition_to_int("wtf") == 3


def test_sqft_prefers_heated_then_gross():
    assert _sqft_of(1500, 2000) == 1500          # heated wins
    assert _sqft_of(None, 2000) == 2000          # gross fallback
    assert _sqft_of(None, None) is None
    assert _sqft_of(0, 0) is None
    assert _sqft_of(1499.6, None) == 1500        # rounds


def test_row_to_candidate_skips_null_price_and_zero_sqft():
    base = {
        "property_id": 7,
        "sale_price": Decimal("250000"),
        "sale_yr": 2025,
        "sale_mo": 6,
        "qual_cd": "01",
        "heated_sq_ft": 1600,
        "sq_ft": 1800,
        "beds": 3,
        "baths": Decimal("2.0"),
        "property_use_code": "0100",
        "building_condition": "Good",
        "subdivision": "OAKS",
        "hcpa_neighborhood_code": "N1",
        "zip": "33601",
        "county_id": "hillsborough",
    }
    ok = _row_to_candidate(base)
    assert ok is not None
    assert ok.sqft == 1600
    assert ok.building_condition == 4
    assert ok.sale_price == Decimal("250000")
    assert ok.county == "hillsborough"

    assert _row_to_candidate({**base, "sale_price": None}) is None
    assert _row_to_candidate({**base, "sale_price": Decimal("0")}) is None
    assert _row_to_candidate({**base, "heated_sq_ft": None, "sq_ft": 0}) is None


def test_row_to_subject_threads_after_repair_condition():
    row = {
        "property_id": 1,
        "heated_sq_ft": 1400,
        "sq_ft": None,
        "beds": 3,
        "baths": Decimal("2.0"),
        "property_use_code": "0100",
        "building_condition": "Fair",
        "subdivision": "OAKS",
        "hcpa_neighborhood_code": "N1",
        "zip": "33601",
        "county_id": "hillsborough",
    }
    subj = _row_to_subject(row, after_repair_condition=5)
    assert subj is not None
    assert subj.sqft == 1400
    assert subj.building_condition == 2      # Fair
    assert subj.after_repair_condition == 5

    assert _row_to_subject({**row, "heated_sq_ft": None, "sq_ft": None}, 5) is None


# ---------------------------------------------------------------------------
# Integration tests (SQLite schema)
# ---------------------------------------------------------------------------

@compiles(JSONB, "sqlite")
def _compile_jsonb_sqlite(type_, compiler, **kw):  # pragma: no cover - DDL glue
    return "JSON"


@compiles(ARRAY, "sqlite")
def _compile_array_sqlite(type_, compiler, **kw):  # pragma: no cover - DDL glue
    return "TEXT"


@pytest.fixture
def db():
    """In-memory SQLite with only the properties + dor_sales tables created.

    The shared `in_memory_db` fixture can't build the full schema under SQLite
    (Postgres-only column types elsewhere), so this fixture creates just the two
    tables this adapter touches.
    """
    engine = create_engine(
        "sqlite:///:memory:", connect_args={"check_same_thread": False}
    )
    tables = [Property.__table__, DorSale.__table__]
    for t in tables:
        t.create(bind=engine)
    session = sessionmaker(bind=engine)()
    try:
        yield session
    finally:
        session.close()
        engine.dispose()


def _prop(session, pid, county, **kw):
    defaults = dict(
        id=pid,
        parcel_id=f"parcel-{county}-{pid}",
        property_use_code="0100",
        heated_sq_ft=1500,
        sq_ft=1600,
        beds=3,
        baths=2,
        building_condition="Average",
        subdivision="OAKS",
        hcpa_neighborhood_code="N1",
        zip="33601",
        county_id=county,
    )
    defaults.update(kw)
    p = Property(**defaults)
    session.add(p)
    return p


def _sale(session, prop_id, county, **kw):
    defaults = dict(
        county_id=county,
        co_no=39,
        parcel_id_dor=f"strap-{prop_id}",
        property_id=prop_id,
        qual_cd="01",
        sale_yr=2025,
        sale_mo=6,
        sale_price=Decimal("250000"),
    )
    defaults.update(kw)
    s = DorSale(**defaults)
    session.add(s)
    return s


def test_three_comps_produce_result(db):
    county = "county-3comps"
    _prop(db, 1000, county, subdivision="ALPHA")       # subject
    for i, price in enumerate((240000, 250000, 260000), start=1):
        pid = 1000 + i
        _prop(db, pid, county, subdivision="ALPHA")
        _sale(db, pid, county, sale_price=Decimal(str(price)),
              parcel_id_dor=f"strap-{pid}")
    db.flush()

    result = compute_arv_for_property(
        db, subject_property_id=1000, as_of_yr=2025, as_of_mo=9,
        after_repair_condition=4,
    )
    assert result.arv_unknown is False
    assert result.comp_count == 3
    assert result.point is not None


def test_repeated_sales_for_one_property_count_as_one_comp(db):
    county = "county-distinct-comps"
    _prop(db, 1100, county, subdivision="ALPHA")
    _prop(db, 1101, county, subdivision="ALPHA")
    _sale(
        db,
        1101,
        county,
        sale_yr=2025,
        sale_mo=6,
        sale_price=Decimal("260000"),
        clerk_no="newer-sale",
    )
    _sale(
        db,
        1101,
        county,
        sale_yr=2024,
        sale_mo=6,
        sale_price=Decimal("220000"),
        clerk_no="older-sale",
    )
    _prop(db, 1102, county, subdivision="ALPHA")
    _sale(db, 1102, county, sale_price=Decimal("250000"))
    db.flush()

    candidates = fetch_candidate_sales(
        db,
        subject_property_id=1100,
        county_id=county,
        property_use_code="0100",
        qualified_qual_codes=["01", "02", "03", "04", "05", "06"],
        as_of_yr=2025,
        as_of_mo=9,
    )

    assert [(c.property_id, c.sale_price) for c in candidates] == [
        (1101, Decimal("260000.00")),
        (1102, Decimal("250000.00")),
    ]

    result = compute_arv_for_property(
        db,
        subject_property_id=1100,
        as_of_yr=2025,
        as_of_mo=9,
        after_repair_condition=4,
    )
    assert result.comp_count == 2
    assert result.weak_comp is True
    assert result.confidence == "low"


def test_multi_parcel_sale_is_not_used_as_a_comp(db):
    county = "county-multi-parcel"
    _prop(db, 1200, county, subdivision="ALPHA")
    _prop(db, 1201, county, subdivision="ALPHA")
    _sale(
        db,
        1201,
        county,
        sale_price=Decimal("900000"),
        multi_par_sal="C",
        parcel_id_dor="strap-1201",
    )
    db.flush()

    candidates = fetch_candidate_sales(
        db,
        subject_property_id=1200,
        county_id=county,
        property_use_code="0100",
        qualified_qual_codes=["01", "02", "03", "04", "05", "06"],
        as_of_yr=2025,
        as_of_mo=9,
    )

    assert candidates == []


def test_null_price_comp_excluded(db):
    county = "county-nullprice"
    _prop(db, 2000, county, subdivision="ALPHA")
    # two valid + one null-price
    for i, price in enumerate((240000, 260000), start=1):
        pid = 2000 + i
        _prop(db, pid, county, subdivision="ALPHA")
        _sale(db, pid, county, sale_price=Decimal(str(price)),
              parcel_id_dor=f"strap-{pid}")
    _prop(db, 2099, county, subdivision="ALPHA")
    _sale(db, 2099, county, sale_price=None, parcel_id_dor="strap-2099")
    db.flush()

    cands = fetch_candidate_sales(
        db, subject_property_id=2000, county_id=county,
        property_use_code="0100", qualified_qual_codes=["01","02","03","04","05","06"],
        as_of_yr=2025, as_of_mo=9,
    )
    ids = {c.property_id for c in cands}
    assert ids == {2001, 2002}


def test_different_use_code_not_fetched(db):
    county = "county-usecode"
    _prop(db, 3000, county, property_use_code="0100")
    _prop(db, 3001, county, property_use_code="0200")   # condo
    _sale(db, 3001, county, parcel_id_dor="strap-3001")
    db.flush()

    cands = fetch_candidate_sales(
        db, subject_property_id=3000, county_id=county,
        property_use_code="0100", qualified_qual_codes=["01","02","03","04","05","06"],
        as_of_yr=2025, as_of_mo=9,
    )
    assert cands == []


def test_other_county_not_fetched(db):
    county = "county-A"
    other = "county-B"
    _prop(db, 4000, county)
    _prop(db, 4001, other)
    _sale(db, 4001, other, parcel_id_dor="strap-4001")
    db.flush()

    cands = fetch_candidate_sales(
        db, subject_property_id=4000, county_id=county,
        property_use_code="0100", qualified_qual_codes=["01","02","03","04","05","06"],
        as_of_yr=2025, as_of_mo=9,
    )
    assert cands == []


def test_stale_sale_not_fetched(db):
    county = "county-stale"
    _prop(db, 5000, county)
    _prop(db, 5001, county)
    _sale(db, 5001, county, sale_yr=2022, sale_mo=1, parcel_id_dor="strap-5001")
    db.flush()

    cands = fetch_candidate_sales(
        db, subject_property_id=5000, county_id=county,
        property_use_code="0100", qualified_qual_codes=["01","02","03","04","05","06"],
        as_of_yr=2025, as_of_mo=9, months=24,
    )
    assert cands == []


def test_subject_absent_returns_unknown(db):
    result = compute_arv_for_property(
        db, subject_property_id=999999, as_of_yr=2025, as_of_mo=9,
        after_repair_condition=4,
    )
    assert result.arv_unknown is True
    assert result.unknown_reason == "subject_unavailable"
    assert result.after_repair_condition == 4


def test_subject_missing_sqft_returns_unknown(db):
    county = "county-nosqft"
    _prop(db, 6000, county, heated_sq_ft=None, sq_ft=None)
    db.flush()

    result = compute_arv_for_property(
        db, subject_property_id=6000, as_of_yr=2025, as_of_mo=9,
        after_repair_condition=4,
    )
    assert result.arv_unknown is True
    assert result.unknown_reason == "subject_unavailable"


# ---------------------------------------------------------------------------
# Finding 1 — SQL admits only qualified codes (01-06)
# ---------------------------------------------------------------------------

def test_sql_excludes_disqualified_code_11(db):
    county = "county-qual"
    _prop(db, 7000, county, subdivision="ALPHA")
    _prop(db, 7001, county, subdivision="ALPHA")
    _prop(db, 7002, county, subdivision="ALPHA")
    _sale(db, 7001, county, qual_cd="01", parcel_id_dor="strap-7001")
    _sale(db, 7002, county, qual_cd="11", parcel_id_dor="strap-7002")  # disqualified
    db.flush()

    cands = fetch_candidate_sales(
        db, subject_property_id=7000, county_id=county,
        property_use_code="0100",
        qualified_qual_codes=["01", "02", "03", "04", "05", "06"],
        as_of_yr=2025, as_of_mo=9,
    )
    ids = {c.property_id for c in cands}
    assert ids == {7001}


# ---------------------------------------------------------------------------
# Finding 4 — infra failure is distinct from missing data
# ---------------------------------------------------------------------------

def test_source_failure_distinct_from_subject_unavailable(db):
    class _BoomSession:
        def execute(self, *a, **k):
            raise OperationalError("SELECT 1", {}, Exception("db down"))

    result = compute_arv_for_property(
        _BoomSession(), subject_property_id=8000, as_of_yr=2025, as_of_mo=9,
        after_repair_condition=4,
    )
    assert result.arv_unknown is True
    assert result.unknown_reason == "source_failure"
    assert result.after_repair_condition == 4


# ---------------------------------------------------------------------------
# Finding 5 — unknown condition is flagged inferred
# ---------------------------------------------------------------------------

def test_condition_with_flag_marks_inferred():
    assert _condition_with_flag("Good") == (4, False)
    assert _condition_with_flag(None) == (3, True)
    assert _condition_with_flag("wat") == (3, True)


def test_null_condition_comp_marked_inferred(db):
    county = "county-cond"
    _prop(db, 9000, county, subdivision="ALPHA")
    _prop(db, 9001, county, subdivision="ALPHA", building_condition=None)
    _sale(db, 9001, county, parcel_id_dor="strap-9001")
    db.flush()

    cands = fetch_candidate_sales(
        db, subject_property_id=9000, county_id=county,
        property_use_code="0100",
        qualified_qual_codes=["01", "02", "03", "04", "05", "06"],
        as_of_yr=2025, as_of_mo=9,
    )
    assert len(cands) == 1
    assert cands[0].condition_inferred is True
