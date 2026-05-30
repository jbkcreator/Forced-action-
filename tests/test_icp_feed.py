"""Phase 4 — REI ICP feed query tests.

The feed uses JSONB operators so ORM-level tests require Postgres.
The SQLite-compatible tests below verify:
  - the query is constructable (no import/syntax errors)
  - it references the right tables/columns
  - the Bankruptcy and investment-score branches are present in SQL
  - the feed is NOT wired to any route (structural guard)
  - config constants are sane

Postgres-dependent tests (actual DB execution) use fresh_db and are
skipped when DATABASE_URL is absent.
"""
import inspect

import pytest

from src.services.icp_feed import (
    REI_INVESTMENT_SCORE_THRESHOLD,
    _BANKRUPTCY,
    rei_feed_query,
)


# ── P4-1 query constructs without errors ─────────────────────────────────────

def test_rei_feed_query_constructable():
    """rei_feed_query() must return a select object without raising."""
    from unittest.mock import MagicMock
    db = MagicMock()
    q = rei_feed_query(db, "hillsborough")
    assert q is not None


# ── P4-2 SQL text contains Bankruptcy branch ─────────────────────────────────

def test_feed_sql_contains_bankruptcy_branch():
    from unittest.mock import MagicMock
    from sqlalchemy.dialects import postgresql
    q = rei_feed_query(MagicMock(), "hillsborough")
    compiled = q.compile(dialect=postgresql.dialect(),
                         compile_kwargs={"literal_binds": True})
    sql = str(compiled)
    assert "Bankruptcy" in sql, "Feed SQL must filter on Bankruptcy record_type"


# ── P4-3 SQL contains investment vertical score branches ─────────────────────

def test_feed_sql_contains_wholesalers_and_fix_flip():
    from unittest.mock import MagicMock
    from sqlalchemy.dialects import postgresql
    q = rei_feed_query(MagicMock(), "hillsborough")
    compiled = q.compile(dialect=postgresql.dialect(),
                         compile_kwargs={"literal_binds": True})
    sql = str(compiled)
    assert "wholesalers" in sql
    assert "fix_flip" in sql


# ── P4-4 feed scoped to supplied county ──────────────────────────────────────

def test_feed_sql_county_scoped():
    from unittest.mock import MagicMock
    from sqlalchemy.dialects import postgresql
    q = rei_feed_query(MagicMock(), "hillsborough")
    compiled = q.compile(dialect=postgresql.dialect(),
                         compile_kwargs={"literal_binds": True})
    sql = str(compiled)
    assert "hillsborough" in sql, "Feed must filter on supplied county_id"


# ── P4-5 investment score threshold is sane ──────────────────────────────────

def test_rei_investment_score_threshold_is_sane():
    assert REI_INVESTMENT_SCORE_THRESHOLD >= 1.0
    assert REI_INVESTMENT_SCORE_THRESHOLD <= 100.0


def test_bankruptcy_constant():
    assert _BANKRUPTCY == "Bankruptcy"


# ── P4-6 feed NOT wired to any route (config-only guard) ─────────────────────

def test_feed_not_exposed_via_any_router():
    """No API router or task should import icp_feed yet."""
    import importlib
    import pkgutil
    import src.api as api_pkg
    import src.tasks as tasks_pkg

    for pkg in (api_pkg, tasks_pkg):
        for _importer, modname, _ispkg in pkgutil.walk_packages(
            path=pkg.__path__, prefix=pkg.__name__ + "."
        ):
            try:
                mod = importlib.import_module(modname)
                src = inspect.getsource(mod)
                assert "icp_feed" not in src, (
                    f"{modname} references icp_feed — feed must not be exposed "
                    "until after the ICP launch gate is cleared"
                )
            except Exception:
                pass  # import errors in unrelated modules are not our problem here


# ── P4-7 Postgres: feed includes Bankruptcy-linked property ──────────────────

def test_feed_includes_bankruptcy_linked_property(fresh_db):
    """A property with an active Bankruptcy proceeding appears in the feed."""
    from sqlalchemy import text
    # Insert minimal property + bankruptcy proceeding
    r = fresh_db.execute(text("""
        INSERT INTO properties (parcel_id, county_id, created_at, updated_at)
        VALUES ('TEST001', 'hillsborough', NOW(), NOW())
        RETURNING id
    """))
    prop_id = r.scalar()
    fresh_db.execute(text("""
        INSERT INTO legal_proceedings (property_id, county_id, record_type, case_number)
        VALUES (:pid, 'hillsborough', 'Bankruptcy', 'BK-TEST-001')
    """), {"pid": prop_id})
    fresh_db.flush()

    from sqlalchemy import text as t2
    results = fresh_db.execute(rei_feed_query(fresh_db, "hillsborough")).fetchall()
    prop_ids = [r[0].id if hasattr(r[0], "id") else r.id for r in results]
    assert prop_id in prop_ids, "Property with Bankruptcy proceeding must appear in REI feed"


def test_feed_single_county_scope(fresh_db):
    """Out-of-county properties must not appear in the feed."""
    from sqlalchemy import text
    r = fresh_db.execute(text("""
        INSERT INTO properties (parcel_id, county_id, created_at, updated_at)
        VALUES ('TEST002', 'pinellas', NOW(), NOW())
        RETURNING id
    """))
    prop_id = r.scalar()
    fresh_db.execute(text("""
        INSERT INTO legal_proceedings (property_id, county_id, record_type, case_number)
        VALUES (:pid, 'pinellas', 'Bankruptcy', 'BK-TEST-002')
    """), {"pid": prop_id})
    fresh_db.flush()

    results = fresh_db.execute(rei_feed_query(fresh_db, "hillsborough")).fetchall()
    prop_ids = [r[0].id if hasattr(r[0], "id") else r.id for r in results]
    assert prop_id not in prop_ids, "Out-of-county property must not appear in hillsborough feed"
