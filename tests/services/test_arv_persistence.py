"""WP-8B ARV persistence + PublishedARV projection tests.

Pure helpers (rounding, hashing, decision) need no DB. The persist/read path
uses in-memory SQLite with only the fa_max_arv_results table created; a
generate_uuidv7 SQLite function is registered so the DB-side default UUID path
is exercised exactly as in prod.
"""
import decimal
import sqlite3
import uuid
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.exc import IntegrityError

# psycopg2 binds Decimal to NUMERIC natively; SQLite's DBAPI does not. Register a
# test-only adapter so the prod code can keep passing Decimal unchanged.
sqlite3.register_adapter(decimal.Decimal, str)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import sessionmaker

from src.core.models import FaMaxArvResult
from src.services.quote_ready.arv_models import ARVResult, SelectedComp
from src.services.quote_ready.arv_persistence import (
    ARV_CALC_VERSION,
    ExistingArvResult,
    PublishedARV,
    compute_arv_input_hash,
    decide_arv_persistence,
    get_published_arv,
    persist_arv_result,
    round_to_5k,
)


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------

def test_round_to_5k():
    assert round_to_5k(Decimal("312400")) == Decimal("310000")
    assert round_to_5k(Decimal("312600")) == Decimal("315000")
    assert round_to_5k(Decimal("312500")) == Decimal("315000")  # half up
    assert round_to_5k(Decimal("315000")) == Decimal("315000")  # exact
    assert round_to_5k(None) is None


def test_input_hash_stable_and_sensitive():
    a = _result(low="300000", high="320000")
    b = _result(low="300000", high="320000")
    c = _result(low="300000", high="325000")
    assert compute_arv_input_hash(a) == compute_arv_input_hash(b)
    assert compute_arv_input_hash(a) != compute_arv_input_hash(c)


def test_hash_detects_unrounded_valuation_change():
    assert compute_arv_input_hash(_result(low="300000")) != compute_arv_input_hash(
        _result(low="302400")
    )


def test_hash_detects_changed_comp_provenance_and_repair_assumption():
    original = _result(low="300100", high="300100", point="300100", comps=[_comp(1)])
    changed_comp = _comp(1).model_copy(
        update={
            "sale_price": Decimal("302400"),
            "building_condition": 4,
            "adjusted_value": Decimal("302400"),
        }
    )
    changed = _result(
        low="302400", high="302400", point="302400", comps=[changed_comp]
    ).model_copy(update={"after_repair_condition": 5})

    assert round_to_5k(original.low) == round_to_5k(changed.low)
    assert compute_arv_input_hash(original) != compute_arv_input_hash(changed)


def test_decide_persistence():
    assert decide_arv_persistence(None, "h", "v").action == "insert"
    latest = ExistingArvResult("id1", "h", "v")
    assert decide_arv_persistence(latest, "h", "v").action == "noop"
    d = decide_arv_persistence(latest, "h2", "v")
    assert d.action == "insert_supersede" and d.supersedes_result_id == "id1"


def test_published_arv_has_no_internal_fields():
    assert not hasattr(PublishedARV, "locality_tier") or "locality_tier" not in PublishedARV.model_fields
    assert "selected_comps" not in PublishedARV.model_fields
    assert "locality_tier" not in PublishedARV.model_fields


# ---------------------------------------------------------------------------
# DB path (SQLite)
# ---------------------------------------------------------------------------

@compiles(JSONB, "sqlite")
def _jsonb_sqlite(type_, compiler, **kw):  # pragma: no cover - DDL glue
    return "JSON"


@compiles(PG_UUID, "sqlite")
def _uuid_sqlite(type_, compiler, **kw):  # pragma: no cover - DDL glue
    return "TEXT"


@pytest.fixture
def db():
    engine = create_engine(
        "sqlite:///:memory:", connect_args={"check_same_thread": False}
    )

    @event.listens_for(engine, "connect")
    def _register_uuidv7(dbapi_conn, _):  # pragma: no cover - test glue
        dbapi_conn.create_function("generate_uuidv7", 0, lambda: str(uuid.uuid4()))

    FaMaxArvResult.__table__.create(bind=engine)
    session = sessionmaker(bind=engine)()
    try:
        yield session
    finally:
        session.close()
        engine.dispose()


def _comp(pid=1):
    return SelectedComp(
        property_id=pid,
        sale_price=Decimal("300000"),
        sale_yr=2025,
        sale_mo=6,
        sqft=1500,
        building_condition=3,
        locality_tier="subdivision",
        price_per_sqft=Decimal("200"),
        adjusted_value=Decimal("310000"),
        sqft_adjustment=Decimal("0"),
        condition_adjustment=Decimal("0"),
    )


def _result(low="300000", high="320000", point="310000", confidence="high",
            comp_count=5, weak_comp=False, locality_tier="subdivision",
            arv_unknown=False, comps=None):
    return ARVResult(
        low=Decimal(low) if low is not None else None,
        high=Decimal(high) if high is not None else None,
        point=Decimal(point) if point is not None else None,
        confidence=confidence,
        comp_count=comp_count,
        weak_comp=weak_comp,
        locality_tier=locality_tier,
        arv_unknown=arv_unknown,
        selected_comps=comps if comps is not None else [_comp(1), _comp(2)],
    )


def test_persist_then_get_published(db):
    rid = persist_arv_result(db, property_id=42, result=_result(), computed_by="test")
    pub = get_published_arv(db, 42)
    assert pub is not None
    assert pub.arv_result_id == rid
    assert pub.low == Decimal("300000")
    assert pub.high == Decimal("320000")
    assert pub.confidence == "high"
    assert pub.comp_count == 5
    assert pub.weak_comp is False
    assert pub.source == "wp8b_comparable_sales"


def test_rounding_persisted(db):
    persist_arv_result(
        db, property_id=7, result=_result(low="302400", high="317600", point="309900"),
        computed_by="test",
    )
    pub = get_published_arv(db, 7)
    assert pub.low == Decimal("300000")
    assert pub.high == Decimal("320000")
    assert pub.point == Decimal("310000")


def test_idempotent_repersist_is_noop(db):
    rid1 = persist_arv_result(db, property_id=1, result=_result(), computed_by="t")
    rid2 = persist_arv_result(db, property_id=1, result=_result(), computed_by="t")
    assert rid1 == rid2
    rows = db.execute(
        __import__("sqlalchemy").text(
            "SELECT COUNT(*) FROM fa_max_arv_results WHERE property_id = 1"
        )
    ).scalar_one()
    assert rows == 1


def test_database_rejects_two_computed_rows_for_one_property(db):
    persist_arv_result(db, property_id=88, result=_result(), computed_by="t")

    with pytest.raises(IntegrityError):
        db.execute(
            text(
                """
                INSERT INTO fa_max_arv_results
                    (property_id, source, arv_unknown, calculation_version,
                     input_hash, status)
                VALUES
                    (88, 'wp8b_comparable_sales', false, 'other', 'other',
                     'computed')
                """
            )
        )
        db.commit()
    db.rollback()


def test_supersede_on_change(db):
    from sqlalchemy import text as _text
    rid1 = persist_arv_result(db, property_id=9, result=_result(high="320000"),
                              computed_by="t")
    rid2 = persist_arv_result(db, property_id=9, result=_result(high="340000"),
                              computed_by="t")
    assert rid1 != rid2
    prior_status = db.execute(
        _text("SELECT status FROM fa_max_arv_results WHERE arv_result_id = :r"),
        {"r": rid1},
    ).scalar_one()
    assert prior_status == "superseded"
    pub = get_published_arv(db, 9)
    assert pub.arv_result_id == rid2
    assert pub.high == Decimal("340000")


def test_unknown_result_reads_as_absent(db):
    persist_arv_result(
        db, property_id=5,
        result=ARVResult(arv_unknown=True, unknown_reason="subject_unavailable"),
        computed_by="t",
    )
    assert get_published_arv(db, 5) is None


def test_missing_property_returns_none(db):
    assert get_published_arv(db, 99999) is None
