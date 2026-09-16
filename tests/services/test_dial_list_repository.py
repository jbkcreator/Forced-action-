"""WP-9 dial-list retrieval adapter tests.

In-memory SQLite; only the tables this adapter touches are created, seeded via
the ORM. Asserts detectors map to the right triggers/flags, the union dedups
by borrower through the pure core, unresolved candidates still appear, and a
DB failure propagates (logged) rather than silently returning empty.
"""
from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import sessionmaker

from src.core.models import (
    AgentLaneOpportunityOutcome,
    BuildingPermit,
    BuyerEntity,
    BuyerEntityLink,
    Deed,
    Financial,
    FinancingIntentScore,
    Foreclosure,
    LegalAndLien,
    LegalProceeding,
    Owner,
    Property,
    TaxDeedAuction,
)
from src.services.dial_list import (
    DialListConfig,
    assemble_dial_candidates,
    generate_dial_list,
)

AS_OF = date(2026, 9, 16)


@compiles(JSONB, "sqlite")
def _compile_jsonb_sqlite(type_, compiler, **kw):  # pragma: no cover - DDL glue
    return "JSON"


@compiles(ARRAY, "sqlite")
def _compile_array_sqlite(type_, compiler, **kw):  # pragma: no cover - DDL glue
    return "TEXT"


@pytest.fixture
def db():
    engine = create_engine(
        "sqlite:///:memory:", connect_args={"check_same_thread": False}
    )
    tables = [
        Property.__table__,
        Owner.__table__,
        Financial.__table__,
        Deed.__table__,
        LegalProceeding.__table__,
        LegalAndLien.__table__,
        Foreclosure.__table__,
        BuildingPermit.__table__,
        FinancingIntentScore.__table__,
        TaxDeedAuction.__table__,
        BuyerEntity.__table__,
        BuyerEntityLink.__table__,
        AgentLaneOpportunityOutcome.__table__,
    ]
    for t in tables:
        t.create(bind=engine)
    session = sessionmaker(bind=engine)()
    try:
        yield session
    finally:
        session.close()
        engine.dispose()


_pid_seq = [0]


def _prop(session, county="hillsborough", property_use_code="0100", **kw):
    _pid_seq[0] += 1
    pid = _pid_seq[0]
    p = Property(id=pid, parcel_id=f"parcel-{pid}", county_id=county,
                 property_use_code=property_use_code, **kw)
    session.add(p)
    session.flush()
    return pid


def _fin(session, pid, **kw):
    session.add(Financial(property_id=pid, county_id="hillsborough", **kw))
    session.flush()


def _owner(session, pid, **kw):
    o = Owner(property_id=pid, **kw)
    session.add(o)
    session.flush()
    return o


def _link_borrower(session, owner_id, entity_id):
    session.add(
        BuyerEntityLink(
            buyer_entity_id=entity_id,
            source_table="owners",
            source_id=owner_id,
            match_confidence=90,
            match_method="exact_name_address",
        )
    )
    session.flush()


def _borrower(session, entity_id, **kw):
    defaults = dict(
        id=entity_id,
        canonical_name=f"ENTITY {entity_id}",
        entity_type="LLC",
        confidence_score=90,
        total_purchase_count=0,
    )
    defaults.update(kw)
    session.add(BuyerEntity(**defaults))
    session.flush()


# ---------------------------------------------------------------------------
# Detector coverage
# ---------------------------------------------------------------------------

def test_cash_purchase_detected(db):
    pid = _prop(db)
    db.add(Deed(property_id=pid, instrument_number="I1", sale_price=Decimal("300000"),
                mortgage_amount=None, record_date=AS_OF - timedelta(days=30),
                county_id="hillsborough"))
    db.flush()
    cands = assemble_dial_candidates(db, as_of=AS_OF)
    assert len(cands) == 1
    assert "cash_purchase" in cands[0].triggers
    assert cands[0].urgency_date == AS_OF - timedelta(days=30)


def test_financed_purchase_not_cash(db):
    pid = _prop(db)
    db.add(Deed(property_id=pid, instrument_number="I2", sale_price=Decimal("300000"),
                mortgage_amount=Decimal("240000"), record_date=AS_OF - timedelta(days=30),
                county_id="hillsborough"))
    db.flush()
    cands = assemble_dial_candidates(db, as_of=AS_OF)
    assert cands == []  # mortgage present → no cash trigger, no other signal


def test_out_of_state_flag(db):
    pid = _prop(db)
    _owner(db, pid, owner_name="ABSENTEE LLC", absentee_status="Out-of-State")
    cands = assemble_dial_candidates(db, as_of=AS_OF)
    assert len(cands) == 1
    assert cands[0].triggers == ["out_of_state"]


def test_financing_intent_feed_maps_tier(db):
    pid = _prop(db)
    db.add(FinancingIntentScore(id=1, property_id=pid, county_id="hillsborough",
                                score_date=AS_OF - timedelta(days=1),
                                financing_intent_score=Decimal("70"), intent_tier="high"))
    db.flush()
    cands = assemble_dial_candidates(db, as_of=AS_OF)
    assert len(cands) == 1
    assert "financing_intent" in cands[0].triggers
    assert cands[0].intent_tier == "high"


def test_builder_flag(db):
    pid = _prop(db)
    db.add(BuildingPermit(property_id=pid, permit_number="P1",
                          permit_type="New Construction - SFR",
                          issue_date=AS_OF - timedelta(days=60),
                          is_enforcement_permit=False, county_id="hillsborough"))
    db.flush()
    cands = assemble_dial_candidates(db, as_of=AS_OF)
    assert len(cands) == 1
    assert cands[0].is_builder is True
    assert "builder" in cands[0].triggers


def test_probate_detected(db):
    pid = _prop(db)
    db.add(LegalProceeding(property_id=pid, record_type="Probate", case_number="C1",
                           filing_date=AS_OF - timedelta(days=100),
                           county_id="hillsborough"))
    db.flush()
    cands = assemble_dial_candidates(db, as_of=AS_OF)
    assert "auction_probate" in cands[0].triggers


# ---------------------------------------------------------------------------
# Union / resolution / enrichment
# ---------------------------------------------------------------------------

def test_union_dedup_by_borrower(db):
    # two properties, same borrower, different triggers → core collapses to one
    _borrower(db, 500, total_purchase_count=6)
    p1 = _prop(db)
    o1 = _owner(db, p1, owner_name="X", absentee_status="Out-of-State")
    _link_borrower(db, o1.id, 500)
    _fin(db, p1, assessed_value_mkt=Decimal("400000"))

    p2 = _prop(db)
    o2 = _owner(db, p2, owner_name="X")
    _link_borrower(db, o2.id, 500)
    db.add(Deed(property_id=p2, instrument_number="I9", sale_price=Decimal("250000"),
                mortgage_amount=None, record_date=AS_OF - timedelta(days=10),
                county_id="hillsborough"))
    _fin(db, p2, assessed_value_mkt=Decimal("100000"))
    db.flush()

    cands = assemble_dial_candidates(db, as_of=AS_OF)
    assert len(cands) == 2  # adapter is property-grained
    assert all(c.buyer_entity_id == 500 for c in cands)
    assert all(c.properties_owned == 6 for c in cands)

    dl = generate_dial_list(db, as_of=AS_OF)
    assert len(dl.entries) == 1  # pure core dedups by borrower
    entry = dl.entries[0]
    assert entry.buyer_entity_id == 500
    # Fix 7: triggers are NOT merged across different properties under the same
    # borrower — only the retained property's own signals are shown so Josh
    # gets accurate context for the actual property he's calling about.
    # p1 (out_of_state, assessed $400K) scores higher → its trigger is kept.
    assert "out_of_state" in entry.triggers
    assert "cash_purchase" not in entry.triggers


def test_maturities_trigger_gated_off_by_default(db):
    pid = _prop(db)
    db.add(LegalAndLien(property_id=pid, record_type="Lien", document_type="ML",
                        filing_date=AS_OF - timedelta(days=330),
                        county_id="hillsborough"))
    db.flush()
    assert assemble_dial_candidates(db, as_of=AS_OF) == []  # off by default


def test_maturities_trigger_when_enabled(db):
    pid = _prop(db)
    db.add(LegalAndLien(property_id=pid, record_type="Lien", document_type="ML",
                        filing_date=AS_OF - timedelta(days=330),  # ~11mo, 12mo term
                        county_id="hillsborough"))
    db.flush()
    cfg = DialListConfig(enable_maturities_trigger=True)
    cands = assemble_dial_candidates(db, as_of=AS_OF, config=cfg)
    assert len(cands) == 1
    assert "maturities" in cands[0].triggers


def test_1031_trigger_when_enabled(db):
    pid = _prop(db)
    db.add(Deed(property_id=pid, instrument_number="IX", grantee="ACME 1031 EXCHANGE LLC",
                sale_price=Decimal("400000"), mortgage_amount=Decimal("1"),
                record_date=AS_OF - timedelta(days=30), county_id="hillsborough"))
    db.flush()
    off = assemble_dial_candidates(db, as_of=AS_OF)
    assert off == []  # off by default (mortgage present → no cash trigger either)
    cfg = DialListConfig(enable_1031_trigger=True)
    cands = assemble_dial_candidates(db, as_of=AS_OF, config=cfg)
    assert len(cands) == 1
    assert "exchange_1031" in cands[0].triggers


def test_listing_triggers_have_no_source(db):
    # enabling them is a no-op (no MLS/listing table) — must not crash
    _prop(db)  # a bare property, no other trigger
    cfg = DialListConfig(enable_price_drop_trigger=True,
                         enable_expired_listing_trigger=True)
    assert assemble_dial_candidates(db, as_of=AS_OF, config=cfg) == []


def test_commercial_use_code_filtered_out(db):
    # a commercial parcel (store, code 11xx) with a live trigger must NOT surface
    pid = _prop(db, property_use_code="1100")
    db.add(Deed(property_id=pid, instrument_number="IC", sale_price=Decimal("9000000"),
                mortgage_amount=None, record_date=AS_OF - timedelta(days=20),
                county_id="hillsborough"))
    db.flush()
    assert assemble_dial_candidates(db, as_of=AS_OF) == []


def test_residential_use_code_kept(db):
    # condo (code 04xx) with a cash purchase stays in scope
    pid = _prop(db, property_use_code="0400")
    db.add(Deed(property_id=pid, instrument_number="IR", sale_price=Decimal("300000"),
                mortgage_amount=None, record_date=AS_OF - timedelta(days=20),
                county_id="hillsborough"))
    db.flush()
    cands = assemble_dial_candidates(db, as_of=AS_OF)
    assert len(cands) == 1 and "cash_purchase" in cands[0].triggers


def test_name_address_phone_populated(db):
    _borrower(db, 900, canonical_name="ACME HOMES LLC", total_purchase_count=3)
    pid = _prop(db, address="123 Main St", city="Tampa", zip="33602")
    o = _owner(db, pid, owner_name="ACME HOMES LLC", phone_1="813-555-0100",
               absentee_status="Out-of-State")
    _link_borrower(db, o.id, 900)
    db.flush()
    c = assemble_dial_candidates(db, as_of=AS_OF)[0]
    assert c.borrower_name == "ACME HOMES LLC"
    assert c.owner_name == "ACME HOMES LLC"
    assert c.property_address == "123 Main St, Tampa 33602"
    assert c.phone == "813-555-0100"


def test_owner_name_fallback_when_unresolved(db):
    pid = _prop(db, address="9 Oak Ave", city="Tampa", zip="33605")
    _owner(db, pid, owner_name="JANE DOE", phone_1="813-555-0200",
           absentee_status="Out-of-State")
    db.flush()
    c = assemble_dial_candidates(db, as_of=AS_OF)[0]
    assert c.borrower_name is None  # unresolved entity
    assert c.owner_name == "JANE DOE"  # fallback name available


def test_unresolved_candidate_still_appears(db):
    pid = _prop(db)
    _owner(db, pid, owner_name="NOENTITY", absentee_status="Out-of-State")
    # no BuyerEntityLink → unresolved
    cands = assemble_dial_candidates(db, as_of=AS_OF)
    assert len(cands) == 1
    assert cands[0].buyer_entity_id is None


def test_assessed_value_fallback_populated(db):
    pid = _prop(db)
    _fin(db, pid, assessed_value_mkt=Decimal("350000"),
         last_sale_price=Decimal("300000"), last_sale_date=AS_OF - timedelta(days=90))
    db.add(FinancingIntentScore(id=2, property_id=pid, county_id="hillsborough",
                                score_date=AS_OF, financing_intent_score=Decimal("50"),
                                intent_tier="medium"))
    db.flush()
    cands = assemble_dial_candidates(db, as_of=AS_OF)
    c = cands[0]
    assert c.assessed_value_mkt == Decimal("350000")
    assert c.last_sale_price == Decimal("300000")
    assert c.arv is None and c.max_ltc is None  # WP-8B ARV not wired yet
    assert c.last_deal_months_ago == 3


# ---------------------------------------------------------------------------
# Failure posture
# ---------------------------------------------------------------------------

def test_source_failure_propagates(db, caplog):
    class _Boom:
        def execute(self, *a, **k):
            raise SQLAlchemyError("db down")

    with pytest.raises(SQLAlchemyError):
        assemble_dial_candidates(_Boom(), as_of=AS_OF)
    assert any("dial_list retrieval failed" in r.message for r in caplog.records)
