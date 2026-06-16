"""
Regression test for the fa073/fa077 ORM drift (ADR 0015).

The contact-freshness columns existed in the DB since fa073 but were never
mapped on Owner/EnrichedContact, so `apply_contact_freshness()` assignments
were silent no-ops (unmapped attributes don't persist). These tests pin the
mapping by round-tripping every drifted column through a real session flush.
"""

from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import sessionmaker


# SQLite shim — same as test_auto_mode_entitlement / anomaly-pager tests
@compiles(JSONB, "sqlite")
def _jsonb_to_json(type_, compiler, **kw):  # noqa: D401
    return "JSON"


@compiles(ARRAY, "sqlite")
def _array_to_text(type_, compiler, **kw):  # noqa: D401
    return "TEXT"


from src.core.models import Base, EnrichedContact, Owner, Property  # noqa: E402


@pytest.fixture()
def session():
    engine = create_engine(
        "sqlite:///:memory:", connect_args={"check_same_thread": False}
    )
    Base.metadata.create_all(
        engine,
        tables=[Property.__table__, Owner.__table__, EnrichedContact.__table__],
    )
    factory = sessionmaker(bind=engine)
    s = factory()
    yield s
    s.close()


@pytest.fixture()
def prop(session):
    p = Property(parcel_id="TEST-PARCEL-001", address="123 Oak St", county_id="hillsborough")
    session.add(p)
    session.flush()
    return p


NOW = datetime(2026, 6, 11, 12, 0, 0, tzinfo=timezone.utc)


def test_owner_contact_freshness_columns_persist(session, prop):
    owner = Owner(
        property_id=prop.id,
        owner_name="ANA R GONZALEZ",
        contact_info_confidence="high",
        contact_info_confidence_score=0.875,
        contact_last_verified_at=NOW,
        contact_next_refresh_at=NOW,
        contact_refresh_status="fresh",
        contact_refresh_reason="age_12_days",
        skip_trace_stale=True,
    )
    session.add(owner)
    session.flush()
    session.expire(owner)

    reloaded = session.get(Owner, owner.id)
    assert reloaded.contact_info_confidence == "high"
    assert float(reloaded.contact_info_confidence_score) == pytest.approx(0.875)
    assert reloaded.contact_last_verified_at is not None
    assert reloaded.contact_next_refresh_at is not None
    assert reloaded.contact_refresh_status == "fresh"
    assert reloaded.contact_refresh_reason == "age_12_days"
    assert reloaded.skip_trace_stale is True


def test_owner_freshness_update_persists(session, prop):
    """The exact write pattern apply_contact_freshness() uses: mutate + flush."""
    owner = Owner(property_id=prop.id, owner_name="THERESA FROMENT")
    session.add(owner)
    session.flush()

    owner.contact_info_confidence = "stale"
    owner.contact_refresh_status = "due"
    session.flush()
    session.expire(owner)

    reloaded = session.get(Owner, owner.id)
    assert reloaded.contact_info_confidence == "stale"
    assert reloaded.contact_refresh_status == "due"


def test_owner_skip_trace_stale_defaults_false(session, prop):
    owner = Owner(property_id=prop.id, owner_name="DAWN GARTNER")
    session.add(owner)
    session.flush()
    session.expire(owner)
    assert session.get(Owner, owner.id).skip_trace_stale is False


def test_enriched_contact_verification_columns_persist(session, prop):
    original = EnrichedContact(
        property_id=prop.id,
        county_id="hillsborough",
        source="tracerfy",
        match_success=True,
        mobile_phone="+18135551234",
        verification_status="valid",
    )
    session.add(original)
    session.flush()

    replacement = EnrichedContact(
        property_id=prop.id,
        county_id="hillsborough",
        source="tracerfy",
        match_success=True,
        mobile_phone="+18135559999",
    )
    session.add(replacement)
    session.flush()

    original.verification_status = "invalid"
    original.superseded_at = NOW
    original.superseded_by_contact_id = replacement.id
    session.flush()
    session.expire(original)

    reloaded = session.get(EnrichedContact, original.id)
    assert reloaded.verification_status == "invalid"
    assert reloaded.superseded_at is not None
    assert reloaded.superseded_by_contact_id == replacement.id
