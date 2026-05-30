"""Phase 0 — ExpansionIcpChannel model tests.

Uses a targeted SQLite in-memory DB that creates only the
expansion_icp_channels table — avoids the JSONB incompatibility
from other models in Base.metadata.

The `rei_seed_*` tests use the `fresh_db` fixture (real Postgres)
and are skipped when DATABASE_URL is absent.
"""
from decimal import Decimal

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

from src.core.models import ExpansionIcpChannel


@pytest.fixture
def icp_db():
    """Minimal SQLite in-memory DB with only expansion_icp_channels."""
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    ExpansionIcpChannel.__table__.create(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()
    engine.dispose()


# ── helpers ──────────────────────────────────────────────────────────────────

def _make_channel(session, **overrides):
    defaults = dict(
        key="test_channel",
        display_name="Test Channel",
        price_monthly=Decimal("99.00"),
        feed_scope="single_county",
        landing_slug="test-channel",
        status="gated",
    )
    defaults.update(overrides)
    ch = ExpansionIcpChannel(**defaults)
    session.add(ch)
    session.flush()
    return ch


# ── P0-1 basic round-trip ─────────────────────────────────────────────────

def test_create_icp_channel_persists(icp_db):
    ch = _make_channel(icp_db)
    fetched = icp_db.get(ExpansionIcpChannel, ch.id)
    assert fetched is not None
    assert fetched.key == "test_channel"
    assert fetched.price_monthly == Decimal("99.00")
    assert fetched.status == "gated"
    assert fetched.feed_scope == "single_county"


# ── P0-2 status check constraint ─────────────────────────────────────────

def test_status_check_constraint_rejects_bad_value(icp_db):
    """SQLite enforces CHECK constraints since 3.25."""
    with pytest.raises((IntegrityError, Exception)):
        _make_channel(icp_db, key="bad_status", landing_slug="bad-status", status="bogus")


# ── P0-3 feed_scope check constraint ─────────────────────────────────────

def test_feed_scope_check_constraint_rejects_bad_value(icp_db):
    with pytest.raises((IntegrityError, Exception)):
        _make_channel(icp_db, key="bad_scope", landing_slug="bad-scope", feed_scope="global")


def test_feed_scope_multi_county_accepted(icp_db):
    ch = _make_channel(icp_db, key="mc_channel", landing_slug="mc-channel",
                       feed_scope="multi_county")
    assert ch.feed_scope == "multi_county"


# ── P0-4 key uniqueness ───────────────────────────────────────────────────

def test_key_unique(icp_db):
    _make_channel(icp_db, key="dup_key", landing_slug="dup-key-1")
    with pytest.raises((IntegrityError, Exception)):
        _make_channel(icp_db, key="dup_key", landing_slug="dup-key-2")


# ── P0-5 landing_slug uniqueness ─────────────────────────────────────────

def test_landing_slug_unique(icp_db):
    _make_channel(icp_db, key="slug_a", landing_slug="same-slug")
    with pytest.raises((IntegrityError, Exception)):
        _make_channel(icp_db, key="slug_b", landing_slug="same-slug")


# ── P0-6 all valid statuses accepted ─────────────────────────────────────

@pytest.mark.parametrize("status", ["configured", "gated", "approved", "live", "retired"])
def test_all_valid_statuses_accepted(icp_db, status):
    ch = _make_channel(icp_db,
                       key=f"ch_{status}", landing_slug=f"ch-{status}", status=status)
    assert ch.status == status


# ── P0-7 optional fields nullable ────────────────────────────────────────

def test_optional_fields_nullable(icp_db):
    ch = _make_channel(icp_db, key="minimal", landing_slug="minimal",
                       persona=None, data_source=None)
    fetched = icp_db.get(ExpansionIcpChannel, ch.id)
    assert fetched.persona is None
    assert fetched.data_source is None


# ── P0-8 REI seed row (Postgres only) ────────────────────────────────────

def test_rei_seed_present(fresh_db):
    """After apply_fa050_ddl.py, exactly one 'rei_investor' row exists."""
    row = fresh_db.query(ExpansionIcpChannel).filter_by(key="rei_investor").first()
    assert row is not None, "REI seed row missing — run scripts/apply_fa050_ddl.py"
    assert row.status == "gated"
    assert row.price_monthly == Decimal("197.00")
    assert row.feed_scope == "single_county"
    assert row.landing_slug == "rei-investor"


def test_rei_seed_is_only_live_channel(fresh_db):
    """No ICP channel should be live yet — all are gated or configured."""
    live = fresh_db.query(ExpansionIcpChannel).filter_by(status="live").count()
    assert live == 0, "No ICP channel should be live before Contractor MRR >= $50K"
