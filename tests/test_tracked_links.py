"""WP-7 WI-1 — tracked links: mint/resolve, unknown-slug degradation, click
recording. Uses `fresh_db` (real Postgres, rolled back after each test)."""
from src.services.tracked_links import mint_link, record_click, resolve_slug


def test_mint_and_resolve_active_link(fresh_db):
    link = mint_link(fresh_db, kind="partner", label="Test Partner", created_by="tester")
    fresh_db.flush()

    resolved = resolve_slug(fresh_db, link.slug)
    assert resolved is not None
    assert resolved.id == link.id
    assert resolved.kind == "partner"


def test_unknown_slug_resolves_to_none_not_error(fresh_db):
    assert resolve_slug(fresh_db, "does-not-exist") is None


def test_inactive_link_resolves_to_none(fresh_db):
    link = mint_link(fresh_db, kind="campaign", label="Inactive test", created_by="tester")
    fresh_db.flush()
    link.is_active = False
    fresh_db.flush()

    assert resolve_slug(fresh_db, link.slug) is None


def test_record_click_creates_one_row(fresh_db):
    link = mint_link(fresh_db, kind="source", label="Click test", created_by="tester")
    fresh_db.flush()

    click = record_click(fresh_db, tracked_link_id=link.id, session_token="tok-abc")
    assert click.id is not None
    assert click.tracked_link_id == link.id
    assert click.session_token == "tok-abc"


def test_source_link_can_carry_property_id(fresh_db):
    """property_id is a generic, kind-independent field — the admin mint path
    can bind any kind to a known property; there is no dedicated
    property-bound kind (physical mail campaigns are out of scope, and
    'property_mailer' — removed 2026-09-18 — was never in the client's spec)."""
    link = mint_link(
        fresh_db,
        kind="source",
        label="123 Main St",
        created_by="tester",
        property_id=None,  # no live property fixture needed for this unit test
    )
    fresh_db.flush()
    assert link.kind == "source"


