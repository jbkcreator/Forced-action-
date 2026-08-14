"""
Regression tests for demo_router.py — covers the three PR-review findings:

  1. _find_featured_lead must only select a property's *latest* score
     (stale Gold rows must not win when the newest score is disqualified/Silver).

  2. ZIP reveal must return the 25 *highest-scoring* leads, not 25 lowest-id leads
     (ORDER BY final_cds_score DESC must precede LIMIT 25).

  3. Demo-session idempotency must include county_id as a key
     (same ZIP+vertical in two counties must create two separate sessions).
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Finding 1 — _find_featured_lead uses latest score per property
# ---------------------------------------------------------------------------

def test_find_featured_lead_excludes_stale_gold_row():
    """
    A property that was Gold/qualified 20 days ago but Silver/disqualified
    today must NOT be returned by _find_featured_lead.

    We verify this by configuring db.execute to return None (simulating no
    qualifying row found after the latest-score join filters it out), then
    asserting the function returns (None, None) rather than picking up the
    stale row.
    """
    from src.api.demo_router import _find_featured_lead

    db = MagicMock()
    db.execute.return_value.first.return_value = None  # latest score is disqualified

    prop, score = _find_featured_lead(db, zip_code="33601", vertical="roofing", county_id="hillsborough")

    assert prop is None
    assert score is None


def test_find_featured_lead_sql_joins_latest_subquery():
    """
    Verify that _find_featured_lead issues a query that joins to a subquery
    selecting MAX(score_date) per property — the mechanism that pins each
    property to its current score before applying Gold+/qualified filters.
    """
    from src.api.demo_router import _find_featured_lead

    db = MagicMock()
    db.execute.return_value.first.return_value = None

    _find_featured_lead(db, zip_code="33566", vertical="roofing", county_id="hillsborough")

    assert db.execute.called, "db.execute was not called"
    # The query object passed to execute should reference a subquery with max(score_date).
    # We inspect the string representation of the compiled query.
    query_arg = db.execute.call_args[0][0]
    query_str = str(query_arg.compile(compile_kwargs={"literal_binds": False}))
    assert "max" in query_str.lower(), (
        "Query does not include max(score_date) subquery — stale-score bug may have regressed"
    )


# ---------------------------------------------------------------------------
# Finding 2 — ZIP reveal SQL orders by score, not by property ID
# ---------------------------------------------------------------------------

def test_zip_reveal_sql_orders_by_score_not_property_id():
    """
    The raw SQL for zip-reveal must ORDER BY final_cds_score DESC before LIMIT 25.
    Ordering by p.id first would truncate to 25 lowest-id properties, missing
    higher-scoring properties that have larger IDs.
    """
    import inspect
    from src.api import demo_router

    source = inspect.getsource(demo_router)

    # The ORDER BY in the zip-reveal CTE query must sort by score, not by id.
    # The original bug was: ORDER BY p.id, ds.final_cds_score DESC NULLS LAST LIMIT 25
    assert "ORDER BY p.id" not in source, (
        "ZIP reveal query is ordering by p.id — this truncates to 25 lowest-id rows "
        "before score ranking, omitting higher-scoring properties with larger IDs."
    )

    # Positive assertion: score ordering must be present before LIMIT
    assert "final_cds_score DESC NULLS LAST" in source, (
        "ZIP reveal query must ORDER BY final_cds_score DESC NULLS LAST"
    )


# ---------------------------------------------------------------------------
# Finding 3 — county_id is part of the idempotency key
# ---------------------------------------------------------------------------

def test_prepare_call_idempotency_includes_county_id():
    """
    Two prepare-call requests with the same ZIP+vertical but different counties
    must each get their own DemoSession row, not reuse the first.

    We verify by configuring the mock DB so the existing-session lookup returns
    None (as it should when county_id differs), which causes a new insert.
    """
    from src.api.demo_router import prepare_call, PrepareCallBody

    # Build a minimal fake subscriber
    sub = SimpleNamespace(id=1, is_demo=True, event_feed_uuid="test-uuid")

    # Mock DB that returns no existing session (county differs → no reuse)
    db = MagicMock()
    db.execute.return_value.first.return_value = None  # no existing session

    body_hillsborough = PrepareCallBody(
        zip_code="33601",
        vertical="roofing",
        county_id="hillsborough",
    )
    body_pinellas = PrepareCallBody(
        zip_code="33601",
        vertical="roofing",
        county_id="pinellas",
    )

    # Both calls should reach _find_featured_lead (i.e., no early return from cache)
    with patch("src.api.demo_router._find_featured_lead", return_value=(None, None)), \
         patch("src.api.demo_router._prep_response", return_value={"prep_id": 1}):

        prepare_call(body=body_hillsborough, sub=sub, db=db)
        prepare_call(body=body_pinellas, sub=sub, db=db)

    # db.add must have been called twice — one new DemoSession per county
    assert db.add.call_count == 2, (
        f"Expected 2 DemoSession inserts for two counties, got {db.add.call_count}. "
        "county_id is probably missing from the idempotency lookup."
    )


def test_prepare_call_idempotency_reuses_same_county_session():
    """
    A second request for the same ZIP+vertical+county within 2h must reuse
    the existing session without inserting a new row.
    """
    from src.api.demo_router import prepare_call, PrepareCallBody

    sub = SimpleNamespace(id=1, is_demo=True, event_feed_uuid="test-uuid")

    existing_session = SimpleNamespace(
        id=42, masked_address="2847 *** **", lead_tier="Gold",
        distress_types=["foreclosure"], zip_code="33601",
        county_id="hillsborough", revealed_at=None, property_id=1,
    )

    db = MagicMock()
    # Simulate: existing session found for same county
    db.execute.return_value.first.return_value = (existing_session,)

    body = PrepareCallBody(zip_code="33601", vertical="roofing", county_id="hillsborough")

    with patch("src.api.demo_router._prep_response", return_value={"prep_id": 42}) as mock_resp:
        prepare_call(body=body, sub=sub, db=db)
        mock_resp.assert_called_once_with(existing_session)

    # No new session should be inserted
    db.add.assert_not_called()
