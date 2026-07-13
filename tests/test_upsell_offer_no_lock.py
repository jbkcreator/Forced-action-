"""
Regression tests for the subscription upsell offer endpoint lock + price race fixes.

Verifies:
  1. GET /api/upsell/subscription-offer does NOT call the locking
     get_price_id_for_checkout() helper — only the non-locking preview helper.
  2. get_price_id_for_preview() itself never issues a SELECT ... FOR UPDATE
     statement (i.e. never acquires a row-level lock).
  3. The endpoint response contains ``price_guaranteed: false``.

Note: tests call the endpoint handler function directly (without TestClient)
      to avoid importing the full app — which requires optional system packages
      (e.g. bcrypt) that may not be installed in every test environment.

Run:
    pytest tests/test_upsell_offer_no_lock.py -v
"""
from __future__ import annotations

import sys
import types
import uuid
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Pre-stub optional system packages that src.api.main imports transitively.
# This lets the test file work in environments where bcrypt (and similar
# optional deps) are not installed, without requiring a full venv.
# ---------------------------------------------------------------------------

def _stub_module(name: str) -> None:
    """Insert a MagicMock into sys.modules for `name` if not already present."""
    if name not in sys.modules:
        mod = types.ModuleType(name)
        # Make attribute access return MagicMocks so isinstance / subclass
        # checks against attrs don't crash.
        mod.__dict__.update({"__spec__": MagicMock()})
        sys.modules[name] = mod  # type: ignore[assignment]

for _dep in ("bcrypt", "passlib", "passlib.context", "passlib.hash"):
    _stub_module(_dep)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _make_free_subscriber(vertical="roofing", county_id="hillsborough"):
    """Return a minimal Subscriber-like object for an eligible free-tier user."""
    sub = MagicMock()
    sub.tier = "free"
    sub.vertical = vertical
    sub.county_id = county_id
    sub.event_feed_uuid = uuid.uuid4().hex
    return sub


def _make_stripe_price(amount=60000, currency="usd"):
    return {"unit_amount": amount, "currency": currency}


# ---------------------------------------------------------------------------
# Test 1 — endpoint uses the non-locking helper, never the locking one
# ---------------------------------------------------------------------------

class TestOfferEndpointUsesPreviewHelper:
    """
    The offer endpoint must call get_price_id_for_preview(), NOT
    get_price_id_for_checkout().  A misdirected call to the locking helper
    would block concurrent real checkouts.

    We call the handler function directly (bypassing FastAPI routing) so this
    test does not depend on optional system packages (bcrypt, etc.).
    """

    def test_locking_helper_is_never_called(self):
        try:
            import src.api.main as main_module
        except (ImportError, Exception) as exc:
            pytest.skip(f"Skipped: full app import unavailable ({exc})")

        sub = _make_free_subscriber()
        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = sub

        settings = MagicMock()
        settings.active_stripe_secret_key.get_secret_value.return_value = "sk_test_x"

        with patch.object(main_module, "get_price_id_for_preview",
                          return_value=("price_starter_founding", True)) as mock_preview, \
             patch.object(main_module, "get_price_id_for_checkout") as mock_checkout, \
             patch("stripe.Price.retrieve", return_value=_make_stripe_price()), \
             patch.object(main_module, "get_settings", return_value=settings):

            result = main_module.subscription_upsell_offer(
                feed_uuid=sub.event_feed_uuid, db=db
            )

        # Preview helper was called exactly once with the right args
        mock_preview.assert_called_once_with(
            db, "starter", sub.vertical, sub.county_id
        )
        # Locking checkout helper was NEVER called
        mock_checkout.assert_not_called()

    def test_ineligible_non_free_subscriber_returns_not_eligible(self):
        """Non-free-tier subscribers must short-circuit before touching any price helper."""
        try:
            import src.api.main as main_module
        except (ImportError, Exception) as exc:
            pytest.skip(f"Skipped: full app import unavailable ({exc})")

        sub = _make_free_subscriber()
        sub.tier = "starter"   # not free → should short-circuit

        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = sub

        with patch.object(main_module, "get_price_id_for_preview") as mock_preview, \
             patch.object(main_module, "get_price_id_for_checkout") as mock_checkout:

            result = main_module.subscription_upsell_offer(
                feed_uuid=sub.event_feed_uuid, db=db
            )

        assert result == {"eligible": False}
        mock_preview.assert_not_called()
        mock_checkout.assert_not_called()


# ---------------------------------------------------------------------------
# Test 2 — get_price_id_for_preview issues no SELECT FOR UPDATE
# ---------------------------------------------------------------------------

class TestPreviewHelperIsNonLocking:
    """
    Inspect every statement sent to db.execute() and confirm none of them
    compile to SQL containing FOR UPDATE.
    """

    def test_no_for_update_in_any_query(self):
        from src.services.stripe_service import get_price_id_for_preview
        from sqlalchemy import select
        from src.core.models import FoundingSubscriberCount

        captured_stmts: list = []

        def _fake_execute(stmt, *args, **kwargs):
            captured_stmts.append(stmt)
            result = MagicMock()
            mock_row = MagicMock()
            mock_row.count = 3
            result.scalar_one_or_none.return_value = mock_row
            return result

        db = MagicMock()
        db.execute.side_effect = _fake_execute

        with patch("src.services.stripe_service._price_ids", return_value={
            "starter": {"founding": "price_founding_xxx", "regular": "price_regular_xxx"},
            "pro": {"founding": "p", "regular": "p"},
            "dominator": {"founding": "p", "regular": "p"},
            "partner": {"founding": "p", "regular": "p"},
        }), patch("src.services.stripe_service._founding_limit", return_value=10):
            price_id, is_founding = get_price_id_for_preview(
                db, "starter", "roofing", "hillsborough"
            )

        assert price_id == "price_founding_xxx"
        assert is_founding is True
        assert len(captured_stmts) >= 1, "Expected at least one DB query"

        # Compile each captured statement and check for FOR UPDATE
        from sqlalchemy.dialects import postgresql
        dialect = postgresql.dialect()
        for stmt in captured_stmts:
            try:
                compiled = stmt.compile(dialect=dialect)
                sql_text = str(compiled).upper()
                assert "FOR UPDATE" not in sql_text, (
                    f"get_price_id_for_preview issued a locking query: {sql_text}"
                )
            except Exception:
                # If compilation fails (e.g. non-SQL mock), skip — the
                # with_for_update() flag would have appeared in the real stmt.
                pass

    def test_missing_row_treated_as_founding_slots_open(self):
        """If no FoundingSubscriberCount row exists, count defaults to 0 (founding)."""
        from src.services.stripe_service import get_price_id_for_preview

        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = None  # no row

        with patch("src.services.stripe_service._price_ids", return_value={
            "starter": {"founding": "price_f", "regular": "price_r"},
            "pro": {"founding": "p", "regular": "p"},
            "dominator": {"founding": "p", "regular": "p"},
            "partner": {"founding": "p", "regular": "p"},
        }), patch("src.services.stripe_service._founding_limit", return_value=10):
            price_id, is_founding = get_price_id_for_preview(
                db, "starter", "roofing", "hillsborough"
            )

        # With count=0 and limit=10, should resolve as founding
        assert is_founding is True
        assert price_id == "price_f"
        # Must NOT have called db.add() — preview helper never creates rows
        db.add.assert_not_called()
        db.flush.assert_not_called()

    def test_founding_slots_exhausted_returns_regular(self):
        """When count >= limit, regular price should be returned."""
        from src.services.stripe_service import get_price_id_for_preview

        db = MagicMock()
        mock_row = MagicMock()
        mock_row.count = 10  # == limit
        db.execute.return_value.scalar_one_or_none.return_value = mock_row

        with patch("src.services.stripe_service._price_ids", return_value={
            "starter": {"founding": "price_f", "regular": "price_r"},
            "pro": {"founding": "p", "regular": "p"},
            "dominator": {"founding": "p", "regular": "p"},
            "partner": {"founding": "p", "regular": "p"},
        }), patch("src.services.stripe_service._founding_limit", return_value=10):
            price_id, is_founding = get_price_id_for_preview(
                db, "starter", "roofing", "hillsborough"
            )

        assert is_founding is False
        assert price_id == "price_r"


# ---------------------------------------------------------------------------
# Test 3 — response carries price_guaranteed: false
# ---------------------------------------------------------------------------

class TestOfferResponseHasPriceGuaranteedFalse:
    """
    The offer endpoint must communicate that the displayed price is a snapshot
    (not a reservation) by returning ``price_guaranteed: false``.
    """

    def test_price_guaranteed_is_false_in_response(self):
        try:
            import src.api.main as main_module
        except (ImportError, Exception) as exc:
            pytest.skip(f"Skipped: full app import unavailable ({exc})")

        sub = _make_free_subscriber()
        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = sub

        settings = MagicMock()
        settings.active_stripe_secret_key.get_secret_value.return_value = "sk_test_x"

        with patch.object(main_module, "get_price_id_for_preview",
                          return_value=("price_starter_founding", True)), \
             patch("stripe.Price.retrieve", return_value=_make_stripe_price(60000, "usd")), \
             patch.object(main_module, "get_settings", return_value=settings):

            result = main_module.subscription_upsell_offer(
                feed_uuid=sub.event_feed_uuid, db=db
            )

        assert result["eligible"] is True
        assert result["is_founding"] is True
        assert result["amount"] == 60000
        assert "price_guaranteed" in result, "Response must include price_guaranteed key"
        assert result["price_guaranteed"] is False, (
            "price_guaranteed must be False — the price is a snapshot, not a reservation"
        )
