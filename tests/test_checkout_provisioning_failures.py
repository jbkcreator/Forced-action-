"""Tests for the durable checkout-provisioning-failure recovery record.

PR #170 review (round 2): a lost ZIP-claim race correctly rolls back the
checkout's own DB transaction, but Stripe has already captured payment and
created the subscription — with nothing durable recorded anywhere, ops could
only find that customer by grepping logs. _on_checkout_completed now writes
a CheckoutProvisioningFailure row via its own committed session
(get_db_context(), independent of the request's session) before raising, so
the record survives that rollback. These tests prove it does — against the
real DB, not a mock, since "survives the rollback" is exactly the property
mocking would hide.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine, text

from config.settings import get_settings


@pytest.fixture
def real_engine():
    settings = get_settings()
    if not settings.database_url:
        pytest.skip("DATABASE_URL not configured")
    engine = create_engine(str(settings.database_url), pool_pre_ping=True)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    yield engine
    engine.dispose()


def _cleanup(engine, cust_id: str) -> None:
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM checkout_provisioning_failures WHERE stripe_customer_id = :c"),
            {"c": cust_id},
        )


class TestDurableRecoveryRecordSurvivesRollback:
    def test_recovery_row_written_via_real_db_even_though_main_session_is_mocked(self, real_engine):
        """
        The outer `db` passed to _on_checkout_completed is a MagicMock — it
        never really commits anything, exactly modeling the real rollback
        (handle_webhook calls db.rollback() after this raises). The recovery
        write goes through get_db_context(), a genuinely separate real
        connection, so the row must exist in the real table regardless of
        what happens to the mocked `db`.
        """
        from src.services.stripe_webhooks import _on_checkout_completed
        from src.services.zip_territory import ZipTerritoryUnavailableError

        cust_id = "cus_test_provisioning_failure_e2e"

        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = None

        session_data = {
            "customer": cust_id,
            "subscription": "sub_test_provisioning_e2e",
            "payment_status": "paid",
            "customer_details": {"email": "lost-race@example.com", "name": "Lost Race"},
            "metadata": {
                "tier": "starter",
                "vertical": "roofing",
                "county_id": "hillsborough",
                "zip_codes": "33601,33602",
                "is_founding": "False",
            },
        }

        try:
            with patch("src.services.zip_territory.claim_zip_territory", side_effect=[True, False]):
                with pytest.raises(ZipTerritoryUnavailableError):
                    _on_checkout_completed(session_data, db)

            with real_engine.connect() as conn:
                row = conn.execute(
                    text(
                        "SELECT stripe_customer_id, stripe_subscription_id, email, "
                        "unclaimed_zips, status, reason "
                        "FROM checkout_provisioning_failures WHERE stripe_customer_id = :c"
                    ),
                    {"c": cust_id},
                ).first()

            assert row is not None, "recovery record was not durably written"
            assert row.stripe_subscription_id == "sub_test_provisioning_e2e"
            assert row.email == "lost-race@example.com"
            assert row.unclaimed_zips == ["33602"]
            assert row.status == "open"
            assert row.reason == "zip_territory_unavailable"
        finally:
            _cleanup(real_engine, cust_id)

    def test_no_recovery_row_when_all_zips_claimed(self, real_engine):
        from src.services.stripe_webhooks import _on_checkout_completed

        cust_id = "cus_test_no_failure_case"
        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = None
        db.execute.return_value.scalar.return_value = 1  # every ZIP claim wins

        session_data = {
            "customer": cust_id,
            "subscription": "sub_test_ok",
            "payment_status": "paid",
            "customer_details": {"email": "fine@example.com", "name": "Fine"},
            "metadata": {
                "tier": "starter", "vertical": "roofing", "county_id": "hillsborough",
                "zip_codes": "33601", "is_founding": "False",
            },
        }

        try:
            _on_checkout_completed(session_data, db)
            with real_engine.connect() as conn:
                row = conn.execute(
                    text("SELECT 1 FROM checkout_provisioning_failures WHERE stripe_customer_id = :c"),
                    {"c": cust_id},
                ).first()
            assert row is None
        finally:
            _cleanup(real_engine, cust_id)


class TestAdminCheckoutProvisioningFailuresEndpoints:
    def _seed_row(self, engine, cust_id: str) -> int:
        with engine.begin() as conn:
            return conn.execute(
                text(
                    "INSERT INTO checkout_provisioning_failures "
                    "(stripe_customer_id, stripe_subscription_id, email, tier, vertical, "
                    " county_id, requested_zips, unclaimed_zips) "
                    "VALUES (:c, 'sub_x', 'a@example.com', 'starter', 'roofing', 'hillsborough', "
                    " '[\"33601\",\"33602\"]'::jsonb, '[\"33602\"]'::jsonb) RETURNING id"
                ),
                {"c": cust_id},
            ).scalar()

    def test_list_and_resolve_round_trip(self, real_engine):
        from fastapi.testclient import TestClient
        from src.api.main import app, get_current_admin

        app.dependency_overrides[get_current_admin] = lambda: {"sub": "test-admin"}
        cust_id = "cus_test_admin_endpoint"
        try:
            failure_id = self._seed_row(real_engine, cust_id)
            client = TestClient(app)

            resp = client.get("/api/admin/checkout-provisioning-failures", params={"status": "open"})
            assert resp.status_code == 200
            items = resp.json()["items"]
            assert any(i["id"] == failure_id for i in items)
            match = next(i for i in items if i["id"] == failure_id)
            assert match["status"] == "open"
            assert match["unclaimed_zips"] == ["33602"]

            resolve_resp = client.post(
                f"/api/admin/checkout-provisioning-failures/{failure_id}/resolve",
                params={"notes": "refunded via Stripe dashboard"},
            )
            assert resolve_resp.status_code == 200
            assert resolve_resp.json()["status"] == "resolved"

            with real_engine.connect() as conn:
                row = conn.execute(
                    text("SELECT status, resolved_by, notes FROM checkout_provisioning_failures WHERE id = :id"),
                    {"id": failure_id},
                ).first()
            assert row.status == "resolved"
            assert row.resolved_by == "test-admin"
            assert row.notes == "refunded via Stripe dashboard"

            # Second resolve is idempotent, not an error.
            resolve_again = client.post(f"/api/admin/checkout-provisioning-failures/{failure_id}/resolve")
            assert resolve_again.status_code == 200
            assert resolve_again.json()["status"] == "resolved"
        finally:
            app.dependency_overrides.pop(get_current_admin, None)
            _cleanup(real_engine, cust_id)

    def test_resolve_unknown_id_is_404(self):
        from fastapi.testclient import TestClient
        from src.api.main import app, get_current_admin

        app.dependency_overrides[get_current_admin] = lambda: {"sub": "test-admin"}
        try:
            client = TestClient(app)
            resp = client.post("/api/admin/checkout-provisioning-failures/999999999/resolve")
            assert resp.status_code == 404
        finally:
            app.dependency_overrides.pop(get_current_admin, None)
