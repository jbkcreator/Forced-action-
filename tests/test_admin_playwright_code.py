"""
Smoke tests for the 5 admin playwright-code routes added in the ORI/scrape_mode
refactor.

Each route is a thin HTTP wrapper around src/utils/action_sequence.py helpers
which are already covered by unit tests. We verify:
  - Auth required (no token → 401)
  - Source ownership enforced (wrong county_id → 404)
  - AST validation runs server-side (unsafe code → 400)
  - The helpers get called with the right args

The action_sequence helpers themselves are patched so these tests don't depend
on the LLM, the DB session_scope, or on the cf_session_manager.
"""

from unittest.mock import patch, MagicMock

import pytest
from fastapi.testclient import TestClient


VALID_CODE = """
async def run_scrape(page, download_dir, start_date, end_date, url, county_id):
    return pd.DataFrame()
""".strip()


@pytest.fixture
def client():
    from src.api.main import app
    return TestClient(app)


@pytest.fixture
def admin_token(monkeypatch):
    from config.settings import settings
    from pydantic import SecretStr
    monkeypatch.setattr(settings, "admin_jwt_secret", SecretStr("test-jwt-secret"))
    monkeypatch.setattr(settings, "admin_password", SecretStr("test-admin-pass"))

    from src.api.admin_router import create_access_token
    return create_access_token({"sub": "admin"})


@pytest.fixture
def auth_headers(admin_token):
    return {"Authorization": f"Bearer {admin_token}"}


@pytest.fixture
def fake_source():
    """A CountySource stand-in returned by the patched DB query."""
    src = MagicMock()
    src.id = 9
    src.county_id = "pinellas"
    src.signal_type = "liens"
    src.url = "https://example.com/"
    src.description = "Pinellas liens"
    src.navigation_hint = "click button"
    src.scrape_mode = "playwright_then_ai"
    src.special_flags = {"cf_bypass_required": True}
    return src


@pytest.fixture
def patch_db_source(fake_source):
    """
    Patch the FastAPI dependency override so admin routes see fake_source
    instead of hitting Postgres.
    """
    from src.api.main import app
    from src.api.admin_router import get_db

    fake_session = MagicMock()
    fake_session.query.return_value.filter_by.return_value.first.return_value = fake_source

    def _override_db():
        yield fake_session

    app.dependency_overrides[get_db] = _override_db
    yield fake_session
    app.dependency_overrides.pop(get_db, None)


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

class TestAuth:

    def test_generate_requires_auth(self, client):
        r = client.post("/api/admin/counties/pinellas/sources/9/playwright-code/generate")
        assert r.status_code in (401, 403)

    def test_save_requires_auth(self, client):
        r = client.post(
            "/api/admin/counties/pinellas/sources/9/playwright-code",
            json={"code": VALID_CODE, "approved": False},
        )
        assert r.status_code in (401, 403)


# ---------------------------------------------------------------------------
# 404 when source not found in this county
# ---------------------------------------------------------------------------

class TestSourceNotFound:

    def test_validate_returns_404(self, client, auth_headers):
        from src.api.main import app
        from src.api.admin_router import get_db

        fake_session = MagicMock()
        fake_session.query.return_value.filter_by.return_value.first.return_value = None

        def _override_db():
            yield fake_session

        app.dependency_overrides[get_db] = _override_db
        try:
            r = client.post(
                "/api/admin/counties/nowhere/sources/999/playwright-code/validate",
                headers=auth_headers,
                json={"code": VALID_CODE},
            )
            assert r.status_code == 404
        finally:
            app.dependency_overrides.pop(get_db, None)


# ---------------------------------------------------------------------------
# /validate — AST safety
# ---------------------------------------------------------------------------

class TestValidate:

    def test_valid_code_returns_ok(self, client, auth_headers, patch_db_source):
        r = client.post(
            "/api/admin/counties/pinellas/sources/9/playwright-code/validate",
            headers=auth_headers,
            json={"code": VALID_CODE},
        )
        assert r.status_code == 200
        body = r.json()
        assert body["valid"] is True
        assert body["errors"] == []

    def test_import_rejected(self, client, auth_headers, patch_db_source):
        unsafe = "import os\n\n" + VALID_CODE
        r = client.post(
            "/api/admin/counties/pinellas/sources/9/playwright-code/validate",
            headers=auth_headers,
            json={"code": unsafe},
        )
        assert r.status_code == 200
        body = r.json()
        assert body["valid"] is False
        assert any("import" in e.lower() for e in body["errors"])

    def test_forbidden_builtin_rejected(self, client, auth_headers, patch_db_source):
        unsafe = """
async def run_scrape(page, download_dir, start_date, end_date, url, county_id):
    eval("1+1")
    return pd.DataFrame()
""".strip()
        r = client.post(
            "/api/admin/counties/pinellas/sources/9/playwright-code/validate",
            headers=auth_headers,
            json={"code": unsafe},
        )
        assert r.status_code == 200
        body = r.json()
        assert body["valid"] is False


# ---------------------------------------------------------------------------
# /generate — calls LLM helper
# ---------------------------------------------------------------------------

class TestGenerate:

    def test_generate_returns_code(self, client, auth_headers, patch_db_source):
        with patch(
            "src.utils.action_sequence.generate_playwright_code",
            return_value=VALID_CODE,
        ) as mock_gen:
            r = client.post(
                "/api/admin/counties/pinellas/sources/9/playwright-code/generate",
                headers=auth_headers,
            )
        assert r.status_code == 200
        body = r.json()
        assert body["code"] == VALID_CODE
        assert body["approved"] is False
        mock_gen.assert_called_once()

    def test_generate_502_on_llm_failure(self, client, auth_headers, patch_db_source):
        from src.utils.action_sequence import PlaywrightCodeError
        with patch(
            "src.utils.action_sequence.generate_playwright_code",
            side_effect=PlaywrightCodeError("LLM exploded"),
        ):
            r = client.post(
                "/api/admin/counties/pinellas/sources/9/playwright-code/generate",
                headers=auth_headers,
            )
        assert r.status_code == 502


# ---------------------------------------------------------------------------
# /save — validates server-side, then persists
# ---------------------------------------------------------------------------

class TestSave:

    def test_save_valid_code(self, client, auth_headers, patch_db_source):
        with patch("src.utils.action_sequence.persist_playwright_code") as mock_persist:
            r = client.post(
                "/api/admin/counties/pinellas/sources/9/playwright-code",
                headers=auth_headers,
                json={"code": VALID_CODE, "approved": True},
            )
        assert r.status_code == 201
        assert r.json()["is_approved"] is True
        mock_persist.assert_called_once()
        # is_approved should propagate to the helper
        kwargs = mock_persist.call_args.kwargs
        assert kwargs.get("is_approved") is True

    def test_save_invalid_code_rejected(self, client, auth_headers, patch_db_source):
        unsafe = "import os\n\n" + VALID_CODE
        with patch("src.utils.action_sequence.persist_playwright_code") as mock_persist:
            r = client.post(
                "/api/admin/counties/pinellas/sources/9/playwright-code",
                headers=auth_headers,
                json={"code": unsafe, "approved": True},
            )
        assert r.status_code == 400
        mock_persist.assert_not_called()


# ---------------------------------------------------------------------------
# /approve
# ---------------------------------------------------------------------------

class TestApprove:

    def test_approve_calls_helper(self, client, auth_headers, patch_db_source):
        with patch("src.utils.action_sequence.approve_playwright_code") as mock_approve:
            r = client.post(
                "/api/admin/counties/pinellas/sources/9/playwright-code/approve",
                headers=auth_headers,
            )
        assert r.status_code == 200
        assert r.json()["is_approved"] is True
        mock_approve.assert_called_once_with(
            "pinellas", 9, approved_by="admin",
        )


# ---------------------------------------------------------------------------
# DELETE /playwright-code
# ---------------------------------------------------------------------------

class TestClear:

    def test_clear_calls_helper(self, client, auth_headers, patch_db_source):
        with patch("src.utils.action_sequence.clear_playwright_code") as mock_clear:
            r = client.delete(
                "/api/admin/counties/pinellas/sources/9/playwright-code",
                headers=auth_headers,
            )
        assert r.status_code == 204
        mock_clear.assert_called_once_with("pinellas", 9)
