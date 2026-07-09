from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient


class _FakeDb:
    pass


@pytest.fixture
def client():
    from src.api.main import app
    from src.api.deps import get_db
    app.dependency_overrides[get_db] = lambda: _FakeDb()
    yield TestClient(app, raise_server_exceptions=False)
    app.dependency_overrides.pop(get_db, None)


def test_valid_token_suppresses_and_returns_200(client):
    with patch("src.api.email_unsubscribe_router.verify_unsubscribe_token", return_value="contact@example.com"), \
         patch("src.api.email_unsubscribe_router.suppress_contact") as mock_suppress:
        resp = client.get("/api/email/unsubscribe", params={"token": "valid"})

    assert resp.status_code == 200
    mock_suppress.assert_called_once()
    _, kwargs = mock_suppress.call_args
    assert kwargs["email"] == "contact@example.com"
    assert kwargs["source"] == "unsubscribe_link"


def test_invalid_token_does_not_suppress(client):
    with patch("src.api.email_unsubscribe_router.verify_unsubscribe_token", return_value=None), \
         patch("src.api.email_unsubscribe_router.suppress_contact") as mock_suppress:
        resp = client.get("/api/email/unsubscribe", params={"token": "garbage"})

    assert resp.status_code == 400
    mock_suppress.assert_not_called()
