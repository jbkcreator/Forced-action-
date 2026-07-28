"""
Cora test fixtures — sandbox Redis + isolated file store, mirroring
tests/scenarios/conftest.py's sandbox pattern (fakeredis via
settings.redis_sandbox) but package-local: nothing here calls
src.agents.supervisor.dispatch_event or touches the old Lifecycle runtime.
"""
from __future__ import annotations

from typing import Iterator
from unittest.mock import MagicMock

import pytest

from config.settings import settings
from src.core import redis_client


@pytest.fixture(autouse=True, scope="session")
def _cora_redis_sandbox() -> Iterator[None]:
    prev = settings.redis_sandbox
    settings.redis_sandbox = True
    redis_client.reset_client_cache()
    yield
    settings.redis_sandbox = prev
    redis_client.reset_client_cache()


@pytest.fixture(autouse=True)
def _cora_flush_redis() -> Iterator[None]:
    """fakeredis contents are process-wide — flush between tests so state never leaks."""
    client = redis_client.get_redis()
    if client is not None:
        try:
            client.flushall()
        except Exception:
            pass
    yield


@pytest.fixture(autouse=True)
def _cora_isolated_store(tmp_path, monkeypatch) -> Iterator[None]:
    """Redirect store.py's JSON-Lines files to a per-test temp dir — never touches data/cora/."""
    from src.agents.cora import store

    data_dir = tmp_path / "cora_data"
    monkeypatch.setattr(store, "DATA_DIR", data_dir)
    monkeypatch.setattr(store, "_OPPORTUNITY_STATE_FILE", data_dir / "opportunity_state.jsonl")
    monkeypatch.setattr(store, "_REPLIES_FILE", data_dir / "replies.jsonl")
    monkeypatch.setattr(store, "_PRE_CALL_BRIEFS_FILE", data_dir / "pre_call_briefs.jsonl")
    yield


@pytest.fixture(autouse=True)
def _no_real_stripe_calls(monkeypatch) -> Iterator[None]:
    """
    offer_links.py's founder_tier path calls the REAL stripe SDK
    (stripe.checkout.Session.create) directly — this repo's Stripe
    integration is LIVE in the shared .env, not sandboxed. Default every
    test to a fake Stripe response so nothing here can hit the real API by
    accident; a test that explicitly wants to exercise the real call can
    override this fixture's targets directly.
    """
    fake_session = MagicMock()
    fake_session.url = "https://checkout.stripe.com/test-fake"
    monkeypatch.setattr("stripe.checkout.Session.create", MagicMock(return_value=fake_session))
    monkeypatch.setattr("src.services.stripe_service._init_stripe", lambda: True)
    yield


@pytest.fixture
def not_suppressed_db(fresh_db, monkeypatch):
    """
    Real Postgres session (rolled back after the test) — store.py's draft
    functions now run real SQL against outbound_drafts, so a MagicMock can
    no longer round-trip a real INSERT/SELECT. Suppression is redirected to
    a plain function stub rather than a real email_opt_outs row, since real
    prospect emails used across these tests never exist there anyway and
    this repo's DATABASE_URL points at the shared prod DB.
    """
    monkeypatch.setattr("src.services.email_suppression.is_email_suppressed", lambda db, email: False)
    return fresh_db


@pytest.fixture
def suppressed_db(fresh_db, monkeypatch):
    monkeypatch.setattr("src.services.email_suppression.is_email_suppressed", lambda db, email: True)
    return fresh_db


@pytest.fixture
def mock_claude(monkeypatch):
    """
    Patches call_claude_with_usage everywhere it's imported into Cora's
    subgraph modules. Returns a MagicMock the test configures per-call via
    `mock_claude.return_value = {...}` or `.side_effect = [...]`.
    """
    mock = MagicMock()
    for module_path in (
        "src.agents.cora.subgraphs.outreach.call_claude_with_usage",
        "src.agents.cora.subgraphs.reply.call_claude_with_usage",
        "src.agents.cora.subgraphs.pre_call.call_claude_with_usage",
    ):
        monkeypatch.setattr(module_path, mock)
    return mock


def compose_result(subject: str, body: str, tokens: int = 100, cost: float = 0.01) -> dict:
    """A call_claude_with_usage()-shaped return for the outreach/reply compose nodes."""
    return {
        "text": f"SUBJECT: {subject}\nBODY: {body}",
        "input_tokens": tokens,
        "output_tokens": tokens,
        "cost_usd": cost,
    }


def classify_result(intent: str, subtype: str | None = None, tokens: int = 50, cost: float = 0.005) -> dict:
    """A call_claude_with_usage()-shaped return for reply.py's tool-use classify node."""
    return {
        "tool_input": {"intent": intent, "subtype": subtype},
        "input_tokens": tokens,
        "output_tokens": tokens,
        "cost_usd": cost,
    }


def brief_result(suggested_opening: str, call_objective: str, tokens: int = 80, cost: float = 0.008) -> dict:
    """A call_claude_with_usage()-shaped return for pre_call.py's compose_brief node."""
    import json

    return {
        "text": json.dumps({"suggested_opening": suggested_opening, "call_objective": call_objective}),
        "input_tokens": tokens,
        "output_tokens": tokens,
        "cost_usd": cost,
    }
