"""
Scenario tests for the minimal Markdown-grounded Concierge Chat.

Run with:
    pytest -m scenario_chat
"""

import uuid
from datetime import datetime, timezone
from unittest.mock import patch

import pytest
import sqlalchemy as sa
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.services import chat_cache, chat_knowledge
from src.services.concierge_chat import (
    _DAILY_CAP_USD,
    _parse_reply,
    handle_user_turn,
)


def test_parse_reply_extracts_followups():
    raw = "Florida and Texas.\n---FOLLOWUPS---\nWhich plan?\n- Coverage in my ZIP?"
    reply, followups = _parse_reply(raw)
    assert reply == "Florida and Texas."
    assert followups == ["Which plan?", "Coverage in my ZIP?"]


def test_parse_reply_missing_separator():
    reply, followups = _parse_reply("Just an answer.")
    assert reply == "Just an answer."
    assert followups == []


def test_parse_reply_caps_at_two():
    raw = "ok\n---FOLLOWUPS---\nq1\nq2\nq3\nq4"
    _, followups = _parse_reply(raw)
    assert followups == ["q1", "q2"]

pytestmark = pytest.mark.scenario_chat


_SCHEMA = """
CREATE TABLE IF NOT EXISTS chat_sessions (
    id TEXT PRIMARY KEY,
    subscriber_id INTEGER,
    anonymous_id TEXT,
    source TEXT NOT NULL DEFAULT 'landing',
    created_at TIMESTAMP NOT NULL,
    last_seen_at TIMESTAMP NOT NULL,
    linked_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS chat_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL REFERENCES chat_sessions(id),
    role TEXT NOT NULL,
    content TEXT,
    intent_label TEXT,
    intent_confidence REAL,
    tool_calls_json TEXT,
    payment_trigger_json TEXT,
    claude_model TEXT,
    tokens_in INTEGER,
    tokens_out INTEGER,
    latency_ms INTEGER,
    error TEXT,
    created_at TIMESTAMP NOT NULL
);
"""


@pytest.fixture(scope="module")
def engine():
    eng = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    with eng.connect() as conn:
        for stmt in _SCHEMA.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                conn.execute(sa.text(stmt))
        conn.commit()
    return eng


@pytest.fixture
def db(engine):
    connection = engine.connect()
    transaction = connection.begin()
    session = Session(bind=connection)
    yield session
    session.close()
    transaction.rollback()
    connection.close()


@pytest.fixture(autouse=True)
def _stub_knowledge():
    """Stub the knowledge loader and clear caches between tests."""
    chat_knowledge.reset_cache()
    chat_cache.clear()
    with patch(
        "src.services.chat_knowledge.get_knowledge",
        return_value="Coverage info: Florida and Texas only.",
    ):
        yield
    chat_knowledge.reset_cache()
    chat_cache.clear()


def _make_session(db: Session) -> str:
    sid = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    db.execute(sa.text(
        "INSERT INTO chat_sessions (id, anonymous_id, source, created_at, last_seen_at) "
        "VALUES (:id, :anon, 'landing', :now, :now)"
    ), {"id": sid, "anon": str(uuid.uuid4()), "now": now})
    db.flush()
    return sid


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_grounded_reply_persists_messages(db):
    """Happy path: user message + assistant reply both persisted, reply returned."""
    session_id = _make_session(db)

    fake = {
        "text": (
            "We operate in Florida and Texas.\n"
            "---FOLLOWUPS---\n"
            "What ZIP codes are available?\n"
            "How much does it cost?\n"
        ),
        "model": "haiku",
        "input_tokens": 800,
        "output_tokens": 30,
        "cost_usd": 0.001,
    }
    with patch(
        "src.services.concierge_chat.call_claude_with_usage",
        return_value=fake,
    ) as mock_call:
        turn = handle_user_turn(session_id, "where do you cover?", db)

    assert turn.content == "We operate in Florida and Texas."
    assert turn.followups == [
        "What ZIP codes are available?",
        "How much does it cost?",
    ]
    assert mock_call.called
    # Knowledge text should appear inside the system prompt
    _, kwargs = mock_call.call_args
    assert "Florida" in kwargs["system"]
    assert kwargs["cache_system"] is True

    rows = db.execute(sa.text(
        "SELECT role, content FROM chat_messages WHERE session_id = :sid ORDER BY id"
    ), {"sid": session_id}).all()
    assert [(r.role, r.content) for r in rows] == [
        ("user", "where do you cover?"),
        ("assistant", "We operate in Florida and Texas."),
    ]


def test_missing_knowledge_returns_unavailable(db):
    """If the knowledge file is missing, the chat returns the 'unavailable' message."""
    session_id = _make_session(db)

    with (
        patch("src.services.chat_knowledge.get_knowledge", return_value=None),
        patch("src.services.concierge_chat.call_claude_with_usage") as mock_call,
    ):
        turn = handle_user_turn(session_id, "hello", db)

    assert "unavailable" in turn.content.lower()
    assert not mock_call.called  # never calls Claude when KB is missing


def test_cost_cap_blocks_turn(db):
    """When session exceeds daily cost cap, returns cost-exceeded message and skips Claude."""
    session_id = _make_session(db)

    # Seed enough Haiku spend to exceed the $0.50 cap
    # cost: (300000 * 0.80 + 100000 * 4.00) / 1e6 = 0.24 + 0.40 = $0.64
    now = datetime.now(timezone.utc).isoformat()
    db.execute(sa.text(
        "INSERT INTO chat_messages (session_id, role, content, claude_model, "
        "tokens_in, tokens_out, created_at) "
        "VALUES (:sid, 'assistant', 'prior', 'haiku', 300000, 100000, :now)"
    ), {"sid": session_id, "now": now})
    db.flush()

    with patch(
        "src.services.concierge_chat.call_claude_with_usage"
    ) as mock_call:
        turn = handle_user_turn(session_id, "another question", db)

    assert "limit" in turn.content.lower() or "support" in turn.content.lower()
    assert not mock_call.called
    assert _DAILY_CAP_USD == 0.50


def test_faq_shortcut_skips_claude(db):
    """A question matching the FAQ regex map returns a canned reply with no LLM call."""
    session_id = _make_session(db)

    with patch(
        "src.services.concierge_chat.call_claude_with_usage"
    ) as mock_call:
        turn = handle_user_turn(session_id, "how much does it cost?", db)

    assert not mock_call.called
    assert "pricing" in turn.content.lower() or "support@forcedaction.ai" in turn.content


def test_cache_hit_skips_claude(db):
    """Identical second question hits the in-memory cache and skips Claude."""
    session_id = _make_session(db)

    fake = {
        "text": "We cover Florida and Texas.\n---FOLLOWUPS---\nWhich plan should I pick?\nIs there a founding rate?",
        "model": "haiku",
        "input_tokens": 600,
        "output_tokens": 20,
        "cost_usd": 0.0006,
    }
    with patch(
        "src.services.concierge_chat.call_claude_with_usage",
        return_value=fake,
    ) as mock_call:
        first = handle_user_turn(session_id, "where do you cover?", db)
        second = handle_user_turn(session_id, "where do you cover?", db)

    assert first.content == "We cover Florida and Texas."
    assert second.content == "We cover Florida and Texas."
    assert mock_call.call_count == 1  # second turn served from cache
