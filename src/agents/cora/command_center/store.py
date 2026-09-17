"""
Command Center DB persistence.

All writes use raw SQL via session.execute(text(...)) per project convention.
Every function accepts an explicit db session — no implicit session creation.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def create_session(
    db: Session,
    session_id: str,
    slack_user_id: Optional[str] = None,
    slack_channel: Optional[str] = None,
    slack_thread_ts: Optional[str] = None,
) -> None:
    db.execute(
        text("""
            INSERT INTO cc_sessions (session_id, slack_user_id, slack_channel, slack_thread_ts)
            VALUES (:session_id, :slack_user_id, :slack_channel, :slack_thread_ts)
            ON CONFLICT (session_id) DO NOTHING
        """),
        {
            "session_id": session_id,
            "slack_user_id": slack_user_id,
            "slack_channel": slack_channel,
            "slack_thread_ts": slack_thread_ts,
        },
    )


def touch_session(db: Session, session_id: str) -> None:
    db.execute(
        text("UPDATE cc_sessions SET last_active_at = now() WHERE session_id = :sid"),
        {"sid": session_id},
    )


def close_session(db: Session, session_id: str, status: str = "completed") -> None:
    db.execute(
        text("UPDATE cc_sessions SET status = :status WHERE session_id = :sid"),
        {"sid": session_id, "status": status},
    )


def append_message(
    db: Session,
    session_id: str,
    turn_number: int,
    role: str,
    content: Any,
) -> None:
    db.execute(
        text("""
            INSERT INTO cc_messages (session_id, turn_number, role, content)
            VALUES (:session_id, :turn_number, :role, CAST(:content AS JSONB))
        """),
        {
            "session_id": session_id,
            "turn_number": turn_number,
            "role": role,
            "content": json.dumps(content, default=str),
        },
    )


def load_session_messages(
    db: Session,
    session_id: str,
    limit: int = 40,
) -> List[Dict[str, Any]]:
    """
    Load the last `limit` message turns for a session, oldest first.
    Returns a list of {"role": str, "content": any} dicts ready to pass
    directly to the Anthropic messages API.
    """
    rows = db.execute(
        text("""
            SELECT role, content
            FROM cc_messages
            WHERE session_id = :sid
            ORDER BY id DESC
            LIMIT :limit
        """),
        {"sid": session_id, "limit": limit},
    ).mappings().all()
    # Reverse to get chronological order (oldest first)
    return [{"role": r["role"], "content": r["content"]} for r in reversed(rows)]


def append_tool_call(
    db: Session,
    session_id: str,
    turn_number: int,
    tool_call_id: Optional[str],
    tool_name: str,
    tool_input: Dict[str, Any],
    tool_output: Optional[Any] = None,
    duration_ms: Optional[int] = None,
    error: Optional[str] = None,
) -> None:
    db.execute(
        text("""
            INSERT INTO cc_tool_calls
                (session_id, turn_number, tool_call_id, tool_name,
                 tool_input, tool_output, duration_ms, error)
            VALUES
                (:session_id, :turn_number, :tool_call_id, :tool_name,
                 CAST(:tool_input AS JSONB), CAST(:tool_output AS JSONB), :duration_ms, :error)
        """),
        {
            "session_id": session_id,
            "turn_number": turn_number,
            "tool_call_id": tool_call_id,
            "tool_name": tool_name,
            "tool_input": json.dumps(tool_input, default=str),
            "tool_output": json.dumps(tool_output, default=str) if tool_output is not None else None,
            "duration_ms": duration_ms,
            "error": error,
        },
    )


def append_llm_op(
    db: Session,
    session_id: str,
    turn_number: int,
    model: str,
    input_tokens: int,
    output_tokens: int,
    cost_usd: float,
    stop_reason: Optional[str] = None,
) -> None:
    db.execute(
        text("""
            INSERT INTO cc_llm_ops
                (session_id, turn_number, model, input_tokens, output_tokens, cost_usd, stop_reason)
            VALUES
                (:session_id, :turn_number, :model, :input_tokens, :output_tokens, :cost_usd, :stop_reason)
        """),
        {
            "session_id": session_id,
            "turn_number": turn_number,
            "model": model,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "cost_usd": cost_usd,
            "stop_reason": stop_reason,
        },
    )


def write_answer(
    db: Session,
    session_id: str,
    question_text: str,
    answer_text: str,
    status: str = "pending",
) -> int:
    row = db.execute(
        text("""
            INSERT INTO cc_answers (session_id, question_text, answer_text, status)
            VALUES (:session_id, :question_text, :answer_text, :status)
            RETURNING id
        """),
        {
            "session_id": session_id,
            "question_text": question_text,
            "answer_text": answer_text,
            "status": status,
        },
    ).mappings().first()
    return row["id"] if row else -1


def mark_answer_delivered(db: Session, answer_id: int) -> None:
    db.execute(
        text("""
            UPDATE cc_answers
            SET status = 'delivered', delivered_at = now()
            WHERE id = :answer_id
        """),
        {"answer_id": answer_id},
    )


def mark_answer_error(db: Session, answer_id: int) -> None:
    db.execute(
        text("UPDATE cc_answers SET status = 'error' WHERE id = :answer_id"),
        {"answer_id": answer_id},
    )


def session_exists(db: Session, session_id: str) -> bool:
    row = db.execute(
        text("SELECT 1 FROM cc_sessions WHERE session_id = :sid"),
        {"sid": session_id},
    ).first()
    return row is not None
