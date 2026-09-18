"""
FA MAX — Command Center schema.

Five tables that back the Command Center chatbot's durable record:

  cc_sessions     One row per conversation session.  A session is one
                  Slack thread (or equivalent); multiple questions belong
                  to the same session.

  cc_messages     Full message-turn log per session.  Every role/content
                  pair (user question, assistant reply, tool_use blocks,
                  tool_result blocks) is stored here so the conversation
                  can be replayed or trimmed without re-running Claude.

  cc_tool_calls   One row per tool invocation inside the agentic loop.
                  Includes timing and the raw input/output for audit.

  cc_llm_ops      One row per Claude API call — model, token counts, cost.
                  Links back to the session and turn for cost attribution.

  cc_answers      One row per final answer surfaced to Josh.  Delivery
                  status tracks whether the answer reached Slack.

Run once:
    PYTHONPATH=. python migrations/apply_command_center.py

Idempotent — CREATE TABLE IF NOT EXISTS throughout.
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context


CREATE_SESSIONS = """
CREATE TABLE IF NOT EXISTS cc_sessions (
    session_id          TEXT            PRIMARY KEY,
    slack_user_id       VARCHAR(80),
    slack_channel       VARCHAR(80),
    slack_thread_ts     VARCHAR(80),
    status              VARCHAR(20)     NOT NULL DEFAULT 'active'
                            CHECK (status IN ('active', 'completed', 'error')),
    started_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),
    last_active_at      TIMESTAMPTZ     NOT NULL DEFAULT now()
)
"""

CREATE_MESSAGES = """
CREATE TABLE IF NOT EXISTS cc_messages (
    id              BIGSERIAL       PRIMARY KEY,
    session_id      TEXT            NOT NULL REFERENCES cc_sessions (session_id) ON DELETE CASCADE,
    turn_number     SMALLINT        NOT NULL,
    role            VARCHAR(20)     NOT NULL CHECK (role IN ('user', 'assistant')),
    content         JSONB           NOT NULL,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT now()
)
"""

CREATE_TOOL_CALLS = """
CREATE TABLE IF NOT EXISTS cc_tool_calls (
    id              BIGSERIAL       PRIMARY KEY,
    session_id      TEXT            NOT NULL REFERENCES cc_sessions (session_id) ON DELETE CASCADE,
    turn_number     SMALLINT        NOT NULL,
    tool_call_id    VARCHAR(80),
    tool_name       VARCHAR(80)     NOT NULL,
    tool_input      JSONB           NOT NULL DEFAULT '{}',
    tool_output     JSONB,
    duration_ms     INTEGER,
    error           TEXT,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT now()
)
"""

CREATE_LLM_OPS = """
CREATE TABLE IF NOT EXISTS cc_llm_ops (
    id              BIGSERIAL       PRIMARY KEY,
    session_id      TEXT            NOT NULL REFERENCES cc_sessions (session_id) ON DELETE CASCADE,
    turn_number     SMALLINT        NOT NULL,
    model           VARCHAR(40)     NOT NULL,
    input_tokens    INTEGER         NOT NULL DEFAULT 0,
    output_tokens   INTEGER         NOT NULL DEFAULT 0,
    cost_usd        NUMERIC(10, 6)  NOT NULL DEFAULT 0,
    stop_reason     VARCHAR(40),
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT now()
)
"""

CREATE_ANSWERS = """
CREATE TABLE IF NOT EXISTS cc_answers (
    id              BIGSERIAL       PRIMARY KEY,
    session_id      TEXT            NOT NULL REFERENCES cc_sessions (session_id) ON DELETE CASCADE,
    question_text   TEXT            NOT NULL,
    answer_text     TEXT            NOT NULL,
    status          VARCHAR(20)     NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'delivered', 'error')),
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT now(),
    delivered_at    TIMESTAMPTZ
)
"""

INDEX_STATEMENTS = [
    "CREATE INDEX IF NOT EXISTS ix_cc_messages_session ON cc_messages (session_id, turn_number)",
    "CREATE INDEX IF NOT EXISTS ix_cc_tool_calls_session ON cc_tool_calls (session_id, turn_number)",
    "CREATE INDEX IF NOT EXISTS ix_cc_llm_ops_session ON cc_llm_ops (session_id)",
    "CREATE INDEX IF NOT EXISTS ix_cc_answers_session ON cc_answers (session_id)",
    "CREATE INDEX IF NOT EXISTS ix_cc_sessions_status ON cc_sessions (status) WHERE status = 'active'",
]


def main() -> int:
    with get_db_context() as db:
        db.execute(text(CREATE_SESSIONS))
        db.execute(text(CREATE_MESSAGES))
        db.execute(text(CREATE_TOOL_CALLS))
        db.execute(text(CREATE_LLM_OPS))
        db.execute(text(CREATE_ANSWERS))
        for stmt in INDEX_STATEMENTS:
            db.execute(text(stmt))
        db.commit()

        tables = db.execute(text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name LIKE 'cc_%'
            ORDER BY table_name
        """)).fetchall()

    print("command center tables:")
    for row in tables:
        print(f"  {row[0]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
