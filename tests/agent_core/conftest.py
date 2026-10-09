"""agent_core tests run the library's real SQL against in-memory SQLite (unqualified table names).

The PostgreSQL DDL itself is covered by the drift test in test_schema.py; nothing here touches
DATABASE_URL.
"""
from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

from packages.agent_core.config import AgentCoreConfig
from packages.agent_core.halt import HaltSwitch
from packages.agent_core.pending_actions import PendingActionQueue
from packages.agent_core.relay import Relay
from packages.agent_core.store import AgentStore

SQLITE_DDL = (
    """
    CREATE TABLE agent_halt_state (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        halted BOOLEAN NOT NULL DEFAULT 0,
        reason TEXT NOT NULL DEFAULT '',
        set_by TEXT,
        set_at TIMESTAMP NOT NULL
    )
    """,
    """
    CREATE TABLE pending_actions (
        action_id INTEGER PRIMARY KEY AUTOINCREMENT,
        tool_name TEXT NOT NULL,
        channel TEXT NOT NULL,
        payload TEXT NOT NULL,
        summary TEXT NOT NULL DEFAULT '',
        status TEXT NOT NULL DEFAULT 'pending'
            CHECK (status IN ('pending','revising','approved','rejected','sending','sent','failed','blocked','expired')),
        requested_by TEXT, source_channel TEXT, source_thread_ts TEXT,
        card_channel TEXT, card_ts TEXT, decided_by TEXT, decided_at TIMESTAMP,
        revision_note TEXT, executed_at TIMESTAMP, provider_ref TEXT, error TEXT,
        created_at TIMESTAMP NOT NULL, updated_at TIMESTAMP NOT NULL,
        revisions TEXT NOT NULL DEFAULT '[]', recipient_phone TEXT, recipient_email TEXT,
        contact_ref TEXT, deal_ref TEXT, idempotency_key TEXT, expires_at TIMESTAMP, revised_by TEXT
    )
    """,
    "CREATE UNIQUE INDEX uq_pending_actions_idempotency_key ON pending_actions (idempotency_key)",
)

APPROVER = "U_APPROVER"
OPERATOR = "U_OPERATOR"
STRANGER = "U_STRANGER"
BOT = "U_BOT"


@pytest.fixture
def store() -> AgentStore:
    engine = create_engine("sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False})
    with engine.begin() as conn:
        for statement in SQLITE_DDL:
            conn.execute(text(statement))
    yield AgentStore(engine, schema=None)
    engine.dispose()


@pytest.fixture
def queue(store: AgentStore, clock: "Clock") -> PendingActionQueue:
    return PendingActionQueue(store, clock=clock)


@pytest.fixture
def halt(store: AgentStore) -> HaltSwitch:
    return HaltSwitch(store, "Cora")


class RecordingExecutor:
    """Egress stand-in: records every payload it was asked to send."""

    def __init__(self, provider_ref: str = "msg-1", error: Exception | None = None) -> None:
        self.sent: list[Mapping[str, Any]] = []
        self._provider_ref = provider_ref
        self._error = error

    def __call__(self, payload: Mapping[str, Any]) -> str:
        if self._error is not None:
            raise self._error
        self.sent.append(payload)
        return self._provider_ref


@pytest.fixture
def executor() -> RecordingExecutor:
    return RecordingExecutor()


@pytest.fixture
def relay(queue: PendingActionQueue, halt: HaltSwitch, executor: RecordingExecutor) -> Relay:
    return Relay(queue, halt, {"ghl_sms": executor})


@pytest.fixture
def config() -> AgentCoreConfig:
    return AgentCoreConfig(
        agent_name="Cora",
        slack_bot_token="xoxb-test",
        slack_app_token="xapp-test",
        slack_channel_id="C_CORA",
        approver_user_ids=frozenset({APPROVER}),
        operator_user_ids=frozenset({OPERATOR}),
    )


class Clock:
    """Controllable UTC clock for expiry tests."""

    def __init__(self, start: datetime | None = None) -> None:
        self.now = start or datetime(2026, 10, 9, 14, 0, tzinfo=timezone.utc)

    def __call__(self) -> datetime:
        return self.now

    def advance(self, delta: timedelta) -> None:
        self.now += delta


@pytest.fixture
def clock() -> Clock:
    return Clock()


def enqueue_sms(queue: PendingActionQueue, body: str = "Hi Sam, Josh here.", **extra: Any) -> int:
    return queue.enqueue(tool_name="send_sms", channel="ghl_sms", payload={"contact_id": "c-42", "body": body},
                         summary="Intro text to Sam", recipient_phone="+17275550100", **extra)
