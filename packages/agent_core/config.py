"""Runtime settings for one agent built on agent_core.

The package never reads the environment itself: the host application builds this object from
its own settings layer, so secrets keep flowing through one place.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

_SCHEMA_NAME = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")

DEFAULT_MODEL = "claude-sonnet-5-5"


@dataclass(frozen=True)
class AgentCoreConfig:
    """Everything one agent process needs. ``approver_user_ids`` is a real authority grant."""

    agent_name: str
    slack_bot_token: str | None = None
    # App-level token (xapp-) for Socket Mode: an outbound WebSocket, so no public endpoint.
    slack_app_token: str | None = None
    # Channel that receives approval cards and briefs.
    slack_channel_id: str | None = None
    # Only these Slack user ids may approve, reject, revise, halt or resume.
    approver_user_ids: frozenset[str] = field(default_factory=frozenset)
    # Users who may talk to the agent. Approvers are always included.
    operator_user_ids: frozenset[str] = field(default_factory=frozenset)
    db_schema: str = "lending"
    timezone: str = "America/New_York"
    model: str = DEFAULT_MODEL
    anthropic_api_key: str | None = None
    calendar_service_account_key_path: str | None = None
    calendar_id: str | None = None
    # How long a queued draft stays approvable before it expires.
    pending_action_ttl_hours: int = 72

    def __post_init__(self) -> None:
        if not _SCHEMA_NAME.match(self.db_schema):
            raise ValueError(f"db_schema must be a lower-case SQL identifier, got {self.db_schema!r}")
        if self.pending_action_ttl_hours < 1:
            raise ValueError("pending_action_ttl_hours must be at least 1")

    @property
    def allowed_user_ids(self) -> frozenset[str]:
        return self.approver_user_ids | self.operator_user_ids

    def missing_for_live_session(self) -> list[str]:
        """Names of settings a live Socket Mode session cannot start without."""
        required = {
            "slack_bot_token": self.slack_bot_token,
            "slack_app_token": self.slack_app_token,
            "slack_channel_id": self.slack_channel_id,
            "approver_user_ids": self.approver_user_ids,
        }
        return [name for name, value in required.items() if not value]


def parse_user_ids(raw: str | None) -> frozenset[str]:
    """``"U1, U2"`` -> ``frozenset({"U1", "U2"})``; blank entries are dropped."""
    return frozenset(part.strip() for part in (raw or "").split(",") if part.strip())
