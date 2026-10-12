"""Database access for agent_core: one Engine, one schema, raw SQL through ``sqlalchemy.text``."""
from __future__ import annotations

import re
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Iterator

from sqlalchemy.engine import Connection, Engine

_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class AgentStore:
    """Qualifies table names with the agent's schema and hands out transactions.

    ``schema=None`` leaves names unqualified, which is how the unit tests run the same SQL
    against in-memory SQLite.
    """

    def __init__(self, engine: Engine, schema: str | None) -> None:
        if schema is not None and not _IDENTIFIER.match(schema):
            raise ValueError(f"schema must be a lower-case SQL identifier, got {schema!r}")
        self.engine = engine
        self.schema = schema

    def table(self, name: str) -> str:
        if not _IDENTIFIER.match(name):
            raise ValueError(f"table name must be a lower-case SQL identifier, got {name!r}")
        return f'"{self.schema}".{name}' if self.schema else name

    @contextmanager
    def transaction(self) -> Iterator[Connection]:
        """Commits on clean exit, rolls back if the body raises."""
        with self.engine.begin() as conn:
            yield conn
